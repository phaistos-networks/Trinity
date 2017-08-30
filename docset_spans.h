#pragma once
#include "docset_iterators.h"

namespace Trinity
{
	// process() could have accepted a RelevanceProvider * instead
	// which would be responsible for returning a document ID e.g via document_id()
	// and a relevance score e.g via relevance()
	//
	// One such subclass could just return the document_id() and relevance() would invoke a prepare_match()
	// like method that would sum scores for each iterator collected.
	//
	// If we wanted to support Lucene's score semantics, where you don't get to evaluate a document and all matching terms, but instead
	// there's a Scorer for each (in practice) iterator and collection scorers just sum their matching iterators etc, we could simply
	// support RelevanceProvider semantics, and that would also make it easy for DocsSetSpanForDisjunctions and DocsSetSpanForDisjunctionsWithSpans 
	// to collect sums and match counts for each document and create a RelevanceProvider that simply returns that, like Lucene does it.
	// 
	// Such a mode would make sense where we want to trade-off flexibility and power(not being able to evaluate a document
	// only when it's matched, and having access to all hits of all matched terms) for faster runtime.
        class MatchesProxy
        {
              public:
                virtual void process(const isrc_docid_t id)
                {
                }

                ~MatchesProxy()
                {
                }
        };

        class DocsSetSpan
        {
              protected:
                const bool isRoot;

              public:
                // process the span/range [min, max)
                // i.e from min inclusive to max exclusive
                //
                // returns an estimate of the next matching document, after max(unless max == DocIDsEND)
                DocsSetSpan(const bool root = false)
                    : isRoot{root}
                {
                }

                virtual isrc_docid_t process(MatchesProxy *, const isrc_docid_t min, const isrc_docid_t max) = 0;

                virtual ~DocsSetSpan()
                {
                }
        };

        class GenericDocsSetSpan final
            : public DocsSetSpan
        {
                Trinity::DocsSetIterators::Iterator *const it;

              public:
                GenericDocsSetSpan(Trinity::DocsSetIterators::Iterator *const i, const bool root = false)
                    : DocsSetSpan{root}, it{i}
                {
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;
        };

        // For a query such as a
        // [ (1 | 10) -APPLE], where
        // the cost of the NOT expression (i.e for APPLE) is lower than the cost of the required expr (i.e (1|10)), then
        // it makes sense to use this DocsSetSpan. It will identify the next filtered document ID, and ask the DocsSetSpan set for req
        // to process the range (prev, nextEcluded]
        //
        // This is faster than using a DocsSetIterators::Filter which will match required and then filter whatever's matched
        class FilteredDocsSetSpan final
            : public DocsSetSpan
        {
              private:
                DocsSetSpan *const req;
                Trinity::DocsSetIterators::Iterator *const exclIt;

              public:
                FilteredDocsSetSpan(DocsSetSpan *const r, Trinity::DocsSetIterators::Iterator *const i, const bool root = false)
                    : DocsSetSpan{root}, req{r}, exclIt{i}
                {
                }

                ~FilteredDocsSetSpan()
                {
                        // we will assume ownership here
                        // not elegant, but practical
                        delete req;
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;
        };

        class DocsSetSpanForDisjunctions final
            : public DocsSetSpan
        {
                // This is based on/inspired by Lucene's BooleanScorer design.
                // Effectively, instead of naively merge-scanning all leaders, there is a prio.queue we use to track
                // all iterators, where the iterator with the lowest documentID is the top.
                //
                // We then start from the top, and identify all other remaining leaders in the queue
                // where their current document ID is within a window.
                // The window is based on the top/first leader documentID
                // and it's just a range from that document ID rounded down to SIZE(windowBase) and upto (windowBase + SIZE).
                //
                // Once we collect all from the PQ (there's a fast-path if we just collected one), we 'll advance
                // them all (i.e call Iterator::next() on each of the decoders) until we have either drained the decoder, or the next
                // document of the decoder. is outside our window. We collect all those IDs into a bitmap (See later), and if
                // the decoder has not been drained, we 'll just push it back to the PQ.
                //
                // The core idea is that we have a bitmap that can hold SIZE bits, and we just set
                // a bit in it based on a (document ID % SIZE), which works because all document IDs we are
                // processing are within a SIZE range.
                // We then just iterate the bitmap and identify the document IDs by considering the bits set and the window base
                // That's pretty much all there is to it.
                //
                // This works great. The key ideas is the use of a PQ instead of a merge-scan among the leaders and the use
                // of a bitmap and draining of iterators based on a document IDs window.
                //
                // Lucene does not consider the document and the matching terms together when computing a score; it computes a score
                // for each matched Query (i.e BooleanQuery, TermQuery etc), and it just sums those scores together.
                // This allows for some optimizations, but the end resuilt is that the score model is not great, and we really
                // need to provide support for a very rich and powerful score model, one where the document and all matching terms
                // and information about them is provided to a score callback and it can make use of that to produce a great relevant score.
                //
                //
                // lucene uses 2k(2048), i.e shift is set to 11
                // We set it to 13, which yields better performance(from 60ms down to 47ms)
                // Setting to 14 yields worse perf. than 13
                //
                // This is a great algorithm for unions vs merge-scan, and what's more, we can also keep track
                // of how many documents matched the same value (see Lucene impl.)

// See comments about RelevanceProvider
// If we get to implement this mode, we 'll need to aggregate some kind of 'score' and count for each matched document
                //#define TRACK_DOCTERMS 1
                static constexpr std::size_t SHIFT{13};
                static constexpr std::size_t SIZE{1 << SHIFT};
                static constexpr std::size_t MASK{SIZE - 1};
                static constexpr std::size_t SET_SIZE{SIZE / sizeof(uint64_t)};

                struct Compare
                {
                        [[gnu::always_inline]] inline bool operator()(const DocsSetIterators::Iterator *a, const DocsSetIterators::Iterator *b) const noexcept
                        {
                                return a->current() < b->current();
                        }
                };

                // Alloc on the heap so that we won't possibly overrun the stack and because
                // it'd help with cache hit rate of locals
                uint64_t *const matching;

#ifdef TRACK_DOCTERMS
                std::vector<std::pair<DocsSetIterators::Iterator *, uint32_t>> tracker[SIZE];
#endif
                Switch::priority_queue<DocsSetIterators::Iterator *, Compare> pq;
                DocsSetIterators::Iterator **const collected;

              public:
                DocsSetSpanForDisjunctions(std::vector<Trinity::DocsSetIterators::Iterator *> &its, const bool root = false)
                    : DocsSetSpan{root}, matching((uint64_t *)calloc(SET_SIZE, sizeof(uint64_t))), pq(its.size() + 16), collected((DocsSetIterators::Iterator **)malloc(sizeof(DocsSetIterators::Iterator *) * (its.size() + 1)))
                {
                        for (auto it : its)
                        {
                                it->next(); // advance to the first
                                pq.push(it);
                        }

                        require(pq.size());
                }

                ~DocsSetSpanForDisjunctions()
                {
                        std::free(matching);
                        std::free(collected);
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;
        };

        // This is similar to DocsSetSpanForDisjunctions, except
        // it operates on DocsSetSpan's instead, and the impl. is somewhat different to account for that, but the core algorithm is the same.
        class DocsSetSpanForDisjunctionsWithSpans final
            : public DocsSetSpan
        {
                static constexpr std::size_t SHIFT{13};
                static constexpr std::size_t SIZE{1 << SHIFT};
                static constexpr std::size_t MASK{SIZE - 1};
                static constexpr std::size_t SET_SIZE{SIZE / sizeof(uint64_t)};

                struct span_ctx final
                {
                        DocsSetSpan *span;
                        isrc_docid_t next{0};

                        inline void advance(const isrc_docid_t min)
                        {
                                // we can safely pass nullptr here, because we are restricting to [min, min) so
                                // in practice it will just advance the span to >= span but won't attempt to invoke the provided MatchesProxy::process()
                                process(nullptr, min, min);
                        }

                        inline void process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
                        {
                                next = span->process(mp, min, max);
                        }

                        struct Compare
                        {
                                [[gnu::always_inline]] inline bool operator()(const span_ctx &a, const span_ctx &b) const noexcept
                                {
                                        return a.next < b.next;
                                }
                        };
                };

                uint64_t *const matching;
                Switch::priority_queue<span_ctx, span_ctx::Compare> pq;
                span_ctx *const collected;

                struct Tracker final
                    : public MatchesProxy
                {
                        uint32_t m{0};
                        uint64_t *const matching;
                        runtime_ctx *const rctx;

                        Tracker(uint64_t *const m, runtime_ctx *const ctx)
                            : matching{m}, rctx{ctx}
                        {
                        }

                        inline void reset()
                        {
                                m = 0;
                        }

                        void process(const isrc_docid_t id) override final;
                } tracker;

              private:
                span_ctx advance(const isrc_docid_t);

              public:
                DocsSetSpanForDisjunctionsWithSpans(std::vector<DocsSetSpan *> &its, const bool root = false);

                ~DocsSetSpanForDisjunctionsWithSpans()
                {
                        std::free(matching);
                        std::free(collected);
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;
        };
}
