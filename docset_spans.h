#pragma once
#include "docset_iterators.h"

namespace Trinity
{
        // DocsSetSpan::process() requires a MatchesProxy *. It's process() method
        // will be invoked for every matched document.
        // The relevant_document_provider::document() and relevant_document_provider::score()
        // can be used by MatchesProxy subclasses to accomplish whatever's necessary.
        class MatchesProxy
        {
              public:
                virtual void process(relevant_document_provider *)
                {
                }

                ~MatchesProxy()
                {
                }
        };

        // Instead of accessing documents sets directly using Iterators, using
        // a DocsSetSpan makes more sense because subclasses can implement fancy schemes
        // for higher performance.
        //
        // There only two methods subclasses override.
        // process() which will
        // process the span/range [min, max), i.e from min inclusive, to max exclusive
        // and will return an estimate of the next matching document, after max(unless max == DocIDsEND)
        // and cost() which should
        // return the evaluation cost; usually by considering the managed
        // iterators or sub-spans.
        //
        // Check FilteredDocsSetSpan and GenericDocsSetSpan for how that works.
        class DocsSetSpan
        {
              protected:
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
                static constexpr std::size_t SHIFT{13};
                static constexpr std::size_t SIZE{1 << SHIFT};
                static constexpr std::size_t MASK{SIZE - 1};
                static constexpr std::size_t SET_SIZE{SIZE / sizeof(uint64_t)};

              protected:
                const bool isRoot;

              public:
                DocsSetSpan(const bool root = false)
                    : isRoot{root}
                {
                }

                // process the span/range [min, max)
                // i.e from min inclusive to max exclusive
                //
                // returns an estimate of the next matching document, after max(unless max == DocIDsEND)
                virtual isrc_docid_t process(MatchesProxy *, const isrc_docid_t min, const isrc_docid_t max) = 0;

                virtual ~DocsSetSpan()
                {
                }

                virtual uint64_t cost() = 0;
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

                uint64_t cost() override final
                {
                        return it->cost();
                }
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
                        // We will assume ownership here
                        // not elegant, but practical
                        delete req;
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;

                uint64_t cost() override final
                {
                        return req->cost();
                }
        };

        class DocsSetSpanForDisjunctions final
            : public DocsSetSpan
        {
              private:
                struct Compare
                {
                        [[gnu::always_inline]] inline bool operator()(const DocsSetIterators::Iterator *a, const DocsSetIterators::Iterator *b) const noexcept
                        {
                                return a->current() < b->current();
                        }
                };

              private:
                uint64_t *const matching;
                Switch::priority_queue<DocsSetIterators::Iterator *, Compare> pq;
                DocsSetIterators::Iterator **const collected;

              public:
                DocsSetSpanForDisjunctions(std::vector<Trinity::DocsSetIterators::Iterator *> &its, const bool root = false)
                    : DocsSetSpan{root}, matching((uint64_t *)calloc(SIZE, sizeof(uint64_t))), pq(its.size() + 16), collected((DocsSetIterators::Iterator **)malloc(sizeof(DocsSetIterators::Iterator *) * (its.size() + 1)))
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

                uint64_t cost() override final
                {
                        uint64_t res{0};

                        for (auto it : pq)
                                res += it->cost();

                        return res;
                }
        };

        class DocsSetSpanForDisjunctionsWithThreshold final
            : public DocsSetSpan
        {
              private:
                struct Compare
                {
                        [[gnu::always_inline]] inline bool operator()(const DocsSetIterators::Iterator *a, const DocsSetIterators::Iterator *b) const noexcept
                        {
                                return a->current() < b->current();
                        }
                };

              private:
                const uint16_t matchThreshold;
                uint64_t *const matching;
                std::pair<double, uint32_t> *const tracker;
                Switch::priority_queue<DocsSetIterators::Iterator *, Compare> pq;
                DocsSetIterators::Iterator **const collected;

              public:
                DocsSetSpanForDisjunctionsWithThreshold(const uint16_t min, std::vector<Trinity::DocsSetIterators::Iterator *> &its, const bool root = false)
                    : DocsSetSpan{root}, matchThreshold{min}, matching((uint64_t *)calloc(SIZE, sizeof(uint64_t))), pq(its.size() + 16), collected((DocsSetIterators::Iterator **)malloc(sizeof(DocsSetIterators::Iterator *) * (its.size() + 1))), tracker((std::pair<double, uint32_t> *)calloc(SIZE, sizeof(std::pair<double, uint32_t>)))
                {
                        expect(min && min <= its.size());
                        expect(its.size() > 1);

                        for (auto it : its)
                        {
                                it->next(); // advance to the first
                                require(it->current());
                                pq.push(it);
                        }
                }

                ~DocsSetSpanForDisjunctionsWithThreshold()
                {
                        std::free(matching);
                        std::free(collected);
                        std::free(tracker);
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;

                uint64_t cost() override final
                {
                        uint64_t res{0};

                        for (auto it : pq)
                                res += it->cost();

                        return res;
                }
        };

        class DocsSetSpanForPartialMatch final
            : public DocsSetSpan
        {
              private:
                struct Compare
                {
                        [[gnu::always_inline]] inline bool operator()(const DocsSetIterators::Iterator *a, const DocsSetIterators::Iterator *b) const noexcept
                        {
                                return a->current() < b->current();
                        }
                };

              private:
                const uint16_t matchThreshold;
                uint64_t *const matching;
                std::pair<double, uint32_t> *const tracker;
                Switch::priority_queue<DocsSetIterators::Iterator *, Compare> pq;
                DocsSetIterators::Iterator **const collected;

              public:
                DocsSetSpanForPartialMatch(std::vector<Trinity::DocsSetIterators::Iterator *> &its, const uint16_t min, const bool root = false)
                    : DocsSetSpan{root}, matchThreshold{min}, matching((uint64_t *)calloc(SET_SIZE, sizeof(uint64_t))), pq(its.size() + 16), tracker((std::pair<double, uint32_t> *)calloc(SIZE, sizeof(std::pair<double, uint32_t>))), collected((DocsSetIterators::Iterator **)malloc(sizeof(DocsSetIterators::Iterator *) * (its.size() + 1)))
                {
                        for (auto it : its)
                        {
                                it->next();
                                pq.push(it);
                        }

                        expect(pq.size());
                }

                ~DocsSetSpanForPartialMatch()
                {
                        std::free(matching);
                        std::free(collected);
                        std::free(tracker);
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;

                uint64_t cost() override final
                {
                        auto res{0};

                        for (auto it : pq)
                                res += it->cost();

                        return res;
                }
        };

        // This is similar to DocsSetSpanForDisjunctions, except
        // it operates on DocsSetSpan's instead, and the impl. is somewhat different to account for that, but the core algorithm is the same.
        class DocsSetSpanForDisjunctionsWithSpans final
            : public DocsSetSpan
        {
              private:
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

              private:
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

                        // See Tracker::process() def. comments
                        inline void reset()
                        {
                                m = 0;
                        }

                        void process(relevant_document_provider *) override final;
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

                uint64_t cost() override final
                {
                        uint64_t res{0};

                        for (auto it : pq)
                                res += it.span->cost();
                        return res;
                }
        };

        // This is similar to DocsSetSpanForDisjunctionsWithSpans, except
        // except that it relies on the (leads, head, tail) scheme used in
        // DisjunctionSome for efficiency (though this only makes sense if e.g
        // you know that evaluating iterators can be expensive to be worth it)
        class DocsSetSpanForDisjunctionsWithSpansAndCost final
            : public DocsSetSpan
        {
              private:
                struct span_ctx final
                {
                        DocsSetSpan *span;
                        uint64_t cost;
                        isrc_docid_t next{0};

                        inline void advance(const isrc_docid_t min)
                        {
                                process(nullptr, min, min);
                        }

                        inline void process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
                        {
                                next = span->process(mp, min, max);
                        }

                        struct CompareByNext
                        {
                                [[gnu::always_inline]] inline bool operator()(const span_ctx *const a, const span_ctx *const b) const noexcept
                                {
                                        return a->next < b->next;
                                }
                        };

                        struct CompareByCost
                        {
                                [[gnu::always_inline]] inline bool operator()(const span_ctx *const a, const span_ctx *const b) const noexcept
                                {
                                        return a->cost < b->cost;
                                }
                        };
                };

              private:
                std::pair<double, uint32_t> *const matchesTracker;
                uint64_t *const matching;
                span_ctx **const leads;
                Switch::priority_queue<span_ctx *, span_ctx::CompareByNext> head;
                Switch::priority_queue<span_ctx *, span_ctx::CompareByCost> tail;
                const uint16_t matchThreshold;
                span_ctx *const storage;
                uint64_t cost_;

                struct Tracker final
                    : public MatchesProxy
                {
                        uint32_t m{0};
                        std::pair<double, uint32_t> *const matchesTracker;
                        uint64_t *const matching;
                        runtime_ctx *const rctx;

                        Tracker(uint64_t *const m, std::pair<double, uint32_t> *const t, runtime_ctx *const ctx)
                            : matchesTracker{t}, matching{m}, rctx{ctx}
                        {
                        }

                        // See Tracker::process() def. comments
                        inline void reset()
                        {
                                m = 0;
                        }

                        void process(relevant_document_provider *) override final;
                } tracker;

              private:
                span_ctx *advance(const isrc_docid_t);

                span_ctx *score_window(span_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max);

                void score_window_single(span_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t windowMin, const isrc_docid_t windowMax, const isrc_docid_t max);

                void score_window_many(MatchesProxy *const mp, const isrc_docid_t windowBase, const isrc_docid_t windowMin, const isrc_docid_t windowMax, uint16_t leadsCnt);

              public:
                DocsSetSpanForDisjunctionsWithSpansAndCost(const uint16_t min, std::vector<DocsSetSpan *> &its, const bool root = false);

                ~DocsSetSpanForDisjunctionsWithSpansAndCost()
                {
                        std::free(matching);
                        std::free(leads);
                        std::free(storage);
                        std::free(matchesTracker);
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;

                uint64_t cost() override final
                {
                        return cost_;
                }
        };

        class DocsSetSpanForDisjunctionsWithThresholdAndCost final
            : public DocsSetSpan
        {
              private:
                struct it_ctx final
                {
                        DocsSetIterators::Iterator *it;
                        uint64_t cost;
                        isrc_docid_t next{0};

                        inline void advance(const isrc_docid_t min)
                        {
                                next = it->advance(min);
                        }

                        struct CompareByNext
                        {
                                [[gnu::always_inline]] inline bool operator()(const it_ctx *const a, const it_ctx *const b) const noexcept
                                {
                                        return a->next < b->next;
                                }
                        };

                        struct CompareByCost
                        {
                                [[gnu::always_inline]] inline bool operator()(const it_ctx *const a, const it_ctx *const b) const noexcept
                                {
                                        return a->cost < b->cost;
                                }
                        };
                };

              private:
                std::pair<double, uint32_t> *const matchesTracker;
                uint64_t *const matching;
                it_ctx **const leads;
                Switch::priority_queue<it_ctx *, it_ctx::CompareByNext> head;
                Switch::priority_queue<it_ctx *, it_ctx::CompareByCost> tail;
                const uint16_t matchThreshold;
                it_ctx *const storage;
                uint64_t cost_;

              private:
                it_ctx *advance(const isrc_docid_t);

                it_ctx *score_window(it_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max);

                void score_window_single(it_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t windowMin, const isrc_docid_t windowMax, const isrc_docid_t max);

                void score_window_many(MatchesProxy *const mp, const isrc_docid_t windowBase, const isrc_docid_t windowMin, const isrc_docid_t windowMax, uint16_t leadsCnt);

              public:
                DocsSetSpanForDisjunctionsWithThresholdAndCost(const uint16_t min, std::vector<DocsSetIterators::Iterator *> &its, const bool root = false);

                ~DocsSetSpanForDisjunctionsWithThresholdAndCost()
                {
                        std::free(matching);
                        std::free(leads);
                        std::free(storage);
                        std::free(matchesTracker);
                }

                isrc_docid_t process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max) override final;

                uint64_t cost() override final
                {
                        return cost_;
                }
        };
}
