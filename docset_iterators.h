// ## Important impl. requirement for all Iterators
// Iterators may not advance themselves (i.e via next() or advance() methods), or any of its sub-iterators they keep track of
// in their constructors. Doing so would cause all kinds of issues with Docsets Spans.
#pragma once
#include "docset_iterators_base.h"
#include <prioqueue.h>

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundefined-inline"
#endif

namespace Trinity
{
        struct queryexec_ctx;
        struct candidate_document;

        namespace Codecs
        {
                struct PostingsListIterator;
        }

        namespace DocsSetIterators
        {
                uint64_t cost(const Iterator *);

		// This provides a DEFAULT scorer based on the iterator type
		// in the future, you should be able to create your own wrappers that provide a score()
		// based on the wrapped/owned iterator.
		//
		// For example, you may want a different IteratorScorer for all Disjunction-based iterators, where
		// you multiply the accumulated score of their matched sub-iterators by a coefficient based on how many
		// sub-iterators matched.
		//
		// It is not supported yet, but the ground-work to supporting has been laid out so all that's left is a way
		// to perhaps augment ast_node with data that can be used by the exec.engine to materialize those wrappers.
		//
		// Besides, we do not expect most Trinity applications will use the ExecFlags::AccumulatedScoreScheme mode.
		Iterator *wrap_iterator(queryexec_ctx *, Iterator *);


                // This is Lucene's MinShouldMatchSumScorer.java port
                // It's a clever design. From their description:
                // It tracks iterators in:
                // - lead: a linked list of iterators that are positioned on the desired docID, i.t (iterator->current() == curDocument.id)
                //
                // - tail: a heap/prioqueue that contains at most (matchThreshold - 1) iterators
                //	that are behind the desired docID. Those iterators are ordered by COST
                // 	so that we can advance the least costly ones first. i.e it tracks
                //	the top-k most expensive trackers ordered by cost ASC, i.e top() is the
                //	least expensive tracker to advance.
                //
                // - head: a heap that tracks iterators that are past the desired documentID
                //	ordered by docID in order to advance quickly to the next candidate.
                // 	i.e it tracks the top-k trackers ordered by docID ASC
                //
                // Finding the next match comes down to first setting the desired docID to the
                // least entry in head, and then advancing tail until there's a match
                //
                // This is suitable for when you need to match between 2 and the total number of iterators
                //
                // It's not that fast though -- over 35% of run-time is spent manipulating head and tail priority queues.
                // push/pop is fairly expensive, and unless we can figure out a way for this to work with sentinels or some other scheme, it's going to
                // be faster to use a much simpler design that doesn't take into account costs etc, as long as it doesn't need to hammer those queues.
                class DisjunctionSome final
                    : public Iterator
                {
                        friend uint64_t cost(const Iterator *);
			friend struct IteratorScorer;

                      public:
                        struct it_tracker
                        {
                                Iterator *it;
                                uint64_t cost;
                                isrc_docid_t id; // we could just use it->current(), but this simplifies processing somewhat
                                it_tracker *next;
                        } * lead{nullptr};

                      public:
                        const uint16_t matchThreshold;

                      protected:
                        uint16_t curDocMatchedItsCnt{0};
                        it_tracker *trackersStorage{nullptr};

                      protected:
                        uint64_t cost_;

                      private:
                        struct Compare
                        {
                                inline bool operator()(const it_tracker *const a, const it_tracker *const b) const noexcept
                                {
                                        return a->id < b->id;
                                }
                        };

                        struct CompareCost
                        {
                                inline bool operator()(const it_tracker *const a, const it_tracker *const b) const noexcept
                                {
                                        return a->cost < b->cost;
                                }
                        };

                        Switch::priority_queue<it_tracker *, Compare> head;
                        Switch::priority_queue<it_tracker *, CompareCost> tail;

                      private:
                        void update_current();

                        void advance_tail(it_tracker *const top);

                        inline void advance_tail()
                        {
                                advance_tail(tail.pop());
                        }

                        isrc_docid_t next_impl();

                        inline void add_lead(it_tracker *const t)
                        {
                                t->next = lead;
                                lead = t;
                                ++curDocMatchedItsCnt;
                        }

                      public:
                        DisjunctionSome(Iterator **const iterators, const uint16_t cnt, const uint16_t minMatch);

                        ~DisjunctionSome()
                        {
                                if (trackersStorage)
                                        std::free(trackersStorage);
                        }

                        // Advancing to the next document. Iterators in the lead LL need to migrate to // tail. If tail is full, we take the least costly iterators and advance them.
                        isrc_docid_t next() override final;

                        isrc_docid_t advance(const isrc_docid_t target) override final;

                        // advance all entries from the tail to know about all matches on the current document
                        void update_matched_cnt();

#ifdef RDP_NEED_TOTAL_MATCHES
			inline uint32_t total_matches() override final
                        {
                                update_matched_cnt();
                                return curDocMatchedItsCnt;
                        }
#endif
                };

                // This is very handy, and we can use it to model NOT clauses
                // e.g (foo NOT bar)
                // so if you want to model e.g
                // (a NOT b)
                // you can use Filter(Iterator(a), Iterator(b))
                struct Filter final
                    : public Iterator
                {
                        friend uint64_t cost(const Iterator *);

                      public:
                        // public so that build_span() can access them without going through accessors
                        Iterator *const req, *const filter;

                      private:
                        bool matches(const isrc_docid_t);

                      public:
                        Filter(Iterator *const r, Iterator *const f) noexcept
                            : Iterator{Type::Filter}, req{r}, filter{f}
                        {
                        }

                        isrc_docid_t next() override final;

                        isrc_docid_t advance(const isrc_docid_t target) override final;

#ifdef RDP_NEED_TOTAL_MATCHES
                        inline uint32_t total_matches() override final
                        {
                                return req->total_matches();
                        }
#endif
                };


                struct Optional final
                    : public Iterator
                {
                      public:
                        Iterator *const main, *const opt;

                      public:
                        Optional(Iterator *const m, Iterator *const o)
                            : Iterator{Type::Optional}, main{m}, opt{o}
                        {
                        }

                        inline isrc_docid_t next() override final
                        {
				return curDocument.id = main->next();
                        }

                        inline isrc_docid_t advance(const isrc_docid_t target) override final
                        {
				return curDocument.id = main->advance(target);
                        }

#ifdef RDP_NEED_TOTAL_MATCHES
			uint32_t total_matches() override final
			{
				auto freq = main->total_matches();
				const auto id = main->current();
				auto optId = opt->current();

				if (optId < id)
					optId = opt->advance(id);
				if (optId == id)
					freq += opt->total_matches();

				return freq;
			}
#endif

                };

                struct idx_stack final
                {
                        uint16_t *const data;
                        uint16_t cnt;

                        idx_stack(const uint16_t capacity)
                            : data((uint16_t *)malloc(sizeof(uint16_t) * capacity))
                        {
                        }

                        ~idx_stack()
                        {
                                std::free(data);
                        }
                };

                struct DisjunctionAllPLI final
                    : public Iterator
                {

                      private:
                        struct Compare
                        {
                                inline bool operator()(const Iterator *const a, const Iterator *const b) const noexcept
                                {
                                        return a->current() < b->current();
                                }
                        };

                      public:
                        idx_stack istack;

                      public:
                        Switch::priority_queue<Codecs::PostingsListIterator *, Compare> pq;

                      public:
                        DisjunctionAllPLI(Iterator **iterators, const isrc_docid_t cnt)
                            : Iterator{Type::DisjunctionAllPLI}, istack(cnt), pq{cnt}
                        {
                                for (uint32_t i{0}; i != cnt; ++i)
                                        pq.push((Codecs::PostingsListIterator *)iterators[i]);
                        }

                        isrc_docid_t next() override final;

                        isrc_docid_t advance(const isrc_docid_t target) override final;

#ifdef RDP_NEED_TOTAL_MATCHES
			uint32_t total_matches() override final
			{
				uint32_t cnt{0};

				pq.for_each_top([&cnt](const auto it) { ++cnt; },
						[](const auto a, const auto b) noexcept {
						return a->current() == b->current();
						});
				return cnt;
			}
#endif

                };

                // Will only match one, and will ignore the rest
                // for 759721, std::priority_queue<> based requires 186ms
                // while this based on Switch::priority_queue<> requires 128ms (32% faster)
                //
                // This is almost identifical to Disjunction, except it doesn't manipulate CDS
                struct Disjunction final
                    : public Iterator
                {
                      private:
                        struct Compare
                        {
                                inline bool operator()(const Iterator *const a, const Iterator *const b) const noexcept
                                {
                                        return a->current() < b->current();
                                }
                        };

                      public:
                        idx_stack istack;

                      public:
                        Switch::priority_queue<Iterator *, Compare> pq;

                      public:
                        Disjunction(Iterator **iterators, const isrc_docid_t cnt)
                            : Iterator{Type::Disjunction}, istack(cnt), pq{cnt}
                        {
                                for (uint32_t i{0}; i != cnt; ++i)
                                        pq.push(iterators[i]);
                        }

                        isrc_docid_t next() override final;

                        isrc_docid_t advance(const isrc_docid_t target) override final;

#ifdef RDP_NEED_TOTAL_MATCHES
			uint32_t total_matches() override final
			{
				uint32_t cnt{0};

				pq.for_each_top([&cnt](const auto it) { ++cnt; },
						[](const auto a, const auto b) noexcept {
						return a->current() == b->current();
						});
				return cnt;
			}
#endif

                };

                struct ConjuctionAllPLI final
                    : public Iterator
                {

                      public:
                        Codecs::PostingsListIterator **const its;
                        uint16_t size;

                      private:
                        isrc_docid_t next_impl(isrc_docid_t id);

                      public:
                        ConjuctionAllPLI(Iterator **iterators, const uint16_t cnt)
                            : Iterator{Type::ConjuctionAllPLI}, size{cnt}, its((Codecs::PostingsListIterator **)malloc(sizeof(Codecs::PostingsListIterator *) * cnt))
                        {
                                require(cnt);
                                memcpy(its, iterators, cnt * sizeof(Codecs::PostingsListIterator *));
                        }

                        ~ConjuctionAllPLI()
                        {
                                std::free(its);
                        }

                        isrc_docid_t advance(const isrc_docid_t target) override final;

                        isrc_docid_t next() override final;

#ifdef RDP_NEED_TOTAL_MATCHES
			inline uint32_t total_matches() override final
			{
				return size;
			}
#endif
                };

                // If we can, and we can, identify the set of leaders that are required like we do now for leadersAndSet
                // then we won't need to use next(), but to use advance, then we can use
                // the next_impl() design to skip ahead to the first likely common among on required decoders, instead of advancing in parallel and in-lock step
                // because e.g in the case of phrases, this can be expensive.
                struct Conjuction final
                    : public Iterator
                {
                      public:
                        Iterator **const its;
                        uint16_t size;

                      private:
                        isrc_docid_t next_impl(isrc_docid_t id);

                      public:
                        Conjuction(Iterator **iterators, const uint16_t cnt)
                            : Iterator{Type::Conjuction}, size{cnt}, its((Iterator **)malloc(sizeof(Iterator *) * cnt))
                        {
                                require(cnt);
                                memcpy(its, iterators, cnt * sizeof(Iterator *));
                        }

                        ~Conjuction()
                        {
                                std::free(its);
                        }

                        isrc_docid_t advance(const isrc_docid_t target) override final;

                        isrc_docid_t next() override final;

#ifdef RDP_NEED_TOTAL_MATCHES
                        inline uint32_t total_matches() override final
                        {
                                return size;
                        }
#endif
                };

                // No longer inherring from Conjuction; some optimization opportunities open up when not doing so
                // also important because Codecs::Decoder's don't capture_matched_term()
                struct Phrase final
                    : public Iterator
                {
                      public:
                        bool consider_phrase_match(); // use by exec() / root iterations

                      public:
                        Codecs::PostingsListIterator **const its;
                        uint16_t size;
			const uint16_t maxMatchCnt;
                        candidate_document *boundDocument{nullptr};
			uint16_t matchCnt{0};

                      private:
                        queryexec_ctx *const rctxRef;
                        isrc_docid_t next_impl(isrc_docid_t id);

                      public:
                        isrc_docid_t lastUncofirmedDID{DocIDsEND};

                      public:
                        Phrase(queryexec_ctx *r, Codecs::PostingsListIterator **iterators, const uint16_t cnt, const bool trackCnt)
                            : Iterator{Type::Phrase}, its((Codecs::PostingsListIterator **)malloc(sizeof(Codecs::PostingsListIterator *) * cnt)), size{cnt}, maxMatchCnt{ uint16_t(trackCnt ? std::numeric_limits<uint16_t>::max() : 1) }, rctxRef{r}
                        {
                                require(cnt);
                                memcpy(its, iterators, sizeof(iterators[0]) * cnt);
                        }

                        ~Phrase()
                        {
                                std::free(its);
                        }

                        isrc_docid_t advance(const isrc_docid_t target) override final;

                        isrc_docid_t next() override final;

#ifdef RDP_NEED_TOTAL_MATCHES
			inline uint32_t total_matches() override final
			{
				return matchCnt;
			}
#endif

                };


                struct VectorIDs final
                    : public Iterator
                {
                        friend uint64_t cost(const Iterator *);

                      protected:
                        std::vector<isrc_docid_t> ids;

                      private:
                        uint32_t idx{0};

                      public:
                        VectorIDs(const std::initializer_list<isrc_docid_t> list)
                            : Iterator{Type::VectorIDs}
                        {
                                for (const auto id : list)
                                        ids.push_back(id);

                                std::sort(ids.begin(), ids.end());
                        }

                        inline isrc_docid_t next() override
                        {
                                return idx == ids.size() ? curDocument.id = DocIDsEND : curDocument.id = ids[idx++];
                        }

                        isrc_docid_t advance(isrc_docid_t target) override
                        {
                                isrc_docid_t id;

                                while ((id = next()) < target)
                                        continue;

                                return id;
                        }

#ifdef RDP_NEED_TOTAL_MATCHES
			inline uint32_t total_matches() override final
			{
				return 1;
			}
#endif
                };
        }

	// See comments about it i relevant_document.h
	struct relevant_document final
		: public IteratorScorer
        {
                double score_;
                uint32_t matchesCnt;

                struct dummy_iterator final
                    : public DocsSetIterators::Iterator
                {
                        isrc_docid_t advance(const isrc_docid_t target) override final
                        {
                                return 0;
                        }

                        inline isrc_docid_t next() override final
                        {
                                return 0;
                        }

			dummy_iterator()
				: Iterator{DocsSetIterators::Type::Dummy}
			{
			}

                } dummyIt;

                relevant_document()
                    : IteratorScorer{&dummyIt}
                {
                }

		inline void set_document(const isrc_docid_t id)
		{
			dummyIt.curDocument.id = id;
		}

                inline double iterator_score() override final
                {
                        return score_;
                }

#ifdef RDP_NEED_TOTAL_MATCHES
                // e.g for a single term, number of times matched in a document, for
                // a conjuction the total number of operands, for a disjunction the total
                // number of operands on document() etc.
                inline uint32_t total_matches() override final
                {
                        return matchesCnt;
                }
#endif
        };
}

#ifdef RDP_NEED_TOTAL_MATCHES
uint32_t Trinity::IteratorScorer::total_matches() 
{
	return it->total_matches();
}
#endif

Trinity::isrc_docid_t Trinity::IteratorScorer::document() const noexcept
{
	return it->current();
}


#ifdef __clang__
#pragma GCC diagnostic pop
#endif
