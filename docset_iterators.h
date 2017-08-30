#pragma once
#include "common.h"
#include <prioqueue.h>
#include <switch.h>

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundefined-inline"
#endif

namespace Trinity
{
        struct runtime_ctx;
        struct candidate_document;

        namespace Codecs
        {
                struct PostingsListIterator;
        }

        namespace DocsSetIterators
        {
                enum class Type : uint8_t
                {
                        PostingsListIterator = 0,
                        Filter,
                        Optional,
                        OptionalOptPLI,
                        OptionalAllPLI,
                        Disjunction,
                        DisjunctionAllPLI,
                        Phrase,
                        Conjuction,
                        ConjuctionAllPLI,
                        Dummy,
                };

                struct Iterator
                {
                      public:
                        // This is here so that we can directly access it without having to go through the vtable
                        // to invoke current() which would be overriden by subclasses -- i.e subclasses are expected
                        // to update curDocument{} so that current() won't be virtual
                        struct __anonymous final
                        {
                                isrc_docid_t id{0};
                        } curDocument;

#if 0 	// we no longer make use of this
                        // Distance from the root
                        // depths >= std::numeric_limits<uint16_t>::max() / 2 are treated specially; they are used
                        // by Filter and Optional iterators, and they in turn set their own iterators's depth to be
                        // >= that magic value.
                        // We may end up using depth for other optimizations later
			//
                        uint16_t depth;
#endif

                        // This is handy, and beats bloating the vtable with e.g a virtual reset_depth(), a virtual ~Iterator() etc, that are only used during engine bootstrap, not runtime execution
                        // All told, depth and type only come down to a 4bytes overhead to Iterator, so that's not that much anyway
                        const Type type;

                      public:
                        Iterator(const Type t)
                            : type{t}
                        {
                        }

                        inline auto current() const noexcept
                        {
                                return curDocument.id;
                        }

                        // Advances to the first beyond the current whose document id that is >= target, and returns that document ID
                        // Example:
                        // isrc_docid_t advance(const isrc_docid_t target) { isrc_docid_t id; while ((id = next()) < target) {} return id; }
                        //
                        // XXX: some of the Iterators will check if current == target, i.e won't next() before they check
                        // for performance and simplicity reasons. It doesn't really affect our use so it is OK
                        // UPDATE: it actually does to some extent
                        virtual isrc_docid_t advance(const isrc_docid_t target) = 0;

                        // If at the end of the set, returns DocIDsEND otherwise advances to the next document and returns the current document
                        virtual isrc_docid_t next() = 0;

                        // If we wanted to support [min, max] range semantics, i.e only accept a documents within tha range min inclusive, max inclusive
                        // it could work like so:
                        // for (;;
                        // {
                        // 	auto id = next();
                        //
                        //	if (id < min) id = advance(min);
                        // 	while (id < max) { consider(id); id = next(); }
                        // }
                };

#if 0
                uint16_t reset_depth(Iterator *const it, const uint16_t d);
#endif

                uint64_t cost(const runtime_ctx *, const Iterator *);

                // This is very handy, and we can use it to model NOT clauses
                // e.g (foo NOT bar)
                // so if you want to model e.g
                // (a NOT b)
                // you can use Filter(Iterator(a), Iterator(b))
                //
                // XXX: See Optional comments
                struct Filter final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);
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
                };

                // This is how we can implement constrrue_expr()
                // we 'll operate on the main iterator and attempt to match an optional iterator, but we will silently ignore
                // its next()/advance() res.
                // e.g [a AND <b>] => Optional(Decoder(a), Decoder(b))
                struct Optional final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

                      public:
                        Iterator *const main, *const opt;

                      public:
                        Optional(Iterator *const m, Iterator *const o)
                            : Iterator{Type::Optional}, main{m}, opt{o}
                        {
                        }

                        inline isrc_docid_t next() override final
                        {
                                const auto id = main->next();

                                opt->advance(id); // It's OK if we drain it
                                return curDocument.id = id;
                        }

                        inline isrc_docid_t advance(const isrc_docid_t target) override final
                        {
                                const auto id = main->advance(target);

                                opt->advance(id);
                                return curDocument.id = id;
                        }
                };

                struct OptionalOptPLI final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

                      public:
                        Iterator *const main;
			Codecs::PostingsListIterator *const opt;

                      public:
                        OptionalOptPLI(Iterator *const m, Iterator *const o)
                            : Iterator{Type::OptionalOptPLI}, main{m}, opt{(Codecs::PostingsListIterator *)o}
                        {
                        }

			isrc_docid_t next() override final;

			isrc_docid_t advance(const isrc_docid_t) override final;
                };

                struct OptionalAllPLI final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

                      public:
			Codecs::PostingsListIterator *const main;
			Codecs::PostingsListIterator *const opt;

                      public:
                        OptionalAllPLI(Iterator *const m, Iterator *const o)
                            : Iterator{Type::OptionalAllPLI}, main{(Codecs::PostingsListIterator *)m}, opt{(Codecs::PostingsListIterator *)o}
                        {
                        }

			isrc_docid_t next() override final;

			isrc_docid_t advance(const isrc_docid_t) override final;
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
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

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
                };


                // Will only match one, and will ignore the rest
                // for 759721, std::priority_queue<> based requires 186ms
                // while this based on Switch::priority_queue<> requires 128ms (32% faster)
                //
                // This is almost identifical to Disjunction, except it doesn't manipulate CDS
                struct Disjunction final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

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
                };

                struct ConjuctionAllPLI final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

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
                };

                // If we can, and we can, identify the set of leaders that are required like we do now for leadersAndSet
                // then we won't need to use next(), but to use advance, then we can use
                // the next_impl() design to skip ahead to the first likely common among on required decoders, instead of advancing in parallel and in-lock step
                // because e.g in the case of phrases, this can be expensive.
                struct Conjuction final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

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
                };

                // No longer inherring from Conjuction; some optimization opportunities open up when not doing so
                // also important because Codecs::Decoder's don't capture_matched_term()
                struct Phrase final
                    : public Iterator
                {
                        friend uint16_t reset_depth(Iterator *, const uint16_t);

                      public:
                        bool consider_phrase_match(); // use by exec() / root iterations

                      public:
                        Codecs::PostingsListIterator **const its;
                        uint16_t size;
                        candidate_document *boundDocument{nullptr};

                      private:
                        runtime_ctx *const rctxRef;
                        isrc_docid_t next_impl(isrc_docid_t id);

                      public:
                        isrc_docid_t lastUncofirmedDID{DocIDsEND};

                      public:
                        Phrase(runtime_ctx *r, Codecs::PostingsListIterator **iterators, const uint16_t cnt)
                            : Iterator{Type::Phrase}, its((Codecs::PostingsListIterator **)malloc(sizeof(Codecs::PostingsListIterator *) * cnt)), size{cnt}, rctxRef{r}
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
                };

#if 0
// This is iportant, so that we can wrap a Phrase into a TwoPhaiseIterator and
// when next() returns, we 'll get to check the phrasee itself 
// e.g a phrase being a subclass of a Conjuction, holding Codecs::Decoder 
struct TwoPhaseIterator final
	: public Iterator
{
      private:
        Iterator *const approx;

      private:
        isrc_docid_t next_impl(isrc_docid_t id)
        {
		for (;;id = approx->next())
                {
                        if (id == DocIDsEND)
                                return curDocument.id = DocIDsEND;
                        else if (iterator->matches())
                                return curDocument.id = id;
                }
        }

      public:
        TwoPhaseIterator(Iterator *const a)
            : approx{a}
        {
        }

        isrc_docid_t advance(const isrc_docid_t id) override final
        {
                return next_impl(approx->advance(id));
        }

        isrc_docid_t next() override final
        {
                return next_impl(approx->next());
        }
};
#endif

                struct VectorIDs final
                    : public Iterator
                {
                      private:
                        std::vector<isrc_docid_t> ids;
                        uint32_t idx{0};

                      public:
                        VectorIDs(const std::initializer_list<isrc_docid_t> list)
                            : Iterator{Type::Dummy}
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
                };
        }
}
#ifdef __clang__
#pragma GCC diagnostic pop
#endif
