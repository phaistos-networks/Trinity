#pragma once
#include "docwordspace.h"
#include "exec.h"
#include "matches.h"
#include "similarity.h"
#include "compilation_ctx.h"

namespace Trinity
{
        // 64bytes alignment seems to yield good results, but crashes if optimizer is enabled (i.e struct alignas(64) exec_node {})
        // (this is because we use simple_allocator::New<> which doesn't respect the specified alignment. Not sure
        // if we should implement support for alignment allocations in simple_allocator)
        struct queryexec_ctx;

        // This is more aking to a short-memory implemented as a stack-sort-of system
        struct candidate_document final
        {
                isrc_docid_t id{0};
                uint16_t rc{1};
                uint16_t bindCnt{0};
                matched_document matchedDocument;
                bool dwsInUse{false};	 // we are only going to reset if we are going to use it
                isrc_docid_t *curDocQueryTokensCaptured;
                uint16_t curDocSeq{UINT16_MAX};
                term_hits *termHits{nullptr};

                candidate_document(queryexec_ctx *const rctx);

                ~candidate_document()
                {
                        std::free(curDocQueryTokensCaptured);
                        delete[] termHits;
                }

                term_hits *materialize_term_hits(queryexec_ctx *, Codecs::PostingsListIterator *, const exec_term_id_t termID);

                inline void retain()
                {
                        ++rc;
                }

                inline auto retained()
                {
                        ++rc;
                        return this;
                }
        };

        // we can use banks to track of all tracked documents
        // where base is e.g (id & ~(SIZE - 1)), i.e rounded down to a number
        // and we can then just dereference entries[id - base] directly
        struct docstracker_bank
        {
                static constexpr std::size_t SIZE{8192};
                static constexpr std::size_t BM_SIZE{(SIZE + 63) / 64};

// Use of bitmaps can result in an almost 100% speedup
#define BANKS_USE_BM 1
                static_assert((SIZE & 1) == 0 && SIZE < std::numeric_limits<uint16_t>::max());

                isrc_docid_t base;

                struct entry
                {
                        candidate_document *document;
                };

#ifdef BANKS_USE_BM
                uint64_t *const bm;
#endif
                entry *const entries;
                uint16_t setCnt{0};

                docstracker_bank()
                    : entries((entry *)malloc(sizeof(entry) * SIZE))
#ifdef BANKS_USE_BM
                    , bm((uint64_t *)malloc(sizeof(uint64_t) * BM_SIZE))
#endif
                {
                }

                ~docstracker_bank()
                {
                        std::free(entries);
#ifdef BANKS_USE_BM
                        std::free(bm);
#endif
                }
        };

        struct iterators_collector
        {
                Codecs::PostingsListIterator **data{nullptr};
                uint16_t cnt{0};

                void init(const uint16_t n)
                {
                        data = (Codecs::PostingsListIterator **)malloc(sizeof(Codecs::PostingsListIterator *) * n);
                }

                ~iterators_collector()
                {
                        if (data)
                                std::free(data);
                }
        };

        // This is initialized by the compiler
        // and used by the VM
        struct queryexec_ctx final
        {
                const bool documentsOnly, accumScoreMode;
                IndexSource *const idxsrc;
                iterators_collector collectedIts;
                Similarity::IndexSourceTermsScorer *scorer{nullptr};

                queryexec_ctx(IndexSource *src, const bool documentsOnly_, const bool accumScoreMode_)
                    : documentsOnly{documentsOnly_}, accumScoreMode{accumScoreMode_}, idxsrc{src}
                {
                }

                ~queryexec_ctx();

                void capture_matched_term(Codecs::PostingsListIterator *);

                // For simplicity's sake, we are just going to map exec_term_id_t => decoders[] without
                // indirection. For each distict/resolved term, we have a decoder and
                // term_hits in decode_ctx.decoders[] and decode_ctx.termHits[]
                // This means you can index them using a termID
                // This means we may have some nullptr in decode_ctx.decoders[] but that's OK
                void prepare_decoder(exec_term_id_t termID);

                inline term_index_ctx term_ctx(const exec_term_id_t termID)
                {
                        return tctxMap[termID].first;
                }

                // Resolves a term to a termID relative to the queryexec_ctx
                // This id is meaningless outside this execution context
                // and we use it because its easier to track/use integers than strings
                // See Termspaces in CONCEPTS.md
                exec_term_id_t resolve_term(const str8_t term);

                DocsSetIterators::Iterator *build_iterator(const exec_node n, const uint32_t execFlags);

                // Instead of having a virtual DocsSetIterators::Iterator::~Iterator()
                // which means we would need another entry in the vtable, which means an higher chance for cache misses, for no really good reason
                // we just track all created DocsSetIterators::Iterators along with its type, and in ~queryexec_ctx() we consider the type, cast and delete it
                DocsSetIterators::Iterator *reg_docset_it(DocsSetIterators::Iterator *it);

                Codecs::PostingsListIterator *reg_pli(Codecs::PostingsListIterator *it);

                // indexed by termID
                query_term_ctx **originalQueryTermCtx;

                struct decode_ctx_struct final
                {
                        Trinity::Codecs::Decoder **decoders{nullptr};
                        uint16_t capacity{0};

                        void check(const uint16_t idx);

                        ~decode_ctx_struct();
                } decode_ctx;

		struct 
		{
			candidate_document **data{nullptr};
			// FIXED: turns out, we can excheed std::numeric_limits<uint16_t>::max() for some queries
			// e.g for bestprice:
			// [ ' apple OR "iphone x" OR "apple iphone x" OR ipod OR "apple ipad" OR "world of warcraft"  OR "world of" OR blizzard OR games OR "apple iphone x" OR "iphone X"  OR "Samsung galaxy" OR "32 GB" OR HTC OR "galaxy s8"  OR "phaistos networks" OR "las vegas" OR cid:806' ]
			// this query could be used to reproduce the problem
			uint32_t size{0};
			uint32_t capacity{0};
		} tracked_docrefs;


                struct _reusable_cds
                {
                        candidate_document **data{nullptr};
                        uint16_t size_{0}, capacity{0};

                        void push_back(candidate_document *d);

                        inline auto size() const noexcept
                        {
                                return size_;
                        }

                        inline candidate_document *pop_one() noexcept
                        {
                                return size_ ? data[--size_] : nullptr;
                        }

                } reusableCDS;
                DocsSetIterators::Iterator *rootIterator{nullptr};

                candidate_document *cds_track(const isrc_docid_t did);

                inline void cds_release(candidate_document *const d)
                {
                        if (1 == d->rc--)
                        {
                                if (auto th = d->termHits)
                                        th->set_docid(0);

                                d->rc = 1;
				d->id = 0;
                                reusableCDS.push_back(d);
                        }
                }

                void gc_retained_docs(const isrc_docid_t);

		void track_docref(candidate_document *);

                candidate_document *lookup_document(const isrc_docid_t);

                candidate_document *document_by_id(const isrc_docid_t id)
                {
                        if (id <= maxTrackedDocumentID)
                        {
                                if (auto ptr = lookup_document_inbank(id))
				{
					require(ptr->id == id);
                                        return ptr->retained();
				}
                        }

                        auto *const res = reusableCDS.pop_one() ?: new candidate_document(this);


			require(res->id == 0);
                        res->id = id;
                        res->dwsInUse = false;

                        if (!documentsOnly)
                        {
                                if (unlikely(res->curDocSeq == UINT16_MAX))
                                {
                                        const auto maxQueryTermIDPlus1 = termsDict.size() + 1;

                                        memset(res->curDocQueryTokensCaptured, 0, sizeof(isrc_docid_t) * maxQueryTermIDPlus1);
                                        res->curDocSeq = 1;
                                }
                                else
                                        ++(res->curDocSeq);
                        }

                        return res;
                }

                void forget_document(candidate_document *);

                std::unordered_map<str8_t, exec_term_id_t> termsDict;
                // TODO: determine suitable allocator bank size based on some meaningful metric
                // e.g total distinct tokens in the query, otherwise we may just end up allocating more memory than
                // we need and for environments where memory pressure is a concern, this may be important.
                // For now, go with large enough bank sizes for the allocators and figure out something later.
                // We should also track allocated (from allocators) memory that is no longer needed so that we can reuse it
                // Maybe we just need a method for allocating arbitrary amount of memory and releasing it back to the runtime ctx
                simple_allocator allocator{4096 * 6};
		std::vector<void *> large_allocs;
                std::unordered_map<exec_term_id_t, std::pair<term_index_ctx, str8_t>> tctxMap;
                std::vector<DocsSetIterators::Iterator *> docsetsIterators;
                std::vector<Codecs::PostingsListIterator *> allIterators;
                docstracker_bank *lastBank{nullptr};

// use of banks provides a noticeable speedup
// 393ms down to 340ms. Not a huge difference, but it's welcome(~13%)
#define USE_BANKS 1

                struct
                {
#ifndef USE_BANKS
                        std::vector<candidate_document *> trackedDocuments[16];
#else
                        std::vector<docstracker_bank *> banks, reusableBanks;
#endif
                        isrc_docid_t maxTrackedDocumentID{0}, lastMatchedDocumentID{0};
                };

#ifdef USE_BANKS
                inline docstracker_bank *bank_for(const Trinity::isrc_docid_t id)
                {
                        const auto base = id & (~(docstracker_bank::SIZE - 1)); // rounded down

                        if (lastBank && lastBank->base == base)
                                return lastBank;
                        else
                        {
                                // consider using counting linear search
                                // may make more sense because it's going to be branchless
                                for (auto b : banks)
                                {
                                        if (b->base == base)
                                        {
                                                lastBank = b;
                                                return b;
                                        }
                                }

                                return new_bank(base);
                        }
                }

                docstracker_bank *new_bank(const isrc_docid_t);

                void forget_document_inbank(candidate_document *);

                candidate_document *lookup_document_inbank(const isrc_docid_t);

                void track_document_inbank(candidate_document *);
#endif

                void prepare_match(candidate_document *);

                inline void track_document(candidate_document *const doc)
                {
#ifndef USE_BANKS
                        auto &v = trackedDocuments[doc->id & (sizeof_array(trackedDocuments) - 1)];

                        v.push_back(doc);
#else
                        track_document_inbank(doc);
#endif
                        maxTrackedDocumentID = std::max(maxTrackedDocumentID, doc->id);
                }
        };
}

