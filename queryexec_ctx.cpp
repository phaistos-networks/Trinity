#include "queryexec_ctx.h"
#include "docset_iterators.h"
#include <unordered_set>

using namespace Trinity;

// This is required to support phrases, and its important for working around a bug that manifests very rarely
// but it does nonetheless.
//
// To replicate [xbox one x] on BP, and consider score for 2155079078, where
// we fail to properly materialize the hists. This is because the iterator for (XBOX), (ONE) were already
// asked to materialize the hits for that document earlier to process a phrase. We are now
// tracking materialized documents required Phrase::consider_phrase_match() and that solved the problem at the potential expense
// of some overhead for track_docref() and gc_retained_docs()
// TODO: quantify that overhead
static constexpr const bool trace_docrefs{false};

void Trinity::queryexec_ctx::track_docref(candidate_document *doc) {
        track_document(doc);

        if (tracked_docrefs.size == tracked_docrefs.capacity) {
                tracked_docrefs.capacity = (tracked_docrefs.capacity * 2) + 128;
                tracked_docrefs.data     = (candidate_document **)realloc(tracked_docrefs.data,
                                                                      sizeof(candidate_document *) * tracked_docrefs.capacity);
        }

        tracked_docrefs.data[tracked_docrefs.size++] = doc;

        if constexpr (trace_docrefs)
                SLog("now ", tracked_docrefs.size, " ", doc->id, "\n");
}

void Trinity::queryexec_ctx::gc_retained_docs(const isrc_docid_t base) {
        std::size_t n{0};
        auto        cnt{tracked_docrefs.size};

        // Trim back, and front
        // this is not optimal because the documents are not ordered by id in tracked_docrefs.data[]
        // but short of using e.g a binary heap/prio.queue, which would provide this guarantee, at the expense of
        // higher maintenance, this is an good compromise.
        //
        // The only problem is that we are likely tracking too many candidate_document instances
        // and this could mean more memory pressure.
        //
        // TODO: we can eliminate memmove() by using a ring instead (front_index, back_index), and making
        // ring size a power of a 2. That way, we will only need to manipulate two indices, and not bother
        // with memmove(), at the expense of higher iteration costs. Need to consider that alternative impl.

        while (cnt) {
                if (auto r = tracked_docrefs.data[cnt - 1]; base > r->id) {
                        forget_document(r);
                        cds_release(r);
                        --cnt;
                } else
                        break;
        }

        while (n < cnt) {
                if (auto r = tracked_docrefs.data[n]; base > r->id) {
                        forget_document(r);
                        cds_release(r);
                        ++n;
                } else
                        break;
        }

        tracked_docrefs.size = cnt;

        if constexpr (trace_docrefs) {
                SLog("GC: For base = ", base, " n = ", n, " tracked_docrefs.size = ", tracked_docrefs.size, "\n");
        }

        if (n) {
                tracked_docrefs.size -= n;
                memmove(tracked_docrefs.data, tracked_docrefs.data + n, tracked_docrefs.size * sizeof(candidate_document *));
        }
}

Codecs::PostingsListIterator *Trinity::queryexec_ctx::reg_pli(Codecs::PostingsListIterator *it) {
        if (accumScoreMode)
                wrap_iterator(this, it);

        allIterators.push_back(it);
        return it;
}

DocsSetIterators::Iterator *Trinity::queryexec_ctx::reg_docset_it(DocsSetIterators::Iterator *it) {
        auto res{it};

        if (accumScoreMode) {
                wrap_iterator(this, it);
                // see comments of IteratorScorer::iterator() decl.
                res = static_cast<IteratorScorer *>(it->rdp)->iterator();
        }

        docsetsIterators.push_back(it);
        return res;
}

queryexec_ctx::~queryexec_ctx() {
        if constexpr (trace_docrefs)
                SLog("Flushing ", tracked_docrefs.size, "\n");

	for (auto p: large_allocs)
		std::free(p);

        while (tracked_docrefs.size) {
                auto d = tracked_docrefs.data[--tracked_docrefs.size];

                if (unlikely(d->rc != 1)) {
                        if constexpr (trace_docrefs) {
                                SLog(ansifmt::bold, ansifmt::color_blue, "Unexpected rc(", d->rc, ") for ", ptr_repr(d), " (", tracked_docrefs.size, ")", ansifmt::reset, "\n");
                        }
                }

                cds_release(d);
        }

        if (auto ptr = tracked_docrefs.data)
                std::free(ptr);

#ifdef USE_BANKS
        for (auto it : banks)
                delete it;
        for (auto it : reusableBanks)
                delete it;

        reusableBanks.clear();
        banks.clear();
#endif

        while (!allIterators.empty()) {
                auto ptr = allIterators.back();

                if (auto rdp = ptr->rdp; rdp != ptr)
                        delete static_cast<IteratorScorer *>(rdp);

                delete ptr;
                allIterators.pop_back();
        }

        for (auto ptr : docsetsIterators) {
                // this is not elegant, but its pragmatic enough to be OK
                if (auto rdp = ptr->rdp; rdp != ptr)
                        delete static_cast<IteratorScorer *>(rdp);

                switch (ptr->type) {
                        case DocsSetIterators::Type::AppIterator:
                                delete static_cast<DocsSetIterators::AppIterator *>(ptr);
                                break;

                        case DocsSetIterators::Type::Filter:
                                delete static_cast<DocsSetIterators::Filter *>(ptr);
                                break;

                        case DocsSetIterators::Type::Optional:
                                delete static_cast<DocsSetIterators::Optional *>(ptr);
                                break;

                        case DocsSetIterators::Type::Disjunction:
                                delete static_cast<DocsSetIterators::Disjunction *>(ptr);
                                break;

                        case DocsSetIterators::Type::DisjunctionAllPLI:
                                delete static_cast<DocsSetIterators::DisjunctionAllPLI *>(ptr);
                                break;

                        case DocsSetIterators::Type::DisjunctionSome:
                                delete static_cast<DocsSetIterators::DisjunctionSome *>(ptr);
                                break;

                        case DocsSetIterators::Type::VectorIDs:
                                delete static_cast<DocsSetIterators::VectorIDs *>(ptr);
                                break;

                        case DocsSetIterators::Type::Conjuction:
                                delete static_cast<DocsSetIterators::Conjuction *>(ptr);
                                break;

                        case DocsSetIterators::Type::ConjuctionAllPLI:
                                delete static_cast<DocsSetIterators::ConjuctionAllPLI *>(ptr);
                                break;

                        case DocsSetIterators::Type::Phrase:
                                delete static_cast<DocsSetIterators::Phrase *>(ptr);
                                break;

                        case DocsSetIterators::Type::Dummy:
                        case DocsSetIterators::Type::PostingsListIterator:
                                break;
                }
        }

        while (auto p = reusableCDS.pop_one())
                delete p;

        if (reusableCDS.data)
                std::free(reusableCDS.data);
}

void queryexec_ctx::prepare_decoder(exec_term_id_t termID) {
        decode_ctx.check(termID);

        if (!decode_ctx.decoders[termID]) {
                const auto p   = tctxMap[termID];
                auto       dec = decode_ctx.decoders[termID] = idxsrc->new_postings_decoder(p.second, p.first);

                dec->set_exec(termID, this);
        }

        require(decode_ctx.decoders[termID]);
}

exec_term_id_t queryexec_ctx::resolve_term(const str8_t term) {
        const auto res = termsDict.insert({term, 0});

        if (res.second) {
                auto       ptr  = &res.first->second;
                const auto tctx = idxsrc->term_ctx(term);

                if (tctx.documents == 0) {
                        // matches no documents, unknown
                        *ptr = 0;
                } else {
                        *ptr = termsDict.size();
                        tctxMap.insert({*ptr, {tctx, term}});
                }
        }

        return res.first->second;
}

void queryexec_ctx::decode_ctx_struct::check(const uint16_t idx) {
        if (idx >= capacity) {
                const auto newCapacity{idx + 8};

                decoders = (Trinity::Codecs::Decoder **)std::realloc(decoders, sizeof(Trinity::Codecs::Decoder *) * newCapacity);
                memset(decoders + capacity, 0, (newCapacity - capacity) * sizeof(Trinity::Codecs::Decoder *));
                capacity = newCapacity;
        }
}

queryexec_ctx::decode_ctx_struct::~decode_ctx_struct() {
        for (size_t i{0}; i != capacity; ++i)
                delete decoders[i];

        if (decoders)
                std::free(decoders);
}

#if 0 // SOLVED
static void will_materialize(void *ptr, const isrc_docid_t did, const uint32_t ref, Trinity::candidate_document *cd)
{
        static std::unordered_map<uintptr_t, std::unique_ptr<std::unordered_set<isrc_docid_t>>> map;
        auto res = map.emplace(uintptr_t(ptr), std::unique_ptr<std::unordered_set<isrc_docid_t>>{});
	static void *tracked{nullptr};
	static void *tracked_cd{nullptr};

	if (tracked_cd && did != 2155079078)
	{
		SLog("TRACKED CD already and now ", did, " ", ref, "\n");
	}

        if (res.second)
                res.first->second.reset(new std::unordered_set<isrc_docid_t>());

        auto s = res.first->second.get();

        if (!s->insert(did).second)
	{
		Print("ALREADY materialized hits for ", ptr_repr(ptr), " ", did, ", ref = ", ref, "\n");
		exit(1);
	}

	if (did == 2155079078)
	{
		SLog("OK materialized now at ", ref, "\n");
		tracked = ptr;
		tracked_cd = cd;
	}
	else if (tracked == ptr)
	{
		SLog("TRACKED now set to ", did, "\n");
	}
}
#endif

// In practice, this is only used by DocsSetIterators::Phrase::consider_phrase_match()
Trinity::term_hits *candidate_document::materialize_term_hits(queryexec_ctx *rctx, Codecs::PostingsListIterator *const it, const exec_term_id_t termID) {
        auto *const __restrict__ th = termHits + termID;
        const auto did              = it->curDocument.id;

        if (th->doc_id != did) {
                const auto docHits = it->freq;

                //will_materialize(it, did, __LINE__, this);

                th->set_docid(did);
                th->set_freq(docHits);

                if (!matchedDocument.dws) {
                        matchedDocument.dws = new DocWordsSpace(rctx->idxsrc->max_indexed_position());
                }

                if (!dwsInUse) {
                        // deferred until we will need to do this
                        matchedDocument.dws->reset();
                        dwsInUse = true; // in use now
                }

                it->materialize_hits(matchedDocument.dws, th->all);
        }

        return th;
}

Trinity::candidate_document::candidate_document(queryexec_ctx *const rctx) {
        const auto maxQueryTermIDPlus1 = rctx->termsDict.size() + 1;

        curDocQueryTokensCaptured    = (isrc_docid_t *)calloc(sizeof(isrc_docid_t), maxQueryTermIDPlus1);
        termHits                     = new term_hits[maxQueryTermIDPlus1];

	if (const auto required = sizeof(matched_query_term) * maxQueryTermIDPlus1;  rctx->allocator.can_allocate(required))
        	matchedDocument.matchedTerms = (matched_query_term *)rctx->allocator.Alloc(required);
	else {
        	matchedDocument.matchedTerms = (matched_query_term *)malloc(required);
		rctx->large_allocs.emplace_back(matchedDocument.matchedTerms);
	}
}

void queryexec_ctx::_reusable_cds::push_back(candidate_document *const d) {
        if (unlikely(size_ == capacity)) {
                // can't hold no more
                delete d;
        } else {
                data[size_++] = d;
        }
}

static void collect_doc_matching_terms(Trinity::DocsSetIterators::Iterator *const it,
                                       const isrc_docid_t                         docID,
                                       Trinity::iterators_collector *const        out) {
        switch (it->type) {
                case DocsSetIterators::Type::Phrase: {
                        const auto I   = static_cast<const DocsSetIterators::Phrase *>(it);
                        const auto n   = I->size;
                        auto       its = I->its;

                        memcpy(out->data + out->cnt, its, sizeof(its[0]) * n);
                        out->cnt += n;
                } break;

                case DocsSetIterators::Type::DisjunctionSome: {
                        auto const d = static_cast<DocsSetIterators::DisjunctionSome *>(it);

                        d->update_matched_cnt();
                        if (d->allPLI) {
                                for (auto i{d->lead}; i; i = i->next)
                                        out->data[out->cnt++] = reinterpret_cast<Codecs::PostingsListIterator *>(i->it);
                        } else {
                                for (auto i{d->lead}; i; i = i->next)
                                        collect_doc_matching_terms(i->it, docID, out);
                        }
                } break;

                case DocsSetIterators::Type::PostingsListIterator:
                        out->data[out->cnt++] = reinterpret_cast<Codecs::PostingsListIterator *>(it);
                        break;

                case DocsSetIterators::Type::Optional: {
                        auto *const opt    = reinterpret_cast<const DocsSetIterators::Optional *>(it);
                        auto        optCur = opt->opt->current();

                        collect_doc_matching_terms(opt->main, docID, out);

                        if (optCur < docID) {
				// we need to advance here
                                optCur = opt->opt->advance(docID);
			}

                        if (optCur == docID)
                                collect_doc_matching_terms(opt->opt, docID, out);
                } break;

                case DocsSetIterators::Type::Filter:
                        collect_doc_matching_terms(reinterpret_cast<const DocsSetIterators::Filter *>(it)->req, docID, out);
                        break;

                case DocsSetIterators::Type::Conjuction: {
                        const auto I   = static_cast<const DocsSetIterators::Conjuction *>(it);
                        const auto n   = I->size;
                        auto       its = I->its;

                        for (size_t i{0}; i != n; ++i)
                                collect_doc_matching_terms(its[i], docID, out);
                } break;

                case DocsSetIterators::Type::ConjuctionAllPLI: {
                        const auto I   = static_cast<const DocsSetIterators::ConjuctionAllPLI *>(it);
                        const auto n   = I->size;
                        auto       its = I->its;

                        memcpy(out->data + out->cnt, its, sizeof(its[0]) * n);
                        out->cnt += n;
                } break;

                case DocsSetIterators::Type::DisjunctionAllPLI: {
                        // See Switch::priority_queue<>::for_each_top()
                        const auto  I    = static_cast<DocsSetIterators::DisjunctionAllPLI *>(it);
                        const auto &pq   = I->pq;
                        const auto  size = pq.size();
                        const auto  heap = pq.data();

                        out->data[out->cnt++] = (Codecs::PostingsListIterator *)heap[0];
                        if (size >= 3) {
                                auto &stack = I->istack;

                                stack.data[0] = 1;
                                stack.data[1] = 2;
                                stack.cnt     = 2;

                                do {
                                        const auto i = stack.data[--stack.cnt];

                                        if (auto it = heap[i]; it->current() == docID) {
                                                out->data[out->cnt++] = (Codecs::PostingsListIterator *)it;

                                                const auto left  = ((i + 1) << 1) - 1;
                                                const auto right = left + 1;

                                                if (right < size) {
                                                        stack.data[stack.cnt++] = left;
                                                        stack.data[stack.cnt++] = right;
                                                } else if (left < size && (it = heap[left])->current() == docID) {
                                                        out->data[out->cnt++] = (Codecs::PostingsListIterator *)it;
                                                }
                                        }

                                } while (stack.cnt);

                        } else if (size == 2 && heap[1]->current() == docID) {
                                out->data[out->cnt++] = (Codecs::PostingsListIterator *)heap[1];
                        }
                } break;

                case DocsSetIterators::Type::Disjunction: {
                        // See Switch::priority_queue<>::for_each_top()
                        const auto  I    = static_cast<DocsSetIterators::Disjunction *>(it);
                        const auto &pq   = I->pq;
                        const auto  size = pq.size();
                        const auto  heap = pq.data();

                        collect_doc_matching_terms(heap[0], docID, out);
                        if (size >= 3) {
                                auto &stack = I->istack;

                                stack.data[0] = 1;
                                stack.data[1] = 2;
                                stack.cnt     = 2;

                                do {
                                        const auto i = stack.data[--stack.cnt];

                                        if (auto it = heap[i]; it->current() == docID) {
                                                collect_doc_matching_terms(it, docID, out);

                                                const auto left  = ((i + 1) << 1) - 1;
                                                const auto right = left + 1;

                                                if (right < size) {
                                                        stack.data[stack.cnt++] = left;
                                                        stack.data[stack.cnt++] = right;
                                                } else if (left < size && (it = heap[left])->current() == docID) {
                                                        collect_doc_matching_terms(it, docID, out);
                                                }
                                        }
                                } while (stack.cnt);
                        } else if (size == 2 && heap[1]->current() == docID) {
                                collect_doc_matching_terms(heap[1], docID, out);
                        }
                } break;

                default:
                        SLog("IMPLEMENT ME\n");
                        exit(1);
        }
}

void Trinity::queryexec_ctx::prepare_match(Trinity::candidate_document *const doc) {
        static constexpr const bool trace{false};
        auto &                      md  = doc->matchedDocument;
        const auto                  did = doc->id;
        auto                        dws = md.dws;
        //const bool trace = doc->id == 2155079078 || doc->id == 2154380143;

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_blue, "Preparing match for ", doc->id, ansifmt::reset, " ", ptr_repr(doc), "\n");

        collectedIts.cnt = 0;
        collect_doc_matching_terms(rootIterator, doc->id, &collectedIts);
        md.matchedTermsCnt = 0;

        if (!dws) {
                if (trace)
                        SLog("dws == nullptr\n");

                dws = md.dws = new DocWordsSpace(idxsrc->max_indexed_position());
        } else if (trace) {
                SLog("Already have dws inuse = ", doc->dwsInUse, "\n");
        }

        if (!doc->dwsInUse) {
                // wasn't already reset
                dws->reset();
        } else {
                // force reset later
                doc->dwsInUse = false;
        }

        const auto  cnt  = collectedIts.cnt;
        auto *const data = collectedIts.data;

        if (trace)
                SLog("collected iterators ", cnt, "\n");

        for (size_t i{0}; i != cnt; ++i) {
                auto *const it  = data[i];
                const auto  tid = it->decoder()->exec_ctx_termid();

                if (trace)
                        SLog(ansifmt::color_green, "term id ", tid, ansifmt::reset, "\n");

                if (const auto *const qti = originalQueryTermCtx[tid]) // not in a NOT branch
                {
                        if (trace)
                                SLog("YES, qti ", doc->curDocQueryTokensCaptured[tid], " ", doc->curDocSeq, "\n");

                        if (doc->curDocQueryTokensCaptured[tid] != doc->curDocSeq) {
                                auto *const p  = md.matchedTerms + md.matchedTermsCnt++;
                                auto *const th = doc->termHits + tid;

                                doc->curDocQueryTokensCaptured[tid] = doc->curDocSeq;
                                p->queryCtx                         = qti;
                                p->hits                             = th;

                                if (trace)
                                        SLog("th->docID = ", th->doc_id, "  ", did, "\n");

                                if (th->doc_id != did) {
                                        // could have been materialized earlier for a phrase check
                                        const auto docHits = it->freq;

                                        if (trace)
                                                SLog(ansifmt::bold, ansifmt::color_green, "YES, need to materialize freq = ", docHits, ansifmt::reset, " ", ptr_repr(it), "\n");

                                        //will_materialize(it, did, __LINE__, doc);

                                        th->set_docid(did);
                                        th->set_freq(docHits);
                                        it->materialize_hits(dws, th->all);
                                }
                        } else if (trace)
                                SLog("Already have materialized hits for (", doc->id, ", ", tid, ")\n");
                }
        }
}

void Trinity::queryexec_ctx::forget_document(candidate_document *const doc) {
#ifdef USE_BANKS
        forget_document_inbank(doc);
#endif
}

Trinity::candidate_document *Trinity::queryexec_ctx::lookup_document(const isrc_docid_t id) {
#ifdef USE_BANKS
        return lookup_document_inbank(id);
#else
        auto &v = trackedDocuments[id & (sizeof_array(trackedDocuments) - 1)];

        if (traceBindings)
                SLog("Lookup among ", v.size(), "\n");

        auto data = v.data();
        auto size = v.size();

        for (size_t i{0}; i < size;) {
                auto doc = data[i];

                if (!doc->bindCnt) {
                        if (traceBindings)
                                SLog("Letting go of document\n");

                        cds_release(doc);
                        data[i] = data[--size];
                        v.pop_back();
                } else if (doc->id == id) {
                        if (traceBindings)
                                SLog("Found at ", i, "\n");

                        return doc;
                } else
                        ++i;
        }
        return nullptr;
#endif
}

#ifdef USE_BANKS
Trinity::docstracker_bank *Trinity::queryexec_ctx::new_bank(const Trinity::isrc_docid_t base) {
        if (reusableBanks.size()) {
                auto b = reusableBanks.back();

                reusableBanks.pop_back();
#ifdef BANKS_USE_BM
                memset(b->bm, 0, docstracker_bank::BM_SIZE * sizeof(uint64_t));
#else
                memset(b->entries, 0, sizeof(docstracker_bank::entry) * docstracker_bank::SIZE);
#endif

                b->base   = base;
                b->setCnt = 0;

                lastBank = b;
                banks.push_back(b);
                return b;
        }

        auto b = new docstracker_bank();

#ifdef BANKS_USE_BM
        memset(b->bm, 0, docstracker_bank::BM_SIZE * sizeof(uint64_t));
#else
        memset(b->entries, 0, sizeof(docstracker_bank::entry) * docstracker_bank::SIZE);
#endif
        b->base   = base;
        b->setCnt = 0;

        lastBank = b;
        banks.push_back(b);

        return b;
}

void Trinity::queryexec_ctx::forget_document_inbank(Trinity::candidate_document *const doc) {
        const auto id = doc->id;
        auto       b  = bank_for(id);

        if (1 == b->setCnt--) {
                if (lastBank == b)
                        lastBank = nullptr;

                for (size_t i{0}; i != banks.size(); ++i) {
                        if (banks[i] == b) {
                                banks[i] = banks.back();
                                banks.pop_back();
                                break;
                        }
                }

                reusableBanks.push_back(b);
                lastBank = nullptr;
        } else {
                const auto idx = id - b->base;

#ifdef BANKS_USE_BM
                b->bm[idx >> 6] &= ~(uint64_t(1) << (idx & (docstracker_bank::SIZE - 1)));
#else
                b->entries[idx].document = nullptr;
#endif
        }
}

Trinity::candidate_document *Trinity::queryexec_ctx::lookup_document_inbank(const isrc_docid_t id) {
        if (const auto b = bank_for(id)) {
                const auto idx = id - b->base;

#ifdef BANKS_USE_BM
                if (b->bm[idx >> 6] & (uint64_t(1) << (idx & (docstracker_bank::SIZE - 1))))
#endif
                        return b->entries[idx].document;
        }

        return nullptr;
}

void Trinity::queryexec_ctx::track_document_inbank(Trinity::candidate_document *const d) {
        const auto id  = d->id;
        auto       b   = bank_for(id);
        const auto idx = id - b->base;

#ifdef BANKS_USE_BM
        b->bm[idx >> 6] |= uint64_t(1) << (idx & (docstracker_bank::SIZE - 1));
#endif
        b->entries[idx].document = d;
        ++(b->setCnt);
}
#endif
