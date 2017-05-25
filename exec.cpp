#include "exec.h"
#include "docwordspace.h"
#include "matches.h"

using namespace Trinity;

namespace // static/local this module
{
        [[maybe_unused]] static constexpr bool traceExec{false};
        static constexpr bool traceCompile{false};

        struct runtime_ctx;

        struct exec_node final // 64bytes alignement seems to yield good results, but crashes if optimizer is enabled (i.e struct alignas(64) exec_node {})
        {
                bool (*fp)(const exec_node &, runtime_ctx &);

                union {
                        void *ptr;
                        uint32_t u32;
                        uint16_t u16;
                };
        };

        // This is initialized by the compiler
        // and used by the VM
        struct runtime_ctx final
        {
                IndexSource *const idxsrc;

#pragma mark structs
                // See compile()
                struct binop_ctx final
                {
                        exec_node lhs;
                        exec_node rhs;
                };

                struct unaryop_ctx final
                {
                        exec_node expr;
                };

                struct phrase final
                {
                        uint8_t size;
                        exec_term_id_t termIDs[0];

                        auto operator==(const phrase &o) const noexcept
                        {
                                if (size == o.size)
                                {
                                        for (uint32_t i{0}; i != size; ++i)
                                        {
                                                if (termIDs[i] != o.termIDs[i])
                                                        return false;
                                        }
                                        return true;
                                }
                                else
                                        return false;
                        }

                        bool is_set(const exec_term_id_t id) const noexcept
                        {
                                for (uint32_t i{0}; i != size; ++i)
                                {
                                        if (termIDs[i] == id)
                                                return true;
                                }
                                return false;
                        }
                };

                struct termsrun final
                {
                        uint16_t size;
                        exec_term_id_t terms[0];

                        auto operator==(const termsrun &o) const noexcept
                        {
                                if (size == o.size)
                                {
                                        for (uint32_t i{0}; i != size; ++i)
                                        {
                                                if (terms[i] != o.terms[i])
                                                        return false;
                                        }
                                        return true;
                                }
                                else
                                        return false;
                        }

                        bool is_set(const exec_term_id_t id) const noexcept
                        {
                                for (uint32_t i{0}; i != size; ++i)
                                {
                                        if (terms[i] == id)
                                                return true;
                                }
                                return false;
                        }

                        bool erase(const exec_term_id_t id) noexcept
                        {
                                for (uint32_t i{0}; i != size; ++i)
                                {
                                        if (terms[i] == id)
                                        {
                                                memmove(terms + i, terms + i + 1, (--size - i) * sizeof(terms[0]));
                                                return true;
                                        }
                                }

                                return false;
                        }

                        bool erase(const termsrun &o)
                        {
                                // Fast scalar intersection scheme designed by N.Kurz.
                                // lemire/SIMDCompressionAndIntersection.cpp
                                const auto *A = terms, *B = o.terms;
                                const auto endA = terms + size;
                                const auto endB = o.terms + o.size;
                                auto out{terms};

                                for (;;)
                                {
                                        while (*A < *B)
                                        {
                                        l1:
                                                *out++ = *A;
                                                if (++A == endA)
                                                {
                                                l2:
                                                        if (const auto n = out - terms; n == size)
                                                                return false;
                                                        else
                                                        {
                                                                size = n;
                                                                return true;
                                                        }
                                                }
                                        }

                                        while (*A > *B)
                                        {
                                                if (++B == endB)
                                                {
                                                        while (A != endA)
                                                                *out++ = *A++;
                                                        goto l2;
                                                }
                                        }

                                        if (*A == *B)
                                        {
                                                if (++A == endA || ++B == endB)
                                                {
                                                        while (A != endA)
                                                                *out++ = *A++;
                                                        goto l2;
                                                }
                                        }
                                        else
                                                goto l1;
                                }
                        }

                        auto empty() const noexcept
                        {
                                return !size;
                        }
                };

		struct cacheable_termsrun
		{
			docid_t lastConsideredDID;
			const  termsrun *run;
			bool res;
		};

                struct phrasesrun final
                {
                        uint16_t size;
                        phrase *phrases[0];
                };

#pragma eval and runtime specific
                runtime_ctx(IndexSource *src)
                    : idxsrc{src}, docWordsSpace{src->max_indexed_position()}
                {
                }

                void materialize_term_hits_impl(const exec_term_id_t termID)
                {
                        auto *const __restrict__ th = decode_ctx.termHits[termID];
                        auto *const __restrict__ dec = decode_ctx.decoders[termID];
                        const auto docHits = dec->curDocument.freq; // see Codecs::Decoder::curDocument comments

                        th->docSeq = curDocSeq;
                        th->set_freq(docHits);
                        dec->materialize_hits(termID, &docWordsSpace, th->all);
                }

                auto materialize_term_hits(const exec_term_id_t termID)
                {
                        auto th = decode_ctx.termHits[termID];

                        if (likely(th->docSeq != curDocSeq))
                        {
                                // Not already materialized
                                materialize_term_hits_impl(termID);
                        }

                        return th;
                }

                void capture_matched_term(const exec_term_id_t termID)
                {
                        static constexpr bool trace{false};

                        if (trace)
                                SLog("Capturing matched term ", termID, "\n");

                        if (const auto qti = originalQueryTermCtx[termID])
                        {
                                // If this not nullptr, it means this was not in a NOT branch so we should track it
                                // We exclude all tokens in NOT rhs branches when we collect the original query tokens
                                if (curDocQueryTokensCaptured[termID] != curDocSeq)
                                {
                                        // Not captured already for this document
                                        auto p = matchedDocument.matchedTerms + matchedDocument.matchedTermsCnt++;
                                        auto th = decode_ctx.termHits[termID];

                                        if (trace)
                                                SLog("Not captured yet, capturing now [", qti->term.token, "]\n");

                                        curDocQueryTokensCaptured[termID] = curDocSeq;
                                        p->queryCtx = qti;
                                        p->hits = th;
                                }
                                else if (trace)
                                        SLog("Already captured\n");
                        }
                        else
                        {
                                // This token was found only in a NOT branch
                                if (trace)
                                        SLog("Term found in a NOT branch\n");
                        }
                }

                // For simplicity's sake, we are just going to map exec_term_id_t => decoders[] without
                // indirection. For each distict/resolved term, we have a decoder and
                // term_hits in decode_ctx.decoders[] and decode_ctx.termHits[]
                // This means you can index them using a termID
                // This means we may have some nullptr in decode_ctx.decoders[] but that's OK
                void prepare_decoder(exec_term_id_t termID)
                {
                        decode_ctx.check(termID);

                        if (!decode_ctx.decoders[termID])
                        {
                                const auto p = tctxMap[termID];

                                decode_ctx.decoders[termID] = idxsrc->new_postings_decoder(p.second, p.first);
                                decode_ctx.termHits[termID] = new term_hits();
                        }

                        require(decode_ctx.decoders[termID]);
                }

                void reset(const docid_t did)
                {
                        curDocID = did;
                        docWordsSpace.reset();
                        matchedDocument.matchedTermsCnt = 0;

                        // see docwordspace.h
                        if (unlikely(curDocSeq == UINT16_MAX))
                        {
                                const auto maxQueryTermIDPlus1 = termsDict.size() + 1;

                                memset(curDocQueryTokensCaptured, 0, sizeof(uint16_t) * maxQueryTermIDPlus1);
                                for (uint32_t i{0}; i != decode_ctx.capacity; ++i)
                                {
                                        if (auto ptr = decode_ctx.termHits[i])
                                                ptr->docSeq = 0;
                                }

                                curDocSeq = 1; // important; set to 1 not 0
                        }
                        else
                        {
                                ++curDocSeq;
                        }
                }

#pragma mark Compiler / Optimizer specific
                term_index_ctx term_ctx(const exec_term_id_t termID)
                {
                        // we should have resolve_term() anyway
                        return tctxMap[termID].first;
                }

                // Resolves a term to a termID relative to the runtime_ctx
                // This id is meaningless outside this execution context
                // and we use it because its easier to track/use integers than strings
                // See Termspaces in CONCEPTS.md
                exec_term_id_t resolve_term(const str8_t term)
                {
                        exec_term_id_t *ptr;

#ifndef LEAN_SWITCH
                        if (termsDict.Add(term, 0, &ptr))
#else
                        auto p = termsDict.insert({term, 0});

                        ptr = &p.first->second;
                        if (p.second)
#endif
                        {
                                const auto tctx = idxsrc->term_ctx(term);

                                if (traceCompile)
                                        SLog(ansifmt::bold, ansifmt::color_green, "[", term, "] ", termsDict.size(), ", documents = ", tctx.documents, ansifmt::reset, "\n");

                                if (tctx.documents == 0)
                                {
                                        // matches no documents, unknown
                                        *ptr = 0;
                                }
                                else
                                {
                                        *ptr = termsDict.size();
                                        tctxMap.insert({*ptr, {tctx, term}});
                                }
                        }

                        return *ptr;
                }

                binop_ctx *register_binop(const exec_node lhs, const exec_node rhs)
                {
                        auto ptr = ctxAllocator.New<binop_ctx>();

                        ptr->lhs = lhs;
                        ptr->rhs = rhs;
                        return ptr;
                }

                unaryop_ctx *register_unaryop(const exec_node expr)
                {
                        auto ptr = ctxAllocator.New<unaryop_ctx>();

                        ptr->expr = expr;
                        return ptr;
                }

                uint16_t register_token(const Trinity::phrase *p)
                {
                        return resolve_term(p->terms[0].token);
                }

                phrase *register_phrase(const Trinity::phrase *p)
                {
                        auto ptr = (phrase *)allocator.Alloc(sizeof(phrase) + sizeof(exec_term_id_t) * p->size);

                        ptr->size = p->size;
                        for (uint32_t i{0}; i != p->size; ++i)
                        {
                                if (const auto id = resolve_term(p->terms[i].token))
                                        ptr->termIDs[i] = id;
                                else
                                        return nullptr;
                        }

                        return ptr;
                }

#pragma mark members
                // This is from the lead tokens
                // We expect all token and phrases opcodes to check against this document
                docid_t curDocID;
                // See docwordspace.h
                uint16_t curDocSeq;

                // indexed by termID
                query_term_ctx **originalQueryTermCtx;

                struct decode_ctx_struct final
                {
                        // decoders[](for decoding terms posting lists) and termHits[] are indexed by termID
                        Trinity::Codecs::Decoder **decoders{nullptr};
                        term_hits **termHits{nullptr};
                        uint16_t capacity{0};

                        void check(const uint16_t idx)
                        {
                                if (idx >= capacity)
                                {
                                        const auto newCapacity{idx + 8};

                                        decoders = (Trinity::Codecs::Decoder **)std::realloc(decoders, sizeof(Trinity::Codecs::Decoder *) * newCapacity);
                                        memset(decoders + capacity, 0, (newCapacity - capacity) * sizeof(Trinity::Codecs::Decoder *));

                                        termHits = (term_hits **)std::realloc(termHits, sizeof(term_hits *) * newCapacity);
                                        memset(termHits + capacity, 0, (newCapacity - capacity) * sizeof(term_hits *));

                                        capacity = newCapacity;
                                }
                        }

                        ~decode_ctx_struct()
                        {
                                for (uint32_t i{0}; i != capacity; ++i)
                                {
                                        delete decoders[i];
                                        delete termHits[i];
                                }

                                if (decoders)
                                        std::free(decoders);

                                if (termHits)
                                        std::free(termHits);
                        }
                } decode_ctx;

                DocWordsSpace docWordsSpace;
                Switch::unordered_map<str8_t, exec_term_id_t> termsDict;
                uint16_t *curDocQueryTokensCaptured;
                matched_document matchedDocument;
		// TODO: determine suitable allocator bank size based on some meaningful metric
		// e.g total distinct tokens in the query, otherwise we may just end up allocating more memory than
		// we need and for environments where memory pressure is a concern, this may be important.
		// For now, go with large enough bank sizes for the allocators and figure out something later.
		// We should also track allocated (from allocators) memory that is no longer needed so that we can reuse it
		// Maybe we just need a method for allocating arbitrary amount of memory and releasing it back to the runtime ctx
                simple_allocator allocator{4096 * 4};
                simple_allocator runsAllocator{2048}, ctxAllocator{2048};
                Switch::unordered_map<exec_term_id_t, std::pair<term_index_ctx, str8_t>> tctxMap;
        };
}

#pragma mark INTERPRETER
#define eval(node, ctx) (node.fp(node, ctx))
static inline bool constfalse_impl(const exec_node &, runtime_ctx &)
{
        return false;
}

static inline bool dummyop_impl(const exec_node &, runtime_ctx &)
{
        return false;
}

static bool matchallterms_impl(const exec_node &self, runtime_ctx &rctx)
{
        static constexpr bool trace{false};
        const auto run = static_cast<const runtime_ctx::termsrun *>(self.ptr);
        uint16_t i{0};
        const auto size = run->size;
        const auto did = rctx.curDocID;

        do
        {
                const auto termID = run->terms[i];
                auto decoder = rctx.decode_ctx.decoders[termID];

                if (trace)
                        SLog("Considering ", termID, "\n");

                if (decoder->seek(did))
                        rctx.capture_matched_term(termID);
                else
                {
                        if (trace)
                                SLog("NO for ", termID, "\n");
                        return false;
                }
        } while (++i != size);

        return true;
}

static bool matchallterms_cacheable_impl(const exec_node &self, runtime_ctx &rctx)
{
        auto *const __restrict__ ctx = static_cast<runtime_ctx::cacheable_termsrun *>(self.ptr);

	if (ctx->lastConsideredDID == rctx.curDocID)
	{
		// cached
		return ctx->res;
	}

	const auto *const __restrict__ run = ctx->run;
        uint16_t i{0};
        const auto size = run->size;
        const auto did = rctx.curDocID;

	ctx->lastConsideredDID = rctx.curDocID;
        do
        {
                const auto termID = run->terms[i];
                auto *const decoder = rctx.decode_ctx.decoders[termID];

                if (decoder->seek(did))
                        rctx.capture_matched_term(termID);
                else
                {
			ctx->res = false;
                        return false;
                }
        } while (++i != size);

	ctx->res = true;
        return true;
}

static bool matchanyterms_impl(const exec_node &self, runtime_ctx &rctx)
{
        uint16_t i{0};
        bool res{false};
        const auto did = rctx.curDocID;
#if 1
        const auto *const __restrict__ run = static_cast<const runtime_ctx::termsrun *>(self.ptr);
        const auto size = run->size;
        const auto *const __restrict__ terms = run->terms;

#if defined(TRINITY_ENABLE_PREFETCH)
        // this helps
        // from 0.091 down to 0.085
        const auto n = size / (64 / sizeof(exec_term_id_t));
        auto ptr = terms;

        for (uint32_t i{0}; i != n; ++i, ptr += (64 / sizeof(exec_term_id_t)))
                _mm_prefetch(ptr, _MM_HINT_NTA);
#endif
        do
        {
                const auto termID = terms[i];
                auto decoder = rctx.decode_ctx.decoders[termID];

                if (decoder->seek(did))
                {
                        rctx.capture_matched_term(termID);
                        res = true;
                }

        } while (++i != size);
#else
        // this works but turns out to be _slower_ than the original implementation for some reason
        // even-though I expected it to have been faster
        auto run = static_cast<runtime_ctx::termsrun *>(self.ptr);
        auto size = run->size;
        auto terms = run->terms;

#if defined(TRINITY_ENABLE_PREFETCH)
        // this helps
        // from 0.091 down to 0.085
        const auto n = size / (64 / sizeof(exec_term_id_t));
        auto ptr = terms;

        for (uint32_t i{0}; i != n; ++i, ptr += (64 / sizeof(exec_term_id_t)))
                _mm_prefetch(ptr, _MM_HINT_NTA);
#endif
        while (i < size)
        {
                const auto termID = terms[i];
                auto decoder = rctx.decode_ctx.decoders[termID];

                if (decoder->seek(did))
                {
                        rctx.capture_matched_term(termID);
                        res = true;
                        ++i;
                }
                else if (unlikely(decoder->curDocument.id == MaxDocIDValue))
                {
                        terms[i] = terms[--size];
                        run->size = size;
                }
                else
                        ++i;
        }
#endif

        return res;
}

static bool matchanyterms_fordocs_impl(const exec_node &self, runtime_ctx &rctx)
{
        uint16_t i{0};
        const auto did = rctx.curDocID;
        const auto *const __restrict__ run = static_cast<const runtime_ctx::termsrun *>(self.ptr);
        const auto size = run->size;
        const auto *const __restrict__ terms = run->terms;

#if defined(TRINITY_ENABLE_PREFETCH)
        const auto n = size / (64 / sizeof(exec_term_id_t));
        auto ptr = terms;

        for (uint32_t i{0}; i != n; ++i, ptr += (64 / sizeof(exec_term_id_t)))
                _mm_prefetch(ptr, _MM_HINT_NTA);
#endif
        do
        {
                const auto termID = terms[i];
                auto decoder = rctx.decode_ctx.decoders[termID];

                if (decoder->seek(did))
                        return true;

        } while (++i != size);

        return false;
}

static inline bool matchterm_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto termID = exec_term_id_t(self.u16);
        auto *const __restrict__ decoder = rctx.decode_ctx.decoders[termID];

        if (decoder->seek(rctx.curDocID))
        {
                rctx.capture_matched_term(termID);
                return true;
        }
        else
                return false;
}

static inline bool unaryand_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto op = (runtime_ctx::unaryop_ctx *)self.ptr;

        return eval(op->expr, rctx);
}

static inline bool unarynot_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto op = (runtime_ctx::unaryop_ctx *)self.ptr;

        return !eval(op->expr, rctx);
}

static bool matchanyphrases_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)self.ptr;
        const auto size = run->size;
        const auto did = rctx.curDocID;
        bool res{false};

        for (uint32_t k{0}; k != size; ++k)
        {
                auto p = run->phrases[k];
                const auto firstTermID = p->termIDs[0];
                auto decoder = rctx.decode_ctx.decoders[firstTermID];

                if (!decoder->seek(did))
                        goto nextPhrase;

                {
                        const auto n = p->size;

                        for (uint32_t i{1}; i != n; ++i)
                        {
                                const auto termID = p->termIDs[i];
                                auto decoder = rctx.decode_ctx.decoders[termID];

                                if (!decoder->seek(did))
                                        goto nextPhrase;

                                rctx.materialize_term_hits(termID);
                        }

                        {
                                auto th = rctx.materialize_term_hits(firstTermID);
                                const auto firstTermFreq = th->freq;
                                const auto firstTermHits = th->all;
                                auto &dws = rctx.docWordsSpace;

                                for (uint32_t i{0}; i != firstTermFreq; ++i)
                                {
                                        if (const auto pos = firstTermHits[i].pos)
                                        {
                                                for (uint8_t k{1};; ++k)
                                                {
                                                        if (k == n)
                                                        {
                                                                // matched seq
                                                                for (uint16_t i{0}; i != n; ++i)
                                                                        rctx.capture_matched_term(p->termIDs[i]);

                                                                res = true;
                                                                goto nextPhrase;
                                                        }

                                                        const auto termID = p->termIDs[k];

                                                        if (!dws.test(termID, pos + k))
                                                                break;
                                                }
                                        }
                                }
                        }
                }

        nextPhrase:;
        }

        return res;
}

static bool matchanyphrases_fordocs_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)self.ptr;
        const auto size = run->size;
        const auto did = rctx.curDocID;

        for (uint32_t k{0}; k != size; ++k)
        {
                auto p = run->phrases[k];
                const auto firstTermID = p->termIDs[0];
                auto decoder = rctx.decode_ctx.decoders[firstTermID];

                if (!decoder->seek(did))
                        goto nextPhrase;

                {
                        const auto n = p->size;

                        for (uint32_t i{1}; i != n; ++i)
                        {
                                const auto termID = p->termIDs[i];
                                auto decoder = rctx.decode_ctx.decoders[termID];

                                if (!decoder->seek(did))
                                        goto nextPhrase;

                                rctx.materialize_term_hits(termID);
                        }

                        {
                                auto th = rctx.materialize_term_hits(firstTermID);
                                const auto firstTermFreq = th->freq;
                                const auto firstTermHits = th->all;
                                auto &dws = rctx.docWordsSpace;

                                for (uint32_t i{0}; i != firstTermFreq; ++i)
                                {
                                        if (const auto pos = firstTermHits[i].pos)
                                        {
                                                for (uint8_t k{1};; ++k)
                                                {
                                                        if (k == n)
                                                                return true;

                                                        const auto termID = p->termIDs[k];

                                                        if (!dws.test(termID, pos + k))
                                                                break;
                                                }
                                        }
                                }
                        }
                }

        nextPhrase:;
        }

        return false;
}

static bool matchallphrases_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)self.ptr;
        const auto size = run->size;
        const auto did = rctx.curDocID;
        auto &dws = rctx.docWordsSpace;

        for (uint32_t k{0}; k != size; ++k)
        {
                const auto p = run->phrases[k];
                const auto firstTermID = p->termIDs[0];
                auto decoder = rctx.decode_ctx.decoders[firstTermID];

                if (!decoder->seek(did))
                        return false;

                const auto n = p->size;

                for (uint32_t i{1}; i != n; ++i)
                {
                        const auto termID = p->termIDs[i];
                        auto decoder = rctx.decode_ctx.decoders[termID];

                        if (!decoder->seek(did))
                                return false;

                        rctx.materialize_term_hits(termID);
                }

                auto th = rctx.materialize_term_hits(firstTermID);
                const auto firstTermFreq = th->freq;
                const auto firstTermHits = th->all;

                for (uint32_t i{0}; i != firstTermFreq; ++i)
                {
                        if (const auto pos = firstTermHits[i].pos)
                        {
                                for (uint8_t k{1};; ++k)
                                {
                                        if (k == n)
                                        {
                                                // matched seq
                                                for (uint16_t i{0}; i != n; ++i)
                                                        rctx.capture_matched_term(p->termIDs[i]);

                                                goto nextPhrase;
                                        }

                                        const auto termID = p->termIDs[k];

                                        if (!dws.test(termID, pos + k))
                                                break;
                                }
                        }
                }
                return false;

        nextPhrase:;
        }

        return true;
}

static bool matchphrase_impl(const exec_node &self, runtime_ctx &rctx)
{
        static constexpr bool trace{false};
        const auto p = (runtime_ctx::phrase *)self.ptr;
        const auto firstTermID = p->termIDs[0];
        auto decoder = rctx.decode_ctx.decoders[firstTermID];
        const auto did = rctx.curDocID;

        if (trace)
                SLog(ansifmt::bold, "PHRASE CHECK document ", rctx.curDocID, ansifmt::reset, "\n");

        if (!decoder->seek(did))
        {
                if (trace)
                        SLog("Failed for first phrase token\n");
                return 0;
        }

        const auto n = p->size;

        for (uint32_t i{1}; i != n; ++i)
        {
                const auto termID = p->termIDs[i];
                auto decoder = rctx.decode_ctx.decoders[termID];

                if (trace)
                        SLog("Phrase token ", i, " ", termID, "\n");

                if (!decoder->seek(did))
                {
                        if (trace)
                                SLog("Failed for phrase token\n");
                        return 0;
                }

                rctx.materialize_term_hits(termID);
        }

        auto th = rctx.materialize_term_hits(firstTermID);
        const auto firstTermFreq = th->freq;
        const auto firstTermHits = th->all;
        auto &dws = rctx.docWordsSpace;

        if (trace)
                SLog("first term freq = ", firstTermFreq, ", id = ", firstTermID, "\n");

        for (uint32_t i{0}; i != firstTermFreq; ++i)
        {
                if (const auto pos = firstTermHits[i].pos)
                {
                        if (trace)
                                SLog("<< POS ", pos, "\n");

                        for (uint8_t k{1};; ++k)
                        {
                                if (trace)
                                        SLog("Check for ", pos + k, " for ", p->termIDs[k], "\n");

                                if (k == n)
                                {
                                        // matched seq
                                        for (uint16_t i{0}; i != n; ++i)
                                                rctx.capture_matched_term(p->termIDs[i]);
                                        return true;
                                }

                                const auto termID = p->termIDs[k];

                                if (!dws.test(termID, pos + k))
                                        break;
                        }
                }
        }

        return false;
}

static inline bool consttrueexpr_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto op = (runtime_ctx::unaryop_ctx *)self.ptr;

        // evaluate but always return true
        eval(op->expr, rctx);
        return true;
}

static inline bool logicaland_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto opctx = (runtime_ctx::binop_ctx *)self.ptr;

        return eval(opctx->lhs, rctx) && eval(opctx->rhs, rctx);
}

static inline bool logicalnot_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto opctx = (runtime_ctx::binop_ctx *)self.ptr;

        return eval(opctx->lhs, rctx) && !eval(opctx->rhs, rctx);
}

static inline bool logicalor_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto *const __restrict__ opctx = (runtime_ctx::binop_ctx *)self.ptr;

        // WAS: return eval(opctx->lhs, rctx) || eval(opctx->rhs, rctx);
        // We need to evaluate both LHS and RHS and return true if either of them is true
        // this is so that we will COLLECT the tokens from both branches
        // e.g [apple OR samsung] should match if either is found, but we need to collect both tokens if possible
        //
        // See logicalor_fordocs_impl where we don't need to do that
        const auto res1 = eval(opctx->lhs, rctx);
        const auto res2 = eval(opctx->rhs, rctx);

        return res1 || res2;
}

static inline bool logicalor_fordocs_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto *const __restrict__ opctx = (runtime_ctx::binop_ctx *)self.ptr;

        return eval(opctx->lhs, rctx) || eval(opctx->rhs, rctx);
}

#pragma mark COMPILER/OPTIMIZER
#define SPECIALIMPL_COLLECTION_LOGICALOR ((void *)uintptr_t(UINTPTR_MAX - 1))
#define SPECIALIMPL_COLLECTION_LOGICALAND ((void *)uintptr_t(UINTPTR_MAX - 2))

static uint32_t reorder_execnode(exec_node &n, bool &updates, runtime_ctx &rctx)
{
        if (n.fp == matchterm_impl)
                return rctx.term_ctx(n.u16).documents;
        else if (n.fp == matchphrase_impl)
        {
                const auto p = static_cast<const runtime_ctx::phrase *>(n.ptr);

                return rctx.term_ctx(p->termIDs[0]).documents;
        }
        else if (n.fp == logicaland_impl)
        {
                auto ctx = static_cast<runtime_ctx::binop_ctx *>(n.ptr);
                const auto lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                if (rhsCost < lhsCost)
                {
                        std::swap(ctx->lhs, ctx->rhs);
                        updates = true;
                        return rhsCost;
                }
                else
                        return lhsCost;
        }
        else if (n.fp == logicalor_impl || n.fp == logicalnot_impl || n.fp == logicalor_fordocs_impl)
        {
                auto *const ctx = static_cast<runtime_ctx::binop_ctx *>(n.ptr);
                const auto lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                return n.fp == logicalor_impl || n.fp == logicalor_fordocs_impl ? (lhsCost + rhsCost) : lhsCost;
        }
        else if (n.fp == unaryand_impl || n.fp == unarynot_impl)
        {
                auto *const ctx = static_cast<runtime_ctx::unaryop_ctx *>(n.ptr);

                return reorder_execnode(ctx->expr, updates, rctx);
        }
        else if (n.fp == consttrueexpr_impl)
        {
                auto ctx = static_cast<runtime_ctx::unaryop_ctx *>(n.ptr);

                reorder_execnode(ctx->expr, updates, rctx);
                // it is important to return UINT32_MAX - 1 so that it will not result in a binop's (lhs, rhs) swap
                // we need to special-case the handling of those nodes
                return UINT32_MAX - 1;
        }
        else if (n.fp == matchallterms_impl)
        {
                const auto run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

                return rctx.term_ctx(run->terms[0]).documents;
        }
        else if (n.fp == matchanyterms_impl || n.fp == matchanyterms_fordocs_impl)
        {
                const auto run = static_cast<const runtime_ctx::termsrun *>(n.ptr);
                uint32_t sum{0};

                for (uint32_t i{0}; i != run->size; ++i)
                        sum += rctx.term_ctx(run->terms[i]).documents;
                return sum;
        }
        else if (n.fp == matchallphrases_impl)
        {
                const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)n.ptr;

                return rctx.term_ctx(run->phrases[0]->termIDs[0]).documents;
        }
        else if (n.fp == matchanyphrases_impl || n.fp == matchanyphrases_fordocs_impl)
        {
                const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)n.ptr;
                uint32_t sum{0};

                for (uint32_t i{0}; i != run->size; ++i)
                        sum += rctx.term_ctx(run->phrases[i]->termIDs[0]).documents;
                return sum;
        }
        else
        {
                std::abort();
        }
}

static exec_node reorder_execnodes(exec_node n, runtime_ctx &rctx)
{
        bool updates;

        do
        {
                updates = false;
                reorder_execnode(n, updates, rctx);
        } while (updates);

        return n;
}

// Considers all binary ops, and potentiall swaps (lhs, rhs) of binary ops,
// but not based on actual cost but on heuristics
struct reorder_ctx final
{
        bool dirty;
};

static void reorder(ast_node *n, reorder_ctx *const ctx)
{
        static constexpr bool trace{false};

        if (n->type == ast_node::Type::UnaryOp)
                reorder(n->unaryop.expr, ctx);
        else if (n->type == ast_node::Type::ConstTrueExpr)
                reorder(n->expr, ctx);
        if (n->type == ast_node::Type::BinOp)
        {
                const auto lhs = n->binop.lhs, rhs = n->binop.rhs;

                reorder(lhs, ctx);
                reorder(rhs, ctx);

                // First off, we will try to shift tokens to the left so that we will end up
                // with tokens before phrases, if that's possible, so that the compiler will get
                // a chance to identify long runs and reduce emitted logicaland_impl, logicalor_impl and logicalnot_impl ops
                if (rhs->is_unary() && rhs->p->size == 1 && lhs->type == ast_node::Type::BinOp && lhs->binop.normalized_operator() == n->binop.normalized_operator() && lhs->binop.rhs->is_unary() && lhs->binop.rhs->p->size > 1)
                {
                        // (ipad OR "apple ipad") OR ipod => (ipad OR ipod) OR "apple ipad"
                        if (trace)
                                SLog("rolling:", *rhs, ",", *lhs->binop.rhs, "\n");
                        std::swap(*rhs, *lhs->binop.rhs);
                        ctx->dirty = true;
                        return;
                }

                if (rhs->type == ast_node::Type::BinOp && lhs->is_unary() && lhs->p->size > 1 && rhs->binop.normalized_operator() == n->binop.normalized_operator() && rhs->binop.lhs->is_unary() && rhs->binop.lhs->p->size == 1)
                {
                        // "video game" AND (warcraft AND ..) => warcraft AND ("Video game" AND ..)
                        std::swap(*lhs, *rhs->binop.lhs);
                        ctx->dirty = true;
                        return;
                }

                if ((n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND || n->binop.op == Operator::OR) && lhs->type == ast_node::Type::Phrase && lhs->p->size > 1 && rhs->type == ast_node::Type::Token)
                {
                        // ["video game" OR game] => [game OR "video game"]
                        std::swap(n->binop.lhs, n->binop.rhs);
                        ctx->dirty = true;
                        return;
                }

                if (n->binop.op == Operator::OR)
                {
                        // ((1 OR <2>) OR 3) => 1 OR 3 OR <2>
                        if (lhs->type == ast_node::Type::BinOp && lhs->binop.op == Operator::OR && lhs->binop.rhs->type == ast_node::Type::ConstTrueExpr && rhs->type != ast_node::Type::ConstTrueExpr)
                        {
                                std::swap(*lhs->binop.rhs, *rhs);
                                ctx->dirty = true;
                                return;
                        }
                }

                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                {
                        if (lhs->type == ast_node::Type::BinOp)
                        {
                                if (rhs->is_unary())
                                {
                                        // [expr AND unary] => [unary AND expr]
                                        std::swap(n->binop.lhs, n->binop.rhs);
                                        ctx->dirty = true;
                                }
                        }
                }
                else if (n->binop.op == Operator::NOT)
                {
                        // (foo OR bar) NOT apple
                        // apple is cheaper to compute so we need to reverse those
                        if (rhs->is_unary() && lhs->type == ast_node::Type::BinOp)
                        {
                                auto llhs = lhs->binop.lhs;
                                auto lrhs = lhs->binop.rhs;

                                if (llhs->is_unary() && lrhs->type == ast_node::Type::BinOp && (lhs->binop.op == Operator::AND || lhs->binop.op == Operator::STRICT_AND))
                                {
                                        // ((pizza AND (sf OR "san francisco")) NOT onions)
                                        // => (pizza NOT onions) AND (sf OR "san francisco")
                                        const auto saved = lhs->binop.op;

                                        if (trace)
                                                SLog("here\n");

                                        lhs->binop.rhs = rhs;
                                        lhs->binop.op = Operator::NOT;

                                        n->binop.op = saved;
                                        n->binop.rhs = lrhs;

                                        ctx->dirty = true;
                                }
                        }
                }
        }
}

// See: IMPLEMENTATION.md
// this is very important, for we need to move tokens before any phrases
// so that we can get a chance to group tokens together, and phrases together
static ast_node *reorder_root(ast_node *r)
{
        reorder_ctx ctx;

        do
        {
                ctx.dirty = false;
                reorder(r, &ctx);
        } while (ctx.dirty);

        if (traceCompile)
                SLog("REORDERED:", *r, "\n");

        return r;
}

template <typename T>
static auto impl_repr(const T *func)
{
        const auto fp = (void *)func;

        if (fp == (void *)logicalor_impl || fp == (void *)logicalor_fordocs_impl)
                return "or"_s8;
        else if (fp == logicaland_impl)
                return "and"_s8;
        else if (fp == logicalnot_impl)
                return "not"_s8;
        else if (fp == matchterm_impl)
                return "term"_s8;
        else if (fp == matchphrase_impl)
                return "phrase"_s8;
        else if (fp == SPECIALIMPL_COLLECTION_LOGICALAND)
                return "(AND collection)"_s8;
        else if (fp == SPECIALIMPL_COLLECTION_LOGICALOR)
                return "(OR collection)"_s8;
        else if (fp == unaryand_impl)
                return "unary and"_s8;
        else if (fp == unarynot_impl)
                return "unary not"_s8;
        else if (fp == consttrueexpr_impl)
                return "const true expr"_s8;
        else if (fp == constfalse_impl)
                return "false"_s8;
        else if (fp == dummyop_impl)
                return "<dummy>"_s8;
        else if (fp == matchallterms_impl)
                return "(all terms)"_s8;
        else if (fp == matchanyterms_impl || fp == matchanyterms_fordocs_impl)
                return "(any terms)"_s8;
        else if (fp == matchallphrases_impl)
                return "(any phrases)"_s8;
        else if (fp == matchanyphrases_impl || fp == matchanyphrases_fordocs_impl)
                return "(any phrases)"_s8;
        else
                return "<other>"_s8;
}

struct execnodes_collection final
{
        uint16_t size;
        exec_node a, b;

        static execnodes_collection *make(simple_allocator &allocator, const exec_node a, const exec_node b)
        {
                auto ptr = allocator.Alloc<execnodes_collection>();

                ptr->a = a;
                ptr->b = b;
                return ptr;
        }
};

template <typename T, typename T2>
static execnodes_collection *try_collect_impl(const exec_node lhs, const exec_node rhs, simple_allocator &a, T fp, T2 fp2)
{
        if ((lhs.fp == matchterm_impl || lhs.fp == matchphrase_impl || lhs.fp == fp || lhs.fp == fp2) && (rhs.fp == matchterm_impl || rhs.fp == matchphrase_impl || rhs.fp == fp || rhs.fp == fp2))
        {
                // collect collection, will turn this into 0+ termruns and 0+ phraseruns
                return execnodes_collection::make(a, lhs, rhs);
        }
        else
                return nullptr;
}

template <typename T, typename T2>
static bool try_collect(exec_node &res, simple_allocator &a, T fp, T2 fp2)
{
        auto opctx = static_cast<runtime_ctx::binop_ctx *>(res.ptr);

        if (auto ptr = try_collect_impl(opctx->lhs, opctx->rhs, a, fp, fp2))
        {
                res.fp = reinterpret_cast<decltype(res.fp)>(fp);
                res.ptr = ptr;
                return true;
        }
        else
                return false;
}

// See: IMPLEMENTATION.md
static exec_node compile_node(const ast_node *const n, runtime_ctx &rctx, simple_allocator &a)
{
        exec_node res;

        require(n);
        switch (n->type)
        {
                case ast_node::Type::Dummy:
                        std::abort();

                case ast_node::Type::Token:
                        res.u16 = rctx.register_token(n->p);
                        if (res.u16)
                                res.fp = matchterm_impl;
                        else
                                res.fp = constfalse_impl;
                        break;

                case ast_node::Type::Phrase:
                        if (n->p->size == 1)
                        {
                                res.u16 = rctx.register_token(n->p);
                                if (res.u16)
                                        res.fp = matchterm_impl;
                                else
                                        res.fp = constfalse_impl;
                        }
                        else
                        {
                                res.ptr = rctx.register_phrase(n->p);
                                if (res.ptr)
                                        res.fp = matchphrase_impl;
                                else
                                        res.fp = constfalse_impl;
                        }
                        break;

                case ast_node::Type::BinOp:
                {
                        auto ctx = rctx.register_binop(compile_node(n->binop.lhs, rctx, a), compile_node(n->binop.rhs, rctx, a));

                        res.ptr = ctx;
                        switch (n->binop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        res.fp = logicaland_impl;
                                        break;

                                case Operator::OR:
                                        res.fp = logicalor_impl;
                                        break;

                                case Operator::NOT:
                                        res.fp = logicalnot_impl;
                                        break;

                                case Operator::NONE:
                                        std::abort();
                                        break;
                        }
                }
                break;

                case ast_node::Type::ConstFalse:
                        res.fp = constfalse_impl;
                        break;

                case ast_node::Type::UnaryOp:
                        switch (n->unaryop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        res.fp = unaryand_impl;
                                        break;

                                case Operator::NOT:
                                        res.fp = unarynot_impl;
                                        break;

                                default:
                                        std::abort();
                        }
                        res.ptr = rctx.register_unaryop(compile_node(n->unaryop.expr, rctx, a));
                        break;

                case ast_node::Type::ConstTrueExpr:
                        // use register_unaryop() for this as well
                        // no need for another register_x()
                        res.ptr = rctx.register_unaryop(compile_node(n->expr, rctx, a));
                        if (static_cast<runtime_ctx::unaryop_ctx *>(res.ptr)->expr.fp == constfalse_impl)
                                res.fp = dummyop_impl;
                        else
                                res.fp = consttrueexpr_impl;
                        break;
        }

        return res;
}

static bool same(const exec_node &a, const exec_node &b)
{
        if (a.fp == matchallterms_impl && b.fp == a.fp)
        {
                const auto *const __restrict__ runa = (runtime_ctx::termsrun *)a.ptr;
                const auto *const __restrict__ runb = (runtime_ctx::termsrun *)b.ptr;

                return *runa == *runb;
        }
        else if (a.fp == matchterm_impl && b.fp == a.fp)
                return a.u16 == b.u16;
        else if (a.fp == matchphrase_impl)
        {
                const auto *const __restrict__ pa = (runtime_ctx::phrase *)a.ptr;
                const auto *const __restrict__ pb = (runtime_ctx::phrase *)b.ptr;

                return *pa == *pb;
        }

        return false;
}

// See: IMPLEMENTATION.md
static void collapse_node(exec_node &n, runtime_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const runtime_ctx::phrase *> &phrases, std::vector<exec_node> &stack)
{
        if (n.fp == consttrueexpr_impl || n.fp == unaryand_impl || n.fp == unarynot_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                collapse_node(ctx->expr, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;
                runtime_ctx::binop_ctx *otherCtx;

                collapse_node(ctx->lhs, rctx, a, terms, phrases, stack);
                collapse_node(ctx->rhs, rctx, a, terms, phrases, stack);

                if (n.fp == logicaland_impl)
                {
                        if (try_collect(n, a, SPECIALIMPL_COLLECTION_LOGICALAND, matchallterms_impl))
                                return;

                        if ((ctx->lhs.fp == matchterm_impl || ctx->lhs.fp == matchphrase_impl || ctx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALAND) && ctx->rhs.fp == logicaland_impl && ((otherCtx = (runtime_ctx::binop_ctx *)ctx->rhs.ptr)->lhs.fp == matchterm_impl || otherCtx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALAND || otherCtx->lhs.fp == matchphrase_impl))
                        {
                                // lord AND (of AND (the AND rings))
                                // (lord of) AND (the AND rings)
                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                ctx->lhs.fp = (decltype(ctx->lhs.fp))SPECIALIMPL_COLLECTION_LOGICALAND;
                                ctx->lhs.ptr = collection;
                                ctx->rhs = otherCtx->rhs;
                                return;
                        }

                        if (ctx->lhs.fp == consttrueexpr_impl && ctx->rhs.fp == consttrueexpr_impl)
                        {
                                auto *const __restrict__ lhsCtx = (runtime_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (runtime_ctx::unaryop_ctx *)ctx->rhs.ptr;

                                if (auto ptr = try_collect_impl(lhsCtx->expr, rhsCtx->expr, a, SPECIALIMPL_COLLECTION_LOGICALAND, matchallterms_impl))
                                {
                                        // reuse lhsCtx
                                        lhsCtx->expr.fp = reinterpret_cast<decltype(lhsCtx->expr.fp)>(SPECIALIMPL_COLLECTION_LOGICALAND);
                                        lhsCtx->expr.ptr = ptr;

                                        n.ptr = lhsCtx;
                                        n.fp = consttrueexpr_impl;
                                        return;
                                }
                        }
                }
                else if (n.fp == logicalor_impl)
                {
                        if (try_collect(n, a, SPECIALIMPL_COLLECTION_LOGICALOR, matchanyterms_impl))
                                return;

                        if ((ctx->lhs.fp == matchterm_impl || ctx->lhs.fp == matchphrase_impl || ctx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALOR) && ctx->rhs.fp == logicalor_impl && ((otherCtx = (runtime_ctx::binop_ctx *)ctx->rhs.ptr)->lhs.fp == matchterm_impl || otherCtx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALOR || otherCtx->lhs.fp == matchphrase_impl))
                        {
                                // lord OR (ring OR (rings OR towers))
                                // (lord OR ring) OR (rings OR towers)
                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                ctx->lhs.fp = (decltype(ctx->lhs.fp))SPECIALIMPL_COLLECTION_LOGICALOR;
                                ctx->lhs.ptr = collection;
                                ctx->rhs = otherCtx->rhs;
                                return;
                        }

                        if (ctx->lhs.fp == consttrueexpr_impl && ctx->rhs.fp == consttrueexpr_impl)
                        {
                                auto *const __restrict__ lhsCtx = (runtime_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (runtime_ctx::unaryop_ctx *)ctx->rhs.ptr;

                                if (auto ptr = try_collect_impl(lhsCtx->expr, rhsCtx->expr, a, SPECIALIMPL_COLLECTION_LOGICALOR, matchanyterms_impl))
                                {
                                        // reuse lhsCtx
                                        lhsCtx->expr.fp = reinterpret_cast<decltype(lhsCtx->expr.fp)>(SPECIALIMPL_COLLECTION_LOGICALOR);
                                        lhsCtx->expr.ptr = ptr;

                                        n.ptr = lhsCtx;
                                        n.fp = consttrueexpr_impl;
                                        return;
                                }
                        }
                }
        }
}

static void trim_phrasesrun(exec_node &n, runtime_ctx::phrasesrun *pr)
{
        auto out = pr->phrases;
        const auto size = pr->size;
        uint32_t k;

        for (uint32_t i{0}; i != size; ++i)
        {
                auto p = pr->phrases[i];

                for (k = i + 1; k != size && !(*p == *pr->phrases[k]); ++k)
                        continue;

                if (k == size)
                        *out++ = p;
        }

        pr->size = out - pr->phrases;

        if (pr->size == 1)
        {
                n.fp = matchphrase_impl;
                n.ptr = pr->phrases[0];
        }
}

static void expand_node(exec_node &n, runtime_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const runtime_ctx::phrase *> &phrases, std::vector<exec_node> &stack)
{
        if (n.fp == consttrueexpr_impl || n.fp == unaryand_impl || n.fp == unarynot_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                expand_node(ctx->expr, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                expand_node(ctx->lhs, rctx, a, terms, phrases, stack);
                expand_node(ctx->rhs, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR || n.fp == SPECIALIMPL_COLLECTION_LOGICALAND)
        {
                auto ctx = (execnodes_collection *)n.ptr;

                terms.clear();
                phrases.clear();
                stack.clear();
                stack.push_back(ctx->a);
                stack.push_back(ctx->b);

                do
                {
                        auto en = stack.back();

                        stack.pop_back();

                        if (en.fp == matchterm_impl)
                        {
                                const auto termID = exec_term_id_t(en.u16);

                                terms.push_back(termID);
                        }
                        else if (en.fp == matchphrase_impl)
                        {
                                const auto p = (runtime_ctx::phrase *)en.ptr;

                                phrases.push_back(p);
                        }
                        else if (en.fp == matchallterms_impl || en.fp == matchanyterms_impl)
                        {
                                const auto run = static_cast<const runtime_ctx::termsrun *>(en.ptr);

                                terms.insert(terms.end(), run->terms, run->terms + run->size);
                        }
                        else if (en.fp == SPECIALIMPL_COLLECTION_LOGICALOR || en.fp == SPECIALIMPL_COLLECTION_LOGICALAND)
                        {
                                auto ctx = (execnodes_collection *)en.ptr;

                                stack.push_back(ctx->a);
                                stack.push_back(ctx->b);
                        }
                } while (stack.size());

                std::sort(terms.begin(), terms.end());
                terms.resize(std::unique(terms.begin(), terms.end()) - terms.begin());

                if (traceCompile)
                        SLog("Expanded terms = ", terms.size(), ", phrases = ", phrases.size(), " ", n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? "OR" : "AND", "\n");

                // We have expanded this collection into tokens and phrases
                // Depending on the number of tokens and phrases we collected we will
                // create:
                // - (matchanyterms_impl/matchallterms_impl OP phrase)
                // - (matchanyterms_impl/matchallterms_impl)
                // - (matchanyterms_impl/matchallterms_impl OP (matchanyphrases_impl/matchallphrases_impl))
                // - (token OP (matchanyphrases_impl/matchallphrases_impl))
                // - (matchanyphrases_impl/matchallphrases_impl)
                if (const auto termsCnt = terms.size(), phrasesCnt = phrases.size(); termsCnt == 1)
                {
                        exec_node termNode;

                        termNode.fp = matchterm_impl;
                        termNode.u16 = terms.front();

                        if (phrasesCnt == 0)
                                n = termNode;
                        else if (phrasesCnt == 1)
                        {
                                exec_node phraseMatchNode;

                                phraseMatchNode.fp = matchphrase_impl;
                                phraseMatchNode.ptr = (void *)phrases.front();

                                n.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? logicalor_impl : logicaland_impl);
                                n.ptr = rctx.register_binop(termNode, phraseMatchNode);
                        }
                        else
                        {
                                exec_node phrasesRunNode;
                                auto phrasesRun = (runtime_ctx::phrasesrun *)rctx.allocator.Alloc(sizeof(runtime_ctx::phrasesrun) + phrasesCnt * sizeof(runtime_ctx::phrase *));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(runtime_ctx::phrase *));

                                phrasesRunNode.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? matchanyphrases_impl : matchallphrases_impl);
                                phrasesRunNode.ptr = (void *)phrasesRun;

                                n.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? logicalor_impl : logicaland_impl);
                                n.ptr = rctx.register_binop(termNode, phrasesRunNode);

                                trim_phrasesrun(n, phrasesRun);
                        }
                }
                else if (termsCnt > 1)
                {
                        auto run = (runtime_ctx::termsrun *)rctx.runsAllocator.Alloc(sizeof(runtime_ctx::termsrun) + sizeof(exec_term_id_t) * termsCnt);
                        exec_node runNode;

                        run->size = termsCnt;
                        memcpy(run->terms, terms.data(), termsCnt * sizeof(exec_term_id_t));
                        runNode.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? matchanyterms_impl : matchallterms_impl);
                        runNode.ptr = run;

                        // see same()
                        // we will eventually sort those terms by evaluation cost, but for now
                        // it is important to sort them by id so that we can easily check for termruns eq.
                        std::sort(run->terms, run->terms + run->size);

                        if (phrasesCnt == 0)
                        {
                                n = runNode;
                        }
                        else if (phrasesCnt == 1)
                        {
                                exec_node phraseNode;

                                phraseNode.fp = matchphrase_impl;
                                phraseNode.ptr = (void *)(phrases.front());

                                n.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? logicalor_impl : logicaland_impl);
                                n.ptr = rctx.register_binop(runNode, phraseNode);
                        }
                        else
                        {
                                exec_node phrasesRunNode;
                                auto phrasesRun = (runtime_ctx::phrasesrun *)rctx.allocator.Alloc(sizeof(runtime_ctx::phrasesrun) + phrasesCnt * sizeof(runtime_ctx::phrase *));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(runtime_ctx::phrase *));

                                phrasesRunNode.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? matchanyphrases_impl : matchallphrases_impl);
                                phrasesRunNode.ptr = (void *)phrasesRun;

                                n.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? logicalor_impl : logicaland_impl);
                                n.ptr = rctx.register_binop(runNode, phrasesRunNode);

                                trim_phrasesrun(n, phrasesRun);
                        }
                }
                else if (termsCnt == 0)
                {
                        if (phrasesCnt == 1)
                        {
                                n.fp = matchphrase_impl;
                                n.ptr = (void *)phrases.front();
                        }
                        else
                        {
                                auto phrasesRun = (runtime_ctx::phrasesrun *)rctx.allocator.Alloc(sizeof(runtime_ctx::phrasesrun) + phrasesCnt * sizeof(runtime_ctx::phrase *));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(runtime_ctx::phrase *));

                                n.fp = (n.fp == SPECIALIMPL_COLLECTION_LOGICALOR ? matchanyphrases_impl : matchallphrases_impl);
                                n.ptr = (void *)phrasesRun;

                                trim_phrasesrun(n, phrasesRun);
                        }
                }

#if 0
		// if we set_dirty() here
		// fails for:
		// ./T ' sf OR "san francisco" OR san-francisco pizza OR pizzaria OR tavern OR inn OR mcdonalds '
		// but if we don't, it fails to optimize:
		// ./T ' "world of warcraft" "mists of pandaria"    pandaria '
#endif
        }
        else if (n.fp == matchallterms_impl || n.fp == matchanyterms_impl)
        {
                const auto run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

                if (run->size == 1)
                {
                        n.fp = matchterm_impl;
                        n.u16 = run->terms[0];
                }
        }
        else if (n.fp == matchphrase_impl)
        {
                const auto p = (runtime_ctx::phrase *)n.ptr;

                if (p->size == 1)
                {
                        n.fp = matchterm_impl;
                        n.u16 = p->termIDs[0];
                }
        }
}

static exec_node optimize_node(exec_node n, runtime_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const runtime_ctx::phrase *> &phrases, std::vector<exec_node> &stack, bool &updates, const exec_node *const root)
{
	const auto saved{n};

#define set_dirty()                     \
        do                              \
        {                               \
                if (traceCompile)       \
                        SLog("HERE from [", saved, "] to [", n, "]\n"); \
                updates = true;         \
        } while (0)

        if (n.fp == consttrueexpr_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = optimize_node(ctx->expr, rctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == constfalse_impl || ctx->expr.fp == dummyop_impl)
                {
                        n.fp = dummyop_impl;
                        set_dirty();
                }
        }
        if (n.fp == unaryand_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = optimize_node(ctx->expr, rctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == constfalse_impl)
                {
                        n.fp = constfalse_impl;
                        set_dirty();
                }
                else if (ctx->expr.fp == dummyop_impl)
                {
                        n.fp = dummyop_impl;
                        set_dirty();
                }
        }
        else if (n.fp == unarynot_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = optimize_node(ctx->expr, rctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == dummyop_impl)
                {
                        n.fp = dummyop_impl;
                        set_dirty();
                }
        }
        else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;
                runtime_ctx::binop_ctx *otherCtx;

                ctx->lhs = optimize_node(ctx->lhs, rctx, a, terms, phrases, stack, updates, root);
                ctx->rhs = optimize_node(ctx->rhs, rctx, a, terms, phrases, stack, updates, root);

                if (ctx->lhs.fp == dummyop_impl && ctx->rhs.fp == dummyop_impl)
                {
                        n.fp = dummyop_impl;
                        set_dirty();
                        return n;
                }
                else if (ctx->rhs.fp == dummyop_impl)
                {
                        n = ctx->lhs;
                        set_dirty();
                        return n;
                }
                else if (ctx->lhs.fp == dummyop_impl)
                {
                        n = ctx->rhs;
                        set_dirty();
                        return n;
                }

                if (n.fp == logicalor_impl)
                {
                        if (ctx->lhs.fp == constfalse_impl)
                        {
                                if (ctx->rhs.fp == constfalse_impl)
                                {
                                        n.fp = constfalse_impl;
                                        set_dirty();
                                }
                                else
                                {
                                        n = ctx->rhs;
                                        set_dirty();
                                }
                                return n;
                        }
                        else if (ctx->rhs.fp == constfalse_impl)
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (same(ctx->lhs, ctx->rhs))
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == constfalse_impl && ctx->rhs.fp == constfalse_impl)
                        {
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == constfalse_impl)
                        {
                                n = ctx->rhs;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->rhs.fp == constfalse_impl)
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                }
                else if (n.fp == logicaland_impl)
                {
                        if (ctx->lhs.fp == constfalse_impl || ctx->rhs.fp == constfalse_impl)
                        {
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }
                        else if (same(ctx->lhs, ctx->rhs))
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == logicalnot_impl && same(static_cast<runtime_ctx::binop_ctx *>(ctx->lhs.ptr)->rhs, ctx->rhs))
                        {
                                // ((1 NOT 2) AND 2)
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == matchterm_impl && ctx->rhs.fp == matchanyterms_impl)
                        {
                                // (1 AND ANY OF[1,4]) => [1 AND ANY OF [4]]
                                auto run = (runtime_ctx::termsrun *)ctx->rhs.ptr;

                                if (run->erase(ctx->lhs.u16))
                                {
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == constfalse_impl || ctx->rhs.fp == constfalse_impl)
                        {
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == matchallterms_impl && ctx->rhs.fp == matchanyterms_impl)
                        {
                                // (ALL OF[2,1] AND ANY OF[2,1])
                                auto runa = (runtime_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (runtime_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == matchanyterms_impl && ctx->rhs.fp == matchallterms_impl)
                        {
                                // (ANY OF[2,1] AND ALL OF[2,1])
                                auto runa = (runtime_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (runtime_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        // ALL OF[3,2,1] AND <ANY OF[5,4,1]>
                        // ALL OF [3, 2, 1] AND ANY_OF<5,4>
                        if (ctx->lhs.fp == matchallterms_impl && ctx->rhs.fp == matchanyterms_impl)
                        {
                                auto runa = (runtime_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (runtime_ctx::termsrun *)ctx->rhs.ptr;

                                if (runb->erase(*runa))
                                {
                                        if (runb->empty())
                                                ctx->rhs.fp = dummyop_impl;

                                        set_dirty();
                                        return n;
                                }
                        }

                        if (same(ctx->lhs, ctx->rhs))
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == matchanyterms_impl && ctx->rhs.fp == matchanyterms_impl)
                        {
                                // (ANY OF[2,1] AND ANY OF[2,1])
                                auto runa = (runtime_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (runtime_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == matchterm_impl && ctx->rhs.fp == matchallphrases_impl)
                        {
                                // (1 AND ALLPHRASES:[[4,3,5][2,3,1]])
                                const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)ctx->rhs.ptr;
                                const auto termID = ctx->lhs.u16;

                                for (uint32_t i{0}; i != run->size; ++i)
                                {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(termID))
                                        {
                                                n = ctx->rhs;
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }
                }
                else if (n.fp == logicalnot_impl)
                {
                        if (ctx->lhs.fp == constfalse_impl)
                        {
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->rhs.fp == constfalse_impl)
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (same(ctx->lhs, ctx->rhs))
                        {
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }

                        if ((ctx->lhs.fp == matchallterms_impl || ctx->lhs.fp == matchanyterms_impl) && ctx->rhs.fp == matchterm_impl)
                        {
                                // ALL OF[1,5] NOT 5) => ALL OF [1] NOT 5
                                auto run = (runtime_ctx::termsrun *)ctx->lhs.ptr;

                                if (run->erase(ctx->rhs.u16))
                                {
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == logicalnot_impl && same((otherCtx = static_cast<runtime_ctx::binop_ctx *>(ctx->lhs.ptr))->lhs, ctx->rhs))
                        {
                                // ((2 NOT 1) NOT 2)
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }

                        if (same(ctx->lhs, ctx->rhs))
                        {
                                n.fp = constfalse_impl;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == matchanyterms_impl && ctx->rhs.fp == matchanyterms_impl)
                        {
                                // (ANY OF[2,1] NOT ANY OF[2,1])
                                auto runa = (runtime_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (runtime_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n.fp = constfalse_impl;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == matchphrase_impl && ctx->rhs.fp == matchterm_impl)
                        {
                                // (([1,2,3] NOT 3) AND ALLPHRASES:[[4,2,5][1,2,3]])
                                const auto p = (runtime_ctx::phrase *)ctx->lhs.ptr;

                                if (p->is_set(ctx->rhs.u16))
                                {
                                        n.fp = constfalse_impl;
                                        set_dirty();
                                        return n;
                                }
                        }
                }
        }
        else if (n.fp == matchanyterms_impl || n.fp == matchallterms_impl)
        {
                auto run = (runtime_ctx::termsrun *)n.ptr;

                if (run->size == 1)
                {
                        n.fp = matchterm_impl;
                        n.u16 = run->terms[0];
                        set_dirty();
                }
                else if (run->empty())
                {
                        n.fp = dummyop_impl;
                        set_dirty();
                }
        }

        return n;
}

static void PrintImpl(Buffer &b, const exec_node &n)
{
        if (n.fp == unaryand_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                b.append("<AND>", ctx->expr);
        }
        else if (n.fp == unarynot_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                b.append("<NOT>", ctx->expr);
        }
        else if (n.fp == consttrueexpr_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                b.append("<", ctx->expr, ">");
        }
        else if (n.fp == logicaland_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                b.append("(", ctx->lhs, " AND ", ctx->rhs, ")");
        }
        else if (n.fp == logicalor_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                b.append("(", ctx->lhs, " OR ", ctx->rhs, ")");
        }
        else if (n.fp == logicalnot_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                b.append("(", ctx->lhs, " NOT ", ctx->rhs, ")");
        }
        else if (n.fp == matchterm_impl)
                b.append(n.u16);
        else if (n.fp == matchphrase_impl)
        {
                const auto p = (runtime_ctx::phrase *)n.ptr;

                b.append('[');
                for (uint32_t i{0}; i != p->size; ++i)
                        b.append(p->termIDs[i], ',');
                b.shrink_by(1);
                b.append(']');
        }
        else if (n.fp == constfalse_impl)
                b.append(false);
        else if (n.fp == dummyop_impl)
                b.append(true);
        else if (n.fp == matchanyterms_impl)
        {
                const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

                b.append("ANY OF[");
                for (uint32_t i{0}; i != run->size; ++i)
                        b.append(run->terms[i], ',');
                b.shrink_by(1);
                b.append("]");
        }
        else if (n.fp == matchallterms_impl)
        {
                const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

                b.append("ALL OF[");
                for (uint32_t i{0}; i != run->size; ++i)
                        b.append(run->terms[i], ',');
                b.shrink_by(1);
                b.append("]");
        }
        else if (n.fp == matchanyphrases_impl)
        {
                b.append("(any phrases)");
        }
        else if (n.fp == matchallphrases_impl)
        {
                const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)n.ptr;

                b.append("ALLPHRASES:[");
                for (uint32_t k{0}; k != run->size; ++k)
                {
                        const auto p = run->phrases[k];

                        b.append('[');
                        for (uint32_t i{0}; i != p->size; ++i)
                                b.append(p->termIDs[i], ',');
                        b.shrink_by(1);
                        b.append(']');
                }
                b.append(']');
        }
        else
        {
                b.append("Missing for ", impl_repr(n.fp));
        }
}

// We make sure to never move an ast_node::Type::ConstTrueExpr to a binop's lhs from its rhs
// in reorder_execnode(), and we also special-case for consttrueexpr_impl
// because getting the leader nodes from queries that include ast_node::Type::ConstTrueExpr is now trivial
//
// We only want to include a consttrueexpr_impl tokens if its parent is a logicalor_impl or if it is a logicaland_impl
// and we have no different impl on either of (lhs, rhs)
//
// See ast_node::Type::ConstTrueExpr def.
static void capture_leader(const exec_node n, std::vector<exec_node> *const out, const size_t threshold)
{
        if (n.fp == matchterm_impl || n.fp == matchphrase_impl)
                out->push_back(n);
        else if (n.fp == matchallterms_impl || n.fp == matchallphrases_impl)
                out->push_back(n);
        else if (n.fp == matchanyterms_impl || n.fp == matchanyphrases_impl || n.fp == matchanyterms_fordocs_impl || n.fp == matchanyphrases_fordocs_impl)
                out->push_back(n);
        else if (n.fp == unaryand_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                capture_leader(ctx->expr, out, threshold);
        }
        else if (n.fp == logicalor_impl || n.fp == logicalor_fordocs_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                capture_leader(ctx->rhs, out, threshold);
                capture_leader(ctx->lhs, out, threshold + 1);
        }
        else if (n.fp == logicaland_impl)
        {
                if (out->size() < threshold)
                {
                        auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                        if (ctx->lhs.fp != consttrueexpr_impl)
                        {
                                // we need to special case those
                                capture_leader(ctx->lhs, out, threshold);
                        }
                        else
                                capture_leader(ctx->rhs, out, threshold);
                }
        }
        else if (n.fp == logicalnot_impl)
        {
                if (out->size() < threshold)
                {
                        auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                        capture_leader(ctx->lhs, out, threshold);
                }
        }
        else if (n.fp == consttrueexpr_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                if (out->size() < threshold)
                        capture_leader(ctx->expr, out, threshold);
        }
}

static exec_node compile(const ast_node *const n, runtime_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> *leaderTermIDs, const uint32_t execFlags)
{
        static constexpr bool traceMetrics{false};
        std::vector<exec_term_id_t> terms;
        std::vector<const runtime_ctx::phrase *> phrases;
        std::vector<exec_node> stack;
        std::vector<exec_node *> stackP;
        std::vector<std::pair<exec_term_id_t, uint32_t>> v;
        uint64_t before;
        const bool documentsOnly = execFlags & uint32_t(ExecFlags::DocumentsOnly);

        // First pass
        // Compile from AST tree to exec_nodes tree
        before = Timings::Microseconds::Tick();

        auto root = compile_node(n, rctx, a);
        bool updates;

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to compile to:", root, "\n");

        if (root.fp == constfalse_impl)
        {
                if (traceCompile)
                        SLog("Nothing to do, compile_node() compiled away the expr.\n");

                return {constfalse_impl, {}};
        }

	if (traceCompile)
		SLog("Before second pass:", root, "\n");

        // Second pass
        // Optimize and expand
        before = Timings::Microseconds::Tick();
        do
        {
                // collapse and expand nodes
                // this was pulled out of optimize_node() in order to safeguard us from some edge conditions
                collapse_node(root, rctx, a, terms, phrases, stack);
                expand_node(root, rctx, a, terms, phrases, stack);


                updates = false;
                root = optimize_node(root, rctx, a, terms, phrases, stack, updates, &root);

                if (root.fp == constfalse_impl || root.fp == dummyop_impl)
                {
                        if (traceCompile)
                                SLog("Nothing to do (false OR noop)\n");

                        return {constfalse_impl, {}};
                }
        } while (updates);

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to expand. Before third pass:", root, "\n");


        // Third pass
        // For matchallterms_impl and matchanyterms_impl, sort the term IDs by number of documents they match
        //
        // Also, if (documentsOnly), we will replace some ops with others.
        // For example, logicalor_impl will be replaced with logicalor_fordocs_impl, which will return
        // if either LHS or RHS match -- it won't evaluate both if LHS matches.
        // From here onwards, you should be prepared to deal with (logicalor_fordocs_impl, matchanyterms_fordocs_impl, matchanyphrases_fordocs_impl) as well
        size_t totalNodes{0};

        before = Timings::Microseconds::Tick();
        stackP.clear();
        stackP.push_back(&root);

        do
        {
                auto ptr = stackP.back();
                auto n = *ptr;

                stackP.pop_back();
                require(n.fp != constfalse_impl);
                require(n.fp != dummyop_impl);

                ++totalNodes;
                if (n.fp == matchallterms_impl)
                {
                        auto ctx = static_cast<runtime_ctx::termsrun *>(n.ptr);

                        v.clear();
                        for (uint32_t i{0}; i != ctx->size; ++i)
                        {
                                const auto termID = ctx->terms[i];

                                if (traceCompile)
                                        SLog("AND ", termID, " ", rctx.term_ctx(termID).documents, "\n");
                                v.push_back({termID, rctx.term_ctx(termID).documents});
                        }

                        std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second < b.second; });

                        for (uint32_t i{0}; i != ctx->size; ++i)
                                ctx->terms[i] = v[i].first;
                }
                else if (n.fp == matchanyterms_impl)
                {
                        // There are no real benefits to sorting terms for matchanyterms_impl but we 'll do it anyway because its cheap
			// This is actually useful, for leaders
                        auto ctx = static_cast<runtime_ctx::termsrun *>(n.ptr);

                        v.clear();
                        for (uint32_t i{0}; i != ctx->size; ++i)
                        {
                                const auto termID = ctx->terms[i];

                                if (traceCompile)
                                        SLog("OR ", termID, " ", rctx.term_ctx(termID).documents, "\n");
                                v.push_back({termID, rctx.term_ctx(termID).documents});
                        }

                        std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second < b.second; });

                        for (uint32_t i{0}; i != ctx->size; ++i)
                                ctx->terms[i] = v[i].first;

                        if (documentsOnly)
                                ptr->fp = matchanyterms_fordocs_impl;
                }
                else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
                {
                        auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                        stackP.push_back(&ctx->lhs);
                        stackP.push_back(&ctx->rhs);

                        if (documentsOnly && n.fp == logicalor_impl)
                                ptr->fp = logicalor_fordocs_impl;
                }
                else if (n.fp == unaryand_impl || n.fp == unarynot_impl || n.fp == consttrueexpr_impl)
                {
                        auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                        stackP.push_back(&ctx->expr);
                }
                else if (documentsOnly && n.fp == matchanyphrases_impl)
                        ptr->fp = matchanyphrases_fordocs_impl;
        } while (stackP.size());

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to sort runs, ", dotnotation_repr(totalNodes), " exec_nodes\n");

        // Fourth Pass
        // Reorder logicaland_impl nodes (lhs, rhs) so that the least expensive to evaluate is always found in the lhs branch
        // This also helps with moving tokens before phrases
        before = Timings::Microseconds::Tick();
        root = reorder_execnodes(root, rctx);

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to reorder exec nodes\n");

        if (traceCompile)
                SLog("COMPILED:", root, "\n");

        // We now need the leader nodes so that we can get the leader term IDs from them.
        // See Trinity::query::leader_nodes()
        // we will op. directly on the exec nodes though here
        std::vector<exec_node> leaderNodes;

        before = Timings::Microseconds::Tick();
        capture_leader(root, &leaderNodes, 1);

        if (traceCompile)
        {
                SLog("LEADERS:", leaderNodes.size(), "\n");

                for (const auto it : leaderNodes)
                        SLog("LEADER:", it, "\n");
        }

        for (const auto &n : leaderNodes)
        {
                if (n.fp == matchterm_impl)
                        leaderTermIDs->push_back(n.u16);
                else if (n.fp == matchphrase_impl)
                {
                        const auto p = static_cast<const runtime_ctx::phrase *>(n.ptr);

                        leaderTermIDs->push_back(p->termIDs[0]);
                }
                else if (n.fp == matchanyterms_impl || n.fp == matchanyterms_fordocs_impl)
                {
                        const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

                        leaderTermIDs->insert(leaderTermIDs->end(), run->terms, run->terms + run->size);
                }
                else if (n.fp == matchallterms_impl)
                {
                        const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

                        leaderTermIDs->push_back(run->terms[0]);
                }
                else if (n.fp == matchanyphrases_impl || n.fp == matchanyphrases_fordocs_impl)
                {
                        const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)n.ptr;

                        for (uint32_t i{0}; i != run->size; ++i)
                                leaderTermIDs->push_back(run->phrases[i]->termIDs[0]);
                }
                else if (n.fp == matchallphrases_impl)
                {
                        const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)n.ptr;
                        exec_term_id_t selectedTerm{0};
                        uint32_t lowest;

                        for (uint32_t i{0}; i != run->size; ++i)
                        {
                                for (uint32_t k{0}; k != run->phrases[i]->size; ++k)
                                {
                                        const auto id = run->phrases[i]->termIDs[k];
                                        const auto cost = rctx.term_ctx(id).documents;

                                        if (!selectedTerm || cost < lowest)
                                        {
                                                lowest = cost;
                                                selectedTerm = id;
                                        }
                                }
                        }

                        leaderTermIDs->push_back(selectedTerm);
                }
                // for [ +the iphone]
                else if (n.fp == unaryand_impl)
                {
                        auto e = (runtime_ctx::unaryop_ctx *)n.ptr;

                        leaderNodes.push_back(e->expr);
                }
                else if (n.fp == logicaland_impl)
                {
                        auto e = (runtime_ctx::binop_ctx *)n.ptr;

                        leaderNodes.push_back(e->lhs);
                }
                else
                        std::abort();
        }

	// https://github.com/phaistos-networks/Trinity/issues/2
	// Now that we have compiled the query into an execution tree, and we have collected the leader tokens
	// we have a chance for a few more optimisations that had to be deferred until we were done with the above.
	// TODO: improve quality of this implementation
        {
                static constexpr bool trace{false};
                std::vector<exec_node *> stack;
                std::vector<exec_node *> matchalltermsNodes;
                std::vector<exec_node *> same;

                stack.push_back(&root);
                do
                {
                        auto *const ptr = stack.back();
                        const auto n = *ptr;

                        stack.pop_back();
                        if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalor_fordocs_impl || n.fp == logicalnot_impl)
                        {
                                auto *const ctx = static_cast<runtime_ctx::binop_ctx *>(n.ptr);

                                stack.push_back(&ctx->lhs);
                                stack.push_back(&ctx->rhs);
                        }
                        else if (n.fp == unaryand_impl || n.fp == unarynot_impl || n.fp == consttrueexpr_impl)
                        {
                                auto *const ctx = static_cast<runtime_ctx::unaryop_ctx *>(n.ptr);

                                stack.push_back(&ctx->expr);
                        }
                        else if (n.fp == matchallterms_impl)
                                matchalltermsNodes.push_back(ptr);
                } while (stack.size());

                std::sort(matchalltermsNodes.begin(), matchalltermsNodes.end(), [](const auto a, const auto b) {
                        const auto ra = static_cast<const runtime_ctx::termsrun *>(a->ptr);
                        const auto rb = static_cast<const runtime_ctx::termsrun *>(b->ptr);

                        return ra->size < rb->size;
                });

                for (uint32_t i{0}; i != matchalltermsNodes.size();)
                {
                        auto n = matchalltermsNodes[i];
                        auto ra = static_cast<const runtime_ctx::termsrun *>(n->ptr);
                        const auto cnt = ra->size; // for all term runs of this size
                        const auto base{i};

                        do
                        {
                                const auto n = matchalltermsNodes[i];

                                if (!n)
                                {
                                        // collected earlier
                                        continue;
                                }

                                const auto ra = static_cast<const runtime_ctx::termsrun *>(n->ptr);

                                same.clear();
                                same.push_back(n);

                                for (uint32_t k{base}; k != matchalltermsNodes.size(); ++k)
                                {
                                        if (k == i)
                                                continue;

                                        auto o = matchalltermsNodes[k];

                                        if (!o)
                                        {
                                                // collected earlier
                                                continue;
                                        }

                                        const auto r = static_cast<runtime_ctx::termsrun *>(o->ptr);

                                        if (r->size != cnt)
                                                break;
                                        else if (*r == *ra)
                                        {
                                                same.push_back(o);
                                                matchalltermsNodes[k] = nullptr;
                                        }
                                }

                                if (trace)
                                        SLog("same.size = ", same.size(), "\n");

                                if (same.size() != 1)
                                {
                                        auto ctr = rctx.allocator.Alloc<runtime_ctx::cacheable_termsrun>();

                                        // need to do something about this
                                        if (trace)
                                        {
                                                Print(*same.front(), "\n");
                                                for (uint32_t i{1}; i != same.size(); ++i)
                                                        require(*static_cast<runtime_ctx::termsrun *>(same[i - 1]->ptr) == *static_cast<runtime_ctx::termsrun *>(same[i]->ptr));
                                        }

                                        matchalltermsNodes[i] = nullptr;

                                        ctr->lastConsideredDID = 0;
                                        ctr->run = static_cast<runtime_ctx::termsrun *>(same.front()->ptr);
                                        ctr->res = false;

                                        // replace all those nodes
                                        for (auto it : same)
                                        {
                                                it->fp = matchallterms_cacheable_impl;
                                                it->ptr = ctr;
                                        }
                                }

                        } while (++i != matchalltermsNodes.size() && ((n = matchalltermsNodes[i]) == nullptr || static_cast<const runtime_ctx::termsrun *>(n->ptr)->size == cnt));
                }
        }

	// JIT:
	// Now that are done building the execution plan (a tree of exec_nodes), it should be fairly simple to
	// perform JIT and compile it down to x86-64 code. 
	// Please see: https://github.com/phaistos-networks/Trinity/wiki/JIT-compilation


        std::sort(leaderTermIDs->begin(), leaderTermIDs->end());
        leaderTermIDs->resize(std::unique(leaderTermIDs->begin(), leaderTermIDs->end()) - leaderTermIDs->begin());

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to capture leaders\n");

        if (traceCompile)
        {
                for (const auto id : *leaderTermIDs)
                        Print("LEADER TERM ID:", id, "\n");
        }

        // NOW, prepare decoders
        // No need to have done so if we could have determined that the query would have failed anyway
        // This could take some time - for 52 distinct terms it takes 0.002s (>1ms)
        //
        // TODO: well, no need to prepare decoders for every distinct term we resolved
        // because 1+ of them may not be referenced in the final execution nodes tree (because may have been dropped by the optimizer, etc)
        // Instead, just identify the distinct termIds from all execution nodes of type (matchterm_impl, matchphrase_impl, matchallphrases_impl, matchanyphrases_impl, matchanyterms_impl, matchallterms_impl)
        before = Timings::Microseconds::Tick();
        for (const auto &kv : rctx.tctxMap)
        {
#ifdef LEAN_SWITCH
                const auto termID = kv.first;
#else
                const auto termID = kv.key();
#endif

                rctx.prepare_decoder(termID);
        }

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " ", Timings::Microseconds::ToMillis(Timings::Microseconds::Since(before)), " ms  to initialize all decoders ", rctx.tctxMap.size(), "\n");

        return root;
}

static exec_node compile_query(ast_node *root, runtime_ctx &rctx, std::vector<exec_term_id_t> *leaderTermIDs, const uint32_t execFlags)
{
        simple_allocator a(4096);

        if (!root)
        {
                if (traceCompile)
                        SLog("No root node\n");

                return {constfalse_impl, {}};
        }

        // Reorder the tree to enable the compiler to create larger collections
        // root is from a clone of the original query, so we are free to modify it anyway we see fit with reorder_root()
        return compile(reorder_root(root), rctx, a, leaderTermIDs, execFlags);
}

#pragma EXECUTION
// If we have multiple segments, we should invoke exec() for each of them
// in parallel or in sequence, collect the top X hits and then later merge them
//
// We will need to create a copy of the `q` after we have normalized it, and then
// we need to reorder and optimize that copy, get leaders and execute it -- for each segment, but
// this is a very fast operation anyway
//
// We can't reuse the same compiled bytecode/runtime_ctx to run the same query across multiple index sources, because
// we optimize based on the index source structure and terms involved in the query.
// It is also very cheap to construct those anyway.
void Trinity::exec_query(const query &in, IndexSource *const __restrict__ idxsrc, masked_documents_registry *const __restrict__ maskedDocumentsRegistry, MatchedIndexDocumentsFilter *__restrict__ const matchesFilter, IndexDocumentsFilter *__restrict__ const documentsFilter, const uint32_t execFlags)
{
        struct query_term_instance final
        {
                str8_t token;
                // see Trinity::phrase
                uint16_t index;
                uint8_t rep;
                uint8_t flags;
                uint8_t toNextSpan;
        };

        if (!in)
        {
                if (traceCompile)
                        SLog("No root node\n");
                return;
        }

        // We need a copy of that query here
        // for we we will need to modify it
	const auto _start = Timings::Microseconds::Tick();
        query q(in);

        // Normalize just in case
        if (!q.normalize())
        {
                if (traceCompile)
                        SLog("No root node after normalization\n");

                return;
        }

        // We need to collect all term instances in the query
        // so that we the score function will be able to take that into account (See matched_document::queryTermInstances)
        // We only need to do this for specific AST branches and node types(i.e we ignore all RHS expressions of logical NOT nodes)
        //
        // This must be performed before any query optimizations, for otherwise because the optimiser will most definitely rearrange the query, doing it after
        // the optimization passes will not capture the original, input query tokens instances information.
        std::vector<query_term_instance> originalQueryTokenInstances;
        const bool documentsOnly = execFlags & uint32_t(ExecFlags::DocumentsOnly);

        {
                std::vector<ast_node *> stack{q.root}; // use a stack because we don't care about the evaluation order
                std::vector<phrase *> collected;

                do
                {
                        auto n = stack.back();

                        stack.pop_back();
                        switch (n->type)
                        {
                                case ast_node::Type::Token:
                                case ast_node::Type::Phrase:
                                        collected.push_back(n->p);
                                        break;

                                case ast_node::Type::UnaryOp:
                                        if (n->unaryop.op != Operator::NOT)
                                                stack.push_back(n->unaryop.expr);
                                        break;

                                case ast_node::Type::ConstTrueExpr:
                                        stack.push_back(n->expr);
                                        break;

                                case ast_node::Type::BinOp:
                                        if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND || n->binop.op == Operator::OR)
                                        {
                                                stack.push_back(n->binop.lhs);
                                                stack.push_back(n->binop.rhs);
                                        }
                                        else if (n->binop.op == Operator::NOT)
                                                stack.push_back(n->binop.lhs);
                                        break;

                                default:
                                        break;
                        }
                } while (stack.size());

                for (const auto it : collected)
                {
                        const uint8_t rep = it->size == 1 ? it->rep : 1;
                        const auto toNextSpan = it->toNextSpan;
                        const auto flags = it->flags;

                        for (uint16_t pos{it->index}, i{0}; i != it->size; ++i, ++pos)
                        {
                                if (traceCompile)
                                        SLog("Collected instance: ", it->terms[i].token, " index:", pos, " rep:", rep, " toNextSpan:", i == (it->size - 1) ? toNextSpan : 1, "\n");

                                originalQueryTokenInstances.push_back({it->terms[i].token, pos, rep, flags, uint8_t(i == (it->size - 1) ? toNextSpan : 1)}); // need to be careful to get this right for phrases
                        }
                }
        }

        if (traceCompile)
                SLog("Compiling:", q, "\n");

        runtime_ctx rctx(idxsrc);
        std::vector<exec_term_id_t> leaderTermIDs;
        const auto before = Timings::Microseconds::Tick();
        const auto rootExecNode = compile_query(q.root, rctx, &leaderTermIDs, execFlags);

        if (traceCompile)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to compile\n");

        if (unlikely(rootExecNode.fp == constfalse_impl))
        {
                if (traceCompile)
                        SLog("Nothing to do\n");

                return;
        }

        // It should be easy to emit machine code from the exec_nodes tree
        // which should result in a respectable speed up.
        // For now, for simplicity and for portability we are not doing it yet, but someome
        // should be able do it without significant effort.

        uint16_t toAdvance[Limits::MaxQueryTokens];
        std::vector<Trinity::Codecs::Decoder *> leaderTermsDecoders;
        // see query_index_terms and MatchedIndexDocumentsFilter::prepare() comments
        query_index_terms **queryIndicesTerms;

        // begin() for the decoder of each of those terms
        for (const auto termID : leaderTermIDs)
        {
                auto decoder = rctx.decode_ctx.decoders[termID];

                require(decoder);
                decoder->begin();
                leaderTermsDecoders.push_back(decoder);
        }

        auto *const __restrict__ leaderDecoders = leaderTermsDecoders.data();
        uint32_t leaderDecodersCnt = leaderTermsDecoders.size();
        const auto maxQueryTermIDPlus1 = rctx.termsDict.size() + 1;

        {
                std::vector<const query_term_instance *> collected;
                std::vector<std::pair<uint16_t, std::pair<exec_term_id_t, uint8_t>>> originalQueryTokensTracker; // query index => (termID, toNextSpan)
                std::vector<std::pair<exec_term_id_t, uint8_t>> list;
                uint16_t maxIndex{0};

                // Build rctx.originalQueryTermInstances
                // It is important to only do this after we have optimised the copied original query, just as it is important
                // to capture the original query instances before we optimise
                //
                // We need access to that information for scoring documents -- see matches.h
                rctx.originalQueryTermCtx = (query_term_ctx **)rctx.allocator.Alloc(sizeof(query_term_ctx *) * maxQueryTermIDPlus1);

                memset(rctx.originalQueryTermCtx, 0, sizeof(query_term_ctx *) * maxQueryTermIDPlus1);
                std::sort(originalQueryTokenInstances.begin(), originalQueryTokenInstances.end(), [](const auto &a, const auto &b) { return terms_cmp(a.token.data(), a.token.size(), b.token.data(), b.token.size()) < 0; });
                for (const auto *p = originalQueryTokenInstances.data(), *const e = p + originalQueryTokenInstances.size(); p != e;)
                {
                        const auto token = p->token;

                        if (traceCompile)
                                SLog("token [", token, "]\n");

                        if (const auto termID = rctx.termsDict[token]) // only if this token has actually been used in the compiled query
                        {
                                collected.clear();
                                do
                                {
                                        collected.push_back(p);
                                } while (++p != e && p->token == token);

                                const auto cnt = collected.size();
                                auto p = (query_term_ctx *)rctx.allocator.Alloc(sizeof(query_term_ctx) + cnt * sizeof(query_term_ctx::instance_struct));

                                p->instancesCnt = cnt;
                                p->term.id = termID;
                                p->term.token = token;

                                std::sort(collected.begin(), collected.end(), [](const auto &a, const auto &b) { return a->index < b->index; });
                                for (size_t i{0}; i != collected.size(); ++i)
                                {
                                        auto it = collected[i];

                                        p->instances[i].index = it->index;
                                        p->instances[i].rep = it->rep;
                                        p->instances[i].flags = it->flags;
                                        p->instances[i].toNextSpan = it->toNextSpan;

                                        maxIndex = std::max(maxIndex, it->index);
                                        originalQueryTokensTracker.push_back({it->index, {termID, it->toNextSpan}});
                                }

                                rctx.originalQueryTermCtx[termID] = p;
                        }
                        else
                        {
                                // this original query token is not used in the optimised query

                                if (traceCompile)
                                        SLog("Ignoring ", token, "\n");

                                do
                                {
                                        ++p;
                                } while (p != e && p->token == token);
                        }
                }

                // See docwordspace.h comments
                // we are allocated (maxIndex + 8) and memset() that to 0 in order to make some optimizations possible in consider()
                queryIndicesTerms = (query_index_terms **)rctx.allocator.Alloc(sizeof(query_index_terms *) * (maxIndex + 8));

                memset(queryIndicesTerms, 0, sizeof(queryIndicesTerms[0]) * (maxIndex + 8));
                std::sort(originalQueryTokensTracker.begin(), originalQueryTokensTracker.end(), [](const auto &a, const auto &b) { return a.second.first < b.second.first || (a.second.first == b.second.first && a.second.second < b.second.second); });

                for (const auto *p = originalQueryTokensTracker.data(), *const e = p + originalQueryTokensTracker.size(); p != e;)
                {
                        const auto idx = p->first;

                        // unique pairs(token, toNextSpan) for idx
                        list.clear();
                        do
                        {
                                const auto pair = p->second;

                                list.push_back(pair);
                                do
                                {
                                        ++p;
                                } while (p != e && p->first == idx && p->second == pair);
                        } while (p != e && p->first == idx);

                        const uint16_t cnt = list.size();
                        auto ptr = (query_index_terms *)rctx.allocator.Alloc(sizeof(query_index_terms) + cnt * sizeof(std::pair<exec_term_id_t, uint8_t>));

                        ptr->cnt = cnt;
                        memcpy(ptr->uniques, list.data(), cnt * sizeof(std::pair<exec_term_id_t, uint8_t>));
                        queryIndicesTerms[idx] = ptr;
                }
        }

        rctx.curDocQueryTokensCaptured = (uint16_t *)rctx.allocator.Alloc(sizeof(uint16_t) * maxQueryTermIDPlus1);
        rctx.matchedDocument.matchedTerms = (matched_query_term *)rctx.allocator.Alloc(sizeof(matched_query_term) * maxQueryTermIDPlus1);
        rctx.curDocSeq = UINT16_MAX; // IMPORTANT

        if (traceCompile)
                SLog("RUNNING\n");

        uint32_t matchedDocuments{0};
        const auto start = Timings::Microseconds::Tick();
        auto &dws = rctx.docWordsSpace;

        matchesFilter->prepare(&dws, const_cast<const query_index_terms **>(queryIndicesTerms));

        // We will specialize on (documentsOnly)
        if (!documentsOnly)
        {
                if (rootExecNode.fp == matchterm_impl)
                {
                        // Fast-path, one token only
                        // Special-case this, e.g for cid:100
                        // For a dataset of 4 million or so documents, for a token that matched
                        // 6749 documents, it took 790us without this specialization, and about 640us when this specialization is enabled (~18% improvement)
                        auto *const decoder = leaderDecoders[0];
                        const auto termID = exec_term_id_t(rootExecNode.u16);
                        auto *const p = &rctx.matchedDocument.matchedTerms[0];
                        auto *const th = rctx.decode_ctx.termHits[termID];

                        require(th);
                        rctx.matchedDocument.matchedTermsCnt = 1;
                        p->queryCtx = rctx.originalQueryTermCtx[termID];
                        p->hits = th;

                        if (documentsFilter)
                        {
                                do
                                {
                                        const auto docID = decoder->curDocument.id;

                                        if ((!documentsFilter || !documentsFilter->filter(docID)) && !maskedDocumentsRegistry->test(docID))
                                        {
                                                // see runtime_ctx::capture_matched_term()
                                                // we won't use runtime_ctx::reset() because it will
                                                // 	docWordsSpace.reset()
                                                rctx.materialize_term_hits_impl(termID);
                                                rctx.matchedDocument.id = docID;

                                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                                        break;
                                        }

                                        ++matchedDocuments;
                                } while (decoder->next());
                        }
                        else
                        {
                                do
                                {
                                        const auto docID = decoder->curDocument.id;

                                        if (!maskedDocumentsRegistry->test(docID))
                                        {
                                                rctx.materialize_term_hits_impl(termID);
                                                rctx.matchedDocument.id = docID;

                                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                                        break;
                                        }

                                        ++matchedDocuments;
                                } while (decoder->next());
                        }
                }
                else
                {
                        for (;;)
                        {
                                // Select document from the leader tokens/decoders
                                auto docID = leaderDecoders[0]->curDocument.id; // see Codecs::Decoder::curDocument comments
                                uint16_t toAdvanceCnt{1};

                                toAdvance[0] = 0;

                                for (uint32_t i{1}; i != leaderDecodersCnt; ++i)
                                {
                                        const auto *const __restrict__ decoder = leaderDecoders[i];
                                        const auto did = decoder->curDocument.id; // see Codecs::Decoder::curDocument comments

                                        if (did < docID)
                                        {
                                                docID = did;
                                                toAdvance[0] = i;
                                                toAdvanceCnt = 1;
                                        }
                                        else if (did == docID)
                                                toAdvance[toAdvanceCnt++] = i;
                                }

                                if ((!documentsFilter || !documentsFilter->filter(docID)) && !maskedDocumentsRegistry->test(docID))
                                {
                                        // now execute rootExecNode
                                        // and it it returns true, compute the document's score
                                        rctx.reset(docID);

                                        if (eval(rootExecNode, rctx))
                                        {
                                                const auto n = rctx.matchedDocument.matchedTermsCnt;
                                                auto *const __restrict__ allMatchedTerms = rctx.matchedDocument.matchedTerms;

                                                rctx.matchedDocument.id = docID;

                                                for (uint16_t i{0}; i != n; ++i)
                                                {
                                                        const auto termID = allMatchedTerms[i].queryCtx->term.id;

                                                        rctx.materialize_term_hits(termID);
                                                }

                                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                                {
                                                        // Eearly abort
                                                        // Maybe the filter has collected as many documents as it needs
                                                        // See https://blog.twitter.com/2010/twitters-new-search-architecture "efficient early query termination"
                                                        // See CONCEPTS.md
                                                        goto l1;
                                                }

                                                ++matchedDocuments;
                                        }
                                }

                                // Advance leader tokens/decoders
                                do
                                {
                                        const auto idx = toAdvance[--toAdvanceCnt];
                                        auto *const decoder = leaderDecoders[idx];

                                        if (!decoder->next())
                                        {
                                                // done with this leaf token
                                                if (!--leaderDecodersCnt)
                                                        goto l1;

                                                memmove(leaderDecoders + idx, leaderDecoders + idx + 1, (leaderDecodersCnt - idx) * sizeof(Trinity::Codecs::Decoder *));
                                        }

                                } while (toAdvanceCnt);
                        }
                }
        }
        else // Documents only; won't materialize
        {
                if (rootExecNode.fp == matchterm_impl)
                {
                        auto *const decoder = leaderDecoders[0];
                        const auto termID = exec_term_id_t(rootExecNode.u16);
                        auto *const p = &rctx.matchedDocument.matchedTerms[0];
                        auto *const th = rctx.decode_ctx.termHits[termID];

                        require(th);
                        rctx.matchedDocument.matchedTermsCnt = 0;
                        p->queryCtx = rctx.originalQueryTermCtx[termID];
                        p->hits = th;

                        if (documentsFilter)
                        {
                                do
                                {
                                        const auto docID = decoder->curDocument.id;

                                        if ((!documentsFilter || !documentsFilter->filter(docID)) && !maskedDocumentsRegistry->test(docID))
                                        {
                                                rctx.matchedDocument.id = docID;
                                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                                        break;
                                        }

                                        ++matchedDocuments;
                                } while (decoder->next());
                        }
                        else
                        {
                                do
                                {
                                        const auto docID = decoder->curDocument.id;

                                        if (!maskedDocumentsRegistry->test(docID))
                                        {
                                                rctx.matchedDocument.id = docID;
                                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                                        break;
                                        }

                                        ++matchedDocuments;
                                } while (decoder->next());
                        }
                }
                else
                {
                        for (;;)
                        {
                                auto docID = leaderDecoders[0]->curDocument.id;
                                uint16_t toAdvanceCnt{1};

                                toAdvance[0] = 0;
                                for (uint32_t i{1}; i != leaderDecodersCnt; ++i)
                                {
                                        const auto *const __restrict__ decoder = leaderDecoders[i];
                                        const auto did = decoder->curDocument.id; // see Codecs::Decoder::curDocument comments

                                        if (did < docID)
                                        {
                                                docID = did;
                                                toAdvance[0] = i;
                                                toAdvanceCnt = 1;
                                        }
                                        else if (did == docID)
                                                toAdvance[toAdvanceCnt++] = i;
                                }


                                if ((!documentsFilter || !documentsFilter->filter(docID)) && !maskedDocumentsRegistry->test(docID))
                                {
                                        rctx.reset(docID);

                                        if (eval(rootExecNode, rctx))
                                        {
                                                rctx.matchedDocument.id = docID;

                                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                                        goto l1;

                                                ++matchedDocuments;
                                        }
                                }

                                do
                                {
                                        const auto idx = toAdvance[--toAdvanceCnt];
                                        auto *const decoder = leaderDecoders[idx];

                                        if (!decoder->next())
                                        {
                                                if (!--leaderDecodersCnt)
                                                        goto l1;

                                                memmove(leaderDecoders + idx, leaderDecoders + idx + 1, (leaderDecodersCnt - idx) * sizeof(Trinity::Codecs::Decoder *));
                                        }

                                } while (toAdvanceCnt);
                        }
                }
        }

l1:
        const auto duration = Timings::Microseconds::Since(start);
	const auto durationAll = Timings::Microseconds::Since(_start);

        if (traceCompile)
                SLog(ansifmt::bold, ansifmt::color_red, dotnotation_repr(matchedDocuments), " matched in ", duration_repr(duration), ansifmt::reset, " (", Timings::Microseconds::ToMillis(duration), " ms) ", duration_repr(durationAll), " all\n");
}
