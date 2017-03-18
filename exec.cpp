#include "exec.h"
#include "docwordspace.h"
#include "matches.h"

using namespace Trinity;

namespace // static/local this module
{
        static constexpr bool traceExec{false};

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
		};

                struct termsrun final
                {
                        uint16_t size;
                        exec_term_id_t terms[0];
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

                        if (const auto qti = originalQueryTermInstances[termID])
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
                                        p->queryTermInstances = qti;
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

                // for simplicity's sake, we are just going to map exec_term_id_t => decoders[] without
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
                exec_term_id_t resolve_term(const str8_t term)
                {
                        exec_term_id_t *ptr;

                        if (termsDict.Add(term, 0, &ptr))
                        {
                                const auto tctx = idxsrc->term_ctx(term);

				SLog(ansifmt::bold, ansifmt::color_green, "[", term, "] ", termsDict.size(), ansifmt::reset, "\n");

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
                        const auto termID = resolve_term(p->terms[0].token);

                        prepare_decoder(termID);
                        return termID;
                }

                phrase *register_phrase(const Trinity::phrase *p)
                {
                        auto ptr = (phrase *)allocator.Alloc(sizeof(phrase) + sizeof(exec_term_id_t) * p->size);

                        ptr->size = p->size;
                        for (uint32_t i{0}; i != p->size; ++i)
                        {
                                if (const auto id = resolve_term(p->terms[i].token))
                                {
                                        prepare_decoder(id);
                                        ptr->termIDs[i] = id;
                                }
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
                query_term_instances **originalQueryTermInstances;

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
                Switch::unordered_map<exec_term_id_t, str8_t> idToTerm; // maybe useful for tracing by the score functions
                uint16_t *curDocQueryTokensCaptured;
                matched_document matchedDocument;
                simple_allocator allocator;
                simple_allocator runsAllocator{512}, ctxAllocator{1024};
                Switch::unordered_map<exec_term_id_t, std::pair<term_index_ctx, str8_t>> tctxMap;
        };
}



#pragma mark INTERPRETER
#define eval(node, ctx) (node.fp(node, ctx))
static inline bool constfalse_impl(const exec_node &, runtime_ctx &)
{
        return false;
}

static inline bool consttrue_impl(const exec_node &, runtime_ctx &)
{
        return false;
}

static bool matchallterms_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto run = static_cast<const runtime_ctx::termsrun *>(self.ptr);
        uint16_t i{0};
        const auto size = run->size;
        const auto did = rctx.curDocID;

        do
        {
                const auto termID = run->terms[i];
                auto decoder = rctx.decode_ctx.decoders[termID];

                if (decoder->seek(did))
                        rctx.capture_matched_term(termID);
                else
                        return false;
        } while (++i != size);

        return true;
}

static bool matchanyterms_impl(const exec_node &self, runtime_ctx &rctx)
{
        uint16_t i{0};
        bool res{false};
        const auto did = rctx.curDocID;
#if 1
        const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(self.ptr);
        const auto size = run->size;
        const auto *__restrict__ terms = run->terms;

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

static inline bool matchterm_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto termID = exec_term_id_t(self.u16);
        auto decoder = rctx.decode_ctx.decoders[termID];
        const auto res = decoder->seek(rctx.curDocID);

        if (res)
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

static bool matchallphrases_impl(const exec_node &self, runtime_ctx &rctx)
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

                                                goto nextPhrase;
                                        }

                                        const auto termID = p->termIDs[k];

                                        if (!dws.test(termID, pos + k))
                                                break;
                                }

                                return false;
                        }
                }

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
                SLog("PHRASE CHECK document ", rctx.curDocID, "\n");

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

        for (uint32_t i{0}; i != firstTermFreq; ++i)
        {
                if (const auto pos = firstTermHits[i].pos)
                {
                        if (trace)
                                SLog("<< POS ", pos, "\n");

                        for (uint8_t k{1};; ++k)
                        {
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
        const auto opctx = (runtime_ctx::binop_ctx *)self.ptr;

        // WAS: return eval(opctx->lhs, rctx) || eval(opctx->rhs, rctx);
        // We need to evaluate both LHS and RHS and return true if either of them is true
        // this is so that we will COLLECT the tokens from both branches
        // e.g [apple OR samsung] should match if either is found, but we need to collect both tokens if possible
        const auto res1 = eval(opctx->lhs, rctx);
        const auto res2 = eval(opctx->rhs, rctx);

        return res1 || res2;
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
        else if (n.fp == logicalor_impl || n.fp == logicalnot_impl)
        {
                auto ctx = static_cast<runtime_ctx::binop_ctx *>(n.ptr);
                const auto lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                return n.fp == logicalor_impl ? (lhsCost + rhsCost) : lhsCost;
        }
        else if (n.fp == unaryand_impl || n.fp == unarynot_impl)
        {
                auto ctx = static_cast<runtime_ctx::unaryop_ctx *>(n.ptr);

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
        else if (n.fp == matchanyterms_impl)
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
        else if (n.fp == matchanyphrases_impl)
        {
                const auto *const __restrict__ run = (runtime_ctx::phrasesrun *)n.ptr;
		uint32_t sum{0};

		for (uint32_t i{0}; i != run->size; ++i)
			sum+=rctx.term_ctx(run->phrases[i]->termIDs[0]).documents;
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
                        // ["video game" OR game]
                        std::swap(n->binop.lhs, n->binop.rhs);
                        ctx->dirty = true;
                        return;
                }

                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                {
                        if (lhs->type == ast_node::Type::BinOp)
                        {
                                if (rhs->is_unary())
                                {
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

static ast_node *reorder_root(ast_node *r)
{
        reorder_ctx ctx;

        do
        {
                ctx.dirty = false;
                reorder(r, &ctx);
        } while (ctx.dirty);

	SLog("REORDERED:", *r, "\n"); 
        return r;
}

template<typename T>
static auto impl_repr(const T *func)
{
	const auto fp = (void *)func;
	
	if (fp == (void *)logicalor_impl)
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
	else if (fp == consttrue_impl)
		return "true"_s8;
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

template <typename T>
static bool try_collect(exec_node &res, simple_allocator &a, T fp)
{
        auto opctx = static_cast<runtime_ctx::binop_ctx *>(res.ptr);

        if ((opctx->lhs.fp == matchterm_impl || opctx->lhs.fp == matchphrase_impl || opctx->lhs.fp == fp)
		&& (opctx->rhs.fp == matchterm_impl || opctx->rhs.fp == matchphrase_impl || opctx->rhs.fp == fp))
        {
                // collect collection, will turn this into 0+ termruns and 0+ phraseruns
                res.fp = reinterpret_cast<decltype(res.fp)>(fp);
                res.ptr = execnodes_collection::make(a, opctx->lhs, opctx->rhs);
                return true;
        }
        else
                return false;
}

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
                        {
                                SLog("false\n");
                                res.fp = constfalse_impl;
                        }
                        break;

                case ast_node::Type::Phrase:
                        if (n->p->size == 1)
                        {
                                res.u16 = rctx.register_token(n->p);
                                if (res.u16)
                                        res.fp = matchterm_impl;
                                else
                                {
                                        SLog("false\n");
                                        res.fp = constfalse_impl;
                                }
                        }
                        else
                        {
                                res.ptr = rctx.register_phrase(n->p);
                                if (res.ptr)
                                        res.fp = matchphrase_impl;
                                else
                                {
                                        SLog("false\n");
                                        res.fp = constfalse_impl;
                                }
                        }
                        break;

                case ast_node::Type::BinOp:
                {
                        runtime_ctx::binop_ctx *const ctx = rctx.register_binop(compile_node(n->binop.lhs, rctx, a), compile_node(n->binop.rhs, rctx, a)), *otherCtx;

                        SLog("binary op:[", ansifmt::color_blue, impl_repr(ctx->lhs.fp), ansifmt::reset, "], [", ansifmt::color_brown, impl_repr(ctx->rhs.fp), ansifmt::reset, "]\n");

                        res.ptr = ctx;
                        switch (n->binop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        SLog("AND\n");
                                        if (try_collect(res, a, SPECIALIMPL_COLLECTION_LOGICALAND))
                                        {
                                        }
                                        else if ((ctx->lhs.fp == matchterm_impl || ctx->lhs.fp == matchphrase_impl || ctx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALAND) && ctx->rhs.fp == logicaland_impl && ((otherCtx = (runtime_ctx::binop_ctx *)ctx->rhs.ptr)->lhs.fp == matchterm_impl || otherCtx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALAND || otherCtx->lhs.fp == matchphrase_impl))
                                        {
                                                // lord AND (of AND (the AND rings))
                                                // (lord of) AND (the AND rings)
                                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                                ctx->lhs.fp = (decltype(ctx->lhs.fp))SPECIALIMPL_COLLECTION_LOGICALAND;
                                                ctx->lhs.ptr = collection;
                                                ctx->rhs = otherCtx->rhs;
                                                res.fp = logicaland_impl;
                                        }
                                        else if (ctx->lhs.fp == constfalse_impl || ctx->rhs.fp == constfalse_impl)
                                        {
                                                SLog("false\n");
                                                res.fp = constfalse_impl;
                                        }
                                        else
                                                res.fp = logicaland_impl;
                                        break;

                                case Operator::OR:
                                        SLog("OR\n");
                                        if (try_collect(res, a, SPECIALIMPL_COLLECTION_LOGICALOR))
                                        {
                                        }
                                        else if ((ctx->lhs.fp == matchterm_impl || ctx->lhs.fp == matchphrase_impl || ctx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALOR) && ctx->rhs.fp == logicalor_impl && ((otherCtx = (runtime_ctx::binop_ctx *)ctx->rhs.ptr)->lhs.fp == matchterm_impl || otherCtx->lhs.fp == SPECIALIMPL_COLLECTION_LOGICALOR || otherCtx->lhs.fp == matchphrase_impl))
                                        {
                                                // lord OR (ring OR (rings OR towers))
                                                // (lord OR ring) OR (rings OR towers)
                                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                                ctx->lhs.fp = (decltype(ctx->lhs.fp))SPECIALIMPL_COLLECTION_LOGICALOR;
                                                ctx->lhs.ptr = collection;
                                                ctx->rhs = otherCtx->rhs;
                                                res.fp = logicalor_impl;
                                        }
                                        else if (ctx->lhs.fp == constfalse_impl && ctx->rhs.fp == constfalse_impl)
                                        {
                                                SLog("false\n");
                                                res.fp = constfalse_impl;
                                        }
                                        else if (ctx->lhs.fp == constfalse_impl)
                                                res = ctx->rhs;
                                        else if (ctx->rhs.fp == constfalse_impl)
                                                res = ctx->lhs;
                                        else
                                                res.fp = logicalor_impl;
                                        break;

                                case Operator::NOT:
                                        SLog("NOT\n");
                                        if (ctx->lhs.fp == constfalse_impl)
                                        {
                                                SLog("false\n");
                                                res.fp = constfalse_impl;
                                        }
                                        else if (ctx->rhs.fp == constfalse_impl)
                                                res = ctx->lhs;
                                        else
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
                        {
                                SLog("false\n");
                                res.fp = constfalse_impl;
                        }
                        else
                                res.fp = consttrueexpr_impl;
                        break;
        }

        return res;
}

static exec_node expand_node(exec_node n, runtime_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const runtime_ctx::phrase *> &phrases, std::vector<exec_node> &stack)
{
        if (n.fp == unaryand_impl || n.fp == unarynot_impl || n.fp == consttrueexpr_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = expand_node(ctx->expr, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
        {
                auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                ctx->lhs = expand_node(ctx->lhs, rctx, a, terms, phrases, stack);
                ctx->rhs = expand_node(ctx->rhs, rctx, a, terms, phrases, stack);
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
                        else if (en.fp == SPECIALIMPL_COLLECTION_LOGICALOR || en.fp == SPECIALIMPL_COLLECTION_LOGICALAND)
                        {
                                auto ctx = (execnodes_collection *)en.ptr;

                                stack.push_back(ctx->a);
                                stack.push_back(ctx->b);
                        }
                } while (stack.size());

		std::sort(terms.begin(), terms.end());
		terms.resize(std::unique(terms.begin(), terms.end()) - terms.begin());

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
			}
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
			b.append(p->termIDs[i],',');
		b.shrink_by(1);
		b.append(']');
	}
	else if (n.fp == constfalse_impl)
		b.append(false);
	else if (n.fp == consttrue_impl)
		b.append(true);
	else if (n.fp == matchanyterms_impl)
	{
        	const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

		b.append("ANY OF[");
		for (uint32_t i{0}; i != run->size; ++i)
			b.append(run->terms[i],',');
		b.shrink_by(1);
		b.append("]");
	}
	else if (n.fp == matchallterms_impl)
	{
        	const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

		b.append("ALL OF[");
		for (uint32_t i{0}; i != run->size; ++i)
			b.append(run->terms[i],',');
		b.shrink_by(1);
		b.append("]");
	}
	else if (n.fp == matchanyphrases_impl)
	{
		b.append("(any phrases)");
	}
	else if (n.fp == matchallphrases_impl)
	{
		b.append("(all phrases)");
	}
	else
	{
		Print("Missing for ", impl_repr(n.fp), "\n");
		std::abort();
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
	else if (n.fp == matchanyterms_impl || n.fp == matchanyphrases_impl)
		out->push_back(n);
        else if (n.fp == unaryand_impl)
        {
                auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                out->push_back(ctx->expr);
        }
        else if (n.fp == logicalor_impl)
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
			out->push_back(ctx->expr);
	}
}

static exec_node compile(const ast_node *const n, runtime_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> *leaderTermIDs)
{
        std::vector<exec_term_id_t> terms;
        std::vector<const runtime_ctx::phrase *> phrases;
        std::vector<exec_node> stack;
        std::vector<std::pair<exec_term_id_t, uint32_t>> v;



        // First pass
	// Compile from AST tree to exec_nodes tree
        auto root = compile_node(n, rctx, a);

	if (root.fp == constfalse_impl)
	{
		SLog("Nothing to do, compile_node() compiled away the expr.\n");
		return {constfalse_impl, {}};
	}




        // Second pass
	// consider all SPECIALIMPL_COLLECTION_LOGICALOR and SPECIALIMPL_COLLECTION_LOGICALAND exec nodes; expand them
	// to valid exec nodes.
        root = expand_node(root, rctx, a, terms, phrases, stack);

	if (root.fp == constfalse_impl)
	{
		SLog("Nothing to do (false OR noop)\n");
		return {constfalse_impl, {}};
	}




        // Third pass
	// For matchallterms_impl and matchanyterms_impl, sort the term IDs by number of documents they match
        stack.clear();
        stack.push_back(root);

        do
        {
                auto n = stack.back();

		require(n.fp != constfalse_impl);

                stack.pop_back();
                if (n.fp == matchallterms_impl)
                {
                        auto ctx = static_cast<runtime_ctx::termsrun *>(n.ptr);

                        v.clear();
                        for (uint32_t i{0}; i != ctx->size; ++i)
                        {
                                const auto termID = ctx->terms[i];

                                SLog("AND ", termID, " ", rctx.term_ctx(termID).documents, "\n");
                                v.push_back({termID, rctx.term_ctx(termID).documents});
                        }

                        std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second < b.second; });

                        for (uint32_t i{0}; i != ctx->size; ++i)
                                ctx->terms[i] = v[i].first;
                }
                else if (n.fp == matchanyterms_impl)
                {
                        auto ctx = static_cast<runtime_ctx::termsrun *>(n.ptr);

                        v.clear();
                        for (uint32_t i{0}; i != ctx->size; ++i)
                        {
                                const auto termID = ctx->terms[i];

                                SLog("OR ", termID, " ", rctx.term_ctx(termID).documents, "\n");
                                v.push_back({termID, rctx.term_ctx(termID).documents});
                        }

                        std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second < b.second; });

                        for (uint32_t i{0}; i != ctx->size; ++i)
                                ctx->terms[i] = v[i].first;
                }
                else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
                {
                        auto ctx = (runtime_ctx::binop_ctx *)n.ptr;

                        stack.push_back(ctx->lhs);
                        stack.push_back(ctx->rhs);
                }
                else if (n.fp == unaryand_impl || n.fp == unarynot_impl || n.fp == consttrueexpr_impl)
                {
                        auto ctx = (runtime_ctx::unaryop_ctx *)n.ptr;

                        stack.push_back(ctx->expr);
                }
        } while (stack.size());


	// Fourth Pass
	// Reorder logicaland_impl nodes (lhs, rhs) so that the least expensive to evaluate is always found in the lhs branch
	root = reorder_execnodes(root, rctx);



	SLog("COMPILED\n");
	Print(root);
	Print("\n");



	// We now need the leader nodes so that we can get the leader term IDs from them.
	// See Trinity::query::leader_nodes()
	// we will op. directly on the exec nodes though here
	std::vector<exec_node> leaderNodes;

	capture_leader(root, &leaderNodes, 1);
	for (const auto it : leaderNodes)
		Print("LEADER:", it, "\n");


	for (const auto &n : leaderNodes)
	{
		if (n.fp == matchterm_impl)
			leaderTermIDs->push_back(n.u16);
		else if (n.fp == matchphrase_impl)
		{
			const auto p = static_cast<const runtime_ctx::phrase *>(n.ptr);

			leaderTermIDs->push_back(p->termIDs[0]);
		}
		else if (n.fp == matchanyterms_impl)
		{
        		const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

			leaderTermIDs->insert(leaderTermIDs->end(), run->terms, run->terms + run->size);
		}
		else if (n.fp == matchallterms_impl)
		{
        		const auto *__restrict__ run = static_cast<const runtime_ctx::termsrun *>(n.ptr);

			leaderTermIDs->push_back(run->terms[0]);
		}
		else if (n.fp == matchanyphrases_impl)
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
                else
			std::abort();
	}

	std::sort(leaderTermIDs->begin(), leaderTermIDs->end());
	leaderTermIDs->resize(std::unique(leaderTermIDs->begin(), leaderTermIDs->end()) - leaderTermIDs->begin());

	for (const auto id : *leaderTermIDs)
		Print("LEADER TERM ID:", id, "\n");
	//exit(0);

        return root;
}

static exec_node compile_query(ast_node *root, runtime_ctx &rctx, std::vector<exec_term_id_t> *leaderTermIDs)
{
        simple_allocator a;

        if (!root)
        {
                SLog("No root node\n");
                return {constfalse_impl, {}};
        }

        // Reorder the tree to enable the compiler to create larger collections
	// root is from a clone of the original query, so we are free to modify it anyway we see fit with reorder_root()
        return compile(reorder_root(root), rctx, a, leaderTermIDs);
}




#pragma EXEUTION
// If we have multiple segments, we should invoke exec() for each of them
// in parallel or in sequence, collect the top X hits and then later merge them
//
// We will need to create a copy of the `q` after we have normalized it, and then
// we need to reorder and optimize that copy, get leaders and execute it -- for each segment, but
// this is a very fast operation anyway
//
// We can't reuse the same compiled bytecode/runtime_ctx to run the same query across multiple index sources, because
// we optimize based on the index source structure and terms involved in the query
// It is also very cheap to construct those.
void Trinity::exec_query(const query &in, IndexSource *const __restrict__ idxsrc, masked_documents_registry *const __restrict__ maskedDocumentsRegistry, MatchedIndexDocumentsFilter *__restrict__ const matchesFilter)
{
        struct query_term_instance final
        {
                str8_t token;
                // see Trinity::phrase
                uint16_t index;
                uint8_t rep;
                uint8_t flags;
        };

        if (!in)
        {
                SLog("No root node\n");
                return;
        }

        // we need a copy of that query here
        // for we we will need to modify it
        query q(in);

        // Normalize just in case
        if (!q.normalize())
        {
                SLog("No root node after normalization\n");
                return;
        }

        // We need to collect all term instances in the query
        // so that we the score function will be able to take that into account (See matched_document::queryTermInstances)
        // We only need to do this for specific AST branches and node types
        //
        // This must be performed before any query optimizations, for otherwise because the optimiser will most definitely rearrange the query, doing it after
        // the optimization passes will not capture the original, input query tokens instances information.
        std::vector<query_term_instance> originalQueryTokenInstances;

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
                        const auto flags = it->flags;

                        for (uint16_t pos{it->index}, i{0}; i != it->size; ++i, ++pos)
                                originalQueryTokenInstances.push_back({it->terms[i].token, pos, rep, flags});
                }
        }



        runtime_ctx rctx(idxsrc);
	std::vector<exec_term_id_t> leaderTermIDs;

        SLog("Compiling:", q, "\n");

        const auto rootExecNode = compile_query(q.root, rctx, &leaderTermIDs);

        if (unlikely(rootExecNode.fp == constfalse_impl))
        {
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
                std::vector<std::pair<uint16_t, exec_term_id_t>> queryInstanceTermIDsTracker;
                std::vector<exec_term_id_t> termIDs;
                uint16_t maxIndex{0};

                // Build rctx.originalQueryTermInstances
                // It is important to only do this after we have optimised the copied original query, just as it is important
                // to capture the original query instances before we optimise
                //
                // We need access to that information for scoring documents -- see matches.h
                rctx.originalQueryTermInstances = (query_term_instances **)rctx.allocator.Alloc(sizeof(query_term_instances *) * maxQueryTermIDPlus1);

                memset(rctx.originalQueryTermInstances, 0, sizeof(query_term_instances *) * maxQueryTermIDPlus1);
                std::sort(originalQueryTokenInstances.begin(), originalQueryTokenInstances.end(), [](const auto &a, const auto &b) { return terms_cmp(a.token.data(), a.token.size(), b.token.data(), b.token.size()) < 0; });
                for (const auto *p = originalQueryTokenInstances.data(), *const e = p + originalQueryTokenInstances.size(); p != e;)
                {
                        const auto token = p->token;

                        SLog("token [", token, "]\n");

                        if (const auto termID = rctx.termsDict[token]) // only if this token has actually been used in the compiled query
                        {
                                collected.clear();
                                do
                                {
                                        collected.push_back(p);
                                } while (++p != e && p->token == token);

                                const auto cnt = collected.size();
                                auto p = (query_term_instances *)rctx.allocator.Alloc(sizeof(query_term_instances) + cnt * sizeof(query_term_instances::instance_struct));

                                p->cnt = cnt;
                                p->term.id = termID;
                                p->term.token = token;

                                std::sort(collected.begin(), collected.end(), [](const auto &a, const auto &b) { return a->index < b->index; });
                                for (size_t i{0}; i != collected.size(); ++i)
                                {
                                        auto it = collected[i];

                                        p->instances[i].index = it->index;
                                        p->instances[i].rep = it->rep;
                                        p->instances[i].flags = it->flags;

                                        maxIndex = std::max(maxIndex, it->index);
                                        queryInstanceTermIDsTracker.push_back({termID, it->index});
                                }

                                rctx.originalQueryTermInstances[termID] = p;
                        }
                        else
                        {
                                // this original query token is not used in the optimised query

                                SLog("Ignoring ", token, "\n");

                                do
                                {
                                        ++p;
                                } while (++p != e && p->token == token);
                        }
                }

                // this facilitates access to all distict termIDs that map to the same position
                // See docwordspace.h comments
                queryIndicesTerms = (query_index_terms **)rctx.allocator.Alloc(sizeof(query_index_terms *) * (maxIndex + 1));

                memset(queryIndicesTerms, 0, sizeof(query_index_terms *) * (maxIndex + 1));
                std::sort(queryInstanceTermIDsTracker.begin(), queryInstanceTermIDsTracker.end(), [](const auto &a, const auto &b) { return a.second < b.second; });
                for (const auto *p = queryInstanceTermIDsTracker.data(), *const e = p + queryInstanceTermIDsTracker.size(); p != e;)
                {
                        const auto idx = p->second;

                        termIDs.clear();
                        do
                        {
                                termIDs.push_back(p->first);
                        } while (++p != e && p->second == idx);

                        // We need the distict terms for the same index
                        std::sort(termIDs.begin(), termIDs.end());
                        termIDs.resize(std::unique(termIDs.begin(), termIDs.end()) - termIDs.begin());

                        const uint16_t cnt = termIDs.size();
                        auto ptr = (query_index_terms *)rctx.allocator.Alloc(sizeof(query_index_terms) + cnt * sizeof(exec_term_id_t));

                        ptr->cnt = cnt;
                        memcpy(ptr->termIDs, termIDs.data(), cnt * sizeof(exec_term_id_t));
                        queryIndicesTerms[idx] = ptr;
                }
        }

        rctx.curDocQueryTokensCaptured = (uint16_t *)rctx.allocator.Alloc(sizeof(uint16_t) * maxQueryTermIDPlus1);
        rctx.matchedDocument.matchedTerms = (matched_query_term *)rctx.allocator.Alloc(sizeof(matched_query_term) * maxQueryTermIDPlus1);
        rctx.curDocSeq = UINT16_MAX; // IMPORTANT

        SLog("RUNNING\n");

        uint32_t matchedDocuments{0};
        const auto start = Timings::Microseconds::Tick();
        auto &dws = rctx.docWordsSpace;

        matchesFilter->prepare(&dws, const_cast<const query_index_terms **>(queryIndicesTerms));

        // TODO: if (q.root->type == ast_node::Type::Token) {}
        // i.e if just a single term was entered, scan that single token's documents  without even having to use a decoder
        // otherwise use the loop that tracks lead tokens
        for (;;)
        {
                // Select document from the leader tokens/decoders
                auto docID = leaderDecoders[0]->curDocument.id; // // see Codecs::Decoder::curDocument comments
                uint8_t toAdvanceCnt{1};

                toAdvance[0] = 0;

                for (uint32_t i{1}; i < leaderDecodersCnt; ++i)
                {
                        const auto decoder = leaderDecoders[i];
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

                if (traceExec)
                        SLog("DOCUMENT ", docID, "\n");

                if (!maskedDocumentsRegistry->test(docID))
                {
                        // now execute rootExecNode
                        // and it it returns true, compute the document's score
                        rctx.reset(docID);

                        if (eval(rootExecNode, rctx))
                        {
                                const auto n = rctx.matchedDocument.matchedTermsCnt;
                                auto *const __restrict__ allMatchedTerms = rctx.matchedDocument.matchedTerms;

                                rctx.matchedDocument.id = docID;

#if defined(TRINITY_ENABLE_PREFETCH) && 0 // turns out we rarely match more than 8 terms so this is not a good idea although
                                {
                                        static constexpr uint32_t stride{64 / sizeof(allMatchedTerms[0])};
                                        const uint32_t iterations = n / stride;
                                        uint32_t i{0};

                                        for (uint32_t k{0}; k != iterations; ++k)
                                        {
                                                for (const auto upto = i + stride; i != upto; ++i)
                                                {
                                                        const auto termID = allMatchedTerms[i].queryTermInstances->term.id;

                                                        rctx.materialize_term_hits(termID);
                                                }
                                        }

                                        while (i != n)
                                        {
                                                const auto termID = allMatchedTerms[i++].queryTermInstances->term.id;

                                                rctx.materialize_term_hits(termID);
                                        }
                                }

#else
                                for (uint16_t i{0}; i != n; ++i)
                                {
                                        const auto termID = allMatchedTerms[i].queryTermInstances->term.id;

                                        rctx.materialize_term_hits(termID);
                                }
#endif

                                if (unlikely(matchesFilter->consider(rctx.matchedDocument) == MatchedIndexDocumentsFilter::ConsiderResponse::Abort))
                                {
                                        // Eearly abort
                                        // Maybe the filter has collected as many documents as it needs
                                        // See https://blog.twitter.com/2010/twitters-new-search-architecture "efficient early query termination"
                                        // See CONCEPTS.md
                                }

#if 0
				SLog("MATCHED ", docID, "\n");
#endif

                                ++matchedDocuments;
                        }
                }
#if 0
		else
			SLog("MASKED ", docID, "\n");
#endif

                // Advance leader tokens/decoders
                do
                {
                        const auto idx = toAdvance[--toAdvanceCnt];
                        auto decoder = leaderDecoders[idx];

                        if (!decoder->next())
                        {
                                // done with this leaf token
                                if (!--leaderDecodersCnt)
                                        goto l1;

                                memmove(leaderDecoders + idx, leaderDecoders + idx + 1, (leaderDecodersCnt - idx) * sizeof(Trinity::Codecs::Decoder *));
                        }

                } while (toAdvanceCnt);
        }

l1:
        const auto duration = Timings::Microseconds::Since(start);

        SLog(ansifmt::bold, ansifmt::color_red, dotnotation_repr(matchedDocuments), " matched in ", duration_repr(duration), ansifmt::reset, " (", Timings::Microseconds::ToMillis(duration), " ms)\n");
}
