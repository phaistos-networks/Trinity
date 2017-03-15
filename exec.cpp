#include "exec.h"
#include "docwordspace.h"
#include "matches.h"

using namespace Trinity;

namespace // static/local this module
{
        static constexpr bool traceExec{false};

        struct runtime_ctx;

        struct exec_node // 64bytes alignement seems to yield good results, but crashes if optimizer is enabled (i.e struct alignas(64) exec_node {})
        {
                // we are now embedding the fp directly into exec_node
                // because unlike a previous design(see git repo history) where
                // we would dereference an eval() local array of pointers and would
                // hold here an index to that array, this is faster
                bool (*fp)(const exec_node &, runtime_ctx &);

                // instead of using unions here
                // we will have a node/impl specific context allocated elsewhere with the appropriate size
                // so that the implementaiton can refer to it
                // maybe we should really embed the pointer here though because sizeof(exec_node) is going to be 16 anyway so we may as well
                // use that space to embed the pointer - and we really don't care for flags
                // turns out, this is faster than dereferencing in runtime_ctx
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
                struct binop_ctx
                {
                        exec_node lhs;
                        exec_node rhs;
                };

                // See compile()
                struct unaryop_ctx
                {
                        exec_node expr;
                };

                // See compile() and OpCodes::MatchPhrase
                struct phrase
                {
                        exec_term_id_t *termIDs; // via resolve_term()
                        uint8_t size;            // total in termIDs
                        uint8_t rep;             // phrase::rep
                        uint16_t index;          // phrase::index
                        uint8_t flags;           // phrase::flags
                };

                struct termsrun
                {
                        uint16_t size;
                        exec_term_id_t terms[0];
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
                // indirection. For each distict/resolved term, we have a decoder and term_hits in decode_ctx.decoders[] and decode_ctx.termHits[]
                // This means you can index them using a termID
                // This means we may have some nullptr in decode_ctx.decoders[] but that's OK
                void prepare_decoder(exec_term_id_t termID)
                {
                        decode_ctx.check(termID);

                        if (!decode_ctx.decoders[termID])
                        {
                                decode_ctx.decoders[termID] = idxsrc->new_postings_decoder(term_ctx(termID));
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
                                for (uint32_t i{0}; i < decode_ctx.capacity; ++i)
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
                        return tctxMap[termID];
                }

                // Resolves a term to a termID relative to the runtime_ctx
                // this id is meaningless outside this execution context
		// and we use it because its easier to track/use integers than strings
                exec_term_id_t resolve_term(const str8_t term)
                {
                        exec_term_id_t *ptr;

                        if (termsDict.Add(term, 0, &ptr))
                        {
                                const auto tctx = idxsrc->term_ctx(term);

                                if (tctx.documents == 0)
                                {
                                        // matches no documents, unknown
                                        *ptr = 0;
                                }
                                else
                                {
                                        *ptr = termsDict.size();
                                        tctxMap.insert({*ptr, tctx});
                                }
                        }

                        return *ptr;
                }

                void *register_termsrun(const termsrun *const run, const exec_term_id_t termID)
                {
                        const uint16_t computedSize = run->size + 1;
                        auto ptr = (termsrun *)runsAllocator.Alloc(sizeof(termsrun) + sizeof(exec_term_id_t) * computedSize);

                        memcpy(ptr->terms, run->terms, sizeof(exec_term_id_t) * run->size);
                        ptr->terms[run->size] = termID;
                        ptr->size = computedSize;
                        return ptr;
                }

                void *register_termsrun(const exec_term_id_t termID, const termsrun *const run)
                {
                        const uint16_t computedSize = run->size + 1;
                        auto ptr = (termsrun *)runsAllocator.Alloc(sizeof(termsrun) + sizeof(exec_term_id_t) * computedSize);

                        ptr->terms[0] = termID;
                        memcpy(ptr->terms + 1, run->terms, sizeof(exec_term_id_t) * run->size);
                        ptr->size = computedSize;
                        return ptr;
                }

                void *register_termsrun(const exec_term_id_t termID1, const exec_term_id_t termID2)
                {
                        static constexpr uint16_t computedSize{2};
                        auto ptr = (termsrun *)runsAllocator.Alloc(sizeof(termsrun) + sizeof(exec_term_id_t) * computedSize);

                        ptr->terms[0] = termID1;
                        ptr->terms[1] = termID2;
                        ptr->size = computedSize;
                        return ptr;
                }

                void *register_termsrun(const termsrun *const run1, const termsrun *run2)
                {
                        const uint16_t computedSize = run1->size + run2->size;
                        auto ptr = (termsrun *)runsAllocator.Alloc(sizeof(termsrun) + sizeof(exec_term_id_t) * computedSize);

                        memcpy(ptr->terms, run1->terms, sizeof(exec_term_id_t) * run1->size);
                        memcpy(ptr->terms + run1->size, run2->terms, sizeof(exec_term_id_t) * run2->size);
                        ptr->size = computedSize;
                        return ptr;
                }

                void *register_binop(const exec_node lhs, const exec_node rhs)
                {
                        auto ptr = allocator.New<binop_ctx>();

                        ptr->lhs = lhs;
                        ptr->rhs = rhs;
                        return ptr;
                }

                void *register_unaryop(const exec_node expr)
                {
                        auto ptr = allocator.New<unaryop_ctx>();

                        ptr->expr = expr;
                        return ptr;
                }

                uint16_t register_token(const Trinity::phrase *p)
                {
                        const auto termID = resolve_term(p->terms[0].token);

                        prepare_decoder(termID);
                        return termID;
                }

                void *register_phrase(const Trinity::phrase *p)
                {
                        auto ptr = allocator.New<phrase>();

                        ptr->rep = p->rep;
                        ptr->index = p->index;
                        ptr->size = p->size;
                        ptr->flags = p->flags;
                        ptr->termIDs = (exec_term_id_t *)allocator.Alloc(sizeof(exec_term_id_t) * p->size);

                        for (uint32_t i{0}; i != p->size; ++i)
                        {
                                const auto id = resolve_term(p->terms[i].token);

                                prepare_decoder(id);
                                ptr->termIDs[i] = id;
                        }

                        return ptr;
                }

                uint32_t term_eval_cost(const exec_term_id_t termID)
                {
                        if (const auto cnt = term_ctx(termID).documents; cnt == 0)
                        {
                                // matches no documents
                                return UINT32_MAX;
                        }
                        else
                                return cnt;
                }

                uint32_t token_eval_cost(const str8_t token)
                {
                        if (const auto termID = resolve_term(token); termID == 0)
                        {
                                // unknown
                                return UINT32_MAX;
                        }
                        else
                                return term_eval_cost(termID);
                }

                uint32_t phrase_eval_cost(const Trinity::phrase *const p)
                {
                        uint32_t sum{0};

                        for (uint32_t i{0}; i != p->size; ++i)
                        {
                                const auto token = p->terms[i].token;

                                if (const auto cost = token_eval_cost(token); cost == UINT32_MAX)
                                        return UINT32_MAX;
                                else
                                        sum += cost;
                        }

                        // Not sure this is the right way to go about it
                        return sum;
                }

#pragma mark members
                // This is from the lead tokens
                // We expect all token and phrases opcodes to check against this document
                docid_t curDocID;
                // See docwordspace.h
                uint16_t curDocSeq;

                // indexed by termID
                query_term_instances **originalQueryTermInstances;

                struct decode_ctx_struct
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
                Switch::unordered_map<exec_term_id_t, str8_t> idToTerm;          // maybe useful for tracing by the score functions
                uint16_t *curDocQueryTokensCaptured;
                matched_document matchedDocument;
                simple_allocator allocator;
                simple_allocator runsAllocator{512};
                Switch::unordered_map<exec_term_id_t, term_index_ctx> tctxMap;
        };
}

#pragma mark OPTIMIZER
static uint32_t optimize_binops_impl(ast_node *const n, bool &updates, runtime_ctx &rctx)
{
        switch (n->type)
        {
                case ast_node::Type::Token:
                        if (const auto cost = rctx.token_eval_cost(n->p->terms[0].token); cost == UINT32_MAX)
                        {
                                updates = true;
                                n->set_const_false();
                                return UINT32_MAX;
                        }
                        else
                                return cost;

                case ast_node::Type::Phrase:
                        if (const auto cost = rctx.phrase_eval_cost(n->p); cost == UINT32_MAX)
                        {
                                updates = true;
                                n->set_const_false();
                                return UINT32_MAX;
                        }
                        else
                                return cost;

                case ast_node::Type::BinOp:
                {
                        const auto lhsCost = optimize_binops_impl(n->binop.lhs, updates, rctx);

                        if (lhsCost == UINT32_MAX)
                        {
                                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                                {
                                        n->set_const_false();
                                        updates = true;
                                        return UINT32_MAX;
                                }
                        }

                        const auto rhsCost = optimize_binops_impl(n->binop.rhs, updates, rctx);

                        if (rhsCost == UINT32_MAX && lhsCost == UINT32_MAX && n->binop.op == Operator::OR)
                        {
                                n->set_const_false();
                                updates = true;
                                return UINT32_MAX;
                        }

                        if (rhsCost < lhsCost && n->binop.op != Operator::NOT) // can't reorder NOT
                        {
                                std::swap(n->binop.lhs, n->binop.rhs);
                        }

                        // Because we re-arrange the branches(lhs,rhs) based on eval.cost
                        // we won't return (lhsCost + rhsCost) for every operator.
                        // We need to specialize.
                        //
                        // Because this is mostly about the leader nodes, and is particularly important for AND operations
                        // this implementation works fine.
                        switch (n->binop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        return lhsCost;

                                case Operator::OR:
                                        return lhsCost + rhsCost;

                                case Operator::NOT:
                                        return lhsCost;

                                case Operator::NONE:
                                        break;
                        }
                }

                case ast_node::Type::ConstTrueExpr:
                        if (const auto cost = optimize_binops_impl(n->expr, updates, rctx); cost == UINT32_MAX)
                        {
                                n->set_dummy();
                                updates = true;
                                return 0; // it is important to just return 0 here, not UINT32_MAX so that a parent binop wont' be dropped. We will drop this node anyway(dummy)
                        }
                        else
                        {
                                // it is also important to return UINT32_MAX - 1 so that
                                // we will not swap a binop's (lhs,rhs)
                                return UINT32_MAX - 1;
                        }

                case ast_node::Type::UnaryOp:
                        if (const auto cost = optimize_binops_impl(n->unaryop.expr, updates, rctx); cost == UINT32_MAX)
                        {
                                n->set_const_false();
                                updates = true;
                                return UINT32_MAX;
                        }
                        else
                                return cost;

                case ast_node::Type::ConstFalse:
                        return UINT32_MAX;

                default:
                        break;
        }

        return 0;
}

// similar to reorder_root(), except this time, binary ops take into account the cost to evaluate each branch
// and potentially swaps LHS and RHS for binary ops, or even sets nodes to ConstFalse or remove them
// (which are GCed by normalize_root() before we retry compilation)
// it is important to first make a pass using reorder_root() and then optimize_binops()
static ast_node *optimize_binops(ast_node *root, runtime_ctx &rctx)
{
        ast_node *normalize_root(ast_node * root); // in queries.cpp

        for (bool updates{false}; root; updates = false)
        {
                optimize_binops_impl(root, updates, rctx);

                if (updates)
                {
                        // 1+ nodes were modified
                        root = normalize_root(root);
                }
                else
                        break;
        }
        return root;
}

// Considers all binary ops, and potentiall swaps (lhs, rhs) of binary ops, but not based on actual cost
// but on heuristics .
// See optimize_binops(), which does a similar job, except it takes into account the cost to evaluate each branch
struct reorder_ctx
{
        bool dirty;
};

static void reorder(ast_node *n, reorder_ctx *const ctx)
{
        if (n->type == ast_node::Type::BinOp)
        {
                const auto lhs = n->binop.lhs, rhs = n->binop.rhs;

                reorder(lhs, ctx);
                reorder(rhs, ctx);

                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                {
                        if (lhs->type == ast_node::Type::BinOp)
                        {
                                if (rhs->is_unary())
                                {
                                        n->binop.lhs = rhs;
                                        n->binop.rhs = lhs;

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

        return r;
}

static bool optimize(Trinity::query &q, runtime_ctx &rctx)
{
        reorder_root(q.root);
        q.root = optimize_binops(q.root, rctx);
        return q.root;
}

#pragma mark INTERPRETER
#define eval(node, ctx) (node.fp(node, ctx))

static inline bool noop_impl(const exec_node &, runtime_ctx &)
{
        return false;
}

static bool matchallterms_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto run = static_cast<const runtime_ctx::termsrun *>(self.ptr);
        uint16_t i{0};
        const auto size = run->size;
        const auto did = rctx.curDocID;

#if 0
        Print("ALL of\n");
        for (uint32_t i{0}; i != size; ++i)
	{
		const auto termID = run->terms[i];

		Print(rctx.originalQueryTermInstances[termID]->term.token, "\n");
	}
	exit(0);
#endif

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
                rctx.capture_matched_term(termID);

        if (traceExec)
                SLog(ansifmt::color_green, "Attempting to match token [", rctx.originalQueryTermInstances[termID]->term.token, "] against ", rctx.curDocID, ansifmt::reset, " => ", res, "\n");
        return res;
}

static bool unaryand_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto op = (runtime_ctx::unaryop_ctx *)self.ptr;

        return eval(op->expr, rctx);
}

static bool unarynot_impl(const exec_node &self, runtime_ctx &rctx)
{
        const auto op = (runtime_ctx::unaryop_ctx *)self.ptr;

        return !eval(op->expr, rctx);
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




#pragma mark COMPILER
static exec_node compile(const ast_node *const n, runtime_ctx &ctx)
{
        exec_node res;

        require(n);
        switch (n->type)
        {
                case ast_node::Type::Dummy:
                        std::abort();

                case ast_node::Type::Token:
                        SLog("Compiling for token [", n->p->terms[0].token, "]\n");
                        res.fp = matchterm_impl;
                        res.u16 = ctx.register_token(n->p);
                        break;

                case ast_node::Type::Phrase:
                        if (n->p->size == 1)
                        {
                                res.fp = matchterm_impl;
                                res.u16 = ctx.register_token(n->p);
                        }
                        else
                        {
                                res.fp = matchphrase_impl;
                                res.ptr = ctx.register_phrase(n->p);
                        }
                        break;

                case ast_node::Type::BinOp:
                        res.ptr = ctx.register_binop(compile(n->binop.lhs, ctx), compile(n->binop.rhs, ctx));
                        switch (n->binop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        // We could have implemented this an exec_nodes optimization pass, but for simplicity we do this here
                                        //
                                        // it is there that we will sort the list of terms involved in a termsrun by documentsCnt so that
                                        // they are ordered by documentsCnt ASC. This is extremely important for AND checks
                                        //
                                        // (We have ordered reordered (lhs, rhs) of the original query's clone binary op AST nodes according to the same
                                        // heristics prior to compiling that query, but now that we are capturing termruns, we need to do
                                        // this for each of those as well. This is cheap because we have already cached that information in runtime_ctx)
                                        if (auto opctx = static_cast<const runtime_ctx::binop_ctx *>(res.ptr); opctx->lhs.fp == matchterm_impl && opctx->rhs.fp == matchterm_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(opctx->lhs.u16, opctx->rhs.u16);
                                                res.fp = matchallterms_impl;
                                        }
                                        else if (opctx->lhs.fp == matchterm_impl && opctx->rhs.fp == matchallterms_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(opctx->lhs.u16, static_cast<const runtime_ctx::termsrun *>(opctx->rhs.ptr));
                                                res.fp = matchallterms_impl;
                                        }
                                        else if (opctx->lhs.fp == matchallterms_impl && opctx->rhs.fp == matchterm_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(static_cast<const runtime_ctx::termsrun *>(opctx->lhs.ptr), opctx->rhs.u16);
                                                res.fp = matchallterms_impl;
                                        }
                                        else if (opctx->lhs.fp == matchallterms_impl && opctx->rhs.fp == matchallterms_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(static_cast<const runtime_ctx::termsrun *>(opctx->lhs.ptr), static_cast<const runtime_ctx::termsrun *>(opctx->rhs.ptr));
                                                res.fp = matchallterms_impl;
                                        }
                                        else
                                                res.fp = logicaland_impl;
                                        break;

                                case Operator::OR:
                                        if (auto opctx = static_cast<const runtime_ctx::binop_ctx *>(res.ptr); opctx->lhs.fp == matchterm_impl && opctx->rhs.fp == matchterm_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(opctx->lhs.u16, opctx->rhs.u16);
                                                res.fp = matchanyterms_impl;
                                        }
                                        else if (opctx->lhs.fp == matchterm_impl && opctx->rhs.fp == matchanyterms_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(opctx->lhs.u16, static_cast<const runtime_ctx::termsrun *>(opctx->rhs.ptr));
                                                res.fp = matchanyterms_impl;
                                        }
                                        else if (opctx->lhs.fp == matchanyterms_impl && opctx->rhs.fp == matchterm_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(static_cast<const runtime_ctx::termsrun *>(opctx->lhs.ptr), opctx->rhs.u16);
                                                res.fp = matchanyterms_impl;
                                        }
                                        else if (opctx->lhs.fp == matchanyterms_impl && opctx->rhs.fp == matchanyterms_impl)
                                        {
                                                res.ptr = ctx.register_termsrun(static_cast<const runtime_ctx::termsrun *>(opctx->lhs.ptr), static_cast<const runtime_ctx::termsrun *>(opctx->rhs.ptr));
                                                res.fp = matchanyterms_impl;
                                        }
                                        else
                                                res.fp = logicaland_impl;
                                        break;

                                case Operator::NOT:
                                        res.fp = logicalnot_impl;
                                        break;

                                case Operator::NONE:
                                        std::abort();
                                        break;
                        }
                        break;

                case ast_node::Type::ConstFalse:
                        res.fp = noop_impl;
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
                        res.ptr = ctx.register_unaryop(compile(n->unaryop.expr, ctx));
                        break;

                case ast_node::Type::ConstTrueExpr:
                        // use register_unaryop() for this as well
                        // no need for another register_x()
                        SLog("Compiling ConstTrueExpr\n");
                        res.ptr = ctx.register_unaryop(compile(n->expr, ctx));
                        res.fp = consttrueexpr_impl;
                        break;
        }

        return res;
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
        struct query_term_instance
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
        uint64_t before;

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


        // Optimizations we shouldn't perform on the parsed query because
        // the rewrite it by potentially moving nodes around or dropping nodes
        // We have tracked whatever we need in the original input query so we are now free to modify it and optimise it
        before = Timings::Microseconds::Tick();
        if (!optimize(q, rctx))
        {
                // After optimizations nothing's left
                SLog("No root node after optimizations\n");
                return;
        }

        SLog(duration_repr(Timings::Microseconds::Since(before)), " to optimize\n");

        SLog("Compiling:", q, "\n");

        // Need to compile before we access the leader nodes
        const auto rootExecNode = compile(q.root, rctx);

        SLog("Optimising exec nodes\n");

        // A final optimization pass for any remaining optimization opportunities on the exec nodes this time (not on the query's AST)
        // We could perhaps integrate them into the compiler pass, but for simplicity we will keep that separate
        // its very cheap to do this anyway
        {
                std::vector<exec_node> stack{rootExecNode};
                std::vector<std::pair<exec_term_id_t, uint32_t>> v;

                do
                {
                        const auto n = stack.back();

                        stack.pop_back();
                        if (n.fp == matchallterms_impl)
                        {
                                // Before we compile `q`, we optimise it, which winds up rewordering binary op ast nodes branches(lhs, rhs) based
                                // on their evaluation cost. That works great. We need to do this for captured termruns though. 
                                auto ctx = static_cast<runtime_ctx::termsrun *>(n.ptr);

                                v.clear();
                                for (uint32_t i{0}; i != ctx->size; ++i)
                                {
                                        const auto termID = ctx->terms[i];

					//SLog(termID, " ", rctx.term_ctx(termID).documents, "\n");
                                        v.push_back({termID, rctx.term_ctx(termID).documents});
                                }

                                std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second < b.second; });

                                for (uint32_t i{0}; i != ctx->size; ++i)
                                        ctx->terms[i] = v[i].first;
                        }
                        else if (n.fp == matchanyterms_impl)
                        {
                                // no need to sort here
                        }
                        else if (n.fp == logicaland_impl || n.fp == logicalor_impl || n.fp == logicalnot_impl)
                        {
                                auto ctx = static_cast<const runtime_ctx::binop_ctx *>(n.ptr);

                                stack.push_back(ctx->lhs);
                                stack.push_back(ctx->rhs);
                        }
                        else if (n.fp == unaryand_impl || n.fp == unarynot_impl)
                                stack.push_back(static_cast<const runtime_ctx::unaryop_ctx *>(n.ptr)->expr);
                        else if (n.fp == consttrueexpr_impl)
                                stack.push_back(static_cast<const runtime_ctx::unaryop_ctx *>(n.ptr)->expr);
                } while (stack.size());
        }

        // We need the leader nodes
        // See leader_nodes() impl. comments
        std::vector<ast_node *> leaderNodes;
        uint16_t toAdvance[Limits::MaxQueryTokens];
        std::vector<Trinity::Codecs::Decoder *> leaderTokensDecoders;
        // see query_index_terms and MatchedIndexDocumentsFilter::prepare() comments
        query_index_terms **queryIndicesTerms;

        {
                Switch::vector<str8_t> leaderTokensV;

                q.leader_nodes(&leaderNodes);

                if (leaderNodes.empty())
                {
                        // Can't process this query; we deal with those
                        // cases in normalize() but just to be on the safe side, check again here and bail
                        // see normalize_root() implementation
                        SLog("No Leader Nodes\n");
                        return;
                }

                SLog("leaderNodes.size = ", leaderNodes.size(), "\n");

                for (const auto n : leaderNodes)
                {
                        if (const auto phraseSize = n->p->size; phraseSize == 1)
                                leaderTokensV.push_back(n->p->terms[0].token);
                        else
                        {
                                // A phrase, choose the token among them with the lowest frequency
                                // i.e the on that matches the fewer documents
                                auto token = n->p->terms[0].token;
                                const auto termID = rctx.resolve_term(token);
                                const auto tctx = rctx.term_ctx(termID);
                                auto low = tctx.documents;

                                for (uint32_t i{1}; i != phraseSize; ++i)
                                {
                                        auto t = n->p->terms[i].token;
                                        const auto termID = rctx.resolve_term(t);
                                        const auto tctx = rctx.term_ctx(termID);

                                        if (tctx.documents < low)
                                        {
                                                token = t;
                                                low = tctx.documents;
                                                if (low == 0)
                                                {
                                                        // early abort
                                                        break;
                                                }
                                        }
                                }

                                leaderTokensV.push_back(token);
                        }
                }

                Dexpect(leaderTokensV.size() < sizeof_array(toAdvance));

                std::sort(leaderTokensV.begin(), leaderTokensV.end(), [](const auto &a, const auto &b) {
                        return terms_cmp(a.data(), a.size(), b.data(), b.size()) < 0;
                });

		// we need the distinct leader tokens
                leaderTokensV.resize(std::unique(leaderTokensV.begin(), leaderTokensV.end()) - leaderTokensV.begin());

                Print("leaderTokens:", leaderTokensV, "\n");

		// begin() for the decoder of each fo those tokens
                for (const auto t : leaderTokensV)
                {
                        const auto termID = rctx.resolve_term(t);
                        require(termID);
                        auto decoder = rctx.decode_ctx.decoders[termID];
                        require(decoder);

                        decoder->begin();
                        leaderTokensDecoders.push_back(decoder);
                }
        }

        require(leaderTokensDecoders.size());
        auto *const __restrict__ leaderDecoders = leaderTokensDecoders.data();
        uint32_t leaderDecodersCnt = leaderTokensDecoders.size();
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

                                // See runtime_ctx::capture_matched_term() comments

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

                                ++matchedDocuments;
                        }
                }




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

        SLog(dotnotation_repr(matchedDocuments), " matched in ", duration_repr(duration), "\n");
}
