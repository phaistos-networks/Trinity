#include "compilation_ctx.h"
#include "common.h"

using namespace Trinity;

namespace // static/local this module
{
        static constexpr bool traceCompile{false};
}

compilation_ctx::phrase *compilation_ctx::register_phrase(const Trinity::phrase *p) {
        auto ptr = static_cast<phrase *>(allocator.Alloc(sizeof(phrase) + sizeof(exec_term_id_t) * p->size));

        ptr->size = p->size;
        for (uint32_t i{0}; i != p->size; ++i) {
                if (const auto id = resolve_query_term(p->terms[i].token))
                        ptr->termIDs[i] = id;
                else
                        return nullptr;
        }

        return ptr;
}

uint8_t compilation_ctx::phrase::intersection(const termsrun *const tr, exec_term_id_t *const out) const noexcept {
        uint16_t n{0};

        for (uint32_t i{0}; i != size; ++i) {
                if (const auto id = termIDs[i]; tr->is_set(id))
                        out[n++] = id;
        }
        return n;
}

uint8_t compilation_ctx::phrase::disjoint_union(const termsrun *const tr, exec_term_id_t *const out) const noexcept {
        uint16_t   n{0};
        const auto cnt = tr->size;

        for (uint32_t i{0}; i != cnt; ++i) {
                if (const auto id = tr->terms[i]; !is_set(id))
                        out[n++] = id;
        }
        return n;
}

bool compilation_ctx::phrase::intersected_by(const termsrun *const tr) const noexcept {
        if (tr->size >= size) {
                for (uint32_t i{0}; i != size; ++i) {
                        if (!tr->is_set(termIDs[i]))
                                return false;
                }
                return true;
        } else
                return false;
}

bool compilation_ctx::phrase::operator==(const phrase &o) const noexcept {
        if (size == o.size) {
                for (uint32_t i{0}; i != size; ++i) {
                        if (termIDs[i] != o.termIDs[i])
                                return false;
                }
                return true;
        } else
                return false;
}

bool compilation_ctx::phrase::is_set(const exec_term_id_t *const l, const uint8_t n) const noexcept {
        if (n <= size) {
                const auto upto = size - n;

                for (uint32_t i{0}; i != upto; ++i) {
                        uint32_t k;

                        for (k = 0; k != n && l[k] == termIDs[i + k]; ++k)
                                continue;

                        if (k == n)
                                return true;
                }
        }

        return false;
}

bool compilation_ctx::phrase::is_set(const exec_term_id_t id) const noexcept {
        for (uint32_t i{0}; i != size; ++i) {
                if (termIDs[i] == id)
                        return true;
        }
        return false;
}

bool compilation_ctx::termsrun::operator==(const termsrun &o) const noexcept {
        if (size == o.size) {
                for (uint32_t i{0}; i != size; ++i) {
                        if (terms[i] != o.terms[i])
                                return false;
                }
                return true;
        } else
                return false;
}

bool compilation_ctx::termsrun::is_set(const exec_term_id_t id) const noexcept {
        for (uint32_t i{0}; i != size; ++i) {
                if (terms[i] == id)
                        return true;
        }
        return false;
}

bool compilation_ctx::termsrun::erase(const exec_term_id_t id) noexcept {
        for (uint32_t i{0}; i != size; ++i) {
                if (terms[i] == id) {
                        memmove(terms + i, terms + i + 1, (--size - i) * sizeof(terms[0]));
                        return true;
                }
        }

        return false;
}

bool compilation_ctx::termsrun::erase(const termsrun &o) {
        // Fast scalar intersection scheme designed by N.Kurz.
        // lemire/SIMDCompressionAndIntersection.cpp
        const auto *A = terms, *B = o.terms;
        const auto  endA = terms + size;
        const auto  endB = o.terms + o.size;
        auto        out{terms};

        for (;;) {
                while (*A < *B) {
                l1:
                        *out++ = *A;
                        if (++A == endA) {
                        l2:
                                if (const auto n = out - terms; n == size)
                                        return false;
                                else {
                                        size = n;
                                        return true;
                                }
                        }
                }

                while (*A > *B) {
                        if (++B == endB) {
                                while (A != endA)
                                        *out++ = *A++;
                                goto l2;
                        }
                }

                if (*A == *B) {
                        if (++A == endA || ++B == endB) {
                                while (A != endA)
                                        *out++ = *A++;
                                goto l2;
                        }
                } else
                        goto l1;
        }
}

static const exec_node *stronger(const exec_node &a, const exec_node &b) noexcept {
        if (a.fp == ENT::matchallphrases || a.fp == ENT::matchphrase)
                return &a;
        else
                return &b;
}

static bool same(const exec_node &a, const exec_node &b) noexcept {
        if (a.fp == ENT::matchallterms && b.fp == a.fp) {
                const auto *const __restrict__ runa = static_cast<compilation_ctx::termsrun *>(a.ptr);
                const auto *const __restrict__ runb = static_cast<compilation_ctx::termsrun *>(b.ptr);

                return *runa == *runb;
        } else if (a.fp == ENT::matchterm && b.fp == a.fp)
                return a.u16 == b.u16;
        else if (a.fp == ENT::matchphrase && b.fp == ENT::matchphrase) {
                const auto *const __restrict__ pa = static_cast<compilation_ctx::phrase *>(a.ptr);
                const auto *const __restrict__ pb = static_cast<compilation_ctx::phrase *>(b.ptr);

                return *pa == *pb;
        }

        return false;
}

static exec_node compile_node(const ast_node *const n, compilation_ctx &cctx, simple_allocator &a) {
        exec_node res;

        require(n);
        switch (n->type) {
                case ast_node::Type::Dummy:
                        std::abort();

                case ast_node::Type::Token:
                        res.u16 = cctx.register_token(n->p);
                        if (res.u16)
                                res.fp = ENT::matchterm;
                        else
                                res.fp = ENT::constfalse;
                        break;

                case ast_node::Type::Phrase:
                        if (n->p->size == 1) {
                                res.u16 = cctx.register_token(n->p);
                                if (res.u16)
                                        res.fp = ENT::matchterm;
                                else
                                        res.fp = ENT::constfalse;
                        } else {
                                res.ptr = cctx.register_phrase(n->p);
                                if (res.ptr)
                                        res.fp = ENT::matchphrase;
                                else
                                        res.fp = ENT::constfalse;
                        }
                        break;

                case ast_node::Type::BinOp: {
                        auto ctx = cctx.register_binop(compile_node(n->binop.lhs, cctx, a), compile_node(n->binop.rhs, cctx, a));

                        res.ptr = ctx;
                        switch (n->binop.op) {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        res.fp = ENT::logicaland;
                                        break;

                                case Operator::OR:
                                        res.fp = ENT::logicalor;
                                        break;

                                case Operator::NOT:
                                        res.fp = ENT::logicalnot;
                                        break;

                                case Operator::NONE:
                                        std::abort();
                                        break;
                        }
                } break;

                case ast_node::Type::ConstFalse:
                        res.fp = ENT::constfalse;
                        break;

                case ast_node::Type::MatchSome:
                        res.fp = ENT::matchsome;
                        {
                                auto pm = static_cast<compilation_ctx::partial_match_ctx *>(a.Alloc(sizeof(compilation_ctx::partial_match_ctx) + sizeof(exec_node) * n->match_some.size));

                                pm->min  = n->match_some.min;
                                pm->size = n->match_some.size;

                                for (uint32_t i{0}; i != n->match_some.size; ++i)
                                        pm->nodes[i] = compile_node(n->match_some.nodes[i], cctx, a);

                                res.ptr = pm;
                        }
                        break;

                case ast_node::Type::UnaryOp:
                        switch (n->unaryop.op) {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        res.fp = ENT::unaryand;
                                        break;

                                case Operator::NOT:
                                        res.fp = ENT::unarynot;
                                        break;

                                default:
                                        std::abort();
                        }
                        res.ptr = cctx.register_unaryop(compile_node(n->unaryop.expr, cctx, a));
                        break;

                case ast_node::Type::ConstTrueExpr:
                        // use register_unaryop() for this as well
                        // no need for another register_x()
                        res.ptr = cctx.register_unaryop(compile_node(n->expr, cctx, a));
                        if (static_cast<compilation_ctx::unaryop_ctx *>(res.ptr)->expr.fp == ENT::constfalse)
                                res.fp = ENT::dummyop;
                        else
                                res.fp = ENT::consttrueexpr;
                        break;
        }

        return res;
}

struct execnodes_collection final {
        uint16_t  size;
        exec_node a, b;

        static execnodes_collection *make(simple_allocator &allocator, const exec_node a, const exec_node b) {
                auto ptr = allocator.Alloc<execnodes_collection>();

                ptr->a = a;
                ptr->b = b;
                return ptr;
        }
};

template <typename T, typename T2>
static execnodes_collection *try_collect_impl(const exec_node lhs, const exec_node rhs, simple_allocator &a, T fp, T2 fp2) {
        if ((lhs.fp == ENT::matchterm || lhs.fp == ENT::matchphrase || lhs.fp == fp || lhs.fp == fp2) && (rhs.fp == ENT::matchterm || rhs.fp == ENT::matchphrase || rhs.fp == fp || rhs.fp == fp2)) {
                // collect collection, will turn this into 0+ termruns and 0+ phraseruns
                return execnodes_collection::make(a, lhs, rhs);
        } else
                return nullptr;
}

template <typename T, typename T2>
static bool try_collect(exec_node &res, simple_allocator &a, T fp, T2 fp2) {
        auto opctx = static_cast<compilation_ctx::binop_ctx *>(res.ptr);

        if (auto ptr = try_collect_impl(opctx->lhs, opctx->rhs, a, fp, fp2)) {
                res.fp  = reinterpret_cast<decltype(res.fp)>(fp);
                res.ptr = ptr;
                return true;
        } else
                return false;
}

static void collapse_node(exec_node &n, compilation_ctx &cctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const compilation_ctx::phrase *> &phrases, std::vector<exec_node> &stack) {
        if (n.fp == ENT::consttrueexpr || n.fp == ENT::unaryand || n.fp == ENT::unarynot) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                collapse_node(ctx->expr, cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::matchsome) {
                auto pm = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != pm->size; ++i)
                        collapse_node(pm->nodes[i], cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes) {
                auto g = static_cast<compilation_ctx::nodes_group *>(n.ptr);

                for (uint32_t i{0}; i != g->size; ++i)
                        collapse_node(g->nodes[i], cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot) {
                auto                        ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);
                compilation_ctx::binop_ctx *otherCtx;

                collapse_node(ctx->lhs, cctx, a, terms, phrases, stack);
                collapse_node(ctx->rhs, cctx, a, terms, phrases, stack);

                if (n.fp == ENT::logicaland) {
                        if (try_collect(n, a, ENT::SPECIALIMPL_COLLECTION_LOGICALAND, ENT::matchallterms))
                                return;

                        if ((ctx->lhs.fp == ENT::matchterm || ctx->lhs.fp == ENT::matchphrase || ctx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND) && ctx->rhs.fp == ENT::logicaland && ((otherCtx = static_cast<compilation_ctx::binop_ctx *>(ctx->rhs.ptr))->lhs.fp == ENT::matchterm || otherCtx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND || otherCtx->lhs.fp == ENT::matchphrase)) {
                                // lord AND (of AND (the AND rings))
                                // (lord of) AND (the AND rings)
                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                ctx->lhs.fp  = static_cast<decltype(ctx->lhs.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALAND);
                                ctx->lhs.ptr = collection;
                                ctx->rhs     = otherCtx->rhs;
                                return;
                        }

                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::consttrueexpr) {
                                // [<foo> AND <bar>] => [ <foo,bar> ]
                                auto *const __restrict__ lhsCtx = static_cast<compilation_ctx::unaryop_ctx *>(ctx->lhs.ptr);
                                auto *const __restrict__ rhsCtx = static_cast<compilation_ctx::unaryop_ctx *>(ctx->rhs.ptr);

                                if (auto ptr = try_collect_impl(lhsCtx->expr, rhsCtx->expr, a, ENT::SPECIALIMPL_COLLECTION_LOGICALAND, ENT::matchallterms)) {
                                        // reuse lhsCtx
                                        lhsCtx->expr.fp  = reinterpret_cast<decltype(lhsCtx->expr.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALAND);
                                        lhsCtx->expr.ptr = ptr;

                                        n.ptr = lhsCtx;
                                        n.fp  = ENT::consttrueexpr;
                                        return;
                                }
                        }

                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::logicaland) {
                                // <foo> AND (<bar> AND whatever)
                                // <foo, bar> AND whatever
                                auto *const __restrict__ lhsCtx = static_cast<compilation_ctx::unaryop_ctx *>(ctx->lhs.ptr);
                                auto *const __restrict__ rhsCtx = static_cast<compilation_ctx::binop_ctx *>(ctx->rhs.ptr);

                                if (rhsCtx->lhs.fp == ENT::consttrueexpr) {
                                        auto *const __restrict__ otherCtx = static_cast<compilation_ctx::unaryop_ctx *>(rhsCtx->lhs.ptr);

                                        if (auto ptr = try_collect_impl(lhsCtx->expr,
                                                                        otherCtx->expr, a, ENT::SPECIALIMPL_COLLECTION_LOGICALAND, ENT::matchallterms)) {
                                                lhsCtx->expr.fp  = reinterpret_cast<decltype(lhsCtx->expr.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALAND);
                                                lhsCtx->expr.ptr = ptr;

                                                ctx->rhs = rhsCtx->rhs;
                                                return;
                                        }
                                }
                        }
                } else if (n.fp == ENT::logicalor) {
                        if (try_collect(n, a, ENT::SPECIALIMPL_COLLECTION_LOGICALOR, ENT::matchanyterms))
                                return;

                        if ((ctx->lhs.fp == ENT::matchterm || ctx->lhs.fp == ENT::matchphrase || ctx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR) && ctx->rhs.fp == ENT::logicalor && ((otherCtx = static_cast<compilation_ctx::binop_ctx *>(ctx->rhs.ptr))->lhs.fp == ENT::matchterm || otherCtx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR || otherCtx->lhs.fp == ENT::matchphrase)) {
                                // lord OR (ring OR (rings OR towers))
                                // (lord OR ring) OR (rings OR towers)
                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                ctx->lhs.fp  = static_cast<decltype(ctx->lhs.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALOR);
                                ctx->lhs.ptr = collection;
                                ctx->rhs     = otherCtx->rhs;
                                return;
                        }

#if 0 // this is wrong, because we don't want e.g [ FOO <BAR> | <PONG> ] to [ FOO AND <FOO | PONG > ]
                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::consttrueexpr)
                        {
                                // [<foo> OR <bar>] => [ OR<foo, bar> ]
                                auto *const __restrict__ lhsCtx = (compilation_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (compilation_ctx::unaryop_ctx *)ctx->rhs.ptr;

                                if (auto ptr = try_collect_impl(lhsCtx->expr, rhsCtx->expr, a, ENT::SPECIALIMPL_COLLECTION_LOGICALOR, ENT::matchanyterms))
                                {
                                        // reuse lhsCtx
                                        lhsCtx->expr.fp = reinterpret_cast<decltype(lhsCtx->expr.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALOR);
                                        lhsCtx->expr.ptr = ptr;

                                        n.ptr = lhsCtx;
                                        n.fp = ENT::consttrueexpr;
                                        return;
                                }
                        }

                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::logicalor)
                        {
                                // <foo> OR (<bar> OR whatever)
                                // <foo, bar> OR whatever
                                auto *const __restrict__ lhsCtx = (compilation_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (compilation_ctx::binop_ctx *)ctx->rhs.ptr;

                                if (rhsCtx->lhs.fp == ENT::consttrueexpr)
                                {
                                        auto *const __restrict__ otherCtx = (compilation_ctx::unaryop_ctx *)rhsCtx->lhs.ptr;

                                        if (auto ptr = try_collect_impl(lhsCtx->expr,
                                                                        otherCtx->expr, a, ENT::SPECIALIMPL_COLLECTION_LOGICALOR, ENT::matchanyterms))
                                        {
                                                lhsCtx->expr.fp = reinterpret_cast<decltype(lhsCtx->expr.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALOR);
                                                lhsCtx->expr.ptr = ptr;

                                                ctx->rhs = rhsCtx->rhs;
                                                return;
                                        }
                                }
                        }
#endif
                }
        }
}

static void trim_phrasesrun(exec_node &n, compilation_ctx::phrasesrun *pr) {
        auto       out  = pr->phrases;
        const auto size = pr->size;
        uint32_t   k;

        for (uint32_t i{0}; i != size; ++i) {
                auto p = pr->phrases[i];

                for (k = i + 1; k != size && !(*p == *pr->phrases[k]); ++k)
                        continue;

                if (k == size)
                        *out++ = p;
        }

        pr->size = out - pr->phrases;

        if (pr->size == 1) {
                n.fp  = ENT::matchphrase;
                n.ptr = pr->phrases[0];
        } else {
                std::sort(pr->phrases, pr->phrases + pr->size, [](const auto a, const auto b) {
                        return a->size < b->size;
                });
        }
}

static void expand_node(exec_node &n, compilation_ctx &cctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const compilation_ctx::phrase *> &phrases, std::vector<exec_node> &stack) {
        if (n.fp == ENT::consttrueexpr || n.fp == ENT::unaryand || n.fp == ENT::unarynot) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                expand_node(ctx->expr, cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::matchsome) {
                auto ctx = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != ctx->size; ++i)
                        expand_node(ctx->nodes[i], cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes) {
                auto g = static_cast<compilation_ctx::nodes_group *>(n.ptr);

                for (uint32_t i{0}; i != g->size; ++i)
                        expand_node(g->nodes[i], cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                expand_node(ctx->lhs, cctx, a, terms, phrases, stack);
                expand_node(ctx->rhs, cctx, a, terms, phrases, stack);
        } else if (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR || n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND) {
                auto ctx = static_cast<execnodes_collection *>(n.ptr);

                terms.clear();
                phrases.clear();
                stack.clear();
                stack.push_back(ctx->a);
                stack.push_back(ctx->b);

                do {
                        auto en = stack.back();

                        stack.pop_back();

                        if (en.fp == ENT::matchterm) {
                                const auto termID = exec_term_id_t(en.u16);

                                terms.push_back(termID);
                        } else if (en.fp == ENT::matchphrase) {
                                const auto p = static_cast<compilation_ctx::phrase *>(en.ptr);

                                phrases.push_back(p);
                        } else if (en.fp == ENT::matchallterms || en.fp == ENT::matchanyterms) {
                                const auto run = static_cast<const compilation_ctx::termsrun *>(en.ptr);

                                terms.insert(terms.end(), run->terms, run->terms + run->size);
                        } else if (en.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR || en.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND) {
                                auto ctx = static_cast<execnodes_collection *>(en.ptr);

                                stack.push_back(ctx->a);
                                stack.push_back(ctx->b);
                        }
                } while (!stack.empty());

                std::sort(terms.begin(), terms.end());
                terms.resize(std::unique(terms.begin(), terms.end()) - terms.begin());

                if (traceCompile)
                        SLog("Expanded terms = ", terms.size(), ", phrases = ", phrases.size(), " ", n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? "OR" : "AND", "\n");

                // We have expanded this collection into tokens and phrases
                // Depending on the number of tokens and phrases we collected we will
                // create:
                // - (ENT::matchanyterms/ENT::matchallterms OP phrase)
                // - (ENT::matchanyterms/ENT::matchallterms)
                // - (ENT::matchanyterms/ENT::matchallterms OP (ENT::matchanyphrases/ENT::matchallphrases))
                // - (token OP (ENT::matchanyphrases/ENT::matchallphrases))
                // - (ENT::matchanyphrases/ENT::matchallphrases)
                if (const auto termsCnt = terms.size(), phrasesCnt = phrases.size(); termsCnt == 1) {
                        exec_node termNode;

                        termNode.fp  = ENT::matchterm;
                        termNode.u16 = terms.front();

                        if (phrasesCnt == 0)
                                n = termNode;
                        else if (phrasesCnt == 1) {
                                exec_node phraseMatchNode;

                                phraseMatchNode.fp  = ENT::matchphrase;
                                phraseMatchNode.ptr = (void *)phrases.front();

                                n.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = cctx.register_binop(termNode, phraseMatchNode);
                        } else {
                                exec_node phrasesRunNode;
                                auto      phrasesRun = static_cast<compilation_ctx::phrasesrun *>(cctx.allocator.Alloc(sizeof(compilation_ctx::phrasesrun) + phrasesCnt * sizeof(compilation_ctx::phrase *)));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(compilation_ctx::phrase *));

                                phrasesRunNode.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyphrases : ENT::matchallphrases);
                                phrasesRunNode.ptr = (void *)phrasesRun;

                                n.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = cctx.register_binop(termNode, phrasesRunNode);

                                trim_phrasesrun(n, phrasesRun);
                        }
                } else if (termsCnt > 1) {
                        auto      run = static_cast<compilation_ctx::termsrun *>(cctx.runsAllocator.Alloc(sizeof(compilation_ctx::termsrun) + sizeof(exec_term_id_t) * termsCnt));
                        exec_node runNode;

                        run->size = termsCnt;
                        memcpy(run->terms, terms.data(), termsCnt * sizeof(exec_term_id_t));
                        runNode.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyterms : ENT::matchallterms);
                        runNode.ptr = run;

                        // see same()
                        // we will eventually sort those terms by evaluation cost, but for now
                        // it is important to sort them by id so that we can easily check for termruns eq.
                        std::sort(run->terms, run->terms + run->size);

                        if (phrasesCnt == 0) {
                                n = runNode;
                        } else if (phrasesCnt == 1) {
                                exec_node phraseNode;

                                phraseNode.fp  = ENT::matchphrase;
                                phraseNode.ptr = (void *)(phrases.front());

                                n.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = cctx.register_binop(runNode, phraseNode);
                        } else {
                                exec_node phrasesRunNode;
                                auto      phrasesRun = static_cast<compilation_ctx::phrasesrun *>(cctx.allocator.Alloc(sizeof(compilation_ctx::phrasesrun) + phrasesCnt * sizeof(compilation_ctx::phrase *)));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(compilation_ctx::phrase *));

                                phrasesRunNode.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyphrases : ENT::matchallphrases);
                                phrasesRunNode.ptr = (void *)phrasesRun;

                                trim_phrasesrun(phrasesRunNode, phrasesRun);

                                n.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = cctx.register_binop(runNode, phrasesRunNode);
                        }
                } else if (termsCnt == 0) {
                        if (phrasesCnt == 1) {
                                n.fp  = ENT::matchphrase;
                                n.ptr = (void *)phrases.front();
                        } else {
                                auto phrasesRun = static_cast<compilation_ctx::phrasesrun *>(cctx.allocator.Alloc(sizeof(compilation_ctx::phrasesrun) + phrasesCnt * sizeof(compilation_ctx::phrase *)));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(compilation_ctx::phrase *));

                                n.fp  = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyphrases : ENT::matchallphrases);
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
        } else if (n.fp == ENT::matchallterms || n.fp == ENT::matchanyterms) {
                const auto run = static_cast<const compilation_ctx::termsrun *>(n.ptr);

                if (run->size == 1) {
                        n.fp  = ENT::matchterm;
                        n.u16 = run->terms[0];
                }
        } else if (n.fp == ENT::matchphrase) {
                const auto p = static_cast<compilation_ctx::phrase *>(n.ptr);

                if (p->size == 1) {
                        n.fp  = ENT::matchterm;
                        n.u16 = p->termIDs[0];
                }
        }
}

static exec_node optimize_node(exec_node n, compilation_ctx &cctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const compilation_ctx::phrase *> &phrases, std::vector<exec_node> &stack, bool &updates, const exec_node *const root) {
        const auto saved{n};

        if (traceCompile)
                SLog("Before OPT:", n, "\n");

#define set_dirty()                                                     \
        do {                                                            \
                if (traceCompile)                                       \
                        SLog("HERE from [", saved, "] to [", n, "]\n"); \
                updates = true;                                         \
        } while (0)

        if (n.fp == ENT::consttrueexpr) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                ctx->expr = optimize_node(ctx->expr, cctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == ENT::constfalse || ctx->expr.fp == ENT::dummyop) {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        } else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes) {
                auto g = static_cast<compilation_ctx::nodes_group *>(n.ptr);

                if (0 == g->size) {
                        n.fp = ENT::constfalse;
                        set_dirty();
                }
        } else if (n.fp == ENT::matchsome) {
                auto       ctx = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);
                const auto saved{ctx->size};

                for (uint32_t i{0}; i < ctx->size; ++i) {
                        ctx->nodes[i] = optimize_node(ctx->nodes[i], cctx, a, terms, phrases, stack, updates, root);

                        if (ctx->nodes[i].fp == ENT::constfalse || ctx->nodes[i].fp == ENT::dummyop)
                                ctx->nodes[i] = ctx->nodes[--(ctx->size)];
                        else
                                ++i;
                }

                if (ctx->min > ctx->size) {
                        n.fp = ENT::constfalse;
                        set_dirty();
                } else {
                        if (ctx->size == 1) {
                                n = ctx->nodes[0];
                                set_dirty();
                        } else if (ctx->min == ctx->size) {
                                // transform to binary op that includes all those
                                auto en = ctx->nodes[0];

                                for (uint32_t i{1}; i != ctx->size; ++i) {
                                        auto b = static_cast<compilation_ctx::binop_ctx *>(a.Alloc(sizeof(compilation_ctx::binop_ctx)));

                                        b->lhs = en;
                                        b->rhs = ctx->nodes[i];
                                        en.fp  = ENT::logicaland;
                                        en.ptr = b;
                                }

                                n = en;
                                set_dirty();
                        } else if (ctx->min == 1) {
                                // TODO(markp): consider expanding to OR sequence
                        } else if (ctx->size != saved) {
                                set_dirty();
                        }
                }
        } else if (n.fp == ENT::unaryand) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                ctx->expr = optimize_node(ctx->expr, cctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == ENT::constfalse) {
                        n.fp = ENT::constfalse;
                        set_dirty();
                } else if (ctx->expr.fp == ENT::dummyop) {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        } else if (n.fp == ENT::unarynot) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                ctx->expr = optimize_node(ctx->expr, cctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == ENT::dummyop) {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        } else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot) {
                auto                        ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);
                compilation_ctx::binop_ctx *otherCtx;

                ctx->lhs = optimize_node(ctx->lhs, cctx, a, terms, phrases, stack, updates, root);
                ctx->rhs = optimize_node(ctx->rhs, cctx, a, terms, phrases, stack, updates, root);

                if (ctx->lhs.fp == ENT::dummyop && ctx->rhs.fp == ENT::dummyop) {
                        n.fp = ENT::dummyop;
                        set_dirty();
                        return n;
                } else if (ctx->rhs.fp == ENT::dummyop) {
                        n = ctx->lhs;
                        set_dirty();
                        return n;
                } else if (ctx->lhs.fp == ENT::dummyop) {
                        n = ctx->rhs;
                        set_dirty();
                        return n;
                }

                if (n.fp == ENT::logicalor) {
                        if (ctx->lhs.fp == ENT::constfalse) {
                                if (ctx->rhs.fp == ENT::constfalse) {
                                        n.fp = ENT::constfalse;
                                        set_dirty();
                                } else {
                                        n = ctx->rhs;
                                        set_dirty();
                                }
                                return n;
                        } else if (ctx->rhs.fp == ENT::constfalse) {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        } else if (same(ctx->lhs, ctx->rhs)) {
                                auto s = stronger(ctx->lhs, ctx->rhs);

                                if (s == &ctx->lhs)
                                        n = ctx->rhs;
                                else
                                        n = ctx->lhs;
                                set_dirty();
                                return n;
                        } else if (ctx->lhs.fp == ENT::constfalse && ctx->rhs.fp == ENT::constfalse) {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        } else if (ctx->lhs.fp == ENT::constfalse) {
                                n = ctx->rhs;
                                set_dirty();
                                return n;
                        } else if (ctx->rhs.fp == ENT::constfalse) {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        } else if (ctx->lhs.fp == ENT::matchphrase && ctx->rhs.fp == ENT::matchallterms) {
                                // ([1,2] OR ALL OF[3,1,2]) => ALL OF [3,1,2]
                                const auto *const __restrict__ run = static_cast<compilation_ctx::termsrun *>(ctx->rhs.ptr);
                                const auto *const phrase           = static_cast<compilation_ctx::phrase *>(ctx->lhs.ptr);

                                if (phrase->intersected_by(run)) {
                                        n = ctx->rhs;
                                        set_dirty();
                                        return n;
                                }
                        } else if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchphrase) {
                                // ([1,2] OR ALL OF[1,3,2]) => ALL OF [1,3,2]
                                const auto *const __restrict__ run = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);
                                const auto *const phrase           = static_cast<compilation_ctx::phrase *>(ctx->rhs.ptr);

                                if (phrase->intersected_by(run)) {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        } else if (ctx->lhs.fp == ENT::consttrueexpr) {
                                auto c = static_cast<const compilation_ctx::unaryop_ctx *>(ctx->lhs.ptr);

                                ctx->lhs = c->expr;
                                set_dirty();
                                return n;
                        }
                        if (ctx->rhs.fp == ENT::consttrueexpr) {
                                auto c = static_cast<const compilation_ctx::unaryop_ctx *>(ctx->rhs.ptr);

                                ctx->rhs = c->expr;
                                set_dirty();
                                return n;
                        }
                } else if (n.fp == ENT::logicaland) {
                        if (ctx->lhs.fp == ENT::constfalse || ctx->rhs.fp == ENT::constfalse) {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        } else if (same(ctx->lhs, ctx->rhs)) {
                                n = *stronger(ctx->lhs, ctx->rhs);
                                set_dirty();
                                return n;
                        } else if (ctx->lhs.fp == ENT::logicalnot && same(static_cast<compilation_ctx::binop_ctx *>(ctx->lhs.ptr)->rhs, ctx->rhs)) {
                                // ((1 NOT 2) AND 2)
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }
#if 0 
			// XXX: this is wrong
			// search for LABEL:alt1 in this file for comments etc
			// TODO: https://github.com/phaistos-networks/Trinity/issues/6
                        else if (ctx->lhs.fp == ENT::matchterm && ctx->rhs.fp == ENT::matchanyterms)
                        {
                                // (1 AND ANY OF[1,4,10]) => [1 AND <ANY OF [4, 10]>]
                                auto run = (compilation_ctx::termsrun *)ctx->rhs.ptr;

                                if (run->erase(ctx->lhs.u16))
                                {
					// [cid:806 AND anyof(cid:806, ipad, xbox)] => [cid:806 AND <anyof(ipad, xbox)>]
					// XXX:verify me
					const auto r = ctx->rhs;

					ctx->rhs.fp = ENT::consttrueexpr;
					ctx->rhs.ptr = cctx.register_unaryop(r);

                                        set_dirty();
                                        return n;
                                }
                        }
#endif

                        if (ctx->lhs.fp == ENT::constfalse || ctx->rhs.fp == ENT::constfalse) {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchanyterms) {
                                // (ALL OF[2,1] AND ANY OF[2,1])
                                auto runa = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);
                                auto runb = static_cast<compilation_ctx::termsrun *>(ctx->rhs.ptr);

                                if (*runa == *runb) {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchanyterms && ctx->rhs.fp == ENT::matchallterms) {
                                // (ANY OF[2,1] AND ALL OF[2,1])
                                auto runa = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);
                                auto runb = static_cast<compilation_ctx::termsrun *>(ctx->rhs.ptr);

                                if (*runa == *runb) {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

// ALL OF[3,2,1] AND <ANY OF[5,4,1]>
// ALL OF [3, 2, 1] AND ANY_OF<5,4>
// XXX: this is WRONG, disabling it for now
// e.g (ALL OF[1,2] AND ANY OF[1,4,5]) shouldn't be transformed to
// 	(ALL OF[1,2] AND ANY OF[4,5])
//
// It would make more sense to translate to
// 	ALL OF[1,2] AND <ANY OF [5,4]>
// but I guess it doens't really matter so much all things considered
// TODO(markp): figure out a better alternative
// LABEL:alt1
// TODO(markp): https://github.com/phaistos-networks/Trinity/issues/6
#if 0
                        if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchanyterms)
                        {
                                auto runa = (compilation_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (compilation_ctx::termsrun *)ctx->rhs.ptr;

                                if (runb->erase(*runa))
                                {
                                        if (runb->empty())
                                                ctx->rhs.fp = ENT::dummyop;

                                        set_dirty();
                                        return n;
                                }
                        }
#endif

                        if (same(ctx->lhs, ctx->rhs)) {
                                n = *stronger(ctx->lhs, ctx->rhs);
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == ENT::matchanyterms && ctx->rhs.fp == ENT::matchanyterms) {
                                // (ANY OF[2,1] AND ANY OF[2,1])
                                auto runa = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);
                                auto runb = static_cast<compilation_ctx::termsrun *>(ctx->rhs.ptr);

                                if (*runa == *runb) {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchterm && ctx->rhs.fp == ENT::matchallphrases) {
                                // (1 AND ALLPHRASES:[[4,3,5][2,3,1]])
                                const auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(ctx->rhs.ptr);
                                const auto termID                  = ctx->lhs.u16;

                                for (uint32_t i{0}; i != run->size; ++i) {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(termID)) {
                                                n = ctx->rhs;
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchphrase) {
                                // ( pc game  hitman ) AND  "pc game"
                                //  hitman AND "pc game"
                                // this is somewhat expensive, but worth it
                                auto       run = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);
                                const auto phr = static_cast<const compilation_ctx::phrase *>(ctx->rhs.ptr);

                                if (phr->size < 128) //arbitrary
                                {
                                        exec_term_id_t terms[128];

                                        const auto cnt = phr->disjoint_union(run, terms);

                                        if (cnt == 0) {
                                                n = ctx->rhs;
                                                set_dirty();
                                                return n;
                                        } else if (cnt < run->size) {
                                                run->size = cnt;
                                                memcpy(run->terms, terms, sizeof(terms[0]) * cnt);
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }

                        if (ctx->rhs.fp == ENT::matchallterms && ctx->lhs.fp == ENT::matchphrase) {
                                // "pc game" AND ( pc game  hitman )
                                //  "pc game" AND hitman
                                // this is somewhat expensive, but worth it
                                auto       run = static_cast<compilation_ctx::termsrun *>(ctx->rhs.ptr);
                                const auto phr = static_cast<const compilation_ctx::phrase *>(ctx->lhs.ptr);

                                if (phr->size < 128) //arbitrary
                                {
                                        exec_term_id_t terms[128];

                                        const auto cnt = phr->disjoint_union(run, terms);

                                        if (cnt == 0) {
                                                n = ctx->rhs;
                                                set_dirty();
                                                return n;
                                        } else if (cnt < run->size) {
                                                run->size = cnt;
                                                memcpy(run->terms, terms, sizeof(terms[0]) * cnt);
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }
                } else if (n.fp == ENT::logicalnot) {
                        if (ctx->lhs.fp == ENT::constfalse) {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        } else if (ctx->rhs.fp == ENT::constfalse) {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        } else if (same(ctx->lhs, ctx->rhs)) {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if ((ctx->lhs.fp == ENT::matchallterms || ctx->lhs.fp == ENT::matchanyterms) && ctx->rhs.fp == ENT::matchterm) {
                                // ALL OF[1,5] NOT 5) => ALL OF [1] NOT 5
                                auto run = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);

                                if (run->erase(ctx->rhs.u16)) {
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::logicalnot && same((otherCtx = static_cast<compilation_ctx::binop_ctx *>(ctx->lhs.ptr))->lhs, ctx->rhs)) {
                                // ((2 NOT 1) NOT 2)
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if (same(ctx->lhs, ctx->rhs)) {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == ENT::matchanyterms && ctx->rhs.fp == ENT::matchanyterms) {
                                // (ANY OF[2,1] NOT ANY OF[2,1])
                                auto runa = static_cast<compilation_ctx::termsrun *>(ctx->lhs.ptr);
                                auto runb = static_cast<compilation_ctx::termsrun *>(ctx->rhs.ptr);

                                if (*runa == *runb) {
                                        n.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchphrase && ctx->rhs.fp == ENT::matchterm) {
                                // (([1,2,3] NOT 3) AND ALLPHRASES:[[4,2,5][1,2,3]])
                                const auto p = static_cast<compilation_ctx::phrase *>(ctx->lhs.ptr);

                                if (p->is_set(ctx->rhs.u16)) {
                                        n.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchanyphrases && ctx->rhs.fp == ENT::matchterm) {
                                auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(ctx->lhs.ptr);
                                const auto termID            = ctx->rhs.u16;

                                for (uint32_t i{0}; i < run->size;) {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(termID))
                                                run->phrases[i] = run->phrases[--(run->size)];
                                        else
                                                ++i;
                                }

                                if (run->size == 0) {
                                        // All phrases negated by the term
                                        ctx->lhs.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                } else if (run->size == 1) {
                                        ctx->lhs.fp  = ENT::matchphrase;
                                        ctx->lhs.ptr = run->phrases[0];
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchanyphrases && ctx->rhs.fp == ENT::matchphrase) {
                                auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(ctx->lhs.ptr);
                                const auto rhsPhrase         = static_cast<const compilation_ctx::phrase *>(ctx->rhs.ptr);

                                for (uint32_t i{0}; i != run->size;) {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(rhsPhrase->termIDs, rhsPhrase->size))
                                                run->phrases[i] = run->phrases[--(run->size)];
                                        else
                                                ++i;
                                }

                                if (run->size == 0) {
                                        // All phrases negated by the term
                                        ctx->lhs.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                } else if (run->size == 1) {
                                        ctx->lhs.fp  = ENT::matchphrase;
                                        ctx->lhs.ptr = run->phrases[0];
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchallphrases && ctx->rhs.fp == ENT::matchphrase) {
                                auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(ctx->lhs.ptr);
                                const auto rhsPhrase         = static_cast<const compilation_ctx::phrase *>(ctx->rhs.ptr);

                                for (uint32_t i{0}; i != run->size; ++i) {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(rhsPhrase->termIDs, rhsPhrase->size)) {
                                                n.fp = ENT::constfalse;
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }

                        // TODO(markp): (any phrases NOT anyterms)
                }
        } else if (n.fp == ENT::matchanyterms || n.fp == ENT::matchallterms) {
                auto run = static_cast<compilation_ctx::termsrun *>(n.ptr);

                if (run->size == 1) {
                        n.fp  = ENT::matchterm;
                        n.u16 = run->terms[0];
                        set_dirty();
                } else if (run->empty()) {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        } else if (n.fp == ENT::matchallphrases) {
                // ("a b", "a b c") => ("a b c")
                auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(n.ptr);
                auto size                    = run->size;

                if (size == 1) {
                        n.fp  = ENT::matchphrase;
                        n.ptr = run->phrases[0];
                        set_dirty();
                        return n;
                }

                const auto saved{size};

                for (uint32_t i{0}; i < size;) {
                        const auto p = run->phrases[i];
                        bool       any{false};

                        for (uint32_t k{0}; k < size;) {
                                if (k != i) {
                                        if (auto o = run->phrases[k]; o->size >= p->size) {
                                                if (memcmp(p->termIDs, o->termIDs, p->size * sizeof(exec_term_id_t)) == 0) {
                                                        run->phrases[i] = run->phrases[--size];
                                                        any             = true;
                                                        break;
                                                }
                                        }
                                }
                                ++k;
                        }

                        if (!any)
                                ++i;
                }

                if (saved != size) {
                        run->size = size;
                        set_dirty();
                }
        } else if (n.fp == ENT::matchanyphrases) {
                auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(n.ptr);

                if (run->size == 1) {
                        n.fp  = ENT::matchphrase;
                        n.ptr = run->phrases[0];
                        set_dirty();
                }
        }

        return n;
#undef set_dirty
}

static auto impl_repr(const ENT fp) noexcept {
        if (fp == ENT::logicalor)
                return "or"_s8;
        else if (fp == ENT::logicaland)
                return "and"_s8;
        else if (fp == ENT::logicalnot)
                return "not"_s8;
        else if (fp == ENT::matchterm)
                return "term"_s8;
        else if (fp == ENT::matchphrase)
                return "phrase"_s8;
        else if (fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND)
                return "(AND collection)"_s8;
        else if (fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR)
                return "(OR collection)"_s8;
        else if (fp == ENT::unaryand)
                return "unary and"_s8;
        else if (fp == ENT::unarynot)
                return "unary not"_s8;
        else if (fp == ENT::consttrueexpr)
                return "const true expr"_s8;
        else if (fp == ENT::matchsome)
                return "[matchsome]"_s8;
        else if (fp == ENT::matchallnodes)
                return "[matchallnodes]"_s8;
        else if (fp == ENT::matchanynodes)
                return "[matchanynodes]"_s8;
        else if (fp == ENT::constfalse)
                return "false"_s8;
        else if (fp == ENT::dummyop)
                return "<dummy>"_s8;
        else if (fp == ENT::matchallterms)
                return "(all terms)"_s8;
        else if (fp == ENT::matchanyterms)
                return "(any terms)"_s8;
        else if (fp == ENT::matchallphrases)
                return "(any phrases)"_s8;
        else if (fp == ENT::matchanyphrases)
                return "(any phrases)"_s8;
        else
                return "<other>"_s8;
}

static void PrintImpl(Buffer &b, const exec_node &n) {
        if (n.fp == ENT::unaryand) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                b.append("<AND>", ctx->expr);
        } else if (n.fp == ENT::unarynot) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                b.append("<NOT>", ctx->expr);
        } else if (n.fp == ENT::consttrueexpr) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                b.append("<", ctx->expr, ">");
        } else if (n.fp == ENT::logicaland) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                b.append("(", ctx->lhs, ansifmt::bold, " AND ", ansifmt::reset, ctx->rhs, ")");
        } else if (n.fp == ENT::logicalor) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                b.append("(", ctx->lhs, " OR ", ctx->rhs, ")");
        } else if (n.fp == ENT::logicalnot) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                b.append("(", ctx->lhs, " NOT ", ctx->rhs, ")");
        } else if (n.fp == ENT::matchterm)
                b.append(n.u16);
        else if (n.fp == ENT::matchphrase) {
                const auto p = static_cast<compilation_ctx::phrase *>(n.ptr);

                b.append('[');
                for (uint32_t i{0}; i != p->size; ++i)
                        b.append(p->termIDs[i], ',');
                b.shrink_by(1);
                b.append(']');
        } else if (n.fp == ENT::constfalse)
                b.append(false);
        else if (n.fp == ENT::dummyop)
                b.append(true);
        else if (n.fp == ENT::matchanyterms) {
                const auto *__restrict__ run = static_cast<const compilation_ctx::termsrun *>(n.ptr);

                b.append("ANY OF[");
                for (uint32_t i{0}; i != run->size; ++i)
                        b.append(run->terms[i], ',');
                b.shrink_by(1);
                b.append("]");
        } else if (n.fp == ENT::matchallterms) {
                const auto *__restrict__ run = static_cast<const compilation_ctx::termsrun *>(n.ptr);

                b.append("ALL OF[");
                for (uint32_t i{0}; i != run->size; ++i)
                        b.append(run->terms[i], ',');
                b.shrink_by(1);
                b.append("]");
        } else if (n.fp == ENT::matchallphrases) {
                const auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(n.ptr);

                b.append("ALLPHRASES:[");
                for (uint32_t k{0}; k != run->size; ++k) {
                        const auto p = run->phrases[k];

                        b.append('[');
                        for (uint32_t i{0}; i != p->size; ++i)
                                b.append(p->termIDs[i], ',');
                        b.shrink_by(1);
                        b.append(']');
                }
                b.append(']');
        } else if (n.fp == ENT::matchanyterms) {
                const auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(n.ptr);

                b.append("ANYPHRASES:[");
                for (uint32_t k{0}; k != run->size; ++k) {
                        const auto p = run->phrases[k];

                        b.append('[');
                        for (uint32_t i{0}; i != p->size; ++i)
                                b.append(p->termIDs[i], ',');
                        b.shrink_by(1);
                        b.append(']');
                }
                b.append(']');
        } else {
                b.append("Missing for ", impl_repr(n.fp));
        }
}

static exec_node compile(const ast_node *const n, compilation_ctx &cctx, simple_allocator &a) {
        static constexpr bool                        traceMetrics{false};
        uint64_t                                     before;
        std::vector<exec_term_id_t>                  terms;
        std::vector<const compilation_ctx::phrase *> phrases;
        std::vector<exec_node>                       stack;

        // First pass
        // Compile from AST tree to exec_nodes tree
        before = Timings::Microseconds::Tick();

        [[maybe_unused]] const auto compile_begin{before};
        auto                        root = compile_node(n, cctx, a);
        bool                        updates;

        if (traceMetrics || traceCompile)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to compile to:", root, "\n");

        if (root.fp == ENT::constfalse) {
                if (traceCompile)
                        SLog("Nothing to do, compile_node() compiled away the expr.\n");

                return {ENT::constfalse, {}};
        }

        if (traceCompile)
                SLog("Before second pass:", root, "\n");

        // Second pass
        // Optimize and expand
        before = Timings::Microseconds::Tick();
        do {
                // collapse and expand nodes
                // this was pulled out of optimize_node() in order to safeguard us from some edge conditions
                if (traceCompile)
                        SLog("Before Collapse:", root, "\n");
                collapse_node(root, cctx, a, terms, phrases, stack);

                if (traceCompile)
                        SLog("Before Expand:", root, "\n");

                expand_node(root, cctx, a, terms, phrases, stack);

                if (traceCompile)
                        SLog("Before Optimizations:", root, "\n");

                updates = false;
                root    = optimize_node(root, cctx, a, terms, phrases, stack, updates, &root);

                if (root.fp == ENT::constfalse || root.fp == ENT::dummyop) {
                        if (traceCompile)
                                SLog("Nothing to do (false OR noop)\n");

                        return {ENT::constfalse, {}};
                }
        } while (updates);

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to expand. Before third pass:", root, "\n");
        else if (traceCompile)
                SLog("Before third pass:", root, "\n");

        return root;
}

// Considers all binary ops, and potentiall swaps (lhs, rhs) of binary ops,
// but not based on actual cost but on heuristics
struct reorder_ctx final {
        bool dirty;
};

static void reorder(ast_node *n, reorder_ctx *const ctx) {
#if 0
#define set_dirty()                                                  \
        do {                                                         \
                if (traceCompile)                                    \
                        SLog("Dirty at ", __LINE__, ": ", *n, "\n"); \
                ctx->dirty = true;                                   \
        } while (0)
#else
#define set_dirty() ctx->dirty = true
#endif

        if (n->type == ast_node::Type::UnaryOp)
                reorder(n->unaryop.expr, ctx);
        else if (n->type == ast_node::Type::ConstTrueExpr)
                reorder(n->expr, ctx);
        else if (n->type == ast_node::Type::MatchSome) {
                for (uint32_t i{0}; i != n->match_some.size; ++i)
                        reorder(n->match_some.nodes[i], ctx);
        }
        if (n->type == ast_node::Type::BinOp) {
                const auto lhs = n->binop.lhs, rhs = n->binop.rhs;

                reorder(lhs, ctx);
                reorder(rhs, ctx);

                // First off, we will try to shift tokens to the left so that we will end up
                // with tokens before phrases, if that's possible, so that the compiler will get
                // a chance to identify long runs and reduce emitted ENT::logicaland, ENT::logicalor and ENT::logicalnot ops
                if (rhs->is_unary() && rhs->p->size == 1 && lhs->type == ast_node::Type::BinOp && lhs->binop.normalized_operator() == n->binop.normalized_operator() && lhs->binop.rhs->is_unary() && lhs->binop.rhs->p->size > 1) {
                        // (ipad OR "apple ipad") OR ipod => (ipad OR ipod) OR "apple ipad"
                        std::swap(*rhs, *lhs->binop.rhs);
                        set_dirty();
                        return;
                }

                if (rhs->type == ast_node::Type::BinOp && lhs->is_unary() && lhs->p->size > 1 && rhs->binop.normalized_operator() == n->binop.normalized_operator() && rhs->binop.lhs->is_unary() && rhs->binop.lhs->p->size == 1) {
                        // "video game" AND (warcraft AND ..) => warcraft AND ("Video game" AND ..)
                        std::swap(*lhs, *rhs->binop.lhs);
                        set_dirty();
                        return;
                }

                if ((n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND || n->binop.op == Operator::OR) && lhs->type == ast_node::Type::Phrase && lhs->p->size > 1 && rhs->type == ast_node::Type::Token) {
                        // ["video game" OR game] => [game OR "video game"]
                        std::swap(n->binop.lhs, n->binop.rhs);
                        set_dirty();
                        return;
                }

                if (n->binop.op == Operator::OR) {
                        // ((1 OR <2>) OR 3) => 1 OR 3 OR <2>
                        if (lhs->type == ast_node::Type::BinOp && lhs->binop.op == Operator::OR && lhs->binop.rhs->type == ast_node::Type::ConstTrueExpr && rhs->type != ast_node::Type::ConstTrueExpr) {
                                std::swap(*lhs->binop.rhs, *rhs);
                                set_dirty();
                                return;
                        }

                        if (rhs->type == ast_node::Type::ConstTrueExpr && lhs->type != ast_node::Type::ConstTrueExpr) {
                                // 09.06.2k17: make sure this makes sense
                                // [foo <the>]  => [ <the> foo ]
                                std::swap(n->binop.lhs, n->binop.rhs);
                                set_dirty();
                                return;
                        }

                        if (lhs->type != ast_node::Type::ConstTrueExpr && rhs->type == ast_node::Type::BinOp && rhs->binop.op == n->binop.op && rhs->binop.lhs->type == ast_node::Type::ConstTrueExpr) {
                                // 09.06.2k17: make sure this makes sense
                                // [foo ( <the> bar )] =>   [<the> (foo bar)]
                                std::swap(n->binop.lhs, rhs->binop.lhs);
                                set_dirty();
                                return;
                        }
                }

                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND) {
                        if (lhs->type == ast_node::Type::BinOp) {
                                if (rhs->is_unary()) {
                                        // [expr AND unary] => [unary AND expr]
                                        std::swap(n->binop.lhs, n->binop.rhs);
                                        set_dirty();
                                        return;
                                }
                        }

                        if (rhs->type == ast_node::Type::ConstTrueExpr && lhs->type != ast_node::Type::ConstTrueExpr) {
                                // 09.06.2k17: make sure this makes sense
                                // [foo <the>]  => [ <the> foo ]
                                std::swap(n->binop.lhs, n->binop.rhs);
                                set_dirty();
                                return;
                        }

                        if (lhs->type != ast_node::Type::ConstTrueExpr && rhs->type == ast_node::Type::BinOp && rhs->binop.op == n->binop.op && rhs->binop.lhs->type == ast_node::Type::ConstTrueExpr) {
                                // 09.06.2k17: make sure this makes sense
                                // [foo ( <the> bar )] =>   [<the> (foo bar)]
                                std::swap(n->binop.lhs, rhs->binop.lhs);
                                set_dirty();
                                return;
                        }
                } else if (n->binop.op == Operator::NOT) {
                        // (foo OR bar) NOT apple
                        // apple is cheaper to compute so we need to reverse those
                        // UPDATE: this is not necessarily true, but whatever the case, we 'll modify the final
                        // exec nodes tree anyway
                        if (rhs->is_unary() && lhs->type == ast_node::Type::BinOp) {
                                auto llhs = lhs->binop.lhs;
                                auto lrhs = lhs->binop.rhs;

                                if (llhs->is_unary() && lrhs->type == ast_node::Type::BinOp && (lhs->binop.op == Operator::AND || lhs->binop.op == Operator::STRICT_AND)) {
                                        // ((pizza AND (sf OR "san francisco")) NOT onions)
                                        // => (pizza NOT onions) AND (sf OR "san francisco")
                                        const auto saved = lhs->binop.op;

                                        lhs->binop.rhs = rhs;
                                        lhs->binop.op  = Operator::NOT;

                                        n->binop.op  = saved;
                                        n->binop.rhs = lrhs;

                                        set_dirty();
                                        return;
                                }
                        }
                }
        }
#undef set_dirty
}

// See: IMPLEMENTATION.md
// this is very important, for we need to move tokens before any phrases
// so that we can get a chance to group tokens together, and phrases together
static ast_node *reorder_root(ast_node *r) {
        reorder_ctx ctx;

        do {
                ctx.dirty = false;
                reorder(r, &ctx);
        } while (ctx.dirty);

        if (traceCompile)
                SLog("REORDERED:", *r, "\n");

        return r;
}

exec_node Trinity::compile_query(ast_node *root, compilation_ctx &cctx) {
        if (!root) {
                if (traceCompile)
                        SLog("No root node\n");

                return {ENT::constfalse, {}};
        }

        simple_allocator a(4096); // for temporaries

        // Reorder the tree to enable the compiler to create larger collections
        // root is from a clone of the original query, so we are free to modify it anyway we see fit with reorder_root()
        return compile(reorder_root(root), cctx, a);
}

void Trinity::group_execnodes(exec_node &n, simple_allocator &a) {
        if (n.fp == ENT::logicaland) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                group_execnodes(ctx->lhs, a);
                group_execnodes(ctx->rhs, a);

                range_base<exec_node *, std::uint16_t> r1, r2;

                r1.Set(&ctx->lhs, 1);
                r2.Set(&ctx->rhs, 1);

                if (ctx->lhs.fp == ENT::matchallnodes) {
                        auto g = static_cast<compilation_ctx::nodes_group *>(ctx->lhs.ptr);

                        r1.Set(g->nodes, g->size);
                }

                if (ctx->rhs.fp == ENT::matchallnodes) {
                        auto g = static_cast<compilation_ctx::nodes_group *>(ctx->rhs.ptr);

                        r2.Set(g->nodes, g->size);
                }

                auto g = static_cast<compilation_ctx::nodes_group *>(a.Alloc(sizeof(compilation_ctx::nodes_group) + sizeof(exec_node) * (r1.size() + r2.size())));

                g->size = r1.size() + r2.size();
                memcpy(g->nodes, r1.offset, r1.size() * sizeof(exec_node));
                memcpy(g->nodes + r1.size(), r2.offset, r2.size() * sizeof(exec_node));

                n.fp  = ENT::matchallnodes;
                n.ptr = g;
        } else if (n.fp == ENT::logicalor) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                group_execnodes(ctx->lhs, a);
                group_execnodes(ctx->rhs, a);

                range_base<exec_node *, std::uint16_t> r1, r2;

                r1.Set(&ctx->lhs, 1);
                r2.Set(&ctx->rhs, 1);

                if (ctx->lhs.fp == ENT::matchanynodes) {
                        auto g = static_cast<compilation_ctx::nodes_group *>(ctx->lhs.ptr);

                        r1.Set(g->nodes, g->size);
                }

                if (ctx->rhs.fp == ENT::matchanynodes) {
                        auto g = static_cast<compilation_ctx::nodes_group *>(ctx->rhs.ptr);

                        r2.Set(g->nodes, g->size);
                }

                auto g = static_cast<compilation_ctx::nodes_group *>(a.Alloc(sizeof(compilation_ctx::nodes_group) + sizeof(exec_node) * (r1.size() + r2.size())));

                g->size = r1.size() + r2.size();
                memcpy(g->nodes, r1.offset, r1.size() * sizeof(exec_node));
                memcpy(g->nodes + r1.size(), r2.offset, r2.size() * sizeof(exec_node));

                n.fp  = ENT::matchanynodes;
                n.ptr = g;
        } else if (n.fp == ENT::logicalnot) {
                auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                group_execnodes(ctx->lhs, a);
                group_execnodes(ctx->rhs, a);
        } else if (n.fp == ENT::unaryand || n.fp == ENT::unarynot || n.fp == ENT::consttrueexpr) {
                auto *const ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                group_execnodes(ctx->expr, a);
        } else if (n.fp == ENT::matchsome) {
                auto pm = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != pm->size; ++i)
                        group_execnodes(pm->nodes[i], a);
        }
}
