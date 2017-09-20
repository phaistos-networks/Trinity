#include "exec.h"
#include "docset_spans.h"
#include "docwordspace.h"
#include "matches.h"
#include "queryexec_ctx.h"
#include "similarity.h"
#include <prioqueue.h>


using namespace Trinity;
thread_local Trinity::queryexec_ctx *curRCTX;

namespace // static/local this module
{
        [[maybe_unused]] static constexpr bool traceExec{false};
        static constexpr bool traceCompile{false};
}

static uint64_t reorder_execnode(exec_node &n, bool &updates, queryexec_ctx &rctx);

//WAS: return rctx.term_ctx(p->termIDs[0]).documents;
//Now it's proprortional to the number of terms of the phrase
//because we 'd like to avoid avoid materializing hits for un-necessary documents if possible, and we
//need to materialize for processing phrases
static uint64_t phrase_cost(queryexec_ctx &rctx, const queryexec_ctx::phrase *const p)
{
        // boost it because its's a phrase so we will need to deserialize hits
        //
        // XXX: we only consider the lead here but that's perhaps not a great idea
        // We should take the phrase size into account, so that a phrase "a b c" should be more expensive than "a b"
        // where they have the same lead, but size is shorter. Conversely "a b" should be more expensive than "a c" if
        // b is more popular than rc
        return rctx.term_ctx(p->termIDs[0]).documents + UINT32_MAX + UINT16_MAX * p->size;
}

static uint64_t reorder_execnode_impl(exec_node &n, bool &updates, queryexec_ctx &rctx)
{
        if (n.fp == ENT::matchterm)
                return rctx.term_ctx(n.u16).documents;
        else if (n.fp == ENT::matchphrase)
        {
                const auto p = static_cast<const queryexec_ctx::phrase *>(n.ptr);

                return phrase_cost(rctx, p);
        }
        else if (n.fp == ENT::logicaland)
        {
                auto ctx = static_cast<queryexec_ctx::binop_ctx *>(n.ptr);
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
        else if (n.fp == ENT::logicalnot)
        {
                auto *const ctx = static_cast<queryexec_ctx::binop_ctx *>(n.ptr);
                const auto lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                [[maybe_unused]] const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                return lhsCost;
        }
        else if (n.fp == ENT::logicalor)
        {
                auto *const ctx = static_cast<queryexec_ctx::binop_ctx *>(n.ptr);
                const auto lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                return lhsCost + rhsCost;
        }
        else if (n.fp == ENT::unaryand || n.fp == ENT::unarynot)
        {
                auto *const ctx = static_cast<queryexec_ctx::unaryop_ctx *>(n.ptr);

                return reorder_execnode(ctx->expr, updates, rctx);
        }
        else if (n.fp == ENT::consttrueexpr)
        {
                auto ctx = static_cast<queryexec_ctx::unaryop_ctx *>(n.ptr);

                // it is important to return UINT64_MAX - 1 so that it will not result in a binop's (lhs, rhs) swap
                // we need to special-case the handling of those nodes
                reorder_execnode(ctx->expr, updates, rctx);
                return UINT64_MAX - 1;
        }
        else if (n.fp == ENT::matchsome)
        {
                auto pm = static_cast<queryexec_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != pm->size; ++i)
                        reorder_execnode(pm->nodes[i], updates, rctx);
                return UINT64_MAX - 1;
        }
        else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes)
        {
                auto g = static_cast<queryexec_ctx::nodes_group *>(n.ptr);

                for (uint32_t i{0}; i != g->size; ++i)
                        reorder_execnode(g->nodes[i], updates, rctx);
                return UINT64_MAX - 1;
        }
        else if (n.fp == ENT::matchallterms)
        {
                const auto run = static_cast<const queryexec_ctx::termsrun *>(n.ptr);

                return rctx.term_ctx(run->terms[0]).documents;
        }
        else if (n.fp == ENT::matchanyterms)
        {
                const auto run = static_cast<const queryexec_ctx::termsrun *>(n.ptr);
                uint64_t sum{0};

                for (uint32_t i{0}; i != run->size; ++i)
                        sum += rctx.term_ctx(run->terms[i]).documents;
                return sum;
        }
        else if (n.fp == ENT::matchallphrases)
        {
                const auto *const __restrict__ run = (const queryexec_ctx::phrasesrun *)n.ptr;

                return phrase_cost(rctx, run->phrases[0]) * run->size; // This may or may not make sense
        }
        else if (n.fp == ENT::matchanyphrases)
        {
                const auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)n.ptr;
                uint64_t sum{0};

                for (uint32_t i{0}; i != run->size; ++i)
                        sum += phrase_cost(rctx, run->phrases[i]);
                return sum;
        }
        else
        {
                std::abort();
        }
}

uint64_t reorder_execnode(exec_node &n, bool &updates, queryexec_ctx &rctx)
{
        return n.cost = reorder_execnode_impl(n, updates, rctx);
}

static exec_node reorder_execnodes(exec_node n, queryexec_ctx &rctx)
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
#if 0
#define set_dirty()                                                  \
        do                                                           \
        {                                                            \
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
        else if (n->type == ast_node::Type::MatchSome)
        {
                for (uint32_t i{0}; i != n->match_some.size; ++i)
                        reorder(n->match_some.nodes[i], ctx);
        }
        if (n->type == ast_node::Type::BinOp)
        {
                const auto lhs = n->binop.lhs, rhs = n->binop.rhs;

                reorder(lhs, ctx);
                reorder(rhs, ctx);

                // First off, we will try to shift tokens to the left so that we will end up
                // with tokens before phrases, if that's possible, so that the compiler will get
                // a chance to identify long runs and reduce emitted ENT::logicaland, ENT::logicalor and ENT::logicalnot ops
                if (rhs->is_unary() && rhs->p->size == 1 && lhs->type == ast_node::Type::BinOp && lhs->binop.normalized_operator() == n->binop.normalized_operator() && lhs->binop.rhs->is_unary() && lhs->binop.rhs->p->size > 1)
                {
                        // (ipad OR "apple ipad") OR ipod => (ipad OR ipod) OR "apple ipad"
                        std::swap(*rhs, *lhs->binop.rhs);
                        set_dirty();
                        return;
                }

                if (rhs->type == ast_node::Type::BinOp && lhs->is_unary() && lhs->p->size > 1 && rhs->binop.normalized_operator() == n->binop.normalized_operator() && rhs->binop.lhs->is_unary() && rhs->binop.lhs->p->size == 1)
                {
                        // "video game" AND (warcraft AND ..) => warcraft AND ("Video game" AND ..)
                        std::swap(*lhs, *rhs->binop.lhs);
                        set_dirty();
                        return;
                }

                if ((n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND || n->binop.op == Operator::OR) && lhs->type == ast_node::Type::Phrase && lhs->p->size > 1 && rhs->type == ast_node::Type::Token)
                {
                        // ["video game" OR game] => [game OR "video game"]
                        std::swap(n->binop.lhs, n->binop.rhs);
                        set_dirty();
                        return;
                }

                if (n->binop.op == Operator::OR)
                {
                        // ((1 OR <2>) OR 3) => 1 OR 3 OR <2>
                        if (lhs->type == ast_node::Type::BinOp && lhs->binop.op == Operator::OR && lhs->binop.rhs->type == ast_node::Type::ConstTrueExpr && rhs->type != ast_node::Type::ConstTrueExpr)
                        {
                                std::swap(*lhs->binop.rhs, *rhs);
                                set_dirty();
                                return;
                        }

                        if (rhs->type == ast_node::Type::ConstTrueExpr && lhs->type != ast_node::Type::ConstTrueExpr)
                        {
                                // 09.06.2k17: make sure this makes sense
                                // [foo <the>]  => [ <the> foo ]
                                std::swap(n->binop.lhs, n->binop.rhs);
                                set_dirty();
                                return;
                        }

                        if (lhs->type != ast_node::Type::ConstTrueExpr && rhs->type == ast_node::Type::BinOp && rhs->binop.op == n->binop.op && rhs->binop.lhs->type == ast_node::Type::ConstTrueExpr)
                        {
                                // 09.06.2k17: make sure this makes sense
                                // [foo ( <the> bar )] =>   [<the> (foo bar)]
                                std::swap(n->binop.lhs, rhs->binop.lhs);
                                set_dirty();
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
                                        set_dirty();
                                        return;
                                }
                        }

                        if (rhs->type == ast_node::Type::ConstTrueExpr && lhs->type != ast_node::Type::ConstTrueExpr)
                        {
                                // 09.06.2k17: make sure this makes sense
                                // [foo <the>]  => [ <the> foo ]
                                std::swap(n->binop.lhs, n->binop.rhs);
                                set_dirty();
                                return;
                        }

                        if (lhs->type != ast_node::Type::ConstTrueExpr && rhs->type == ast_node::Type::BinOp && rhs->binop.op == n->binop.op && rhs->binop.lhs->type == ast_node::Type::ConstTrueExpr)
                        {
                                // 09.06.2k17: make sure this makes sense
                                // [foo ( <the> bar )] =>   [<the> (foo bar)]
                                std::swap(n->binop.lhs, rhs->binop.lhs);
                                set_dirty();
                                return;
                        }
                }
                else if (n->binop.op == Operator::NOT)
                {
                        // (foo OR bar) NOT apple
                        // apple is cheaper to compute so we need to reverse those
                        // UPDATE: this is not necessarily true, but whatever the case, we 'll modify the final
                        // exec nodes tree anyway
                        if (rhs->is_unary() && lhs->type == ast_node::Type::BinOp)
                        {
                                auto llhs = lhs->binop.lhs;
                                auto lrhs = lhs->binop.rhs;

                                if (llhs->is_unary() && lrhs->type == ast_node::Type::BinOp && (lhs->binop.op == Operator::AND || lhs->binop.op == Operator::STRICT_AND))
                                {
                                        // ((pizza AND (sf OR "san francisco")) NOT onions)
                                        // => (pizza NOT onions) AND (sf OR "san francisco")
                                        const auto saved = lhs->binop.op;

                                        lhs->binop.rhs = rhs;
                                        lhs->binop.op = Operator::NOT;

                                        n->binop.op = saved;
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

static auto impl_repr(const ENT fp) noexcept
{
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
        if ((lhs.fp == ENT::matchterm || lhs.fp == ENT::matchphrase || lhs.fp == fp || lhs.fp == fp2) && (rhs.fp == ENT::matchterm || rhs.fp == ENT::matchphrase || rhs.fp == fp || rhs.fp == fp2))
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
        auto opctx = static_cast<queryexec_ctx::binop_ctx *>(res.ptr);

        if (auto ptr = try_collect_impl(opctx->lhs, opctx->rhs, a, fp, fp2))
        {
                res.fp = reinterpret_cast<decltype(res.fp)>(fp);
                res.ptr = ptr;
                return true;
        }
        else
                return false;
}

static exec_node compile_node(const ast_node *const n, queryexec_ctx &rctx, simple_allocator &a)
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
                                res.fp = ENT::matchterm;
                        else
                                res.fp = ENT::constfalse;
                        break;

                case ast_node::Type::Phrase:
                        if (n->p->size == 1)
                        {
                                res.u16 = rctx.register_token(n->p);
                                if (res.u16)
                                        res.fp = ENT::matchterm;
                                else
                                        res.fp = ENT::constfalse;
                        }
                        else
                        {
                                res.ptr = rctx.register_phrase(n->p);
                                if (res.ptr)
                                        res.fp = ENT::matchphrase;
                                else
                                        res.fp = ENT::constfalse;
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
                }
                break;

                case ast_node::Type::ConstFalse:
                        res.fp = ENT::constfalse;
                        break;

                case ast_node::Type::MatchSome:
                        res.fp = ENT::matchsome;
                        {
                                auto pm = (queryexec_ctx::partial_match_ctx *)a.Alloc(sizeof(queryexec_ctx::partial_match_ctx) + sizeof(exec_node) * n->match_some.size);

                                pm->min = n->match_some.min;
                                pm->size = n->match_some.size;

                                for (uint32_t i{0}; i != n->match_some.size; ++i)
                                        pm->nodes[i] = compile_node(n->match_some.nodes[i], rctx, a);

                                res.ptr = pm;
                        }
                        break;

                case ast_node::Type::UnaryOp:
                        switch (n->unaryop.op)
                        {
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
                        res.ptr = rctx.register_unaryop(compile_node(n->unaryop.expr, rctx, a));
                        break;

                case ast_node::Type::ConstTrueExpr:
                        // use register_unaryop() for this as well
                        // no need for another register_x()
                        res.ptr = rctx.register_unaryop(compile_node(n->expr, rctx, a));
                        if (static_cast<queryexec_ctx::unaryop_ctx *>(res.ptr)->expr.fp == ENT::constfalse)
                                res.fp = ENT::dummyop;
                        else
                                res.fp = ENT::consttrueexpr;
                        break;
        }

        return res;
}

static const exec_node *stronger(const exec_node &a, const exec_node &b) noexcept
{
        if (a.fp == ENT::matchallphrases || a.fp == ENT::matchphrase)
                return &a;
        else
                return &b;
}

static bool same(const exec_node &a, const exec_node &b) noexcept
{
        if (a.fp == ENT::matchallterms && b.fp == a.fp)
        {
                const auto *const __restrict__ runa = (queryexec_ctx::termsrun *)a.ptr;
                const auto *const __restrict__ runb = (queryexec_ctx::termsrun *)b.ptr;

                return *runa == *runb;
        }
        else if (a.fp == ENT::matchterm && b.fp == a.fp)
                return a.u16 == b.u16;
        else if (a.fp == ENT::matchphrase && b.fp == ENT::matchphrase)
        {
                const auto *const __restrict__ pa = (queryexec_ctx::phrase *)a.ptr;
                const auto *const __restrict__ pb = (queryexec_ctx::phrase *)b.ptr;

                return *pa == *pb;
        }

        return false;
}

static void group_execnodes(exec_node &n, simple_allocator &a)
{
        if (n.fp == ENT::logicaland)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

                group_execnodes(ctx->lhs, a);
                group_execnodes(ctx->rhs, a);

                range_base<exec_node *, std::uint16_t> r1, r2;

                r1.Set(&ctx->lhs, 1);
                r2.Set(&ctx->rhs, 1);

                if (ctx->lhs.fp == ENT::matchallnodes)
                {
                        auto g = static_cast<queryexec_ctx::nodes_group *>(ctx->lhs.ptr);

                        r1.Set(g->nodes, g->size);
                }

                if (ctx->rhs.fp == ENT::matchallnodes)
                {
                        auto g = static_cast<queryexec_ctx::nodes_group *>(ctx->rhs.ptr);

                        r2.Set(g->nodes, g->size);
                }

                auto g = (queryexec_ctx::nodes_group *)a.Alloc(sizeof(queryexec_ctx::nodes_group) + sizeof(exec_node) * (r1.size() + r2.size()));

                g->size = r1.size() + r2.size();
                memcpy(g->nodes, r1.offset, r1.size() * sizeof(exec_node));
                memcpy(g->nodes + r1.size(), r2.offset, r2.size() * sizeof(exec_node));

                n.fp = ENT::matchallnodes;
                n.ptr = g;
        }
        else if (n.fp == ENT::logicalor)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

		group_execnodes(ctx->lhs, a);
		group_execnodes(ctx->rhs, a);

                range_base<exec_node *, std::uint16_t> r1, r2;

                r1.Set(&ctx->lhs, 1);
                r2.Set(&ctx->rhs, 1);

                if (ctx->lhs.fp == ENT::matchanynodes)
                {
                        auto g = static_cast<queryexec_ctx::nodes_group *>(ctx->lhs.ptr);

                        r1.Set(g->nodes, g->size);
                }

                if (ctx->rhs.fp == ENT::matchanynodes)
                {
                        auto g = static_cast<queryexec_ctx::nodes_group *>(ctx->rhs.ptr);

                        r2.Set(g->nodes, g->size);
                }

                auto g = (queryexec_ctx::nodes_group *)a.Alloc(sizeof(queryexec_ctx::nodes_group) + sizeof(exec_node) * (r1.size() + r2.size()));

                g->size = r1.size() + r2.size();
                memcpy(g->nodes, r1.offset, r1.size() * sizeof(exec_node));
                memcpy(g->nodes + r1.size(), r2.offset, r2.size() * sizeof(exec_node));

                n.fp = ENT::matchanynodes;
                n.ptr = g;
        }
        else if (n.fp == ENT::logicalnot)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

		group_execnodes(ctx->lhs, a);
		group_execnodes(ctx->rhs, a);
        }
        else if (n.fp == ENT::unaryand || n.fp == ENT::unarynot || n.fp == ENT::consttrueexpr)
        {
                auto *const ctx = static_cast<queryexec_ctx::unaryop_ctx *>(n.ptr);

                group_execnodes(ctx->expr, a);
        }
        else if (n.fp == ENT::matchsome)
        {
                auto pm = static_cast<queryexec_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != pm->size; ++i)
                        group_execnodes(pm->nodes[i], a);
        }
}

static void collapse_node(exec_node &n, queryexec_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const queryexec_ctx::phrase *> &phrases, std::vector<exec_node> &stack)
{
        if (n.fp == ENT::consttrueexpr || n.fp == ENT::unaryand || n.fp == ENT::unarynot)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                collapse_node(ctx->expr, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::matchsome)
        {
                auto pm = static_cast<queryexec_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != pm->size; ++i)
                        collapse_node(pm->nodes[i], rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes)
        {
                auto g = static_cast<queryexec_ctx::nodes_group *>(n.ptr);

                for (uint32_t i{0}; i != g->size; ++i)
                        collapse_node(g->nodes[i], rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;
                queryexec_ctx::binop_ctx *otherCtx;

                collapse_node(ctx->lhs, rctx, a, terms, phrases, stack);
                collapse_node(ctx->rhs, rctx, a, terms, phrases, stack);

                if (n.fp == ENT::logicaland)
                {
                        if (try_collect(n, a, ENT::SPECIALIMPL_COLLECTION_LOGICALAND, ENT::matchallterms))
                                return;

                        if ((ctx->lhs.fp == ENT::matchterm || ctx->lhs.fp == ENT::matchphrase || ctx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND) && ctx->rhs.fp == ENT::logicaland && ((otherCtx = (queryexec_ctx::binop_ctx *)ctx->rhs.ptr)->lhs.fp == ENT::matchterm || otherCtx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND || otherCtx->lhs.fp == ENT::matchphrase))
                        {
                                // lord AND (of AND (the AND rings))
                                // (lord of) AND (the AND rings)
                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                ctx->lhs.fp = (decltype(ctx->lhs.fp))ENT::SPECIALIMPL_COLLECTION_LOGICALAND;
                                ctx->lhs.ptr = collection;
                                ctx->rhs = otherCtx->rhs;
                                return;
                        }

                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::consttrueexpr)
                        {
                                // [<foo> AND <bar>] => [ <foo,bar> ]
                                auto *const __restrict__ lhsCtx = (queryexec_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (queryexec_ctx::unaryop_ctx *)ctx->rhs.ptr;

                                if (auto ptr = try_collect_impl(lhsCtx->expr, rhsCtx->expr, a, ENT::SPECIALIMPL_COLLECTION_LOGICALAND, ENT::matchallterms))
                                {
                                        // reuse lhsCtx
                                        lhsCtx->expr.fp = reinterpret_cast<decltype(lhsCtx->expr.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALAND);
                                        lhsCtx->expr.ptr = ptr;

                                        n.ptr = lhsCtx;
                                        n.fp = ENT::consttrueexpr;
                                        return;
                                }
                        }

                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::logicaland)
                        {
                                // <foo> AND (<bar> AND whatever)
                                // <foo, bar> AND whatever
                                auto *const __restrict__ lhsCtx = (queryexec_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (queryexec_ctx::binop_ctx *)ctx->rhs.ptr;

                                if (rhsCtx->lhs.fp == ENT::consttrueexpr)
                                {
                                        auto *const __restrict__ otherCtx = (queryexec_ctx::unaryop_ctx *)rhsCtx->lhs.ptr;

                                        if (auto ptr = try_collect_impl(lhsCtx->expr,
                                                                        otherCtx->expr, a, ENT::SPECIALIMPL_COLLECTION_LOGICALAND, ENT::matchallterms))
                                        {
                                                lhsCtx->expr.fp = reinterpret_cast<decltype(lhsCtx->expr.fp)>(ENT::SPECIALIMPL_COLLECTION_LOGICALAND);
                                                lhsCtx->expr.ptr = ptr;

                                                ctx->rhs = rhsCtx->rhs;
                                                return;
                                        }
                                }
                        }
                }
                else if (n.fp == ENT::logicalor)
                {
                        if (try_collect(n, a, ENT::SPECIALIMPL_COLLECTION_LOGICALOR, ENT::matchanyterms))
                                return;

                        if ((ctx->lhs.fp == ENT::matchterm || ctx->lhs.fp == ENT::matchphrase || ctx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR) && ctx->rhs.fp == ENT::logicalor && ((otherCtx = (queryexec_ctx::binop_ctx *)ctx->rhs.ptr)->lhs.fp == ENT::matchterm || otherCtx->lhs.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR || otherCtx->lhs.fp == ENT::matchphrase))
                        {
                                // lord OR (ring OR (rings OR towers))
                                // (lord OR ring) OR (rings OR towers)
                                auto collection = execnodes_collection::make(a, ctx->lhs, otherCtx->lhs);

                                ctx->lhs.fp = (decltype(ctx->lhs.fp))ENT::SPECIALIMPL_COLLECTION_LOGICALOR;
                                ctx->lhs.ptr = collection;
                                ctx->rhs = otherCtx->rhs;
                                return;
                        }

#if 0 // this is wrong, because we don't want e.g [ FOO <BAR> | <PONG> ] to [ FOO AND <FOO | PONG > ]
                        if (ctx->lhs.fp == ENT::consttrueexpr && ctx->rhs.fp == ENT::consttrueexpr)
                        {
                                // [<foo> OR <bar>] => [ OR<foo, bar> ]
                                auto *const __restrict__ lhsCtx = (queryexec_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (queryexec_ctx::unaryop_ctx *)ctx->rhs.ptr;

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
                                auto *const __restrict__ lhsCtx = (queryexec_ctx::unaryop_ctx *)ctx->lhs.ptr;
                                auto *const __restrict__ rhsCtx = (queryexec_ctx::binop_ctx *)ctx->rhs.ptr;

                                if (rhsCtx->lhs.fp == ENT::consttrueexpr)
                                {
                                        auto *const __restrict__ otherCtx = (queryexec_ctx::unaryop_ctx *)rhsCtx->lhs.ptr;

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

static void trim_phrasesrun(exec_node &n, queryexec_ctx::phrasesrun *pr)
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
                n.fp = ENT::matchphrase;
                n.ptr = pr->phrases[0];
        }
        else
        {
                std::sort(pr->phrases, pr->phrases + pr->size, [](const auto a, const auto b) {
                        return a->size < b->size;
                });
        }
}

static void expand_node(exec_node &n, queryexec_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const queryexec_ctx::phrase *> &phrases, std::vector<exec_node> &stack)
{
        if (n.fp == ENT::consttrueexpr || n.fp == ENT::unaryand || n.fp == ENT::unarynot)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                expand_node(ctx->expr, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::matchsome)
        {
                auto ctx = static_cast<queryexec_ctx::partial_match_ctx *>(n.ptr);

                for (uint32_t i{0}; i != ctx->size; ++i)
                        expand_node(ctx->nodes[i], rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes)
        {
                auto g = static_cast<queryexec_ctx::nodes_group *>(n.ptr);

                for (uint32_t i{0}; i != g->size; ++i)
                        expand_node(g->nodes[i], rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

                expand_node(ctx->lhs, rctx, a, terms, phrases, stack);
                expand_node(ctx->rhs, rctx, a, terms, phrases, stack);
        }
        else if (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR || n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND)
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

                        if (en.fp == ENT::matchterm)
                        {
                                const auto termID = exec_term_id_t(en.u16);

                                terms.push_back(termID);
                        }
                        else if (en.fp == ENT::matchphrase)
                        {
                                const auto p = (queryexec_ctx::phrase *)en.ptr;

                                phrases.push_back(p);
                        }
                        else if (en.fp == ENT::matchallterms || en.fp == ENT::matchanyterms)
                        {
                                const auto run = static_cast<const queryexec_ctx::termsrun *>(en.ptr);

                                terms.insert(terms.end(), run->terms, run->terms + run->size);
                        }
                        else if (en.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR || en.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALAND)
                        {
                                auto ctx = (execnodes_collection *)en.ptr;

                                stack.push_back(ctx->a);
                                stack.push_back(ctx->b);
                        }
                } while (stack.size());

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
                if (const auto termsCnt = terms.size(), phrasesCnt = phrases.size(); termsCnt == 1)
                {
                        exec_node termNode;

                        termNode.fp = ENT::matchterm;
                        termNode.u16 = terms.front();

                        if (phrasesCnt == 0)
                                n = termNode;
                        else if (phrasesCnt == 1)
                        {
                                exec_node phraseMatchNode;

                                phraseMatchNode.fp = ENT::matchphrase;
                                phraseMatchNode.ptr = (void *)phrases.front();

                                n.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = rctx.register_binop(termNode, phraseMatchNode);
                        }
                        else
                        {
                                exec_node phrasesRunNode;
                                auto phrasesRun = (queryexec_ctx::phrasesrun *)rctx.allocator.Alloc(sizeof(queryexec_ctx::phrasesrun) + phrasesCnt * sizeof(queryexec_ctx::phrase *));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(queryexec_ctx::phrase *));

                                phrasesRunNode.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyphrases : ENT::matchallphrases);
                                phrasesRunNode.ptr = (void *)phrasesRun;

                                n.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = rctx.register_binop(termNode, phrasesRunNode);

                                trim_phrasesrun(n, phrasesRun);
                        }
                }
                else if (termsCnt > 1)
                {
                        auto run = (queryexec_ctx::termsrun *)rctx.runsAllocator.Alloc(sizeof(queryexec_ctx::termsrun) + sizeof(exec_term_id_t) * termsCnt);
                        exec_node runNode;

                        run->size = termsCnt;
                        memcpy(run->terms, terms.data(), termsCnt * sizeof(exec_term_id_t));
                        runNode.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyterms : ENT::matchallterms);
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

                                phraseNode.fp = ENT::matchphrase;
                                phraseNode.ptr = (void *)(phrases.front());

                                n.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = rctx.register_binop(runNode, phraseNode);
                        }
                        else
                        {
                                exec_node phrasesRunNode;
                                auto phrasesRun = (queryexec_ctx::phrasesrun *)rctx.allocator.Alloc(sizeof(queryexec_ctx::phrasesrun) + phrasesCnt * sizeof(queryexec_ctx::phrase *));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(queryexec_ctx::phrase *));

                                phrasesRunNode.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyphrases : ENT::matchallphrases);
                                phrasesRunNode.ptr = (void *)phrasesRun;

                                trim_phrasesrun(phrasesRunNode, phrasesRun);

                                n.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::logicalor : ENT::logicaland);
                                n.ptr = rctx.register_binop(runNode, phrasesRunNode);
                        }
                }
                else if (termsCnt == 0)
                {
                        if (phrasesCnt == 1)
                        {
                                n.fp = ENT::matchphrase;
                                n.ptr = (void *)phrases.front();
                        }
                        else
                        {
                                auto phrasesRun = (queryexec_ctx::phrasesrun *)rctx.allocator.Alloc(sizeof(queryexec_ctx::phrasesrun) + phrasesCnt * sizeof(queryexec_ctx::phrase *));

                                phrasesRun->size = phrasesCnt;
                                memcpy(phrasesRun->phrases, phrases.data(), phrasesCnt * sizeof(queryexec_ctx::phrase *));

                                n.fp = (n.fp == ENT::SPECIALIMPL_COLLECTION_LOGICALOR ? ENT::matchanyphrases : ENT::matchallphrases);
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
        else if (n.fp == ENT::matchallterms || n.fp == ENT::matchanyterms)
        {
                const auto run = static_cast<const queryexec_ctx::termsrun *>(n.ptr);

                if (run->size == 1)
                {
                        n.fp = ENT::matchterm;
                        n.u16 = run->terms[0];
                }
        }
        else if (n.fp == ENT::matchphrase)
        {
                const auto p = (queryexec_ctx::phrase *)n.ptr;

                if (p->size == 1)
                {
                        n.fp = ENT::matchterm;
                        n.u16 = p->termIDs[0];
                }
        }
}

static exec_node optimize_node(exec_node n, queryexec_ctx &rctx, simple_allocator &a, std::vector<exec_term_id_t> &terms, std::vector<const queryexec_ctx::phrase *> &phrases, std::vector<exec_node> &stack, bool &updates, const exec_node *const root)
{
        const auto saved{n};

        if (traceCompile)
                SLog("Before OPT:", n, "\n");

#define set_dirty()                                                     \
        do                                                              \
        {                                                               \
                if (traceCompile)                                       \
                        SLog("HERE from [", saved, "] to [", n, "]\n"); \
                updates = true;                                         \
        } while (0)

        if (n.fp == ENT::consttrueexpr)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = optimize_node(ctx->expr, rctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == ENT::constfalse || ctx->expr.fp == ENT::dummyop)
                {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        }
	else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes)
	{
		auto g = static_cast<queryexec_ctx::nodes_group *>(n.ptr);

		if (0 == g->size)
		{
			n.fp = ENT::constfalse;
			set_dirty();
		}
	}
        else if (n.fp == ENT::matchsome)
        {
                auto ctx = static_cast<queryexec_ctx::partial_match_ctx *>(n.ptr);
                const auto saved{ctx->size};

                for (uint32_t i{0}; i < ctx->size; ++i)
                {
                        ctx->nodes[i] = optimize_node(ctx->nodes[i], rctx, a, terms, phrases, stack, updates, root);

                        if (ctx->nodes[i].fp == ENT::constfalse || ctx->nodes[i].fp == ENT::dummyop)
                                ctx->nodes[i] = ctx->nodes[--(ctx->size)];
                        else
                                ++i;
                }

                if (ctx->min > ctx->size)
                {
                        n.fp = ENT::constfalse;
                        set_dirty();
                }
                else
                {
                        if (ctx->size == 1)
                        {
                                n = ctx->nodes[0];
                                set_dirty();
                        }
                        else if (ctx->min == ctx->size)
                        {
                                // transform to binary op that includes all those
                                auto en = ctx->nodes[0];

                                for (uint32_t i{1}; i != ctx->size; ++i)
                                {
                                        auto b = (queryexec_ctx::binop_ctx *)a.Alloc(sizeof(queryexec_ctx::binop_ctx));

                                        b->lhs = en;
                                        b->rhs = ctx->nodes[i];
                                        en.fp = ENT::logicaland;
                                        en.ptr = b;
                                }

                                n = en;
                                set_dirty();
                        }
                        else if (ctx->min == 1)
                        {
                                // TODO: consider expanding to OR sequence
                        }
                        else if (ctx->size != saved)
                        {
                                set_dirty();
                        }
                }
        }
        else if (n.fp == ENT::unaryand)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = optimize_node(ctx->expr, rctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == ENT::constfalse)
                {
                        n.fp = ENT::constfalse;
                        set_dirty();
                }
                else if (ctx->expr.fp == ENT::dummyop)
                {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        }
        else if (n.fp == ENT::unarynot)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                ctx->expr = optimize_node(ctx->expr, rctx, a, terms, phrases, stack, updates, root);
                if (ctx->expr.fp == ENT::dummyop)
                {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        }
        else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;
                queryexec_ctx::binop_ctx *otherCtx;

                ctx->lhs = optimize_node(ctx->lhs, rctx, a, terms, phrases, stack, updates, root);
                ctx->rhs = optimize_node(ctx->rhs, rctx, a, terms, phrases, stack, updates, root);

                if (ctx->lhs.fp == ENT::dummyop && ctx->rhs.fp == ENT::dummyop)
                {
                        n.fp = ENT::dummyop;
                        set_dirty();
                        return n;
                }
                else if (ctx->rhs.fp == ENT::dummyop)
                {
                        n = ctx->lhs;
                        set_dirty();
                        return n;
                }
                else if (ctx->lhs.fp == ENT::dummyop)
                {
                        n = ctx->rhs;
                        set_dirty();
                        return n;
                }

                if (n.fp == ENT::logicalor)
                {
                        if (ctx->lhs.fp == ENT::constfalse)
                        {
                                if (ctx->rhs.fp == ENT::constfalse)
                                {
                                        n.fp = ENT::constfalse;
                                        set_dirty();
                                }
                                else
                                {
                                        n = ctx->rhs;
                                        set_dirty();
                                }
                                return n;
                        }
                        else if (ctx->rhs.fp == ENT::constfalse)
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (same(ctx->lhs, ctx->rhs))
                        {
                                auto s = stronger(ctx->lhs, ctx->rhs);

                                if (s == &ctx->lhs)
                                        n = ctx->rhs;
                                else
                                        n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == ENT::constfalse && ctx->rhs.fp == ENT::constfalse)
                        {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == ENT::constfalse)
                        {
                                n = ctx->rhs;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->rhs.fp == ENT::constfalse)
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == ENT::matchphrase && ctx->rhs.fp == ENT::matchallterms)
                        {
                                // ([1,2] OR ALL OF[3,1,2]) => ALL OF [3,1,2]
                                const auto *const __restrict__ run = (queryexec_ctx::termsrun *)ctx->rhs.ptr;
                                const auto *const phrase = (queryexec_ctx::phrase *)ctx->lhs.ptr;

                                if (phrase->intersected_by(run))
                                {
                                        n = ctx->rhs;
                                        set_dirty();
                                        return n;
                                }
                        }
                        else if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchphrase)
                        {
                                // ([1,2] OR ALL OF[1,3,2]) => ALL OF [1,3,2]
                                const auto *const __restrict__ run = (queryexec_ctx::termsrun *)ctx->lhs.ptr;
                                const auto *const phrase = (queryexec_ctx::phrase *)ctx->rhs.ptr;

                                if (phrase->intersected_by(run))
                                {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }
                        else if (ctx->lhs.fp == ENT::consttrueexpr)
                        {
                                auto c = static_cast<const queryexec_ctx::unaryop_ctx *>(ctx->lhs.ptr);

                                ctx->lhs = c->expr;
                                set_dirty();
                                return n;
                        }
                        if (ctx->rhs.fp == ENT::consttrueexpr)
                        {
                                auto c = static_cast<const queryexec_ctx::unaryop_ctx *>(ctx->rhs.ptr);

                                ctx->rhs = c->expr;
                                set_dirty();
                                return n;
                        }
                }
                else if (n.fp == ENT::logicaland)
                {
                        if (ctx->lhs.fp == ENT::constfalse || ctx->rhs.fp == ENT::constfalse)
                        {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }
                        else if (same(ctx->lhs, ctx->rhs))
                        {
                                n = *stronger(ctx->lhs, ctx->rhs);
                                set_dirty();
                                return n;
                        }
                        else if (ctx->lhs.fp == ENT::logicalnot && same(static_cast<queryexec_ctx::binop_ctx *>(ctx->lhs.ptr)->rhs, ctx->rhs))
                        {
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
                                auto run = (queryexec_ctx::termsrun *)ctx->rhs.ptr;

                                if (run->erase(ctx->lhs.u16))
                                {
					// [cid:806 AND anyof(cid:806, ipad, xbox)] => [cid:806 AND <anyof(ipad, xbox)>]
					// XXX:verify me
					const auto r = ctx->rhs;

					ctx->rhs.fp = ENT::consttrueexpr;
					ctx->rhs.ptr = rctx.register_unaryop(r);

                                        set_dirty();
                                        return n;
                                }
                        }
#endif

                        if (ctx->lhs.fp == ENT::constfalse || ctx->rhs.fp == ENT::constfalse)
                        {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchanyterms)
                        {
                                // (ALL OF[2,1] AND ANY OF[2,1])
                                auto runa = (queryexec_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (queryexec_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchanyterms && ctx->rhs.fp == ENT::matchallterms)
                        {
                                // (ANY OF[2,1] AND ALL OF[2,1])
                                auto runa = (queryexec_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (queryexec_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
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
// TODO: figure out a better alternative
// LABEL:alt1
// TODO: https://github.com/phaistos-networks/Trinity/issues/6
#if 0
                        if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchanyterms)
                        {
                                auto runa = (queryexec_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (queryexec_ctx::termsrun *)ctx->rhs.ptr;

                                if (runb->erase(*runa))
                                {
                                        if (runb->empty())
                                                ctx->rhs.fp = ENT::dummyop;

                                        set_dirty();
                                        return n;
                                }
                        }
#endif

                        if (same(ctx->lhs, ctx->rhs))
                        {
                                n = *stronger(ctx->lhs, ctx->rhs);
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == ENT::matchanyterms && ctx->rhs.fp == ENT::matchanyterms)
                        {
                                // (ANY OF[2,1] AND ANY OF[2,1])
                                auto runa = (queryexec_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (queryexec_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n = ctx->lhs;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchterm && ctx->rhs.fp == ENT::matchallphrases)
                        {
                                // (1 AND ALLPHRASES:[[4,3,5][2,3,1]])
                                const auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)ctx->rhs.ptr;
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

                        if (ctx->lhs.fp == ENT::matchallterms && ctx->rhs.fp == ENT::matchphrase)
                        {
                                // ( pc game  hitman ) AND  "pc game"
                                //  hitman AND "pc game"
                                // this is somewhat expensive, but worth it
                                auto run = static_cast<queryexec_ctx::termsrun *>(ctx->lhs.ptr);
                                const auto phr = static_cast<const queryexec_ctx::phrase *>(ctx->rhs.ptr);

                                if (phr->size < 128) //arbitrary
                                {
                                        exec_term_id_t terms[128];

                                        const auto cnt = phr->disjoint_union(run, terms);

                                        if (cnt == 0)
                                        {
                                                n = ctx->rhs;
                                                set_dirty();
                                                return n;
                                        }
                                        else if (cnt < run->size)
                                        {
                                                run->size = cnt;
                                                memcpy(run->terms, terms, sizeof(terms[0]) * cnt);
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }

                        if (ctx->rhs.fp == ENT::matchallterms && ctx->lhs.fp == ENT::matchphrase)
                        {
                                // "pc game" AND ( pc game  hitman )
                                //  "pc game" AND hitman
                                // this is somewhat expensive, but worth it
                                auto run = static_cast<queryexec_ctx::termsrun *>(ctx->rhs.ptr);
                                const auto phr = static_cast<const queryexec_ctx::phrase *>(ctx->lhs.ptr);

                                if (phr->size < 128) //arbitrary
                                {
                                        exec_term_id_t terms[128];

                                        const auto cnt = phr->disjoint_union(run, terms);

                                        if (cnt == 0)
                                        {
                                                n = ctx->rhs;
                                                set_dirty();
                                                return n;
                                        }
                                        else if (cnt < run->size)
                                        {
                                                run->size = cnt;
                                                memcpy(run->terms, terms, sizeof(terms[0]) * cnt);
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }
                }
                else if (n.fp == ENT::logicalnot)
                {
                        if (ctx->lhs.fp == ENT::constfalse)
                        {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }
                        else if (ctx->rhs.fp == ENT::constfalse)
                        {
                                n = ctx->lhs;
                                set_dirty();
                                return n;
                        }
                        else if (same(ctx->lhs, ctx->rhs))
                        {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if ((ctx->lhs.fp == ENT::matchallterms || ctx->lhs.fp == ENT::matchanyterms) && ctx->rhs.fp == ENT::matchterm)
                        {
                                // ALL OF[1,5] NOT 5) => ALL OF [1] NOT 5
                                auto run = (queryexec_ctx::termsrun *)ctx->lhs.ptr;

                                if (run->erase(ctx->rhs.u16))
                                {
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::logicalnot && same((otherCtx = static_cast<queryexec_ctx::binop_ctx *>(ctx->lhs.ptr))->lhs, ctx->rhs))
                        {
                                // ((2 NOT 1) NOT 2)
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if (same(ctx->lhs, ctx->rhs))
                        {
                                n.fp = ENT::constfalse;
                                set_dirty();
                                return n;
                        }

                        if (ctx->lhs.fp == ENT::matchanyterms && ctx->rhs.fp == ENT::matchanyterms)
                        {
                                // (ANY OF[2,1] NOT ANY OF[2,1])
                                auto runa = (queryexec_ctx::termsrun *)ctx->lhs.ptr;
                                auto runb = (queryexec_ctx::termsrun *)ctx->rhs.ptr;

                                if (*runa == *runb)
                                {
                                        n.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchphrase && ctx->rhs.fp == ENT::matchterm)
                        {
                                // (([1,2,3] NOT 3) AND ALLPHRASES:[[4,2,5][1,2,3]])
                                const auto p = (queryexec_ctx::phrase *)ctx->lhs.ptr;

                                if (p->is_set(ctx->rhs.u16))
                                {
                                        n.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchanyphrases && ctx->rhs.fp == ENT::matchterm)
                        {
                                auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)ctx->lhs.ptr;
                                const auto termID = ctx->rhs.u16;

                                for (uint32_t i{0}; i < run->size;)
                                {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(termID))
                                                run->phrases[i] = run->phrases[--(run->size)];
                                        else
                                                ++i;
                                }

                                if (run->size == 0)
                                {
                                        // All phrases negated by the term
                                        ctx->lhs.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                }
                                else if (run->size == 1)
                                {
                                        ctx->lhs.fp = ENT::matchphrase;
                                        ctx->lhs.ptr = run->phrases[0];
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchanyphrases && ctx->rhs.fp == ENT::matchphrase)
                        {
                                auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)ctx->lhs.ptr;
                                const auto rhsPhrase = static_cast<const queryexec_ctx::phrase *>(ctx->rhs.ptr);

                                for (uint32_t i{0}; i != run->size;)
                                {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(rhsPhrase->termIDs, rhsPhrase->size))
                                                run->phrases[i] = run->phrases[--(run->size)];
                                        else
                                                ++i;
                                }

                                if (run->size == 0)
                                {
                                        // All phrases negated by the term
                                        ctx->lhs.fp = ENT::constfalse;
                                        set_dirty();
                                        return n;
                                }
                                else if (run->size == 1)
                                {
                                        ctx->lhs.fp = ENT::matchphrase;
                                        ctx->lhs.ptr = run->phrases[0];
                                        set_dirty();
                                        return n;
                                }
                        }

                        if (ctx->lhs.fp == ENT::matchallphrases && ctx->rhs.fp == ENT::matchphrase)
                        {
                                auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)ctx->lhs.ptr;
                                const auto rhsPhrase = static_cast<const queryexec_ctx::phrase *>(ctx->rhs.ptr);

                                for (uint32_t i{0}; i != run->size; ++i)
                                {
                                        const auto p = run->phrases[i];

                                        if (p->is_set(rhsPhrase->termIDs, rhsPhrase->size))
                                        {
                                                n.fp = ENT::constfalse;
                                                set_dirty();
                                                return n;
                                        }
                                }
                        }

                        // TODO: (any phrases NOT anyterms)
                }
        }
        else if (n.fp == ENT::matchanyterms || n.fp == ENT::matchallterms)
        {
                auto run = (queryexec_ctx::termsrun *)n.ptr;

                if (run->size == 1)
                {
                        n.fp = ENT::matchterm;
                        n.u16 = run->terms[0];
                        set_dirty();
                }
                else if (run->empty())
                {
                        n.fp = ENT::dummyop;
                        set_dirty();
                }
        }
        else if (n.fp == ENT::matchallphrases)
        {
                // ("a b", "a b c") => ("a b c")
                auto *const __restrict__ run = static_cast<queryexec_ctx::phrasesrun *>(n.ptr);
                auto size = run->size;

                if (size == 1)
                {
                        n.fp = ENT::matchphrase;
                        n.ptr = run->phrases[0];
                        set_dirty();
                        return n;
                }

                const auto saved{size};

                for (uint32_t i{0}; i < size;)
                {
                        const auto p = run->phrases[i];
                        bool any{false};

                        for (uint32_t k{0}; k < size;)
                        {
                                if (k != i)
                                {
                                        if (auto o = run->phrases[k]; o->size >= p->size)
                                        {
                                                if (memcmp(p->termIDs, o->termIDs, p->size * sizeof(exec_term_id_t)) == 0)
                                                {
                                                        run->phrases[i] = run->phrases[--size];
                                                        any = true;
                                                        break;
                                                }
                                        }
                                }
                                ++k;
                        }

                        if (!any)
                                ++i;
                }

                if (saved != size)
                {
                        run->size = size;
                        set_dirty();
                }
        }
        else if (n.fp == ENT::matchanyphrases)
        {
                auto *const __restrict__ run = static_cast<queryexec_ctx::phrasesrun *>(n.ptr);

                if (run->size == 1)
                {
                        n.fp = ENT::matchphrase;
                        n.ptr = run->phrases[0];
                        set_dirty();
                }
        }

        return n;
}

static void PrintImpl(Buffer &b, const exec_node &n)
{
        if (n.fp == ENT::unaryand)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                b.append("<AND>", ctx->expr);
        }
        else if (n.fp == ENT::unarynot)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                b.append("<NOT>", ctx->expr);
        }
        else if (n.fp == ENT::consttrueexpr)
        {
                auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                b.append("<", ctx->expr, ">");
        }
        else if (n.fp == ENT::logicaland)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

                b.append("(", ctx->lhs, ansifmt::bold, " AND ", ansifmt::reset, ctx->rhs, ")");
        }
        else if (n.fp == ENT::logicalor)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

                b.append("(", ctx->lhs, " OR ", ctx->rhs, ")");
        }
        else if (n.fp == ENT::logicalnot)
        {
                auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

                b.append("(", ctx->lhs, " NOT ", ctx->rhs, ")");
        }
        else if (n.fp == ENT::matchterm)
                b.append(n.u16);
        else if (n.fp == ENT::matchphrase)
        {
                const auto p = (queryexec_ctx::phrase *)n.ptr;

                b.append('[');
                for (uint32_t i{0}; i != p->size; ++i)
                        b.append(p->termIDs[i], ',');
                b.shrink_by(1);
                b.append(']');
        }
        else if (n.fp == ENT::constfalse)
                b.append(false);
        else if (n.fp == ENT::dummyop)
                b.append(true);
        else if (n.fp == ENT::matchanyterms)
        {
                const auto *__restrict__ run = static_cast<const queryexec_ctx::termsrun *>(n.ptr);

                b.append("ANY OF[");
                for (uint32_t i{0}; i != run->size; ++i)
                        b.append(run->terms[i], ',');
                b.shrink_by(1);
                b.append("]");
        }
        else if (n.fp == ENT::matchallterms)
        {
                const auto *__restrict__ run = static_cast<const queryexec_ctx::termsrun *>(n.ptr);

                b.append("ALL OF[");
                for (uint32_t i{0}; i != run->size; ++i)
                        b.append(run->terms[i], ',');
                b.shrink_by(1);
                b.append("]");
        }
        else if (n.fp == ENT::matchallphrases)
        {
                const auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)n.ptr;

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
        else if (n.fp == ENT::matchanyterms)
        {
                const auto *const __restrict__ run = (queryexec_ctx::phrasesrun *)n.ptr;

                b.append("ANYPHRASES:[");
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

static exec_node compile(const ast_node *const n, queryexec_ctx &rctx, simple_allocator &a, const uint32_t execFlags)
{
        static constexpr bool traceMetrics{false};
        std::vector<exec_term_id_t> terms;
        std::vector<const queryexec_ctx::phrase *> phrases;
        std::vector<exec_node> stack;
        std::vector<exec_node *> stackP;
        std::vector<std::pair<exec_term_id_t, uint32_t>> v;
        uint64_t before;

        // First pass
        // Compile from AST tree to exec_nodes tree
        before = Timings::Microseconds::Tick();

        const auto compile_begin{before};
        auto root = compile_node(n, rctx, a);
        bool updates;

        if (traceMetrics || traceCompile)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to compile to:", root, "\n");

        if (root.fp == ENT::constfalse)
        {
                if (traceCompile)
                        SLog("Nothing to do, compile_node() compiled away the expr.\n");

                return {ENT::constfalse, {}};
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
                if (traceCompile)
                        SLog("Before Collapse:", root, "\n");
                collapse_node(root, rctx, a, terms, phrases, stack);

                if (traceCompile)
                        SLog("Before Expand:", root, "\n");

                expand_node(root, rctx, a, terms, phrases, stack);

                if (traceCompile)
                        SLog("Before Optimizations:", root, "\n");

                updates = false;
                root = optimize_node(root, rctx, a, terms, phrases, stack, updates, &root);

                if (root.fp == ENT::constfalse || root.fp == ENT::dummyop)
                {
                        if (traceCompile)
                                SLog("Nothing to do (false OR noop)\n");

                        return {ENT::constfalse, {}};
                }
        } while (updates);

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to expand. Before third pass:", root, "\n");
        else if (traceCompile)
                SLog("Before third pass:", root, "\n");

        // Third pass
        size_t totalNodes{0};

        before = Timings::Microseconds::Tick();
        stackP.clear();
        stackP.push_back(&root);

        do
        {
                auto ptr = stackP.back();
                auto n = *ptr;

                stackP.pop_back();
                require(n.fp != ENT::constfalse);
                require(n.fp != ENT::dummyop);

                ++totalNodes;
                if (n.fp == ENT::matchallterms)
                {
                        auto ctx = static_cast<queryexec_ctx::termsrun *>(n.ptr);

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
                else if (n.fp == ENT::matchanyterms)
                {
                        // There are no real benefits to sorting terms for ENT::matchanyterms but we 'll do it anyway because its cheap
                        // This is actually useful, for leaders(deprecated)
                        auto ctx = static_cast<queryexec_ctx::termsrun *>(n.ptr);

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
                }
                else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot)
                {
                        auto ctx = (queryexec_ctx::binop_ctx *)n.ptr;

                        stackP.push_back(&ctx->lhs);
                        stackP.push_back(&ctx->rhs);
                }
                else if (n.fp == ENT::unaryand || n.fp == ENT::unarynot || n.fp == ENT::consttrueexpr)
                {
                        auto ctx = (queryexec_ctx::unaryop_ctx *)n.ptr;

                        stackP.push_back(&ctx->expr);
                }
        } while (stackP.size());

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to sort runs, ", dotnotation_repr(totalNodes), " exec_nodes\n");

        // Fourth Pass
        // Reorder ENT::logicaland nodes (lhs, rhs) so that the least expensive to evaluate is always found in the lhs branch
        // This also helps with moving tokens before phrases
        before = Timings::Microseconds::Tick();

        if (traceCompile)
                SLog("Before reordering:", root, "\n");

        root = reorder_execnodes(root, rctx);

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to reorder exec nodes\n");

        if (traceCompile)
        {
                SLog("COMPILED:", root, "in ", duration_repr(Timings::Microseconds::Since(compile_begin)), "\n");
                //exit(0);
        }

        // JIT:
        // Now that are done building the execution plan (a tree of exec_nodes), it should be fairly simple to
        // perform JIT and compile it down to x86-64 code.
        // Please see: https://github.com/phaistos-networks/Trinity/wiki/JIT-compilation

        // NOW, prepare decoders
        // No need to have done so if we could have determined that the query would have failed anyway
        // This could take some time - for 52 distinct terms it takes 0.002s (>1ms)
        before = Timings::Microseconds::Tick();
        for (const auto &kv : rctx.tctxMap)
        {
                const auto termID = kv.first;

                rctx.prepare_decoder(termID);
        }

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " ", Timings::Microseconds::ToMillis(Timings::Microseconds::Since(before)), " ms  to initialize all decoders ", rctx.tctxMap.size(), "\n");


        return root;
}

static exec_node compile_query(ast_node *root, queryexec_ctx &rctx, const uint32_t execFlags)
{
        simple_allocator a(4096);

        if (!root)
        {
                if (traceCompile)
                        SLog("No root node\n");

                return {ENT::constfalse, {}};
        }

        // Reorder the tree to enable the compiler to create larger collections
        // root is from a clone of the original query, so we are free to modify it anyway we see fit with reorder_root()
        return compile(reorder_root(root), rctx, a, execFlags);
}

static bool all_pli(const std::vector<DocsSetIterators::Iterator *> &its) noexcept
{
        for (const auto it : its)
        {
                if (it->type != DocsSetIterators::Type::PostingsListIterator)
                        return false;
        }
        return true;
}

DocsSetIterators::Iterator *queryexec_ctx::build_iterator(const exec_node n, const uint32_t execFlags)
{
        if (n.fp == ENT::matchallterms)
        {
                const auto run = static_cast<const termsrun *>(n.ptr);
                DocsSetIterators::Iterator *decoders[run->size];

                for (uint32_t i{0}; i != run->size; ++i)
                {
                        auto pli = reg_pli(decode_ctx.decoders[run->terms[i]]->new_iterator());

                        decoders[i] = pli;
                }

                return reg_docset_it(new DocsSetIterators::Conjuction(decoders, run->size));
        }
        else if (n.fp == ENT::matchanyterms)
        {
                const auto run = static_cast<const termsrun *>(n.ptr);
                DocsSetIterators::Iterator *decoders[run->size];

                for (uint32_t i{0}; i != run->size; ++i)
                {
                        auto pli = reg_pli(decode_ctx.decoders[run->terms[i]]->new_iterator());

                        decoders[i] = pli;
                }

                return reg_docset_it(new DocsSetIterators::DisjunctionAllPLI(decoders, run->size));
                //SLog("foo\n"); return reg_docset_it(new DocsSetIterators::DisjunctionSome(decoders, run->size, 16));
        }
        else if (n.fp == ENT::matchphrase)
        {
                const auto p = static_cast<const queryexec_ctx::phrase *>(n.ptr);
                Codecs::PostingsListIterator *its[p->size];

                for (uint32_t i{0}; i != p->size; ++i)
                {
                        const auto info = tctxMap[p->termIDs[i]];

                        require(info.first.documents);
                        require(info.second);

                        its[i] = reg_pli(decode_ctx.decoders[p->termIDs[i]]->new_iterator());
                }

                return reg_docset_it(new DocsSetIterators::Phrase(this, its, p->size, execFlags & unsigned(ExecFlags::AccumulatedScoreScheme)));
        }
        else if (n.fp == ENT::matchanyphrases)
        {
                const auto run = static_cast<const queryexec_ctx::phrasesrun *>(n.ptr);
                DocsSetIterators::Iterator *its[run->size];

                for (uint32_t pit{0}; pit != run->size; ++pit)
                {
                        const auto p = run->phrases[pit];
                        Codecs::PostingsListIterator *tits[p->size];

                        for (uint32_t i{0}; i != p->size; ++i)
                                tits[i] = reg_pli(decode_ctx.decoders[p->termIDs[i]]->new_iterator());

                        its[pit] = reg_docset_it(new DocsSetIterators::Phrase(this, tits, p->size, execFlags & unsigned(ExecFlags::AccumulatedScoreScheme)));
                }

                return reg_docset_it(new DocsSetIterators::Disjunction(its, run->size));
        }
        else if (n.fp == ENT::matchallphrases)
        {
                const auto run = static_cast<const queryexec_ctx::phrasesrun *>(n.ptr);
                DocsSetIterators::Iterator *its[run->size];

                for (uint32_t pit{0}; pit != run->size; ++pit)
                {
                        const auto p = run->phrases[pit];
                        Codecs::PostingsListIterator *tits[p->size];

                        for (uint32_t i{0}; i != p->size; ++i)
                                tits[i] = reg_pli(decode_ctx.decoders[p->termIDs[i]]->new_iterator());

                        its[pit] = reg_docset_it(new DocsSetIterators::Phrase(this, tits, p->size, execFlags & unsigned(ExecFlags::AccumulatedScoreScheme)));
                }

                return reg_docset_it(new DocsSetIterators::Conjuction(its, run->size));
        }
        else if (n.fp == ENT::logicalor)
        {
                // <foo> | bar => (foo | bar)
                const auto e = static_cast<const queryexec_ctx::binop_ctx *>(n.ptr);
                std::vector<DocsSetIterators::Iterator *> its;
                DocsSetIterators::Iterator *v[2] = {build_iterator(e->lhs, execFlags), build_iterator(e->rhs, execFlags)};

                // Pulling Iterators from (lhs, rhs) to this disjunction when possible is extremely important
                // Over 50% perf.improvement
                for (uint32_t i{0}; i != 2; ++i)
                {
                        auto it = v[i];

                        if (it->type == DocsSetIterators::Type::Disjunction)
                        {
                                auto internal = static_cast<DocsSetIterators::Disjunction *>(it);

                                while (internal->pq.size())
                                {
                                        its.push_back(internal->pq.top());
                                        internal->pq.pop();
                                }
                        }
                        else if (it->type == DocsSetIterators::Type::DisjunctionAllPLI)
                        {
                                auto internal = static_cast<DocsSetIterators::DisjunctionAllPLI *>(it);

                                while (internal->pq.size())
                                {
                                        its.push_back(internal->pq.top());
                                        internal->pq.pop();
                                }
                        }
                        else
                                its.push_back(it);
                }

                if (traceCompile)
                        SLog("Final ", its.size(), " ", execFlags & unsigned(ExecFlags::DocumentsOnly), ": ", all_pli(its), "\n");

                return reg_docset_it(all_pli(its)
                                         ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::DisjunctionAllPLI(its.data(), its.size()))
                                         : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Disjunction(its.data(), its.size())));
        }
        else if (n.fp == ENT::logicaland)
        {
                const auto e = static_cast<const queryexec_ctx::binop_ctx *>(n.ptr);

                if (e->lhs.fp == ENT::consttrueexpr)
                {
                        const auto op = static_cast<const queryexec_ctx::unaryop_ctx *>(e->lhs.ptr);

                        return reg_docset_it(new DocsSetIterators::Optional(build_iterator(e->rhs, execFlags), build_iterator(op->expr, execFlags)));
                }
                else if (e->rhs.fp == ENT::consttrueexpr)
                {
                        const auto op = static_cast<const queryexec_ctx::unaryop_ctx *>(e->rhs.ptr);

                        return reg_docset_it(new DocsSetIterators::Optional(build_iterator(e->lhs, execFlags), build_iterator(op->expr, execFlags)));
                }
                else
                {
                        std::vector<DocsSetIterators::Iterator *> its;
                        Trinity::DocsSetIterators::Iterator *v[2] = {build_iterator(e->lhs, execFlags), build_iterator(e->rhs, execFlags)};

                        for (uint32_t i{0}; i != 2; ++i)
                        {
                                auto it = v[i];

                                if (it->type == DocsSetIterators::Type::Conjuction || it->type == DocsSetIterators::Type::ConjuctionAllPLI)
                                {
                                        const auto internal = static_cast<DocsSetIterators::Conjuction *>(it);

                                        for (uint32_t i{0}; i != internal->size; ++i)
                                                its.push_back(internal->its[i]);
                                }
                                else
                                        its.push_back(it);
                        }

                        if (traceCompile)
                                SLog("final ", its.size(), "\n");

                        return reg_docset_it(all_pli(its)
                                                 ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::ConjuctionAllPLI(its.data(), its.size()))
                                                 : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Conjuction(its.data(), its.size())));
                }
        }
	else if (n.fp == ENT::matchallnodes)
        {
                std::vector<DocsSetIterators::Iterator *> its;
                const auto g = static_cast<const queryexec_ctx::nodes_group *>(n.ptr);

                its.reserve(g->size);
                for (uint32_t i{0}; i != g->size; ++i)
                        its.push_back(build_iterator(g->nodes[i], execFlags));

                return reg_docset_it(all_pli(its)
                                         ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::ConjuctionAllPLI(its.data(), its.size()))
                                         : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Conjuction(its.data(), its.size())));
        }
	else if (n.fp == ENT::matchanynodes)
        {
                std::vector<DocsSetIterators::Iterator *> its;
                const auto g = static_cast<const queryexec_ctx::nodes_group *>(n.ptr);

                its.reserve(g->size);
                for (uint32_t i{0}; i != g->size; ++i)
                        its.push_back(build_iterator(g->nodes[i], execFlags));

                return reg_docset_it(all_pli(its)
                                         ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::DisjunctionAllPLI(its.data(), its.size()))
                                         : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Disjunction(its.data(), its.size())));
        }
        else if (n.fp == ENT::logicalnot)
        {
                const auto e = static_cast<const queryexec_ctx::binop_ctx *>(n.ptr);

                return reg_docset_it(new DocsSetIterators::Filter(build_iterator(e->lhs, execFlags), build_iterator(e->rhs, execFlags)));
        }
        else if (n.fp == ENT::matchterm)
        {
                return reg_pli(decode_ctx.decoders[n.u16]->new_iterator());
        }
        else if (n.fp == ENT::unaryand)
        {
                auto *const ctx = static_cast<const queryexec_ctx::unaryop_ctx *>(n.ptr);

                return build_iterator(ctx->expr, execFlags);
        }
        else if (n.fp == ENT::consttrueexpr)
        {
                // not part of a binary op.
                const auto op = static_cast<const queryexec_ctx::unaryop_ctx *>(n.ptr);

                return build_iterator(op->expr, execFlags);
        }
        else
        {
                if (traceCompile || traceExec)
                {
                        SLog("Not supported:", n, "\n");
                        exit(1);
                }
                else
                        std::abort();
        }
}

static std::unique_ptr<DocsSetSpan> build_span(DocsSetIterators::Iterator *root, queryexec_ctx *const rctx)
{
        if (root->type == DocsSetIterators::Type::DisjunctionSome && (rctx->documentsOnly || rctx->accumScoreMode))
        {
                auto d = static_cast<DocsSetIterators::DisjunctionSome *>(root);
                std::vector<Trinity::DocsSetIterators::Iterator *> its;

                for (auto it{d->lead}; it; it = it->next)
                        its.push_back(it->it);

                // Either DocsSetSpanForDisjunctionsWithThresholdAndCost or DocsSetSpanForDisjunctionsWithThreshold
                // take the same time if we are dealing with iterators that are just PostingsListIterator
                // though if we have phrases and other complex binary ops, cost makes more sense, so we 'll settle for
                // DocsSetSpanForDisjunctionsWithThresholdAndCost
                return std::make_unique<DocsSetSpanForDisjunctionsWithThresholdAndCost>(d->matchThreshold, its, rctx->accumScoreMode);
                //return std::make_unique<DocsSetSpanForDisjunctionsWithThreshold>(d->matchThreshold, its, rctx->accumScoreMode);
        }
        else if ((rctx->documentsOnly  || rctx->accumScoreMode) && (root->type == DocsSetIterators::Type::Disjunction || root->type == DocsSetIterators::Type::DisjunctionAllPLI))
        {
                std::vector<Trinity::DocsSetIterators::Iterator *> its;

                switch (root->type)
                {
                        case DocsSetIterators::Type::Disjunction:
                                for (auto containerIt : static_cast<DocsSetIterators::Disjunction *>(root)->pq)
                                        its.push_back(containerIt);
                                break;

                        case DocsSetIterators::Type::DisjunctionAllPLI:
                                for (auto containerIt : static_cast<DocsSetIterators::DisjunctionAllPLI *>(root)->pq)
                                        its.push_back(containerIt);
                                break;

                        default:
                                std::abort();
                }

		if (rctx->accumScoreMode)
			return std::make_unique<DocsSetSpanForDisjunctionsWithThreshold>(1, its, true);
		else
			return std::unique_ptr<DocsSetSpanForDisjunctions>(new DocsSetSpanForDisjunctions(its));
        }
        else if (root->type == DocsSetIterators::Type::Filter)
        {
                const auto f = static_cast<DocsSetIterators::Filter *>(root);
                const auto filterCost = Trinity::DocsSetIterators::cost(f->filter);
                const auto reqCost = Trinity::DocsSetIterators::cost(f->req);

                if (traceCompile || traceExec)
                        SLog("filterCost ", filterCost, " reqCost ", reqCost, "\n");

                if (filterCost <= reqCost)
                {
                        auto req = build_span(f->req, rctx);

                        return std::unique_ptr<FilteredDocsSetSpan>(new FilteredDocsSetSpan(req.release(), f->filter));
                }
                else
                        return std::unique_ptr<GenericDocsSetSpan>(new GenericDocsSetSpan(root));
        }
        else
        {
                return std::unique_ptr<GenericDocsSetSpan>(new GenericDocsSetSpan(root));
        }
}

void Trinity::exec_query(const query &in,
                         IndexSource *const __restrict__ idxsrc,
                         masked_documents_registry *const __restrict__ maskedDocumentsRegistry,
                         MatchedIndexDocumentsFilter *__restrict__ const matchesFilter,
                         IndexDocumentsFilter *__restrict__ const documentsFilter,
                         const uint32_t execFlags, 
			 Similarity::IndexSourceTermsScorer *scorer)
{
        struct query_term_instance final
            : public query_term_ctx::instance_struct
        {
                str8_t token;
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
        query q(in, true); // shallow copy, no need for a deep copy here

        // Normalize just in case
        if (!q.normalize())
        {
                if (traceCompile)
                        SLog("No root node after normalization\n");

                return;
        }

        const bool documentsOnly = execFlags & uint32_t(ExecFlags::DocumentsOnly);
        const bool accumScoreMode = execFlags & uint32_t(ExecFlags::AccumulatedScoreScheme);
        const bool defaultMode = !documentsOnly && !accumScoreMode;

        // We need to collect all term instances in the query
        // so that we the score function will be able to take that into account (See matched_document::queryTermInstances)
        // We only need to do this for specific AST branches and node types(i.e we ignore all RHS expressions of logical NOT nodes)
        //
        // This must be performed before any query optimizations, for otherwise because the optimiser will most definitely rearrange the query, doing it after
        // the optimization passes will not capture the original, input query tokens instances information.
        //
        // This is required if the default execution mode is selected
        std::vector<query_term_instance> originalQueryTokenInstances;

        if (accumScoreMode)
        {
                // Just in case
                expect(scorer);
        }

        if (defaultMode)
        {
                std::vector<ast_node *> stack{q.root}; // use a stack because we don't care about the evaluation order
                std::vector<phrase *> collected;

                // collect phrases from the AST
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

                                case ast_node::Type::MatchSome:
                                        stack.insert(stack.end(), n->match_some.nodes, n->match_some.nodes + n->match_some.size);
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

                for (const auto it : collected) // collected phrases
                {
                        const uint8_t rep = it->size == 1 ? it->rep : 1;
                        const auto toNextSpan{it->toNextSpan};
                        const auto flags{it->flags};
                        const auto rewriteRange{it->rewrite_ctx.range};
                        const auto translationCoefficient{it->rewrite_ctx.translationCoefficient};
                        const auto srcSeqSize{it->rewrite_ctx.srcSeqSize};

                        // for each phrase token
                        for (uint16_t pos{it->index}, i{0}; i != it->size; ++i, ++pos)
                        {
                                if (traceCompile)
                                        SLog("Collected instance: [", it->terms[i].token, "] index:", pos, " rep:", rep, " toNextSpan:", i == (it->size - 1) ? toNextSpan : 1, "\n");

                                originalQueryTokenInstances.push_back({{pos, flags, rep, uint8_t(i == (it->size - 1) ? toNextSpan : 1), {rewriteRange, translationCoefficient, srcSeqSize}}, it->terms[i].token}); // need to be careful to get this right for phrases
                        }
                }
        }

        if (traceCompile)
                SLog("Compiling:", q, "\n");

        queryexec_ctx rctx(idxsrc, documentsOnly, accumScoreMode);
        const auto before = Timings::Microseconds::Tick();
        auto rootExecNode = compile_query(q.root, rctx, execFlags);

        curRCTX = &rctx;
        curRCTX->scorer = scorer;

        if (traceCompile)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to compile, ", duration_repr(Timings::Microseconds::Since(_start)), " since start\n");

        if (unlikely(rootExecNode.fp == ENT::dummyop || rootExecNode.fp == ENT::constfalse))
        {
                if (traceCompile)
                        SLog("Nothing to do\n");

                return;
        }

	// Now that we have compiled the AST into an execution nodes tree, we could
	// group nodes into matchallnodes and matchanynodes groups.
	// There is really no need to do it now, but for a Percolator like scheme, where
	// you want to attempt to matchd documents against queries, it would be very handy.
	// 
	// TODO: we need to move some state out of queryexec_ctx, to e.g a compilation_ctx
	// which can exist independently of a queryexec_ctx, so that we can use it for a perconalation impl.
	// group_execnodes(rootExecNode, rctx.allocator);	



        // see query_index_terms and MatchedIndexDocumentsFilter::prepare() comments
        query_index_terms **queryIndicesTerms;
        const auto maxQueryTermIDPlus1 = rctx.termsDict.size() + 1;

        if (defaultMode)
        {
                std::vector<const query_term_instance *> collected;
                std::vector<std::pair<uint16_t, query_index_term>> originalQueryTokensTracker; // query index => (termID, toNextSpan)
                std::vector<query_index_term> list;
                uint16_t maxIndex{0};

                // Build rctx.originalQueryTermInstances
                // It is important to only do this after we have optimised the copied original query, just as it is important
                // to capture the original query instances before we optimise.
                //
                // We need access to that information for scoring documents -- see matches.h
                rctx.originalQueryTermCtx = (query_term_ctx **)rctx.allocator.Alloc(sizeof(query_term_ctx *) * maxQueryTermIDPlus1);

                memset(rctx.originalQueryTermCtx, 0, sizeof(query_term_ctx *) * maxQueryTermIDPlus1);
                std::sort(originalQueryTokenInstances.begin(), originalQueryTokenInstances.end(), [](const auto &a, const auto &b) { return terms_cmp(a.token.data(), a.token.size(), b.token.data(), b.token.size()) < 0; });

                for (const auto *p = originalQueryTokenInstances.data(), *const e = p + originalQueryTokenInstances.size(); p != e;)
                {
                        const auto token = p->token;

                        if (traceCompile)
                                SLog("Collecting token [", token, "]\n");

                        if (const auto termID = rctx.termsDict[token]) // only if this token has actually been used in the compiled query
                        {
                                collected.clear();
                                do
                                {
                                        collected.push_back(p);
                                } while (++p != e && p->token == token);

                                if (traceCompile)
                                        SLog("Collected ", collected.size(), " for token [", token, "]\n");

                                const auto cnt = collected.size();

                                // XXX: maybe we should just support more instances?
                                Dexpect(cnt <= sizeof(query_term_ctx::instancesCnt) << 8);

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
                                        p->instances[i].rewrite_ctx.range = it->rewrite_ctx.range;
                                        p->instances[i].rewrite_ctx.translationCoefficient = it->rewrite_ctx.translationCoefficient;
                                        p->instances[i].rewrite_ctx.srcSeqSize = it->rewrite_ctx.srcSeqSize;

                                        if (traceCompile)
                                                SLog("<<<<<< token index ", it->index, "\n");

                                        maxIndex = std::max(maxIndex, it->index);
                                        originalQueryTokensTracker.push_back({it->index, {.termID = termID, .flags = it->flags, .toNextSpan = it->toNextSpan}});
                                }

                                rctx.originalQueryTermCtx[termID] = p;
                        }
                        else
                        {
                                // this original query token is not used in the optimised query
                                // rctx.originalQueryTermCtx[termID] will be nullptr
                                // see capture_matched_term() for why this is important.

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
                std::sort(originalQueryTokensTracker.begin(), originalQueryTokensTracker.end(), [](const auto &a, const auto &b) noexcept {
                        if (a.first < b.first)
                                return true;
                        else if (a.first == b.first)
                        {
                                if (a.second.termID < b.second.termID)
                                        return true;
                                else if (a.second.termID == b.second.termID)
                                {
                                        if (a.second.toNextSpan < b.second.toNextSpan)
                                                return true;
                                        else if (a.second.toNextSpan == b.second.toNextSpan)
                                                return a.second.flags < b.second.flags;
                                }
                        }

                        return false;
                });

                if (execFlags & uint32_t(ExecFlags::DisregardTokenFlagsForQueryIndicesTerms))
                {
                        for (const auto *p = originalQueryTokensTracker.data(), *const e = p + originalQueryTokensTracker.size(); p != e;)
                        {
                                const auto idx = p->first;

                                list.clear();
                                do
                                {
                                        const auto info = p->second;

                                        list.push_back({.termID = info.termID, .flags = 0, .toNextSpan = info.toNextSpan});
                                        do
                                        {
                                                ++p;
                                        } while (p != e && p->first == idx && p->second.termID == info.termID && p->second.toNextSpan == info.toNextSpan);
                                } while (p != e && p->first == idx);

                                if (traceCompile)
                                {
                                        SLog("For index ", idx, " ", list.size(), "\n");

                                        for (const auto &it : list)
                                                SLog("(", it.termID, ", ", it.toNextSpan, ")\n");
                                }

                                const uint16_t cnt = list.size();
                                auto ptr = (query_index_terms *)rctx.allocator.Alloc(sizeof(query_index_terms) + cnt * sizeof(query_index_term));

                                ptr->cnt = cnt;
                                memcpy(ptr->uniques, list.data(), cnt * sizeof(query_index_term));
                                queryIndicesTerms[idx] = ptr;
                        }
                }
                else
                {
                        for (const auto *p = originalQueryTokensTracker.data(), *const e = p + originalQueryTokensTracker.size(); p != e;)
                        {
                                const auto idx = p->first;

                                // unique query_index_term for idx
                                list.clear();
                                do
                                {
                                        const auto info = p->second;

                                        list.push_back(info);
                                        do
                                        {
                                                ++p;
                                        } while (p != e && p->first == idx && p->second == info);

                                } while (p != e && p->first == idx);

                                if (traceCompile)
                                {
                                        SLog("For index ", idx, " ", list.size(), "\n");

                                        for (const auto &it : list)
                                                SLog("(", it.termID, ", ", it.toNextSpan, ")\n");
                                }

                                const uint16_t cnt = list.size();
                                auto ptr = (query_index_terms *)rctx.allocator.Alloc(sizeof(query_index_terms) + cnt * sizeof(query_index_term));

                                ptr->cnt = cnt;
                                memcpy(ptr->uniques, list.data(), cnt * sizeof(query_index_term));
                                queryIndicesTerms[idx] = ptr;
                        }
                }
        }

        isrc_docid_t matchedDocuments{0}; // isrc_docid_t so that we can support whatever number of distinct documents are allowed by sizeof(isrc_docid_t)
        [[maybe_unused]] const auto start = Timings::Microseconds::Tick();
        const auto requireDocIDTranslation = idxsrc->require_docid_translation();

        if (defaultMode)
        {
                // doesn't make sense in other exec.modes
                matchesFilter->prepare(const_cast<const query_index_terms **>(queryIndicesTerms));
        }

        if (traceCompile)
                SLog("RUNNING: ", duration_repr(Timings::Microseconds::Since(_start)), " since start, documentsOnly = ", documentsOnly, "\n");

#pragma mark Execution
        try
        {
                if (rootExecNode.fp == ENT::matchterm && !accumScoreMode)
                {
                        isrc_docid_t docID;

                        // SPECIALIZATION: single term
                        if (traceCompile)
                                SLog("SPECIALIZATION: single term\n");

                        if (documentsOnly)
                        {
                                // SPECIALIZATION: 1 term, documents only
                                const auto termID = exec_term_id_t(rootExecNode.u16);
                                auto *const decoder = rctx.decode_ctx.decoders[termID];
                                auto *const it = rctx.reg_pli(decoder->new_iterator());

                                if (traceCompile)
                                        SLog("SPECIALIZATION: documentsOnly\n");

                                if (documentsFilter)
                                {
                                        while (likely((docID = it->next()) != DocIDsEND))
                                        {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID))
                                                        matchesFilter->consider(globalDocID);
                                        }
                                }
                                else if (nullptr == maskedDocumentsRegistry || maskedDocumentsRegistry->empty())
                                {
                                        while (likely((docID = it->next()) != DocIDsEND))
                                                matchesFilter->consider(requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID);
                                }
                                else
                                {
                                        while (likely((docID = it->next()) != DocIDsEND))
                                        {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                if (!maskedDocumentsRegistry->test(globalDocID))
                                                        matchesFilter->consider(globalDocID);
                                        }
                                }
                        }
                        else
                        {
                                // SPECIALIZATION: 1 term, collect terms
                                const auto termID = exec_term_id_t(rootExecNode.u16);
                                auto cd_ = std::make_unique<candidate_document>(&rctx);
                                auto *const cd = cd_.get();
                                matched_document &matchedDocument{cd->matchedDocument};
                                auto *const decoder = rctx.decode_ctx.decoders[termID];
                                auto *const it = rctx.reg_pli(decoder->new_iterator());
                                auto *const p = &matchedDocument.matchedTerms[0];
                                auto *const th = &cd->termHits[termID];
                                auto *const __restrict__ dws = matchedDocument.dws = new DocWordsSpace(idxsrc->max_indexed_position());

                                require(th);
                                th->set_freq(1);
                                matchedDocument.matchedTermsCnt = 1;
                                p->queryCtx = rctx.originalQueryTermCtx[termID];
                                p->hits = th;

                                if (traceExec || traceCompile)
                                        SLog("SPECIALIZATION: collect terms\n");

                                if (documentsFilter)
                                {
                                        if (maskedDocumentsRegistry && false == maskedDocumentsRegistry->empty())
                                        {
                                                if (traceExec)
                                                        SLog("documentsFilter AND maskedDocumentsRegistry\n");

                                                while (likely((docID = it->next()) != DocIDsEND))
                                                {
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                        if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID))
                                                        {
                                                                it->materialize_hits(dws, th->all);
                                                                matchedDocument.id = globalDocID;
                                                                matchesFilter->consider(matchedDocument);
                                                        }
                                                }
                                        }
                                        else
                                        {
                                                if (traceExec)
                                                        SLog("documentsFilter\n");

                                                while (likely((docID = it->next()) != DocIDsEND))
                                                {
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                        if (!documentsFilter->filter(globalDocID))
                                                        {
                                                                it->materialize_hits(dws, th->all);
                                                                matchedDocument.id = globalDocID;
                                                                matchesFilter->consider(matchedDocument);
                                                        }
                                                }
                                        }
                                }
                                else if (maskedDocumentsRegistry && false == maskedDocumentsRegistry->empty())
                                {
                                        if (traceExec)
                                                SLog("maskedDocumentsRegistry\n");

                                        while (likely((docID = it->next()) != DocIDsEND))
                                        {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                if (!maskedDocumentsRegistry->test(globalDocID))
                                                {
                                                        it->materialize_hits(dws, th->all);
                                                        matchedDocument.id = globalDocID;
                                                        matchesFilter->consider(matchedDocument);
                                                }
                                        }
                                }
                                else
                                {
                                        if (traceExec)
                                                SLog("No filtering\n");

                                        while (likely((docID = it->next()) != DocIDsEND))
                                        {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                it->materialize_hits(dws, th->all);
                                                matchedDocument.id = globalDocID;
                                                matchesFilter->consider(matchedDocument);
                                        }
                                }
                        }
                }
                else
                {
                        auto *const sit = rctx.build_iterator(rootExecNode, execFlags);
                        // Over-estimate capacity, make sure we won't overrun any buffers
                        const std::size_t capacity = rctx.tctxMap.size() + rctx.allIterators.size() + rctx.docsetsIterators.size() + 64;
                        auto span = build_span(sit, &rctx);

                        rctx.collectedIts.init(capacity);
                        rctx.reusableCDS.capacity = std::max<uint16_t>(512, capacity);
                        rctx.reusableCDS.data = (candidate_document **)malloc(sizeof(candidate_document *) * rctx.reusableCDS.capacity);
                        rctx.rootIterator = sit;

                        // We will create different Handlers depending on the mode and other execution options so
                        // because process() is a hot method and we 'd like to reduce checks in there if we can
                        if (documentsOnly)
                        {
                                if (documentsFilter)
                                {
                                        if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty())
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        const bool requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) override final
                                                        {
                                                                const auto id = rdp->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID))
                                                                {
                                                                        matchesFilter->consider(globalDocID);
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}, documentsFilter{df}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                        else
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        const bool requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) override final
                                                        {
                                                                const auto id = rdp->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID))
                                                                {
                                                                        matchesFilter->consider(globalDocID);
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, documentsFilter{df}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                }
                                else if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty())
                                {
                                        struct Handler
                                            : public MatchesProxy
                                        {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const idxsrc;
                                                const bool requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *const rdp) override final
                                                {
                                                        const auto id = rdp->document();
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        if (!maskedDocumentsRegistry->test(globalDocID))
                                                        {
                                                                matchesFilter->consider(globalDocID);
                                                                ++n;
                                                        }
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}
                                                {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                                else
                                {
                                        if (idxsrc->require_docid_translation())
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) override final
                                                        {
                                                                const auto id = rdp->document();

                                                                matchesFilter->consider(idxsrc->translate_docid(id));
                                                                ++n;
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                            : idxsrc{src}, ctx{c}, matchesFilter{mf}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                        else
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) override final
                                                        {
                                                                const auto id = rdp->document();

                                                                matchesFilter->consider(id);
                                                                ++n;
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                            : idxsrc{src}, ctx{c}, matchesFilter{mf}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                }
                        }
                        else if (accumScoreMode)
                        {
                                if (documentsFilter)
                                {
                                        if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty())
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        const bool requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) override final
                                                        {
                                                                const auto id = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID))
                                                                {
                                                                        matchesFilter->consider(globalDocID, relDoc->score());
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}, documentsFilter{df}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                        else
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        const bool requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) override final
                                                        {
                                                                const auto id = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID))
                                                                {
                                                                        matchesFilter->consider(globalDocID, relDoc->score());
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, documentsFilter{df}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                }
                                else if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty())
                                {
                                        struct Handler
                                            : public MatchesProxy
                                        {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const idxsrc;
                                                const bool requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) override final
                                                {
                                                        const auto id = relDoc->document();
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        if (!maskedDocumentsRegistry->test(globalDocID))
                                                        {
                                                                matchesFilter->consider(globalDocID, relDoc->score());
                                                                ++n;
                                                        }
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}
                                                {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                                else
                                {
                                        struct Handler
                                            : public MatchesProxy
                                        {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const idxsrc;
                                                const bool requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) override final
                                                {
                                                        const auto id = relDoc->document();
                                                        [[maybe_unused]] const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        matchesFilter->consider(globalDocID, relDoc->score());
                                                        ++n;
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}
                                                {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                        }
                        else // documentsOnly == false
                        {
                                if (documentsFilter)
                                {
                                        if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty())
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        const bool requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) override final
                                                        {
                                                                const auto id = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID))
                                                                {
                                                                        auto doc = ctx->document_by_id(id);

                                                                        ctx->prepare_match(doc);
                                                                        ctx->cds_release(doc);

                                                                        auto &matchedDocument = doc->matchedDocument;

                                                                        matchedDocument.id = globalDocID;
                                                                        matchesFilter->consider(matchedDocument);
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}, documentsFilter{df}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                        else
                                        {
                                                struct Handler
                                                    : public MatchesProxy
                                                {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const idxsrc;
                                                        const bool requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) override final
                                                        {
                                                                const auto id = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID))
                                                                {
                                                                        auto doc = ctx->document_by_id(id);

                                                                        ctx->prepare_match(doc);
                                                                        ctx->cds_release(doc);

                                                                        auto &matchedDocument = doc->matchedDocument;

                                                                        matchedDocument.id = globalDocID;
                                                                        matchesFilter->consider(matchedDocument);
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, documentsFilter{df}
                                                        {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                }
                                else if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty())
                                {
                                        struct Handler
                                            : public MatchesProxy
                                        {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const idxsrc;
                                                const bool requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) override final
                                                {
                                                        const auto id = relDoc->document();
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        if (!maskedDocumentsRegistry->test(globalDocID))
                                                        {
                                                                auto doc = ctx->document_by_id(id);

                                                                ctx->prepare_match(doc);
                                                                ctx->cds_release(doc);

                                                                auto &matchedDocument = doc->matchedDocument;

                                                                matchedDocument.id = globalDocID;
                                                                matchesFilter->consider(matchedDocument);
                                                                ++n;
                                                        }
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}
                                                {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                                else
                                {
                                        struct Handler
                                            : public MatchesProxy
                                        {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const idxsrc;
                                                const bool requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) override final
                                                {
                                                        const auto id = relDoc->document();
                                                        [[maybe_unused]] const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;
                                                        auto doc = ctx->document_by_id(id);

                                                        ctx->prepare_match(doc);
                                                        ctx->cds_release(doc);

                                                        auto &matchedDocument = doc->matchedDocument;

                                                        matchedDocument.id = globalDocID;
                                                        matchesFilter->consider(matchedDocument);
                                                        ++n;
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}
                                                {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                        }
                }
        }
        catch (const aborted_search_exception &e)
        {
                // search was aborted
        }
        catch (...)
        {
                // something else, throw it and let someone else handle it
                throw;
        }

        const auto duration = Timings::Microseconds::Since(start);
        const auto durationAll = Timings::Microseconds::Since(_start);

        if (traceCompile || traceExec)
                SLog(ansifmt::bold, ansifmt::color_red, dotnotation_repr(matchedDocuments), " matched in ", duration_repr(duration), ansifmt::reset, " (", Timings::Microseconds::ToMillis(duration), " ms) ", duration_repr(durationAll), " all\n");
}
