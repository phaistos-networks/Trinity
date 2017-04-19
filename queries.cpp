#include "queries.h"

using namespace Trinity;

namespace
{
        static constexpr bool traceParser{false};
};

static constexpr uint8_t OpPrio(const Operator op) noexcept
{
        switch (op)
        {
                case Operator::STRICT_AND:
                        return 8;

                case Operator::AND:
                        return 8;

                case Operator::NOT:
                        return 8;

                case Operator::OR:
                        return 7;

                default:
                        return 0;
        }
}

static str32_t parse_term(ast_parser &ctx)
{
        // Will strip characters that are not part of a valid token
        // and will pay attention to special group termination characters
        for (;;)
        {
                if (const auto pair = ctx.token_parser(ctx.content, ctx.lastParsedToken); pair.second)
                {
                        ctx.content.strip_prefix(pair.first);

                        if (unlikely(pair.second > Trinity::Limits::MaxTermLength))
			{
				// TODO: what's the right thing to do here?
				return {};
			}
			else
                                return {ctx.lastParsedToken, pair.second};
                }
		else if (pair.first)
                {
                        // We may still have skipped past some content  if we didn't consume any
                        ctx.content.strip_prefix(pair.first);
                        continue;
                }

                if (ctx.content.empty() || (ctx.groupTerm.size() && ctx.groupTerm.back() == ctx.content.front()))
                        return {};
                else
                {
			// whitespace or other content here
                        ctx.content.strip_prefix(1);
                }
        }
}

static ast_node *parse_phrase_or_token(ast_parser &ctx)
{
        ctx.skip_ws();
        if (ctx.content && ctx.content.StripPrefix(_S("\"")))
        {
                auto &terms = ctx.terms;
                uint8_t n{0};
                term t;

                for (;;)
                {
                        ctx.skip_ws();
                        if (!ctx.content || ctx.content.StripPrefix(_S("\"")))
                                break;

                        if (const auto token = parse_term(ctx))
                        {
                                if (unlikely(token.size() > Limits::MaxTermLength))
                                        return ctx.alloc_node(ast_node::Type::ConstFalse);

                                t.token.Set(token.data(), uint8_t(token.size()));

                                if (n != sizeof_array(terms))
                                {
                                        // silently ignore the rest
                                        // Not sure this is ideal though
                                        ctx.track_term(t);
                                        terms[n++] = t;
                                }
                        }
                        else
                                ctx.content.strip_prefix(1);
                }

                if (!n)
                        return nullptr;
                else
                {
                        auto node = ctx.alloc_node(ast_node::Type::Phrase);
                        auto p = (phrase *)ctx.allocator.Alloc(sizeof(phrase) + sizeof(term) * n);

                        p->size = n;
                        std::copy(terms, terms + n, p->terms);
                        p->rep = 1;
			p->toNextSpan = n;
                        p->flags = 0;
                        node->p = p;
                        return node;
                }
        }
        else if (const auto token = parse_term(ctx))
        {
                if (unlikely(token.size() > Limits::MaxTermLength))
                        return ctx.alloc_node(ast_node::Type::ConstFalse);

                term t;

                t.token.Set(token.data(), uint8_t(token.size()));

                auto node = ctx.alloc_node(ast_node::Type::Token);
                auto p = (phrase *)ctx.allocator.Alloc(sizeof(phrase) + sizeof(term));

                ctx.track_term(t);

                p->size = 1;
                p->terms[0] = t;
                p->rep = 1;
		p->toNextSpan = 1;
                p->flags = 0;
                node->p = p;
                return node;
        }
        else
                return nullptr;
}

static std::pair<Operator, uint8_t> parse_operator_impl(ast_parser &ctx)
{
        auto s = ctx.content;
        Operator res;

        if (s.StripPrefix(_S("AND")))
                res = Operator::STRICT_AND;
        else if (s.StripPrefix(_S("OR")))
                res = Operator::OR;
        else if (s.StripPrefix(_S("NOT")))
                res = Operator::NOT;
        else if (s)
        {
                const auto f = s.front();

                switch (f)
                {
                        case '+':
                                return {Operator::STRICT_AND, 1};
                        case '-':
                                return {Operator::NOT, 1};
                }

                if (ctx.groupTerm.size() && ctx.groupTerm.front() == f)
                        return {Operator::NONE, 0};
                else
                        return {Operator::AND, 0};

#if 0
                if (isalnum(f) || f == '\"' || f == '(')
                        return {Operator::AND, 0};
                else
                        return {Operator::NONE, 0};
#else
                return {Operator::AND, 0};
#endif
        }
        else
                return {Operator::NONE, 0};

        if (s && !isalnum(s.front()))
        {
                s.strip_prefix(1);
                return {res, s.p - ctx.content.p};
        }

        return {Operator::NONE, 0};
}

static auto parse_operator(ast_parser &ctx)
{
        ctx.skip_ws();
        return parse_operator_impl(ctx);
}

static void PrintImpl(Buffer &b, const Operator op)
{
        switch (op)
        {
                case Operator::AND:
                        break;

                case Operator::STRICT_AND:
                        b.append("AND"_s8);
                        break;

                case Operator::NOT:
                        b.append("NOT"_s8);
                        break;

                case Operator::OR:
                        b.append("OR"_s8);
                        break;

                case Operator::NONE:
                        b.append("<none>");
                        break;

                default:
                        IMPLEMENT_ME();
        }
}

void PrintImpl(Buffer &b, const Trinity::phrase &p)
{
        b.append('"');
        for (uint32_t i{0}; i != p.size; ++i)
                b.append(p.terms[i].token, ' ');
        if (p.size)
                b.shrink_by(1);
        b.append('"');
        if (p.rep > 1)
                b.append('(', p.rep, ')');
	if (p.flags)
		b.append("(F:", p.flags, ')');
        b.append('[', p.index, ',', p.toNextSpan, ']');
}

static void print_token(Buffer &b, const phrase *const p)
{
        b.append(p->terms[0].token);
        if (p->rep > 1)
                b.append('(', p->rep, ')');
        b.append('[', p->index, ',', p->toNextSpan, ']');
	if (p->flags)
		b.append("(F:", p->flags, ")");
}

void PrintImpl(Buffer &b, const Trinity::ast_node &n)
{
        switch (n.type)
        {
                case ast_node::Type::Dummy:
                        b.append("<dummy>"_s8);
                        break;

                case ast_node::Type::ConstFalse:
                        b.append("<FALSE>"_s8);
                        break;

                case ast_node::Type::Phrase:
			b.append(*n.p);
                        break;

                case ast_node::Type::Token:
                        print_token(b, n.p);
                        break;

                case ast_node::Type::BinOp:
                {
                        const bool useparens = n.binop.op != Operator::AND || n.binop.lhs->type == ast_node::Type::BinOp || n.binop.rhs->type == ast_node::Type::BinOp;

                        if (useparens)
                                b.append('(');

                        b.append(*n.binop.lhs);
                        if (n.binop.op != Operator::AND)
                                b.append(' ');
                        b.append(ansifmt::color_green, ansifmt::bold, n.binop.op, ansifmt::reset);
                        b.append(' ', *n.binop.rhs);

                        if (useparens)
                                b.append(')');
                        else
                                b.append(' ');
                }
                break;

                case ast_node::Type::ConstTrueExpr:
                        b.append('<');
                        b.append(*n.expr);
                        b.append('>');
                        break;

                case ast_node::Type::UnaryOp:
                        if (n.unaryop.op == Operator::AND || n.unaryop.op == Operator::STRICT_AND)
                                b.append('+');
                        else if (n.unaryop.op == Operator::NOT)
                                b.append('-');
                        b.append(*n.unaryop.expr);
                        break;

                default:
                        Print("Missing for ", unsigned(n.type), "\n");
                        IMPLEMENT_ME();
        }
}

static ast_node *parse_expr(ast_parser &);

static ast_node *parse_unary(ast_parser &ctx)
{
        ctx.skip_ws();

#if 1
        // enable this for debugging
        if (ctx.content.StripPrefix(_S("<")))
        {
                ctx.groupTerm.push_back('>');

                auto e = parse_expr(ctx) ?: ctx.parse_failnode();

                ctx.skip_ws();
                if (!ctx.content.StripPrefix(_S(">")))
                {
                        if (e->type != ast_node::Type::Dummy)
                                e = ctx.parse_failnode();
                }
                else
                        ctx.groupTerm.pop_back();

                auto res = ctx.alloc_node(ast_node::Type::ConstTrueExpr);

                res->expr = e;
                return res;
        }
#endif

        if (ctx.content.StripPrefix(_S("(")))
        {
                ctx.groupTerm.push_back(')');

                auto e = parse_expr(ctx) ?: ctx.parse_failnode();

                ctx.skip_ws();
                if (!ctx.content.StripPrefix(_S(")")))
                {
                        if (e->type != ast_node::Type::Dummy)
                                e = ctx.parse_failnode();
                }
                else
                        ctx.groupTerm.pop_back();

                return e;
        }
        else
        {
                const auto r = parse_operator(ctx);

                if (r.first != Operator::NONE && r.first != Operator::AND)
                {
                        ctx.content.strip_prefix(r.second);
                        ctx.skip_ws();

                        auto exprNode = parse_phrase_or_token(ctx) ?: ctx.parse_failnode();
                        auto n = ctx.alloc_node(ast_node::Type::UnaryOp);

                        n->unaryop.op = r.first;
                        n->unaryop.expr = exprNode;
                        return n;
                }
                else if (const auto n = parse_phrase_or_token(ctx))
                        return n;
                else
                        return ctx.parse_failnode();
        }
}

static ast_node *parse_subexpr(ast_parser &ctx, const uint16_t limit)
{
        uint8_t prio;
        auto cur = parse_unary(ctx);

        Drequire(cur); // can't fail
        for (auto op = parse_operator(ctx); op.first != Operator::NONE && (prio = OpPrio(op.first)) < limit; op = parse_operator(ctx))
        {
                ctx.content.strip_prefix(op.second);
                ctx.skip_ws();

                auto v = parse_subexpr(ctx, prio);

                if (!v)
                {
                        // We can't fail, so set this to a dummy node
                        // the optimizer will deal with it
                        v = ctx.parse_failnode();
                }

                if (op.first == Operator::AND && unary_same_type(cur, v) && *cur->p == *v->p)
                {
                        // [apple AND apple] => [apple rep=2]
                        cur->p->rep += v->p->rep;
                }
                else if (op.first == Operator::AND && cur->type == ast_node::Type::BinOp && cur->binop.op == Operator::AND && unary_same_type(cur->binop.rhs, v) && *cur->binop.rhs->p == *v->p)
                {
                        // (of AND apple) AND apple
                        cur->binop.rhs->p->rep += v->p->rep;
                        //v = ctx.alloc_node(ast_node::Type::Dummy);
                }
                else
                {
                        auto n = ctx.alloc_node(ast_node::Type::BinOp);

                        n->binop.op = op.first;
                        n->binop.lhs = cur;
                        n->binop.rhs = v;

                        cur = n;
                }
        }

        return cur;
}

ast_node *parse_expr(ast_parser &ctx)
{
        ctx.skip_ws();

        return parse_subexpr(ctx, UnaryOperatorPrio);
}

void ast_parser::track_term(term &t)
{
        for (const auto &it : distinctTokens)
        {
                if (it == t.token)
                {
                        t.token = it;
                        return;
                }
        }

        t.token.p = allocator.CopyOf(t.token.data(), t.token.size());
        distinctTokens.push_back(t.token);
}

ast_node *normalize_root(ast_node *root);

ast_node *ast_parser::parse()
{
        require(distinctTokens.empty()); // in case we invoked parse() earlier

        auto n = parse_expr(*this);

        if (n)
                n = normalize_root(n);

        return n;
}

#pragma mark Normalization
struct normalizer_ctx
{
        uint32_t updates;
        uint32_t tokensCnt{0};
};

static void normalize(ast_node *, normalizer_ctx &);

// if `n` is modified by any sub-pass, return immediately
// so that sub-sequent passes in this method won't access `n` assuming it hasn't changed
// since passed as an argument
static void normalize_bin(ast_node *const n, normalizer_ctx &ctx)
{
        auto lhs = n->binop.lhs, rhs = n->binop.rhs;

        require(lhs);
        require(rhs);

        normalize(lhs, ctx);
        normalize(rhs, ctx);

        if (lhs->is_dummy() && rhs->is_dummy())
        {
                ++ctx.updates;
                n->set_dummy();
                return;
        }
        else if (rhs->is_dummy())
        {
                ++ctx.updates;
                *n = *lhs;
                return;
        }
        else if (lhs->is_dummy())
        {
                ++ctx.updates;
                *n = *rhs;
                return;
        }

        if (n->binop.op == Operator::NOT && lhs->type == ast_node::Type::BinOp && lhs->binop.op == Operator::OR && lhs->binop.lhs->is_unary() && rhs->is_unary() && *lhs->binop.lhs->p == *rhs->p)
        {
                // [ foo OR bar NOT foo ] => [bar]
                if (traceParser)
                        SLog("HERE\n");

                *n = *lhs->binop.rhs;
                ++ctx.updates;
                return;
        }

        if (n->binop.op == Operator::NOT && lhs->type == ast_node::Type::BinOp && lhs->binop.normalized_operator() == Operator::AND && lhs->binop.lhs->is_unary() && rhs->is_unary() && *lhs->binop.lhs->p == *rhs->p)
        {
                // [ foo AND bar NOT foo ] => [bar]
                if (traceParser)
                        SLog("HERE\n");

                n->set_const_false();
                ++ctx.updates;
                return;
        }

        if (n->binop.op == Operator::NOT && lhs->type == ast_node::Type::BinOp && lhs->binop.normalized_operator() == Operator::NOT && lhs->binop.lhs->is_unary() && rhs->is_unary() && *lhs->binop.lhs->p == *rhs->p)
        {
                // [ foo NOT bar NOT foo ] => [bar]
                if (traceParser)
                        SLog("HERE\n");

                n->set_const_false();
                ++ctx.updates;
                return;
        }

        if (lhs->is_const_false())
        {
                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                {
                        n->set_const_false();
                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::OR)
                {
                        if (rhs->is_const_false())
                                n->set_const_false();
                        else
                                *n = *rhs;

                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::NOT)
                {
                        n->set_const_false();
                        ++ctx.updates;
                        return;
                }
        }

        if (rhs->is_const_false())
        {
                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                {
                        n->set_const_false();
                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::OR)
                {
                        *n = *lhs;
                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::NOT)
                {
                        *n = *lhs;
                        ++ctx.updates;
                        return;
                }
        }

        if (unary_same_type(lhs, rhs))
        {
                // [phrase] [OP] [phrase]
                // => unaryop(phrase, op)
                const auto pl = lhs->p, pr = rhs->p;

                if (n->binop.op != Operator::AND)
                {
                        if (*pl == *pr)
                        {
                                if (n->binop.op == Operator::STRICT_AND)
                                {
                                        *n = *lhs;
                                }
                                else if (n->binop.op == Operator::NOT)
                                {
                                        if (traceParser)
                                                SLog("here\n");

                                        n->set_const_false();
                                }
                                else
                                {
                                        if (traceParser)
                                                SLog("here:", *n, "\n");

                                        if (n->binop.op == Operator::OR)
                                        {
                                                *n = *lhs;
                                        }
                                        else
                                        {
                                                n->type = ast_node::Type::UnaryOp;
                                                n->unaryop.op = n->binop.op;
                                                n->unaryop.expr = lhs;
                                        }
                                }
                                ++ctx.updates;

                                if (traceParser)
                                        SLog("Now:", *n, "\n");
                                return;
                        }
                }
        }

        if (rhs->type == ast_node::Type::UnaryOp)
        {
                if ((rhs->unaryop.op == Operator::AND || rhs->unaryop.op == Operator::STRICT_AND) && lhs->is_unary() && unary_same_type(lhs, rhs->unaryop.expr) && *lhs->p == *rhs->unaryop.expr->p)
                {
                        if (n->binop.op == Operator::NOT)
                        {
                                // [APPLE NOT +APPLE]
                                if (traceParser)
                                        SLog("here\n");

                                n->set_const_false();
                                ++ctx.updates;
                                return;
                        }
                        else if (n->binop.op == Operator::OR)
                        {
                                // [APPLE OR +APPLE]
                                if (traceParser)
                                        SLog("here\n");

                                *n = *rhs;
                                ++ctx.updates;
                                return;
                        }
                        else
                        {
                                // [APPLE AND +APPLE]
                                if (traceParser)
                                        SLog("here\n");

                                *n = *rhs;
                                ++ctx.updates;
                                return;
                        }
                }
        }

        if (lhs->type == ast_node::Type::UnaryOp)
        {
                if (rhs->type == ast_node::Type::UnaryOp && lhs->unaryop.op == rhs->unaryop.op && lhs->unaryop.op == n->binop.op && unary_same_type(lhs->unaryop.expr, rhs->unaryop.expr) && *lhs->unaryop.expr->p == *rhs->unaryop.expr->p)
                {
                        if (traceParser)
                                SLog("here\n");

                        n->type = ast_node::Type::UnaryOp;
                        n->unaryop.op = n->binop.op;
                        n->unaryop.expr = lhs->unaryop.expr;
                        ++ctx.updates;
                        return;
                }
                else if ((lhs->unaryop.op == Operator::AND || lhs->unaryop.op == Operator::STRICT_AND) && rhs->is_unary() && unary_same_type(rhs, lhs->unaryop.expr) && *rhs->p == *lhs->unaryop.expr->p)
                {
                        if (n->binop.op == Operator::NOT)
                        {
                                // [+APPLE NOT APPLE]
                                if (traceParser)
                                        SLog("here\n");

                                n->set_const_false();
                                ++ctx.updates;
                                return;
                        }
                        else if (n->binop.op == Operator::OR)
                        {
                                // [+APPLE OR APPLE]
                                if (traceParser)
                                        SLog("here\n");

                                *n = *lhs;
                                ++ctx.updates;
                                return;
                        }
                        else
                        {
                                // [+APPLE AND APPLE]
                                if (traceParser)
                                        SLog("here\n");

                                *n = *lhs;
                                ++ctx.updates;
                                return;
                        }
                }
        }

        if (n->binop.op == Operator::NOT)
        {
                if (lhs->type == ast_node::Type::UnaryOp && lhs->unaryop.op == Operator::NOT && unary_same_type(lhs->unaryop.expr, rhs) && *lhs->unaryop.expr->p == *rhs->p)
                {
                        if (traceParser)
                                SLog("here\n");

                        // [NOT apple NOT apple]
                        n->type = lhs->type;
                        n->unaryop = lhs->unaryop;
                        ++ctx.updates;
                        return;
                }
        }

        if (lhs->is_dummy() && rhs->is_dummy())
        {
                if (traceParser)
                        SLog("here\n");

                n->set_dummy();
                ++ctx.updates;
                return;
        }

        if (rhs->is_dummy())
        {
                if (lhs->is_unary())
                {
                        const auto op = n->binop.op;

                        if (traceParser)
                                SLog("here\n");

                        n->type = ast_node::Type::UnaryOp;
                        n->unaryop.op = op;
                        n->unaryop.expr = lhs;
                        ++ctx.updates;
                        return;
                }
        }

        if (n->binop.op == Operator::AND || n->binop.op == Operator::OR)
        {
                if (rhs->type == ast_node::Type::UnaryOp && rhs->unaryop.op == Operator::NOT)
                {
                        if (traceParser)
                                SLog("here\n");

                        n->binop.op = rhs->unaryop.op;
                        n->binop.rhs = rhs->unaryop.expr;
                        ++ctx.updates;
                        return;
                }

                if (lhs->type == ast_node::Type::UnaryOp && lhs->unaryop.op == Operator::NOT)
                {
                        if (traceParser)
                                SLog("here:", *n, "\n");

                        n->binop.op = lhs->unaryop.op;
                        n->binop.lhs = rhs;
                        n->binop.rhs = lhs->unaryop.expr;
                        ++ctx.updates;

                        if (traceParser)
                                SLog("now:", *n, "\n");
                        return;
                }
        }

        if (n->binop.op == Operator::AND && rhs->is_dummy())
        {
                // (foo AND bar)  AND dummy
                if (traceParser)
                        SLog("here\n");

                *n = *lhs;
                ++ctx.updates;
                return;
        }

        if (n->binop.op == Operator::AND && lhs->is_dummy())
        {
                // (foo AND bar)  AND dummy
                if (traceParser)
                        SLog("here\n");

                *n = *rhs;
                ++ctx.updates;
                return;
        }

        if (lhs->is_dummy())
        {
                if (rhs->is_unary())
                {
                        const auto op = n->binop.op;

                        n->type = ast_node::Type::UnaryOp;
                        n->unaryop.op = op;
                        n->unaryop.expr = rhs;
                        ++ctx.updates;

                        if (traceParser)
                                SLog("now:", *n, "\n");
                        return;
                }
        }

        require(rhs);
        require(lhs);
        require(lhs->binop.lhs);

        if (lhs->type == ast_node::Type::BinOp && unary_same_type(rhs, lhs->binop.rhs) && *rhs->p == *lhs->binop.rhs->p)
        {
                if (traceParser)
                        SLog("consider\n");

                if (lhs->binop.op == n->binop.op)
                {
                        // [macboook OR macbook OR macbook] => [macboook OR macbook]
                        if (traceParser)
                                SLog("here\n");

                        rhs->set_dummy();
                        ++ctx.updates;
                        return;
                }
                else if (lhs->binop.op == Operator::NOT && (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND))
                {
                        // [macboook NOT macbook AND macbook]
                        if (traceParser)
                                SLog("here\n");

                        rhs->set_const_false();
                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::NOT && (lhs->binop.op == Operator::AND || lhs->binop.op == Operator::STRICT_AND))
                {
                        // [macboook AND macbook NOT macbook]
                        if (traceParser)
                                SLog("here\n");

                        n->set_const_false();
                        ++ctx.updates;
                        return;
                }
        }

        if (n->binop.normalized_operator() == Operator::AND && lhs->type == ast_node::Type::BinOp && rhs->is_unary() && lhs->binop.op == Operator::OR && lhs->binop.lhs->is_unary() && *rhs->p == *lhs->binop.lhs->p)
        {
                // [apple OR "macbook pro" apple]  => ["macbook pro"]
                if (traceParser)
                        SLog("here\n");

                *lhs = *lhs->binop.rhs;
                ++ctx.updates;
                return;
        }

        if (n->binop.op == Operator::NOT && rhs->type == ast_node::Type::BinOp && lhs->is_unary() && rhs->binop.lhs->is_unary() && *lhs->p == *rhs->binop.lhs->p)
        {
                // [warcraft NOT (warcraft OR apple)] =>  []
                if (traceParser)
                        SLog("here\n");

                n->set_const_false();
                ++ctx.updates;
                return;
        }

        if (n->binop.normalized_operator() == Operator::AND && rhs->type == ast_node::Type::BinOp && lhs->is_unary() && rhs->binop.lhs->is_unary() && *lhs->p == *rhs->binop.lhs->p)
        {
                // [warcraft (warcraft OR apple)] =>  [warcraft and apple]
                if (traceParser)
                        SLog("here\n");

                *n->binop.rhs = *rhs->binop.rhs;
                ++ctx.updates;
                return;
        }

        if (n->binop.op == Operator::NOT && lhs->is_unary() && rhs->type == ast_node::Type::BinOp && rhs->binop.op == Operator::OR && ((rhs->binop.lhs->is_unary() && *lhs->p == *rhs->binop.lhs->p) || (rhs->binop.rhs->is_unary() && *lhs->p == *rhs->binop.rhs->p)))
        {
                // iphone NOT (ipad OR iphone)
                n->set_const_false();
                ++ctx.updates;
                return;
        }

	if (n->binop.op == Operator::NOT && lhs->is_unary() && rhs->type == ast_node::Type::BinOp && rhs->binop.rhs->is_unary() && *lhs->p == *rhs->binop.rhs->p)
	{
		// foo NOT (ipad AND foo)
		n->set_const_false();
		++ctx.updates;
		return;
	}
}

// We implement most of the rules here in expand_node(). See IMPLEMENTATION.md
static void normalize(ast_node *const n, normalizer_ctx &ctx)
{
        if (n->type == ast_node::Type::BinOp)
                normalize_bin(n, ctx);
        else if (n->is_unary() && n->p->size == 0)
        {
                if (traceParser)
                        SLog("here\n");

                n->set_dummy();
                ++ctx.updates;
        }
        else if (n->type == ast_node::Type::ConstTrueExpr)
        {
                normalize(n->expr, ctx);
                if (n->expr->is_dummy() || n->expr->is_const_false())
                {
                        if (traceParser)
                                SLog("here\n");

                        n->set_dummy();
                        ++ctx.updates;
                }
        }
        else if (n->type == ast_node::Type::UnaryOp)
        {
                normalize(n->unaryop.expr, ctx);
                if (n->unaryop.expr->is_dummy())
                {
                        if (traceParser)
                                SLog("here\n");

                        n->set_dummy();
                        ++ctx.updates;
                }
                else if (n->unaryop.op == Operator::AND)
                {
                        if (traceParser)
                                SLog("here\n");

                        *n = *n->unaryop.expr;
                        ++ctx.updates;
                }
                else if (n->unaryop.op == Operator::OR)
                {
                        if (traceParser)
                                SLog("here:", *n, "\n");

                        *n = *n->unaryop.expr;
                        ++ctx.updates;
                }
        }
        else if (n->type == ast_node::Type::Token || n->type == ast_node::Type::Phrase)
                ctx.tokensCnt += n->p->size;
}

// This is somewhat complicated because it takes into account phrases and OR groups
// In fact, this here function took longer than to implement than most if not all other functions and systems here. It just didn't feel right until it did.
// This method, rewrite_query() and the impl. of the algorithm that considers matched sequences in Consider() have been particularly challenging to get right.
struct query_assign_ctx final
{
	uint32_t nextIndex;
        std::vector<std::vector<phrase *> *> stack;
};

#if 0
// debug impl.
static void assign_query_indices(ast_node *const n, query_assign_ctx &ctx, phrase *&firstPhrase, phrase *&lastPhrase)
{
        if (n->is_unary())
        {
                if (ctx.stack.size())
                        ctx.stack.back()->push_back(n->p);

                if (!firstPhrase)
                        firstPhrase = n->p;
                lastPhrase = n->p;

                n->p->index = ctx.nextIndex;
                ctx.nextIndex += n->p->size;
        }
        else if (n->type == ast_node::Type::UnaryOp)
                assign_query_indices(n->unaryop.expr, ctx, firstPhrase, lastPhrase);
        else if (n->type == ast_node::Type::ConstTrueExpr)
                assign_query_indices(n->expr, ctx, firstPhrase, lastPhrase);
        else if (n->type == ast_node::Type::BinOp)
        {
                auto lhs = n->binop.lhs, rhs = n->binop.rhs;
                const auto op = n->binop.op;
                std::unique_ptr<std::vector<phrase *>> s;

                SLog("LHS:", *lhs, "\n");
                SLog(ansifmt::bold, ansifmt::color_green, n->binop.op, ansifmt::reset, "\n");
                SLog("RHS:", *rhs, "\n");

                if (op == Operator::AND || op == Operator::STRICT_AND)
                {
                        phrase *first{nullptr}, *last{nullptr};
                        auto u = std::make_unique<std::vector<phrase *>>();

                        ctx.stack.push_back(u.get());
                        assign_query_indices(lhs, ctx, first, last); // last phrase from lhs to
                        firstPhrase = first;
                        ctx.stack.pop_back();

                        const auto savedLast{last}, savedFirst{first};
                        const auto savedIdx{ctx.nextIndex};

                        first = nullptr;
                        assign_query_indices(rhs, ctx, first, last); // first phrase from rhs
                        lastPhrase = last;

                        if (savedLast && first)
                        {
                                SLog(ansifmt::color_red, "JOIN:", *savedLast, " TO ", *first, " => ", savedIdx, ansifmt::reset, "\n");

                                for (const auto p : *u)
                                        Print(">>>>> ", *p, "\n");

                                for (auto p : *u)
                                        p->toNextSpan = savedIdx - p->index;
                        }
                }
                else if (op == Operator::NOT)
                {
                        phrase *first{nullptr}, *last{nullptr};

                        if (op == Operator::NOT)
                        {
                                // we do not care for the RHS(not)
                                // but we need to advance nextIndex by 4 so that we won't consider whatever's on the RHS adjacent to whatever was before the LHS
                                ctx.nextIndex += 4;
                        }

                        assign_query_indices(rhs, ctx, first, last);
                }
                else
                {
                        // this is a bit more involved because of the semantics we are trying to enforce
                        // re: OR groups and phrases
                        const auto saved{ctx.nextIndex};
                        uint32_t next;

                        assign_query_indices(lhs, ctx, firstPhrase, lastPhrase);

                        const auto maxL{ctx.nextIndex};

                        ctx.nextIndex = saved;
                        next = saved;

                        assign_query_indices(rhs, ctx, firstPhrase, lastPhrase);

                        const auto maxR{ctx.nextIndex};

                        ctx.nextIndex = std::max(maxL, maxR);
                }
        }
}
#else
static void assign_query_indices(ast_node *const n, query_assign_ctx &ctx)
{
        if (n->is_unary())
        {
                if (ctx.stack.size())
                        ctx.stack.back()->push_back(n->p);

                n->p->index = ctx.nextIndex;
                ctx.nextIndex += n->p->size;
        }
        else if (n->type == ast_node::Type::UnaryOp)
                assign_query_indices(n->unaryop.expr, ctx);
        else if (n->type == ast_node::Type::ConstTrueExpr)
                assign_query_indices(n->expr, ctx);
        else if (n->type == ast_node::Type::BinOp)
        {
                const auto lhs = n->binop.lhs, rhs = n->binop.rhs;
                const auto op = n->binop.op;

                if (op == Operator::AND || op == Operator::STRICT_AND)
                {
                        auto u = std::make_unique<std::vector<phrase *>>();

                        ctx.stack.push_back(u.get());
                        assign_query_indices(lhs, ctx);
                        ctx.stack.pop_back();

                        for (auto p : *u)
                                p->toNextSpan = ctx.nextIndex - p->index;

                        assign_query_indices(rhs, ctx);
                }
                else if (op == Operator::NOT)
                {
                        // We do not care for the RHS(not)
                        // but we need to advance nextIndex by 4 so that we won't consider whatever's on the RHS adjacent to whatever was before the LHS
                        assign_query_indices(lhs, ctx);
                        ctx.nextIndex += 4;
                }
                else
                {
                        // This is a bit more involved because of the semantics we are trying to enforce
                        // re: OR groups and phrases
                        const auto saved{ctx.nextIndex};

                        assign_query_indices(lhs, ctx);

                        const auto maxL{ctx.nextIndex};

                        ctx.nextIndex = saved;
                        assign_query_indices(rhs, ctx);

                        const auto maxR{ctx.nextIndex};

                        ctx.nextIndex = std::max(maxL, maxR);
                }
        }
}
#endif

// See: IMPLEMENTATION.md
ast_node *normalize_root(ast_node *root)
{
        if (!root)
                return nullptr;

        normalizer_ctx ctx;

        do
        {
                ctx.updates = 0;
                ctx.tokensCnt = 0;
                normalize(root, ctx);
        } while (ctx.updates);

        if (unlikely(ctx.tokensCnt > Limits::MaxQueryTokens))
        {
                if (traceParser)
                        SLog("Too many query tokens\n");

                root = nullptr;
        }
        else if (root->is_dummy())
        {
                if (traceParser)
                        SLog("Ignoring dummy root\n");

                root = nullptr;
        }
        else if (root->is_const_false())
        {
                if (traceParser)
                        SLog("Ignoring const false\n");

                root = nullptr;
        }
        else if (root->type == ast_node::Type::UnaryOp)
        {
                if (root->unaryop.op == Operator::NOT)
                {
                        if (traceParser)
                                SLog("Ignoring unary NOT\n");

                        root = nullptr;
                }
                else if (root->unaryop.op == Operator::OR || root->unaryop.op == Operator::AND)
                {
                        if (traceParser)
                                SLog("From op\n");

                        *root = *root->unaryop.expr;
                }
        }
        else if (!root->any_leader_tokens())
        {
                // e.g [  -foo ( -bar -hello)  ]
                if (traceParser)
                        SLog("No Leader Tokens\n");

                root = nullptr;
        }

        if (root)
        {
                // We are assigning phrase indices here
                // We originally assigned indices when we parsed the query, but this
                // is not optimal, because the query can be updated, or we can just build
                // the query ourselves programmatically, and whatever the case, we need
                // to get those phrase indices properly
		//
                // Because we are going to normalize_root() whenever we update the query structure and
                // when we are committing the changes, we are guaranteed to get this right
		//
		// This is very tricky because of OR expressions, and because multiple OR expresions starting from the same 'index' can be of variable length in terms of tokens
		// we need to also track a skip/jump value.
		//
		// See Trinity::phrase decl. comments 
		query_assign_ctx ctx;

		ctx.nextIndex = 0;
#if 0
		phrase *firstPhrase{nullptr}, *lastPhrase{nullptr};
                assign_query_indices(root, ctx, firstPhrase, lastPhrase);
#else
                assign_query_indices(root, ctx);
#endif

#if 0
		if (traceParser || true)
		{
			Print("AFTER ASSIGNING INDICES:", *root, "\n"); 
			exit(0);
		}
#endif

        }

        return root;
}

#pragma mark query utility functions
ast_node *ast_node::copy(simple_allocator *const a)
{
        const ast_node *const n{this};
        auto res = ast_node::make(*a, n->type);

        switch (res->type)
        {
                case ast_node::Type::Token:
                case ast_node::Type::Phrase:
                        res->p = n->p;
                        break;

                case ast_node::Type::ConstTrueExpr:
                        res->expr = n->expr->copy(a);
                        break;

                case ast_node::Type::UnaryOp:
                        res->unaryop.op = n->unaryop.op;
                        res->unaryop.expr = n->unaryop.expr->copy(a);
                        break;

                case ast_node::Type::BinOp:
                        res->binop.op = n->binop.op;
                        res->binop.lhs = n->binop.lhs->copy(a);
                        res->binop.rhs = n->binop.rhs->copy(a);
                        break;

                default:
                        break;
        }
        return res;
}

static void capture_leader(ast_node *const n, std::vector<ast_node *> *const out, const size_t threshold)
{
        switch (n->type)
        {
                case ast_node::Type::Token:
                case ast_node::Type::Phrase:
                        out->push_back(n);
                        break;

                case ast_node::Type::BinOp:
                        if (n->binop.op == Operator::OR)
                        {
                                capture_leader(n->binop.rhs, out, threshold);
                                capture_leader(n->binop.lhs, out, threshold + 1);
                        }
                        else if ((n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND) && out->size() < threshold)
                        {
                                // we assume we have reordered and normalized bimops
                                // and lhs is cheaper to evaluate than rhs
                                if (n->binop.lhs->type != ast_node::Type::ConstTrueExpr)
                                        capture_leader(n->binop.lhs, out, threshold);
                                else
                                        capture_leader(n->binop.rhs, out, threshold);
                        }
                        else if (n->binop.op == Operator::NOT && out->size() < threshold)
                                capture_leader(n->binop.lhs, out, threshold);
                        break;

                case ast_node::Type::UnaryOp:
                        if (n->unaryop.op == Operator::AND || n->unaryop.op == Operator::STRICT_AND)
                                out->push_back(n->unaryop.expr);
                        break;

                default:
                        break;
        }
}

ast_node *Trinity::normalize_ast(ast_node *n)
{
        return normalize_root(n);
}

bool query::normalize()
{
        if (root)
        {
                root = normalize_root(root);
                return root;
        }
        else
                return false;
}

void ast_node::set_alltokens_flags(const uint8_t flags)
{
        switch (type)
        {
                case Type::Token:
                case Type::Phrase:
                        p->flags = flags;
                        break;

                case Type::BinOp:
                        binop.lhs->set_alltokens_flags(flags);
                        binop.rhs->set_alltokens_flags(flags);
                        break;

                case Type::UnaryOp:
                        unaryop.expr->set_alltokens_flags(flags);
                        break;

                case Type::ConstTrueExpr:
                        expr->set_alltokens_flags(flags);
                        break;

                default:
                        break;
        }
}

bool ast_node::any_leader_tokens() const
{
        std::vector<const ast_node *> stack{this};

        do
        {
                auto n = stack.back();

                stack.pop_back();
                switch (n->type)
                {
                        case ast_node::Type::Token:
                        case ast_node::Type::Phrase:
                                return true;

                        case ast_node::Type::BinOp:
                                if (n->binop.op == Operator::NOT)
                                        stack.push_back(n->binop.lhs);
                                else
                                {
                                        stack.push_back(n->binop.lhs);
                                        stack.push_back(n->binop.rhs);
                                }
                                break;

                        case ast_node::Type::UnaryOp:
                                if (n->unaryop.op == Operator::AND || n->unaryop.op == Operator::STRICT_AND)
                                        stack.push_back(n->unaryop.expr);
                                break;

                        default:
                                break;
                }

        } while (stack.size());
        return false;
}

void query::leader_nodes(std::vector<ast_node *> *const out)
{
        if (!root)
                return;

        capture_leader(root, out, 1);
}

Switch::vector<ast_node *> &query::nodes(ast_node *root, Switch::vector<ast_node *> *const res)
{
        if (root)
        {
                uint32_t i{0};

                res->push_back(root);
                while (i != res->size())
                {
                        auto n = res->at(i++);

                        switch (n->type)
                        {
                                case ast_node::Type::BinOp:
                                        res->push_back(n->binop.lhs);
                                        res->push_back(n->binop.rhs);
                                        break;

                                case ast_node::Type::UnaryOp:
                                        res->push_back(n->unaryop.expr);
                                        break;

                                case ast_node::Type::ConstTrueExpr:
                                        res->push_back(n->expr);
                                        break;

                                default:
                                        break;
                        }
                }
        }

        return *res;
}

bool query::parse(const str32_t in, std::pair<uint32_t, uint8_t> (*tp)(const str32_t, char_t *))
{
        ast_parser ctx{in, allocator, tp};

	tokensParser = tp; // May come in handy later
        root = parse_expr(ctx);

        if (!root)
        {
                Print("Failed to parse\n");
                return false;
        }

        // perform trivial optimizations here
        // This is important, because this is where we remove DUMMY nodes etc

        Print(ansifmt::color_red, "parsed:", ansifmt::reset, *root, "\n");
        Print(ansifmt::bold, ansifmt::color_blue, "OPTIMIZING AST", ansifmt::reset, "\n");

        root = normalize_root(root);

        if (!root)
                return false;

        Print(ansifmt::color_red, "normalized:", ansifmt::reset, *root, "\n");

        return true;
}
