#include "queries.h"

using namespace Trinity;

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
        // TODO:what about unexpected characters e.g ',./+><' etc?
        // this breaks our parser
        if (const auto len = ctx.token_parser(ctx.content))
        {
		const str32_t res(ctx.content.p, len);

                ctx.content.strip_prefix(len);
                return res;
        }
        else
                return {};
}

static ast_node *parse_phrase_or_token(ast_parser &ctx)
{
        ctx.skip_ws();
        if (ctx.content && ctx.content.StripPrefix(_S("\"")))
        {
                auto &terms = ctx.terms;
                uint8_t n{0};

                for (;;)
                {
                        ctx.skip_ws();
                        if (!ctx.content || ctx.content.StripPrefix(_S("\"")))
                                break;

                        if (const auto token = parse_term(ctx))
                        {
				if (unlikely(token.size() > Limits::MaxTermLength))
					return ctx.alloc_node(ast_node::Type::ConstFalse);
				
				term t;

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

                if (isalnum(f) || f == '\"' || f == '(')
                        return {Operator::AND, 0};
                else
                        return {Operator::NONE, 0};
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

static void print_phrase(Buffer &b, const phrase *const p)
{
        b.append('"');
        for (uint32_t i{0}; i != p->size; ++i)
                b.append(p->terms[i].token, ' ');
        if (p->size)
                b.shrink_by(1);
        b.append('"');
        if (p->rep > 1)
                b.append('(', p->rep, ')');
        b.append('[', p->index, ']');
}

static void print_token(Buffer &b, const phrase *const p)
{
        b.append(p->terms[0].token);
        if (p->rep > 1)
                b.append('(', p->rep, ')');
        b.append('[', p->index, ']');
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
                        print_phrase(b, n.p);
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
                auto e = parse_expr(ctx) ?: ctx.parse_failnode();

                ctx.skip_ws();
                if (!ctx.content.StripPrefix(_S(">")))
                {
                        if (e->type != ast_node::Type::Dummy)
                                e = ctx.parse_failnode();
                }

		auto res = ctx.alloc_node(ast_node::Type::ConstTrueExpr);

		res->expr = e;
		return res;
	}
#endif

        if (ctx.content.StripPrefix(_S("(")))
        {
                auto e = parse_expr(ctx) ?: ctx.parse_failnode();

                ctx.skip_ws();
                if (!ctx.content.StripPrefix(_S(")")))
                {
                        if (e->type != ast_node::Type::Dummy)
                                e = ctx.parse_failnode();
                }

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

                SLog("lhs = ", *cur, "\n");
                SLog(op.first, "\n");
                SLog("rhs = ", *v, "\n");

                if (op.first == Operator::AND && unary_same_type(cur, v) && *cur->p == *v->p)
                {
                        // [apple AND apple] => [apple rep=2]
                        cur->p->rep += v->p->rep;
                }
                else if (op.first == Operator::AND && cur->type == ast_node::Type::BinOp && cur->binop.op == Operator::AND && unary_same_type(cur->binop.rhs, v) && *cur->binop.rhs->p == *v->p)
                {
                        // (of AND apple) AND apple
                        cur->binop.rhs->p->rep += v->p->rep;
                        v = ctx.alloc_node(ast_node::Type::Dummy);
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
//
// TODO:
// 	[ (sf OR "san franscisco") sf ]
//  	[  foo NOT (foo OR bar) ]
static void normalize_bin(ast_node *const n, normalizer_ctx &ctx)
{
        // foo OR foo OR foo
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
                        *n = *rhs;
                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::NOT)
                {
                        *n = *rhs;
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
                                        SLog("here\n");
                                        n->set_const_false();
                                }
                                else
                                {
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
                                SLog("here\n");
                                n->set_const_false();
                                ++ctx.updates;
                                return;
                        }
			else if (n->binop.op == Operator::OR)
			{
				// [APPLE OR +APPLE]
				SLog("here\n");
				*n = *rhs;
				++ctx.updates;
				return;
			}
			else 
			{
				// [APPLE AND +APPLE]
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
                                SLog("here\n");
                                n->set_const_false();
                                ++ctx.updates;
                                return;
                        }
			else if (n->binop.op == Operator::OR)
			{
				// [+APPLE OR APPLE]
				SLog("here\n");
				*n = *lhs;
				++ctx.updates;
				return;
			}
			else 
			{
				// [+APPLE AND APPLE]
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
                        SLog("here\n");
                        n->binop.op = rhs->unaryop.op;
                        n->binop.rhs = rhs->unaryop.expr;
                        ++ctx.updates;
                        return;
                }

                if (lhs->type == ast_node::Type::UnaryOp && lhs->unaryop.op == Operator::NOT)
                {
                        SLog("here:", *n, "\n");
                        n->binop.op = lhs->unaryop.op;
                        n->binop.lhs = rhs;
                        n->binop.rhs = lhs->unaryop.expr;
                        ++ctx.updates;
                        SLog("now:", *n, "\n");
                        return;
                }
        }

        if (n->binop.op == Operator::AND && rhs->is_dummy())
        {
                // (foo AND bar)  AND dummy
                SLog("here\n");
                *n = *lhs;
                ++ctx.updates;
                return;
        }

        if (n->binop.op == Operator::AND && lhs->is_dummy())
        {
                // (foo AND bar)  AND dummy
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
                        SLog("now:", *n, "\n");
                        return;
                }
        }

        require(rhs);
        require(lhs);
        require(lhs->binop.lhs);

        if (lhs->type == ast_node::Type::BinOp && unary_same_type(rhs, lhs->binop.rhs) && *rhs->p == *lhs->binop.rhs->p)
        {
                SLog("consider\n");
                if (lhs->binop.op == n->binop.op)
                {
                        // [macboook OR macbook OR macbook] => [macboook OR macbook]
                        SLog("here\n");

                        rhs->set_dummy();
                        ++ctx.updates;
                        return;
                }
                else if (lhs->binop.op == Operator::NOT && (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND))
                {
                        // [macboook NOT macbook AND macbook]
                        SLog("here\n");

                        rhs->set_const_false();
                        ++ctx.updates;
                        return;
                }
                else if (n->binop.op == Operator::NOT && (lhs->binop.op == Operator::AND || lhs->binop.op == Operator::STRICT_AND))
                {
                        // [macboook AND macbook NOT macbook]
                        SLog("here\n");

                        n->set_const_false();
                        ++ctx.updates;
                        return;
                }
        }
}

static void normalize(ast_node *const n, normalizer_ctx &ctx)
{
        if (n->type == ast_node::Type::BinOp)
                normalize_bin(n, ctx);
        else if (n->is_unary() && n->p->size == 0)
        {
                SLog("here\n");
                n->set_dummy();
                ++ctx.updates;
        }
	else if (n->type == ast_node::Type::ConstTrueExpr)
	{
		normalize(n->expr, ctx);
		if (n->expr->is_dummy() || n->expr->is_const_false())
		{
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
                        SLog("here\n");
                        n->set_dummy();
                        ++ctx.updates;
                }
                else if (n->unaryop.op == Operator::AND)
                {
                        SLog("here\n");
                        *n = *n->unaryop.expr;
                        ++ctx.updates;
                }
                else if (n->unaryop.op == Operator::OR)
                {
                        SLog("here:", *n, "\n");
                        *n = *n->unaryop.expr;
                        ++ctx.updates;
                        SLog("after:", *n, "\n");
                }
        }
	else if (n->type == ast_node::Type::Token || n->type == ast_node::Type::Phrase)
		ctx.tokensCnt += n->p->size;
}

static void assign_phrase_index(ast_node *const n, uint32_t &nextIndex)
{
        if (n->is_unary())
        {
                n->p->index = nextIndex;
                nextIndex += n->p->size;
        }
        else if (n->type == ast_node::Type::UnaryOp)
        {
                assign_phrase_index(n->unaryop.expr, nextIndex);
        }
        else if (n->type == ast_node::Type::BinOp)
        {
                auto lhs = n->binop.lhs, rhs = n->binop.rhs;

                assign_phrase_index(lhs, nextIndex);

                if (n->binop.op == Operator::OR)
                {
                        if (lhs->is_unary())
                                nextIndex -= lhs->p->size;
                        else if (lhs->type == ast_node::Type::BinOp && lhs->binop.op == Operator::OR && lhs->binop.rhs->is_unary())
                                nextIndex -= lhs->binop.rhs->p->size;
                }
                else if (n->binop.op == Operator::NOT)
                        nextIndex += 8;
                else if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                {
                        if (lhs->type == ast_node::Type::BinOp && (lhs->binop.op != Operator::AND && lhs->binop.op != Operator::STRICT_AND && lhs->binop.op != Operator::OR))
                                nextIndex += 4;
                }

                assign_phrase_index(rhs, nextIndex);
        }
}

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
		SLog("Too many query tokens\n");
		root = nullptr;
	}
        else if (root->is_dummy())
        {
                Print("Ignoring dummy root\n");
                root = nullptr;
        }
        else if (root->is_const_false())
        {
                Print("Ignoring const false\n");
                root = nullptr;
        }
        else if (root->type == ast_node::Type::UnaryOp)
        {
                if (root->unaryop.op == Operator::NOT)
                {
                        Print("Ignoring unary NOT\n");
                        root = nullptr;
                }
                else if (root->unaryop.op == Operator::OR || root->unaryop.op == Operator::AND)
                {
                        SLog("From op\n");
                        *root = *root->unaryop.expr;
                }
        }
	else if (!root->any_leader_tokens())
	{
		// e.g [  -foo ( -bar -hello)  ] 
		Print("No Leader Tokens\n");
		root = nullptr;
	}

        if (root)
        {
                // We are assining phrase indices here
                // We originally assigned indices when we parsed the query, but this
                // is not optimal, because the query can be updated, or we can just build
                // the query ourselves programmatically, and whatever the case, we need
                // to get those phrase indices properly
                // Because we are going to normalize_root() whenever we update the query structure and
                // when we are committing the changes, we are guaranteed to get this right
                // Keeping this simpler is a great side-effect
                uint32_t nextIndex{0};

                assign_phrase_index(root, nextIndex);
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
        std::vector<const ast_node *> stack;

        stack.push_back(this);
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
        // This is a proof of concept
        // you shouldn't need to do this for queries, only for runtime queries
        // where we have gotten a chance to reorder branches
        // so that lhs is always the cheaper to evaluate
        // see reorder_root() and optimize_binops()
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

bool query::parse(const str32_t in, uint32_t(*tp)(const str32_t))
{
        ast_parser ctx{in, allocator, tp};

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
