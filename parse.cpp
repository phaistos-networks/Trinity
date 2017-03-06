// New trinity infrastructure
#include <switch.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include <text.h>


#pragma mark Parser Infrastructure

static constexpr uint8_t UnaryOperatorPrio{100};

enum class Operator : uint8_t
{
        NONE,
        AND,
        NOT,
        OR,
        STRICT_AND, 	// mostly equivalent to "token"
};

struct phrase;

struct ast_node
{
        union {
                struct
                {
                        ast_node *lhs, *rhs;
                        Operator op;
                } binop;

                struct
                {
                        ast_node *expr;
                        Operator op;
                } unaryop;

		// for both type::Token and type::Phrase
                phrase *p;
        };

        enum class Type : uint8_t
        {
                BinOp = 0,
		// We need different types for phrases and tokens, because
		// we may have a phrase with a single token, but we need to know that it is indeed a phrase
		// so that we won't try to expand it e.g "ipad" should not be expanded to synonyms etc
		Token,
		Phrase,
                UnaryOp,
                Dummy,
		ConstFalse
        } type;


	// this is handy if you want to delete a node
	// normalize_root() will GC those nodes
	constexpr void set_dummy() noexcept
	{
		type = Type::Dummy;
	}

	constexpr bool is_unary() const noexcept
	{
		return type == Type::Phrase || type == Type::Token;
	}

	constexpr bool is_dummy() const noexcept
	{
		return type == Type::Dummy;
	}

	constexpr void set_const_false() noexcept
	{
		type = Type::ConstFalse;
	}

	constexpr bool is_const_false() const noexcept
	{
		return type == Type::ConstFalse;
	}

	static ast_node *make(simple_allocator &a, const Type t)
	{
		auto r = a.Alloc<ast_node>();

		r->type = t;
		return r;
	}
};

static constexpr auto unary_same_type(const ast_node *a, const ast_node *b) noexcept
{
	return a->type == b->type && (a->type == ast_node::Type::Phrase || b->type == ast_node::Type::Token);
}

struct term
{
        strwlen8_t token;
};

// TODO: should hold a uint32_t(*)(const char *, const char *) cb
// for specifying the parser
struct parse_ctx
{
        strwlen32_t content;
        simple_allocator &allocator;
        term terms[128];

        auto *alloc_node(const ast_node::Type t)
        {
		return ast_node::make(allocator, t);
	}

	parse_ctx(const strwlen32_t input, simple_allocator &a)
		: content{input}, allocator{a}
	{

	}

        inline void skip_ws()
        {
                content.trim_leading_whitespace();
        }

        auto parse_failnode()
        {
                // We no longer return nullptr
                // because we really can't fail (not strict)
                return alloc_node(ast_node::Type::Dummy);
        }
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

struct phrase
{
        uint8_t size;
	// repetition; e.g for [APPLE APPLE APPLE APPLE] repetition is 5
	// This is important; we need to capture e.g tom tom (the manufucacturer) and boing boing
	// but we don't want to capture the same phrase/word twice if in order
	uint8_t rep;  
	// index in the query
	uint8_t index;
	// flags. Usually 0, but e.g if you are rewritting a [wow] to [wow OR "world of warcraft"] you 
	// maybe want "world of warcraft" flags to be 1 (i.e derived)
	uint8_t flags;
        term terms[0];

        bool operator==(const phrase &o) const noexcept
        {
                if (size == o.size)
                {
                        uint8_t i;

                        for (i = 0; i != size && terms[i].token == o.terms[i].token; ++i)
                                continue;

                        return i == size;
                }
                else
                        return false;
        }
};

static term parse_term(parse_ctx &ctx)
{
	// TODO:what about unexpected characters e.g ',./+><' etc?
	// this breaks our parser
        if (const auto len = Text::TermLength(ctx.content.p, ctx.content.end()))
        {
                const term res{{ctx.content.p, len}};

                ctx.content.strip_prefix(len);
                return res;
        }
        else
                return {};
}

static ast_node *parse_phrase_or_token(parse_ctx &ctx)
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

                        if (const auto t = parse_term(ctx); t.token)
                        {
				if (n != sizeof_array(terms))
				{
					// silently ignore the rest
					// Not sure this is ideal though
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
        else if (const auto t = parse_term(ctx); t.token)
        {
                auto node = ctx.alloc_node(ast_node::Type::Token);
                auto p = (phrase *)ctx.allocator.Alloc(sizeof(phrase) + sizeof(term));

                p->size = 1;
                p->terms[0] = t;
		p->rep = 1;
		node->p = p;
                return node;
        }
        else
                return nullptr;
}

static std::pair<Operator, uint8_t> parse_operator_impl(parse_ctx &ctx)
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

static auto parse_operator(parse_ctx &ctx)
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

static void PrintImpl(Buffer &b, const ast_node &n)
{
        switch (n.type)
        {
                case ast_node::Type::Dummy:
                        b.append("<dummy>"_s8);
                        break;

                case ast_node::Type::ConstFalse:
                        b.append("<NO>"_s8);
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

                case ast_node::Type::UnaryOp:
                        b.append(n.unaryop.op);
                        if (n.unaryop.op != Operator::AND)
                                b.append(' ');
                        b.append(*n.unaryop.expr);
                        break;

                default:
                        Print("Missing for ", unsigned(n.type), "\n");
                        IMPLEMENT_ME();
        }
}

static ast_node *parse_expr(parse_ctx &);

static ast_node *parse_unary(parse_ctx &ctx)
{
        ctx.skip_ws();

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

			auto exprNode = parse_phrase_or_token(ctx)?: ctx.parse_failnode();
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


static ast_node *parse_subexpr(parse_ctx &ctx, const uint16_t limit)
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
		else if (op.first == Operator::AND  
			&& cur->type == ast_node::Type::BinOp && cur->binop.op == Operator::AND 
			&& unary_same_type(cur->binop.rhs, v)
			&& *cur->binop.rhs->p == *v->p)
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

static ast_node *parse_expr(parse_ctx &ctx)
{
        ctx.skip_ws();

        return parse_subexpr(ctx, UnaryOperatorPrio);
}

// handy utility function for e.g replacing runs
static ast_node *parse_expr(const strwlen32_t e, simple_allocator &a)
{
	parse_ctx ctx(e, a);

	return parse_expr(ctx);
}



#pragma mark Normalization
struct normalizer_ctx
{
        uint32_t updates;
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

        if (lhs->type == ast_node::Type::UnaryOp)
        {
                if (rhs->type == ast_node::Type::UnaryOp && lhs->unaryop.op == rhs->unaryop.op && lhs->unaryop.op == n->binop.op 
			&& unary_same_type(lhs->unaryop.expr, rhs->unaryop.expr)
			&& *lhs->unaryop.expr->p == *rhs->unaryop.expr->p)
                {
                        SLog("here\n");
                        n->type = ast_node::Type::UnaryOp;
                        n->unaryop.op = n->binop.op;
                        n->unaryop.expr = lhs->unaryop.expr;
                        ++ctx.updates;
                        return;
                }
#if  0 	// What's this? doesn't make any sense
                else if (rhs->type == ast_node::Type::Unary && lhs->unaryop.expr->type == ast_node::Type::Unary && rhs->unaryop.expr->type == ast_node::Type::Unary && *lhs->unaryop.expr->p == *rhs->p)
                {
                        SLog("here\n");
                        n->type = ast_node::Type::UnaryOp;
                        n->unaryop.op = n->binop.op;
                        n->unaryop.expr = lhs->unaryop.expr;
                        ++ctx.updates;
                        return;
                }
#endif
        }

        if (n->binop.op == Operator::NOT)
        {
                if (lhs->type == ast_node::Type::UnaryOp && lhs->unaryop.op == Operator::NOT 
			&& unary_same_type(lhs->unaryop.expr, rhs)
			&& *lhs->unaryop.expr->p == *rhs->p)
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

	if (lhs->type == ast_node::Type::BinOp
		&& unary_same_type(rhs, lhs->binop.rhs)
		&& *rhs->p == *lhs->binop.rhs->p)
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
        else if (n->type == ast_node::Type::UnaryOp)
        {
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
				nextIndex+=4;
		}

		assign_phrase_index(rhs, nextIndex);
	}
}

static ast_node *normalize_root(ast_node *root)
{
        if (!root)
                return nullptr;

        normalizer_ctx ctx;

        do
        {
                ctx.updates = 0;
                normalize(root, ctx);
        } while (ctx.updates);

        if (root->is_dummy())
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
static ast_node *copy_of(const ast_node *const n, simple_allocator *const a)
{
        auto res = ast_node::make(*a, n->type);

        switch (res->type)
        {
                case ast_node::Type::Token:
                case ast_node::Type::Phrase:
                        res->p = n->p;
                        break;

                case ast_node::Type::UnaryOp:
                        res->unaryop.op = n->unaryop.op;
                        res->unaryop.expr = copy_of(n->unaryop.expr, a);
                        break;

                case ast_node::Type::BinOp:
                        res->binop.op = n->binop.op;
                        res->binop.lhs = copy_of(n->binop.lhs, a);
                        res->binop.rhs = copy_of(n->binop.rhs, a);
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
				capture_leader(n->binop.lhs, out, threshold);
			}
			else if (n->binop.op == Operator::NOT && out->size() < threshold)
				capture_leader(n->binop.lhs, out, threshold);
			break;

		case ast_node::Type::UnaryOp:
			if (n->unaryop.op == Operator::AND)
				out->push_back(n->unaryop.expr);
			break;

		default:
			break;
	}
}



#pragma mark public API
struct query
{
	ast_node *root;
	simple_allocator allocator;

	bool normalize()
        {
                if (root)
                {
                        root = normalize_root(root);
                        return root;
                }
                else
                        return false;
        }


	/*
	A query can have an AND token so that we can consume documents from its docslist and check the query against that, 
	but it can also be a simple OR query (e.g [apple OR imac OR macbook]), in which case we don¢t have a single token 
	to consume from. Instead, we need to identify the ¡lead tokens¢ (in the case of an ¡AND¢ that¢s token, but in 
	the case of an OR sequence, that¢s all the distinct tokens in that OR sequence), and then use a merge-scan to get 
	the lowest next ID from those, and then execute the query based on that query and then advance all lowest such tokens

	It is a bit more complicated than that, but capture_leader() will properly identify the lead nodes

	e.g for
	- [ipad pro] : the lead token is ipad. So we just scan the ipad documents and advance those
	- [apple OR macbook OR iphone NOT cable]: the lead tokens are (apple, macbook, iphone)
	- [apple OR (macbook pro)] : lead tokens are [apple, macbook]
	*/
        void leader_nodes(std::vector<ast_node *> *const out)
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

        bool parse(const strwlen32_t in)
        {
		parse_ctx ctx{in, allocator};

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

        query() = default;

        query(const strwlen32_t in)
        {
                if (!parse(in))
                        throw Switch::data_error("Failed to parse query");
        }

	query(const query &o)
	{
		root = o.root ? copy_of(o.root, &allocator) : nullptr;
	}

	query(query &&o)
	{
		root = std::exchange(o.root, nullptr);
		allocator = std::move(o.allocator);
	}

	query &operator=(const query &o)
	{
		allocator.reuse();

		root = o.root ? copy_of(o.root, &allocator) : nullptr;
		return *this;
	}



	// utility method; returns all nodes
	static auto &nodes(ast_node *root, Switch::vector<ast_node *> *const res)
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

                                        default:
                                                break;
                                }
                        }
                }

                return *res;
        }

	static auto nodes(ast_node *root)
	{
		Switch::vector<ast_node *> out;
		
		return nodes(root, &out);
	}

	auto nodes() const
	{
		return nodes(root);
	}

        // If all you want to do is remove a node, set it a dummy
	// If all you want to do is replace a single node (i.e not a run), just replace
	// that node's value directly.
	// This is a utility method for replacing a run (see process_runs() method) with a new
	// expression(node)
	static void replace_run(ast_node **run, const size_t cnt, ast_node *newExprNode)
	{
		// Just set all nodes _except_ the first to dummy
		// and replace value of the first with the newExprNode

		for (uint32_t i{1}; i < cnt; ++i)
			run[i]->set_dummy();

		*run[0] = *newExprNode;

		// That's all there is to it
		// Now just make sure you normalize_root()
	}


	// you should _never_ process_runs() after you have invoked commit()
	//
	// This was initially implemented using a deque<> but turns out, because we
	// keep track of phrase indices, its trivial and cheap to do it this way now
	//
	// - You can _delete_ a node by setting its type to dummy
	// - it will be trivial to change a node/token to a binop or a series of binops e.g like [sf] = > [(sf OR "san francisco" OR "francisco")]
	// - changing multiple tokens (a run) is easy if you:
	//	1. set all nodes in the run you are interested in EXCEPT the first to dummy
	// 	2. create a new dummy/expression for e.g all alternative spellings 
	//		e.g for the run [world of warcraft] you may want to replace with
	//		[wow OR (world of warcraft) OR war-craft]
	//		You may just want to use build_expr() to create the new expression node
	//	3. directly replace the value of the first node with the new node
	//		*first_node = *new_expr;
	//
	// 	see replace_run() utility method comments
	//
	// query::commit() will invoke normalize_root() so it will clean up those dummy nodes
	// You _must_ commit() if you modify the query
	//
	// Make sure you check the repetition count
	// 
	// XXX: TODO:
	// when we rewrite queries, what phase index do we assign to all new nodes?
	/// TODO: for e.g [amiga NOT (video game)] we probably don't care for
	// (video game) because its in a NOT block. True for OR groups as well
	template<typename L>
	void process_runs(const bool includePhrases, const bool andOnly, L &&cb)
	{
		Switch::vector<ast_node *> unaryNodes, stack, run;

		stack.push_back(root);
		do
		{
			auto n = stack.back();

			stack.pop();
			switch (n->type)
			{
				case ast_node::Type::Token:
					unaryNodes.push_back(n);
					break;

                                case ast_node::Type::Phrase:
                                        if (includePhrases)
                                                unaryNodes.push_back(n);
                                        break;

                                case ast_node::Type::BinOp:
				if (!andOnly || n->binop.op == Operator::AND)
                                {
                                        stack.push_back(n->binop.lhs);
                                        stack.push_back(n->binop.rhs);
                                }
                                break;
				
				case ast_node::Type::UnaryOp:
					stack.push_back(n->unaryop.expr);
					break;

				case ast_node::Type::Dummy:
				case ast_node::Type::ConstFalse:
					break;
			}
		} while (stack.size());

		std::sort(unaryNodes.begin(), unaryNodes.end(), [](const auto a, const auto b)
		{
			return a->p->index < b->p->index;
		});

		for (const auto *p = unaryNodes.begin(), *const e = p + unaryNodes.size(); p != e; )
		{
			auto idx = (*p)->p->index;
			const auto base = p;

			run.clear();
			do
			{
				run.push_back(*p);
				++idx;
			} while (++p != e && (includePhrases  || (*p)->p->size == 1) && (*p)->p->index == idx);

			cb(run);
		}
	}
};



#pragma mark compiler and execution pipeline
// tightly packed node (4 bytes)
struct exec_node
{
	// dereference implementation in an array of void(*)(void)
	// so that we won't have to use 8 bytes for the pointer
	uint8_t implIdx;

	uint8_t flags;

	// instead of using unions here
	// we will have a node/impl specific context allocated elsehwere with the appropriate size
	// so that the implementaiton can refer to it
	uint16_t nodeCtxIdx;
};

static_assert(sizeof(exec_node) == sizeof(uint32_t), "Unexpected sizeof(exec_node)");

struct runtime_ctx
{
	struct binop_ctx
	{
		exec_node lhs;
		exec_node rhs;
	};

	struct unaryop_ctx
	{
		exec_node expr;
	};


	uint32_t curDocID;

	struct
	{
		binop_ctx *binaryOps;
		unaryop_ctx *unaryOps;
	} evalnode_contexts;


	std::vector<binop_ctx> binOpContexts;
	std::vector<unaryop_ctx> unaryOpContexts;

	uint16_t register_binop(const exec_node lhs, const exec_node rhs)
	{
		binOpContexts.push_back({lhs, rhs});
		return binOpContexts.size() - 1;
	}

	uint16_t register_unaryop(const exec_node expr)
	{
		unaryOpContexts.push_back({expr});
		return unaryOpContexts.size() - 1;
	}

	uint16_t register_token(const phrase *p)
	{
		return 0;
	}

	uint16_t register_phrase(const phrase *p)
	{
		return 0;
	}

	uint32_t token_eval_cost(const strwlen8_t token)
	{
		return 1;
	}

	// TODO: for phrases, we should return the (lowest cardinality among all phrase tokens) * n->p->size
	// TODO: query via a callback or lambda the environment for the cardinality of the tokens of the phrase
	// if any of them has cardilality zero, { n->set_const_false() ; updates = true; return 0; }
	// and normalize_root() later so that it will be removed, otherwise return the lowest cardinality among the phrase tokens X phrase tokens count
	uint32_t phrase_eval_cost(const phrase *const p)
	{
		return p->size;
	}
};

static uint32_t optimize_binops_impl(ast_node *const n, bool &updates, runtime_ctx &rctx)
{
        switch (n->type)
        {
                case ast_node::Type::Token:
			return rctx.token_eval_cost(n->p->terms[0].token);

                case ast_node::Type::Phrase:
			return rctx.phrase_eval_cost(n->p);

                case ast_node::Type::BinOp:
                {
                        const auto lhsCost = optimize_binops_impl(n->binop.lhs, updates, rctx);

                        if (lhsCost == UINT32_MAX)
                        {
                                if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND)
                                {
                                        n->set_const_false();
                                        return UINT32_MAX;
                                }
                        }

                        const auto rhsCost = optimize_binops_impl(n->binop.rhs, updates, rctx);

                        if (rhsCost == UINT32_MAX && lhsCost == UINT32_MAX && n->binop.op == Operator::OR)
                        {
                                n->set_const_false();
                                return UINT32_MAX;
                        }

                        if (rhsCost < lhsCost && n->binop.op != Operator::NOT) // can't reorder NOT
                        {
                                std::swap(n->binop.lhs, n->binop.rhs);
                        }

                        return lhsCost + rhsCost;
                }

                case ast_node::Type::UnaryOp:
                        if (const auto cost = optimize_binops_impl(n->unaryop.expr, updates, rctx); cost == UINT32_MAX)
                        {
                                n->set_const_false();
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
// it is important to first make a pass using reorder_root() and then optimize_binops()
static ast_node *optimize_binops(ast_node *root, runtime_ctx &rctx)
{
        for (bool updates = false; root; updates = false)
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



// TODO: consider passing exec_node& instead of exec_node
// to impl. so that can modify themselves if needed
typedef uint8_t (*node_impl)(exec_node, runtime_ctx &);

enum class OpCodes : uint8_t
{
        MatchToken = 0,
        LogicalAnd,
        LogicalOr,
        MatchPhrase,
	LogicalNot,
        UnaryAnd,
        UnaryNot,
        ConstFalse
};

static inline uint8_t eval(const exec_node node, runtime_ctx &ctx);

static uint8_t logical_and_impl(const exec_node self, runtime_ctx &ctx)
{
	const auto binopctx = &ctx.evalnode_contexts.binaryOps[self.nodeCtxIdx];

	return eval(binopctx->lhs, ctx) && eval(binopctx->rhs, ctx);
}

static inline uint8_t noop_impl(const exec_node, runtime_ctx &)
{
	return 0;
}

static constexpr node_impl implementations[] = 
{
	noop_impl,
	noop_impl,
	noop_impl,
	noop_impl,
	noop_impl,
	noop_impl,
	noop_impl,
	noop_impl,
};

uint8_t eval(const exec_node node, runtime_ctx &ctx)
{
	return implementations[node.implIdx](node, ctx);
}

static exec_node compile(const ast_node *const n, runtime_ctx &ctx)
{
        exec_node res;

        res.flags = 0;
        require(n);
        switch (n->type)
        {
		case ast_node::Type::Dummy:
			std::abort();

                case ast_node::Type::Token:
                        res.implIdx = (unsigned)OpCodes::MatchToken;
                        res.nodeCtxIdx = ctx.register_token(n->p);
                        break;

                case ast_node::Type::Phrase:
                        if (n->p->size == 1)
                        {
                                res.implIdx = (unsigned)OpCodes::MatchToken;
                                res.nodeCtxIdx = ctx.register_token(n->p);
                        }
                        else
                        {
                                res.implIdx = (unsigned)OpCodes::MatchPhrase;
                                res.nodeCtxIdx = ctx.register_phrase(n->p);
                        }
                        break;

                case ast_node::Type::BinOp:
                        switch (n->binop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        res.implIdx = (unsigned)OpCodes::LogicalAnd;
                                        break;

                                case Operator::OR:
                                        res.implIdx = (unsigned)OpCodes::LogicalOr;
                                        break;

                                case Operator::NOT:
                                        res.implIdx = (unsigned)OpCodes::LogicalNot;
                                        break;

				case Operator::NONE:
					std::abort();
					break;
                        }
                        res.nodeCtxIdx = ctx.register_binop(compile(n->binop.lhs, ctx), compile(n->binop.rhs, ctx));
                        break;

                case ast_node::Type::ConstFalse:
                        res.implIdx = (unsigned)OpCodes::ConstFalse;
                        break;

                case ast_node::Type::UnaryOp:
                        switch (n->unaryop.op)
                        {
                                case Operator::AND:
                                case Operator::STRICT_AND:
                                        res.implIdx = (unsigned)OpCodes::UnaryAnd;
                                        break;

                                case Operator::NOT:
                                        res.implIdx = (unsigned)OpCodes::UnaryNot;
                                        break;

				default:
					std::abort();
                        }
                        res.nodeCtxIdx = ctx.register_unaryop(compile(n->unaryop.expr, ctx));
                        break;
        }

        return res;
}

// Considers all binary ops, and potentiall swaps (lhs, rhs) of binary ops, but not based on actual cost
// See optimize_binops(), which does a similar job, except it takes into account the cost to evaluate each branch
struct reorder_ctx
{
        bool dirty;
};

static void reorder(ast_node *n, reorder_ctx *const ctx)
{
        if (n->type == ast_node::Type::BinOp)
        {
                auto lhs = n->binop.lhs, rhs = n->binop.rhs;

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


// If we have multiple segments, we should invoke exec() for each of them
// in parallel or in sequence, collect the top X hits and then later merge them
//
// We will need to create a copy of the `q` after we have normalized it, and then
// we need to reorder and optimize that copy, get leaders and execute it -- for each segment, but
// this is a very fast operation anyway
static bool exec(const query &in)
{
	if (!in.root)
		return false;

	// we need a copy of that query here
	// for we we will need to modify it
	query q(in);


	// Normalize just in case
	if (!q.normalize())
		return false;

	runtime_ctx rctx;

	// Optimizations we shouldn't perform on the parsed query because
	// the rewrite it by potentially moving nodes around or dropping nodes
	reorder_root(q.root);
	q.root = optimize_binops(q.root, rctx);

	if (!q.root)
	{
		// After optimizations nothing's left
		return false;
	}


	// We need the leader nodes 
	// See leader_nodes() impl. comments
	std::vector<ast_node *> leaderNodes;
	Switch::vector<strwlen8_t> leaderTokensV;
	uint16_t toAdvance[1024];

	q.leader_nodes(&leaderNodes);

	for (const auto n : leaderNodes)
	{
		for (uint32_t i{0}; i != n->p->size; ++i)
			leaderTokensV.push_back(n->p->terms[i].token);
	}

        std::sort(leaderTokensV.begin(), leaderTokensV.end(), [](const auto &a, const auto &b) {
                return Text::StrnncasecmpISO88597(a.data(), a.size(), b.data(), b.size()) < 0;
        });

	leaderTokensV.resize(std::unique(leaderTokensV.begin(), leaderTokensV.end()) - leaderTokensV.begin());

	Print("leaderTokens:", leaderTokensV, "\n");

	// TODO: reset() for every leader token 




        // We can now compile the AST to an optimised bytecode representation
	auto leaderTokens = leaderTokensV.data();
	uint32_t leaderTokensCnt = leaderTokensV.size();
	const auto rootExecNode = compile(q.root, rctx);


#if 0 
	// TODO: if (q.root->type == ast_node::Type::Token) {} 
	// i.e if just a single term was entered, scan that single token's documents  without even having to use a decoder
	// otherwise use the loop that tracks lead tokens
	for (;;)
        {
                uint32_t docID{UINT32_MAX};
		uint8_t toAdvanceCnt{0};

                for (uint32_t i{0}; i != leaderTokensCnt; ++i)
                {
                        const auto token = leaderTokens[i];
                        const auto token_docid = rctx.cur_token_document(token);

                        if (token_docid < docID)
                        {
				docID = token_docid;
                                toAdvance[0] = i;
                                toAdvanceCnt = 1;
                        }
                        else if (token_docid == docID)
                                toAdvance[toAdvanceCnt++] = i;
                }

                if (!toAdvanceCnt)
                {
                        // we are done
                        break;
                }

		// now execute rootExecNode 
		// and it it returns true, compute the document's score
		rctx.curDocID = docID;

                while (toAdvanceCnt)
                {
                        const auto idx = --toAdvanceCnt;
                        const auto token = toAdvance[idx];

                        if (!rctx.next(token))
                        {
                                // done with this leaf token
                                memmove(leadTokens + idx, leadTokens + idx + 1, (--leadTokensCnt - idx) * sizeof(leadTokens[0]));
                        }
                }
        }
#endif

        return true;
}


























int main(int argc, char *argv[])
{
        query q;

        if (!q.parse(strwlen32_t(argv[1])))
		return 1;

#if 1
        q.process_runs(true, true, [&q](auto &v) {
		Buffer b("RUN:"_s32);

		for(const auto n : v)
			print_phrase(b, n->p);
		Print(b, "\n");

		for (uint32_t i{0}; i != v.size(); ++i)
		{	
			const auto it = v[i];

			if (it->p->size == 1 && it->p->terms[0].token.Eq(_S("jump")))
			{
				*it = *parse_expr("(jump OR hop OR leap)"_s32, q.allocator);
			}
                        else if (i +3 <= v.size() && it->p->size == 1 && it->p->terms[0].token.Eq(_S("world")) 
				&& v[i+1]->p->size == 1 && v[i+1]->p->terms[0].token.Eq(_S("of"))
				&& v[i+2]->p->size == 1 && v[i+2]->p->terms[0].token.Eq(_S("warcraft")))
			{
				query::replace_run(v.data() + i, 3, parse_expr("(wow OR (world of warcraft))"_s32, q.allocator));
			}
			else if (it->p->size == 1 && it->p->terms[0].token.Eq(_S("puppy")))
			{
				*it = *parse_expr("puppy OR (kitten OR kittens OR cat OR cats OR puppies OR dogs OR pets)"_s32, q.allocator);
			}
                }
        });

	q.normalize();
	Print("HERE:", *q.root, "\n");


	{
		std::vector<ast_node *> nodes;

		q.leader_nodes(&nodes);

		Print(nodes.size(), " leader nodes\n");
		for (const auto it : nodes)
			Print(*it, "\n");

	}




	exec(q);

#endif

        return 0;
}
