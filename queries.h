#pragma once
#include <switch.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include <text.h>

namespace Trinity
{
        static constexpr uint8_t UnaryOperatorPrio{100};

        enum class Operator : uint8_t
        {
                NONE,
                AND,
                NOT,
                OR,
                STRICT_AND, // mostly equivalent to "token"
        };

        struct phrase;

        struct ast_node final
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

		ast_node *copy(simple_allocator *const a);
        };

        static constexpr auto unary_same_type(const ast_node *a, const ast_node *b) noexcept
        {
                return a->type == b->type && (a->type == ast_node::Type::Phrase || b->type == ast_node::Type::Token);
        }

        struct term final
        {
                strwlen8_t token;
        };

        // TODO: should hold a uint32_t(*)(const char *, const char *) cb
        // for specifying the parser
        struct parse_ctx final
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

		ast_node *parse();

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

        struct phrase final
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


        struct query final
        { 
                ast_node *root;
                simple_allocator allocator;

                bool normalize();

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
                void leader_nodes(std::vector<ast_node *> *const out);

                bool parse(const strwlen32_t in);

                query() = default;

		inline operator bool() const noexcept
		{
			return root;
		}

                query(const strwlen32_t in)
                {
                        if (!parse(in))
                                throw Switch::data_error("Failed to parse query");
                }

                query(const query &o)
                {
                        root = o.root ? o.root->copy(&allocator) : nullptr;
                }

                query(query &&o)
                {
                        root = std::exchange(o.root, nullptr);
                        allocator = std::move(o.allocator);
                }

                query &operator=(const query &o)
                {
                        allocator.reuse();

                        root = o.root ? o.root->copy(&allocator) : nullptr;
                        return *this;
                }

                // utility method; returns all nodes
                static Switch::vector<ast_node *> &nodes(ast_node *root, Switch::vector<ast_node *> *const res);

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
                template <typename L>
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

                        std::sort(unaryNodes.begin(), unaryNodes.end(), [](const auto a, const auto b) {
                                return a->p->index < b->p->index;
                        });

                        for (const auto *p = unaryNodes.begin(), *const e = p + unaryNodes.size(); p != e;)
                        {
                                auto idx = (*p)->p->index;

                                run.clear();
                                do
                                {
                                        run.push_back(*p);
                                        ++idx;
                                } while (++p != e && (includePhrases || (*p)->p->size == 1) && (*p)->p->index == idx);

                                cb(run);
                        }
                }
        };
}
