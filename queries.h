#pragma once
#include "common.h"
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
                        ast_node *expr;
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
                        Dummy, // Also treat as a ConstTrue node (normalize will deal with it)
                        ConstFalse,
                        // Suppose you want to match special tokens FOO|BAR and FANCY|TERM if they are found in the index, i.e treat them as optional.
                        // That¢s easy. You can get the original query e.g [apple iphone] and transform it like so `(FOO|BAR OR FANCY|TERM) OR (apple iPhone)` and that¢d work great.
                        // However, imagine you want to match FOO|BAR and FANCY|TERM but ONLY IF the original query also matches. You can¢t practically construct that query like the one above.
                        // This is where you 'd use this node. When evaluated it will always return true after evaluating its expression.
                        // This allows for this kind of expression and other such use cases.
                        ConstTrueExpr,
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

                // see query::leader_nodes() comments
                bool any_leader_tokens() const;

                // Revursively sets p->flags to `flags` to all nodes of this branch
                //
                // This is helpful when you for example rewrite an original query to incorporate synonyms etc
                // and you want to know in your MatchedIndexDocumentsFilter::consume() impl if the term hit is for a token from
                // any of the tokens added to the query after the rewrite.
                void set_alltokens_flags(const uint8_t flags);
        };

        static constexpr auto unary_same_type(const ast_node *a, const ast_node *b) noexcept
        {
                return a->type == b->type && (a->type == ast_node::Type::Phrase || b->type == ast_node::Type::Token);
        }

        struct term final
        {
                // for now, just the token but maybe in the future we 'll want to extend to support flags etc
                str8_t token;
        };

        // This is an AST parser
        //
        // Encapsulates the input query(text) to be parsed,
        // a reference to the allocator to use, and the function to use for parsing tokens from the content
        //
        // This is mainly used by `Trinity::query` to parse its root node, but you can use it for parsing expressions in order to replace runs with them.
        // See app.cpp for examples.
        //
        // A query only holds its root node and its own allocator, and uses a ast_parser to parse the query input. See query::parse()
        //
        // Keep in mind that you can always copy nodes/asts using ast_node::copy()
        struct ast_parser final
        {
                str32_t content;
                simple_allocator &allocator;
                term terms[Trinity::Limits::MaxPhraseSize];
                uint32_t (*token_parser)(const str32_t);
                std::vector<str8_t> distinctTokens;
		// facilitates parsing
		std::vector<str8_t::value_type> groupTerm;

                auto *alloc_node(const ast_node::Type t)
                {
                        return ast_node::make(allocator, t);
                }

                ast_parser(const str32_t input, simple_allocator &a, uint32_t (*p)(const str32_t) = default_token_parser_impl)
                    : content{input}, allocator{a}, token_parser{p}
                {
                }

                // you may NOT invoke parse() again on this context
                // because content, allocator, distinctTokens will already be initialized and updated
                //
                // Make sure that the allocate you provide is not deleted or reused before you are done accessing
                // any query tokens
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

                void track_term(term &t);
        };

        struct phrase final
        {
                // total terms (1 for a single token)
                uint8_t size;

                // repetition; e.g for [APPLE APPLE APPLE APPLE] repetition is 5
                // This is important; we need to capture e.g tom tom (the manufucacturer) and boing boing
                // but we don't want to capture the same phrase/word twice if in order
                uint8_t rep;

                // index in the query
                uint16_t index;

                // flags. Usually 0, but e.g if you are rewritting a [wow] to [wow OR "world of warcraft"] you
                // maybe want "world of warcraft" flags to be 1 (i.e derived)
                //
                // See ast_node::set_alltokens_flags()
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

        // see query::normalize()
        ast_node *normalize_ast(ast_node *);

        // this is really just a container for an ast root node and the allocator
        // used to allocate the AST from.
        // It also provides a few useful methods that operate on the root and may make use of the allocator
        struct query final
        {
                ast_node *root;
                simple_allocator allocator{512};

                // Normalize a query.
                // This is invoked when you initially parse the query, but if you
                // rewrite it (i.e its AST) by e.g replacing runs or otherwise modifying or deleting ast nodes
                // you must normalize it to fix any issues etc
                //
                // You can also use Trinity::normalize_ast() if you want to do this youserlf
		// When you Trinity::exec_query(), it will be normalized again just in case
                bool normalize();

                /*
		   A query can have an AND token so that we can consume documents from its docslist and check the query against that, 
		   but it can also be a simple OR query (e.g [apple OR imac OR macbook]), in which case we don¢t have a single token 
		   to consume from. Instead, we need to identify the ¡lead tokens¢ (in the case of an ¡AND¢ that¢s token, but in 
		   the case of an OR sequence, that¢s all the distinct tokens in that OR sequence), and then use a merge-scan to get 
		   the lowest next ID from those, and then execute the query based on that query and then advance all lowest such tokens.

		   It is a bit more complicated than that, but capture_leader() will properly identify the lead nodes

		   e.g for
		   - [ipad pro] : the lead token is ipad. So we just scan the ipad documents and advance those
		   - [apple OR macbook OR iphone NOT cable]: the lead tokens are (apple, macbook, iphone)
		   - [apple OR (macbook pro)] : lead tokens are [apple, macbook]
		 */
                void leader_nodes(std::vector<ast_node *> *const out);

                bool parse(const str32_t in, uint32_t (*tp)(const str32_t) = default_token_parser_impl);

                query() = default;

                inline operator bool() const noexcept
                {
                        return root;
                }

                query(const str32_t in, uint32_t (*tp)(const str32_t) = default_token_parser_impl)
                {
                        if (!parse(in, tp))
                                throw Switch::data_error("Failed to parse query");
                }

		query(ast_node *r)
			: root{r}
		{

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
                //
                // See also ast_node::set_alltokens_flags()
                template <typename L>
                void process_runs(const bool includePhrases, const bool andOnly, L &&cb)
                {
                        std::vector<ast_node *> unaryNodes, stack, run;

                        stack.push_back(root);
                        do
                        {
                                auto n = stack.back();

                                stack.pop_back();
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
                                        case ast_node::Type::ConstTrueExpr:
                                                break;
                                }
                        } while (stack.size());

                        std::sort(unaryNodes.begin(), unaryNodes.end(), [](const auto a, const auto b) {
                                return a->p->index < b->p->index;
                        });

                        for (const auto *p = unaryNodes.data(), *const e = p + unaryNodes.size(); p != e;)
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

void PrintImpl(Buffer &b, const Trinity::ast_node &n);
inline void PrintImpl(Buffer &b, const Trinity::query &q)
{
        if (q.root)
                b.append(*q.root);
        else
                b.append("<not initialized>"_s32);
}
