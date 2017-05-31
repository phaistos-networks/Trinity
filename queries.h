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

        // A query is an ASTree
        struct ast_node final
        {
                union {
                        struct
                        {
                                ast_node *lhs, *rhs;
                                Operator op;

                                constexpr auto normalized_operator() const noexcept
                                {
                                        return op == Operator::STRICT_AND ? Operator::AND : op;
                                }
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
                        Dummy,      // also semantically equivalent to both a 'true' node and a useless node. normalize_root() will deal with such nodes(GC)
                        ConstFalse, // normalize_root() will GC it
                        // Suppose you want to match special tokens FOO|BAR and FANCY|TERM if they are found in the index, i.e treat them as optional.
                        // That¢s easy. You can get the original query e.g [apple iphone] and transform it like so [(FOO|BAR OR FANCY|TERM) OR (apple iPhone)] and that¢d work great.
                        //
                        // However, imagine you want to match FOO|BAR and FANCY|TERM but ONLY IF the original query also matches. You can¢t practically construct that query like the one above.
                        // This is where you 'd use this node. When evaluated it will always return true after evaluating its expression.
                        // You will transform it to [(FOO|BAR OR FANCY|TERM) AND (apple iphone)]
                        // If [apple iphone] matches THEN it will attempt to match [FOO|BAR OR FANCY|TERM], but even if that sub-expression fails, it won't matter.
                        // This allows for this kind of expression and other such use cases.
                        //
                        // The only challenge is properly handling this node(and the related exec_node) properly when capturing leading nodes. See
                        // the respective methods implementations for details.
                        //
                        // You an also think of this as an 'optional match' node.
                        ConstTrueExpr,
                } type;

                // this is handy if you want to delete a node
                // normalize_root() will GC those nodes
                constexpr void set_dummy() noexcept
                {
                        type = Type::Dummy;
                }

                constexpr bool is_binop() const noexcept
                {
                        return type == Type::BinOp;
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

                constexpr bool is_token() const noexcept
                {
                        return type == Type::Token;
                }

                constexpr bool is_phrase() const noexcept
                {
                        return type == Type::Phrase;
                }

                static ast_node *make(simple_allocator &a, const Type t)
                {
                        auto r = a.Alloc<ast_node>();

                        r->type = t;
                        return r;
                }

                static ast_node *make_binop(simple_allocator &a)
                {
			return make(a, Type::BinOp);
                }

                ast_node *copy(simple_allocator *const a);

		// same as copy(), except we are not copying tokens
                ast_node *shallow_copy(simple_allocator *const a);

                // see query::leader_nodes() comments
                bool any_leader_tokens() const;

                // Revursively sets p->flags to `flags` to all nodes of this branch
                //
                // This is helpful when you for example rewrite an original query to incorporate synonyms etc
                // and you want to know in your MatchedIndexDocumentsFilter::consume() impl if the term hit is for a token from
                // any of the tokens added to the query after the rewrite.
                void set_alltokens_flags(const uint8_t flags);

                size_t nodes_count() const noexcept
                {
                        switch (type)
                        {
                                case Type::BinOp:
                                        return binop.lhs->nodes_count() + binop.rhs->nodes_count() + 1;

                                case Type::ConstTrueExpr:
                                case Type::UnaryOp:
                                        return 1 + expr->nodes_count();

                                default:
                                        return 1;
                        }
                }
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
                const char_t *contentBase;
                simple_allocator &allocator;
                term terms[Trinity::Limits::MaxPhraseSize];
                // It is important that your queries token parser semantics are also implemented in your documents content parser
                std::pair<uint32_t, uint8_t> (*token_parser)(const str32_t, char_t *);
                std::vector<str8_t> distinctTokens;
                // facilitates parsing
                std::vector<char_t> groupTerm;
                char_t lastParsedToken[255];

                auto *alloc_node(const ast_node::Type t)
                {
                        return ast_node::make(allocator, t);
                }

                ast_parser(const str32_t input, simple_allocator &a, std::pair<uint32_t, uint8_t> (*p)(const str32_t, char_t *) = default_token_parser_impl)
                    : content{input}, contentBase{content.data()}, allocator{a}, token_parser{p}
                {
                }

                // You may NOT invoke parse() more than once
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
                // This is important; we need to capture e.g [tom tom] (the manufucacturer) and [boing boing]
                // but we don't want to capture the same phrase/word twice if in order.
                // This is useful in MatchedIndexDocumentsFilter::consider() implementations for scoring documents based
                // on the query structure and the matched terms.
                // See Trinity::rewrite_query() where we consider this when rewriting sequences
                uint8_t rep;

                // index in the query
                uint16_t index;
                // See assign_query_indices() for how this is assigned
                //
                // This is how many terms/tokens to advance from this index(i.e query token) to get to the next term in the query and skip
                // all other tokens in the same OR group as this token.
                //
                // This is useful in MatchedIndexDocumentsFilter::consider()
                //
                // For example:
                // [world of warcraft OR (war craft) mists of pandaria]
                //---------------------------------------------
                // INDEX 	|TOKEN 		|TONEXTSPAN
                //---------------------------------------------
                // 0 		WORLD 	 	1
                // 1 		OF 		1
                // 2 		WARCRAFT 	2
                // 2 		WAR 		1
                // 3 		CRAFT 		1
                // 4 		MISTS 		1
                // 5 		OF 		1
                // 6 		PANDARIA 	1
                // If you are going to try to match a sequence for matched term  'WARCRAFT'
                // the only query instance of it is at index 2 and the next term in the query past
                // any other 'equivalent' OR groups(in this case there is only one other OR expression matching warcraft, [war craft]) is 2 positions ahead to 4 (i.e to mists)
                //
                // The semantics of (index, toNextSpan) are somewhat complicated, but it's only because it is required for accurately and relatively effortlessly being able to
                // capture sequences. A sequence is a 2+ consequtive tokens in a query.
                uint8_t toNextSpan;

                // flags. Usually 0, but e.g if you are rewritting a [wow] to [wow OR "world of warcraft"] you
                // maybe want "world of warcraft" flags to be 1 (i.e derived). This can be very useful for scoring matches
                //
                // See ast_node::set_alltokens_flags()
                uint8_t flags;

                // This is the range in the input query string
                // This is handy for e.g spell checking runs where you want to highlight corrected tokens/phrases in the input query
                // and you 'd rather not have to go through hoops to accomplish it
                range_base<uint16_t, uint16_t> inputRange;

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

                // handy utility method
                static auto make(const str8_t *tokens, const uint8_t n, simple_allocator *const a)
                {
                        auto p = (phrase *)a->Alloc(sizeof(phrase) + sizeof(term) * n);

                        p->flags = 0;
                        p->rep = 1;
                        p->inputRange.reset();
                        p->toNextSpan = 1;
                        p->size = n;

                        for (uint32_t i{0}; i != n; ++i)
                                p->terms[i].token.Set(a->CopyOf(tokens[i].data(), tokens[i].size()), tokens[i].size());
                        return p;
                }
        };

        // see query::normalize()
        ast_node *normalize_ast(ast_node *);

        // This is really just a container for an ast root node and the allocator
        // used to allocate the AST from.
        // It also provides a few useful methods that operate on the root and may make use of the allocator
        struct query final
        {
                ast_node *root;
                simple_allocator allocator{512};
                // parse() will set tokensParser; this may come in handy elsewhere, e.g see rewrite_query() impl.
                std::pair<uint32_t, uint8_t> (*tokensParser)(const str32_t, char_t *);

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
		   to consume from. Instead, we need to identify the "lead tokens" (in the case of an AND that's token, but in 
		   the case of an OR sequence, that's all the distinct tokens in that OR sequence), and then use a merge-scan to get 
		   the lowest next ID from those, and then execute the query based on that query and then advance all lowest such tokens.

		   It is a bit more complicated than that, but capture_leader() will properly identify the lead nodes

		   e.g for
		   - [ipad pro] : the lead token is ipad. So we just scan the ipad documents and advance those
		   - [apple OR macbook OR iphone NOT cable]: the lead tokens are (apple, macbook, iphone)
		   - [apple OR (macbook pro)] : lead tokens are [apple, macbook]

		   * the execution engine uses a specialised such implementation for exec_nodes
		   * for simplicity and performance.
		   *
		   * See also ast_node::Type::ConstTrueExpr comments
		 */
                void leader_nodes(std::vector<ast_node *> *const out);

                bool parse(const str32_t in, std::pair<uint32_t, uint8_t> (*tp)(const str32_t, char_t *) = default_token_parser_impl);

                // This is handy. When we copy a query to another query, we want to make sure
                // that tokens point to the destination query allocator, not the source, because it is possible for
                // the source query to go away
                static void bind_tokens_to_allocator(ast_node *, simple_allocator *);

		query()
			: root{nullptr}, tokensParser{nullptr}
		{

		}

                inline operator bool() const noexcept
                {
                        return root;
                }

                query(const str32_t in, std::pair<uint32_t, uint8_t> (*tp)(const str32_t, char_t *) = default_token_parser_impl)
                    : tokensParser{tp}
                {
                        if (!parse(in, tp))
                                throw Switch::data_error("Failed to parse query");
                }

                query(ast_node *r)
                    : root{r}
                {
                }

                explicit query(const query &o)
                {
                        tokensParser = o.tokensParser;
                        root = o.root ? o.root->copy(&allocator) : nullptr;
                        if (root)
                                bind_tokens_to_allocator(root, &allocator);
                }

                query(query &&o)
                {
                        root = std::exchange(o.root, nullptr);
                        tokensParser = o.tokensParser;
                        allocator = std::move(o.allocator);
                }

                query &operator=(const query &o)
                {
                        allocator.reuse();

                        root = o.root ? o.root->copy(&allocator) : nullptr;
                        tokensParser = o.tokensParser;
                        if (root)
                                bind_tokens_to_allocator(root, &allocator);
                        return *this;
                }

                // utility method; returns all nodes
                static std::vector<ast_node *> &nodes(ast_node *root, std::vector<ast_node *> *const res);

                static auto nodes(ast_node *root)
                {
                        std::vector<ast_node *> out;

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
                //
                // This is great for augmenting a query with synonyms e.g
                // for query [sofa] you could replace sofa with
                // [sofa OR couch OR sofas OR couches OR lounges OR lounge]
                // and also great for spell checking queries. You can process_runs() and then spell check every run
                // and if you have an alternative spelling for the run, then replace the run with a new ast that contains
                // the original and the suggested
                // e.g for [world of worrcraft video game]
                // [ ((world of worrcraft video game) OR (world of warcraft video game)) ]
                //
                // XXX: you need to be careful if you are replacing a node's value (e.g *n = *anotherNode)
                // and that anotherNode is e.g a binop and either of its (lhs, rhs) references itself.
                // In that case, just create another node (e.g use clone() method) and use that
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

                // Make sure you check the repetition count
                // See also ast_node::set_alltokens_flags()
                //
                // andOnly: if false, will also consider runs of STRICT_AND nodes
                template <typename L>
                void process_runs(const bool includePhrases, const bool processStrictAND, const bool processNOT, L &&cb)
                {
                        static thread_local std::vector<std::pair<uint32_t, ast_node *>> unaryNodes, stack;
                        static thread_local std::vector<ast_node *> run;
                        uint32_t segments{0};

                        stack.clear();
                        unaryNodes.clear();

                        if (root)
                                stack.push_back({0, root});

                        while (stack.size())
                        {
                                const auto pair = stack.back();
                                auto n = pair.second;
                                const auto seg = pair.first;

                                stack.pop_back();
                                switch (n->type)
                                {
                                        case ast_node::Type::Token:
                                                unaryNodes.push_back({seg, n});
                                                break;

                                        case ast_node::Type::Phrase:
                                                if (includePhrases)
                                                        unaryNodes.push_back({seg, n});
                                                break;

                                        case ast_node::Type::BinOp:
                                                if (n->binop.op == Operator::AND)
                                                {
                                                        stack.push_back({seg, n->binop.lhs});
                                                        stack.push_back({seg, n->binop.rhs});
                                                }
                                                else if (n->binop.op == Operator::NOT)
                                                {
                                                        stack.push_back({seg, n->binop.lhs});
                                                        if (processNOT)
                                                        {
                                                                ++segments;
                                                                stack.push_back({segments, n->binop.rhs});
                                                        }
                                                }
                                                else if (n->binop.op == Operator::OR)
                                                {
                                                        ++segments;
                                                        stack.push_back({segments, n->binop.lhs});
                                                        ++segments;
                                                        stack.push_back({segments, n->binop.rhs});
                                                }
                                                else if (processStrictAND && n->binop.op == Operator::STRICT_AND)
                                                {
                                                        stack.push_back({seg, n->binop.lhs});
                                                        stack.push_back({seg, n->binop.rhs});
                                                }
                                                break;

                                        case ast_node::Type::UnaryOp:
                                                if (n->unaryop.op != Operator::STRICT_AND || processStrictAND)
                                                        stack.push_back({seg, n->unaryop.expr});
                                                break;

                                        case ast_node::Type::Dummy:
                                        case ast_node::Type::ConstFalse:
                                        case ast_node::Type::ConstTrueExpr:
                                                break;
                                }
                        }

                        std::sort(unaryNodes.begin(), unaryNodes.end(), [](const auto &a, const auto &b) {
                                return a.first < b.first || (a.first == b.first && a.second->p->index < b.second->p->index);
                        });

                        for (const auto *p = unaryNodes.data(), *const e = p + unaryNodes.size(); p != e;)
                        {
                                const auto segment = p->first;

                                run.clear();
                                do
                                {
                                        run.push_back(p->second);
                                } while (++p != e && p->first == segment);

                                cb(run);
                        }
                }


		void reset()
		{
			root = nullptr;
			allocator.reuse();
		}
        };
}

void PrintImpl(Buffer &b, const Trinity::ast_node &n);
void PrintImpl(Buffer &b, const Trinity::phrase &);
inline void PrintImpl(Buffer &b, const Trinity::query &q)
{
        if (q.root)
                b.append(*q.root);
        else
                b.append("<not initialized>"_s32);
}
