// Query rewrites is as important, maybe even more so, than a good ranking (precision) function.
// However, it's also important to consider the final query complexity. A simple query with say, 5-8 terms, can easily blow up to
// be extremely concpet, many 100s of nodes in size, so you need to compromise and accept tradeoffs.
//
// Having a "budget" helps, although its not optimal; query normalization may drop nodes count, by a large factor, and when the query is compiled the actual final
// nodes generated may be even fewe, by an even larger factor -- but we want rewrites to be fast, so short of normalizing the query after every rewrite, and/or accepting
/// many, many query nodes and somehow trimming during execution, this is a good compromise.
#pragma once
#include "queries.h"
#include <set>

// This is important so that we will disregard dupes, and also, if e.g
// (united, states, of, america) => [usa]
// and (united, states) => [usa]
// we will ignore the second (united, states) rule because we already matched it earlier (we process by match span descending)
#define TRINITY_QUERIES_REWRITE_FILTER 1
// This is no longer necessary because of the new algorithm's semantics
#define _TRINITY_QRW_USECACHE 1

namespace Trinity
{
        struct flow;
        struct flow_ent
        {
                enum class Type : uint8_t
                {
                        Node,
                        Flow
                } type;

                union {
                        ast_node *n;
                        flow *f;
                };

                flow_ent(ast_node *const node)
                    : type{Type::Node}, n{node}
                {
                }

                flow_ent(flow *const _flow)
                    : type{Type::Flow}, f{_flow}
                {
                }

                inline ast_node *materialize(simple_allocator &a) const;

                flow_ent()
                    : n{nullptr}
                {
                }

                flow_ent(const flow_ent &o)
                {
                        type = o.type;
                        n = o.n;
                }

                flow_ent(flow_ent &&o)
                {
                        type = o.type;
                        n = o.n;
                }

                auto &operator=(const flow_ent &o)
                {
                        type = o.type;
                        n = o.n;
                        return *this;
                }

                auto &operator=(flow_ent &&o)
                {
                        type = o.type;
                        n = o.n;
                        return *this;
                }
        };

	struct gen_ctx;
        struct flow
        {
                range32_t range;
                flow *parent{nullptr};
                Operator op{Operator::OR};
                std::vector<flow_ent> ents;

                void replace_child_flow(flow *from, flow *to)
                {
                        for (auto &it : ents)
                        {
                                if (it.type == flow_ent::Type::Flow && it.f == from)
                                {
                                        it.f = to;
                                        to->parent = this;
                                }
                        }
                }

                flow(flow &&o)
                    : range{o.range}, parent{o.parent}, op{o.op}
                {
                        ents = std::move(o.ents);
                }

                auto &operator=(flow &&o)
                {
                        range = o.range;
                        parent = o.parent;
                        op = o.op;
                        ents = std::move(o.ents);
                        return *this;
                }

                // with no contructor (e.g not even flow() = default;) it will break in so many ways
                flow() = default;

                bool overlaps(const range32_t range) const noexcept;

                void push_back_flow(flow *const f)
                {
                        f->parent = this;
                        ents.push_back({f});
                }

                bool replace_self(flow *with)
                {
                        if (auto p = parent)
                        {
                                for (uint32_t i{0}; i != p->ents.size(); ++i)
                                {
                                        if (p->ents[i].type == flow_ent::Type::Flow && p->ents[i].f == this)
                                        {
                                                with->range.Set(UINT32_MAX, 0);
                                                with->parent = parent;
                                                p->ents[i].f = with;
                                                for (auto &it : ents)
                                                {
                                                        if (it.type == flow_ent::Type::Flow && it.f->parent == this)
                                                                it.f->parent = with;
                                                }

                                                return true;
                                        }
                                }
                        }
                        return false;
                }


                ast_node *materialize(simple_allocator &a) const
                {
                        static ast_node dummy{.type = ast_node::Type::Dummy};

                        if (const auto n = ents.size())
                        {
                                ast_node *lhs{nullptr};

                                for (uint32_t i{0}; i != n;)
                                {
                                        ast_node *node;

                                        if (ents[i].type == flow_ent::Type::Flow)
                                        {
                                                // sequences of flows in ents[] need to be combined with OR
                                                auto localLHS = ents[i].materialize(a);
                                                const auto op = ents[i].f->op;

                                                for (++i; i != n && ents[i].type == flow_ent::Type::Flow && ents[i].f->op == op; ++i)
                                                {
                                                        auto b = ast_node::make_binop(a);

                                                        b->binop.op = op;
                                                        b->binop.lhs = localLHS;
                                                        b->binop.rhs = ents[i].materialize(a);
                                                        localLHS = b;
                                                }
                                                node = localLHS;
                                        }
                                        else
                                        {
                                                node = ents[i++].materialize(a);
                                        }

                                        if (!lhs)
                                                lhs = node;
                                        else
                                        {
                                                auto bn = ast_node::make_binop(a);

                                                bn->binop.op = Operator::AND;
                                                bn->binop.lhs = lhs;
                                                bn->binop.rhs = node;
                                                lhs = bn;
                                        }
                                }

                                return lhs ?: &dummy;
                        }

                        return &dummy;
                }

                void validate() const noexcept
                {
#if 0
                        std::size_t n{0};

                        for (auto it = this->parent; it; it = it->parent)
                        {
                                if (++n == 100)
                                        std::abort();
                        }
#endif
                }

	        inline void push_back_node(const std::pair<range32_t, ast_node *> p, gen_ctx &, std::vector<flow *> &, uint32_t &, const Operator op = Operator::AND);
        };

        inline flow *flow_for_node(const std::pair<range32_t, ast_node *> p, gen_ctx &, std::vector<flow *> &, uint32_t &);

        struct gen_ctx
        {
                std::vector<std::pair<str32_t, uint8_t>> allAlts;
                std::vector<std::pair<range_base<uint32_t, uint8_t>, range_base<uint32_t, uint16_t>>> cache;
                simple_allocator allocator, flowsAllocator{4096};
                // could have used just one container insted of two, and a map or multimap for tracking flows by range
		// TODO: we can just reuse allocatedFlows (reusable = std::move(allocatedFlows)) 
                std::vector<flow *> allocatedFlows, flows, flows_1, flows_2;
                uint32_t logicalIndex;
                uint32_t K;

		void prepare_run_capture()
		{
			flows.clear();
                        while (allocatedFlows.size())
                        {
                                allocatedFlows.back()->~flow();
                                allocatedFlows.pop_back();
                        }
			flowsAllocator.reuse();
		}

                template <typename... T>
                auto new_flow(T &&... o)
                {
                        auto res = flowsAllocator.construct<flow>(std::forward<T>(o)...);

                        allocatedFlows.push_back(res);
                        return res;
                }

                void clear(const uint8_t _k)
                {
                        allAlts.clear();
                        cache.clear();
                        allocator.reuse();
                        K = _k;
                }

                range_base<uint32_t, uint16_t> try_populate(range_base<uint32_t, uint8_t> r)
                {
                        r.offset += logicalIndex;

                        for (const auto &it : cache)
                        {
                                if (it.first == r)
                                        return it.second;
                        }

                        return {UINT32_MAX, 0};
                }

                void insert(range_base<uint32_t, uint8_t> k, const range_base<uint32_t, uint16_t> v)
                {
                        k.offset += logicalIndex;

                        cache.push_back({k, v});
                }
        };


        // generate a list of expressions(ast_nodes) from a run, starting at (i), upto (i + maxSpan)
        template <typename L>
        static auto run_next(size_t &budget, query &q, const std::vector<ast_node *> &run, const uint32_t i, const uint8_t maxSpan, L &&l, gen_ctx &genCtx)
        {
                static constexpr bool trace{false};
                require(i < run.size());
                const auto token = run[i]->p->terms[0].token;
                static thread_local std::vector<std::pair<std::pair<str32_t, uint8_t>, uint8_t>> v;
                static thread_local std::vector<std::pair<str32_t, uint8_t>> alts;
#ifndef _TRINITY_QRW_USECACHE
                static thread_local simple_allocator altAllocatorInstance;
                auto &altAllocator = altAllocatorInstance;
#endif
                const auto normalizedMaxSpan = std::min<uint8_t>(maxSpan, genCtx.K);
                strwlen8_t tokens[normalizedMaxSpan];
                std::vector<std::pair<Trinity::ast_node *, uint8_t>> expressions;
                auto tokensParser = q.tokensParser;
                auto &allocator = q.allocator;

                if (trace)
                        SLog(ansifmt::bold, ansifmt::color_green, "AT ", i, " ", token, ansifmt::reset, " (maxSpan = ", maxSpan, "(", normalizedMaxSpan, "))\n");

                alts.clear();

                if (run[i]->p->rep > 1 || run[i]->p->flags)
                {
                        // special care for reps
                        auto n = allocator.Alloc<ast_node>();

                        if (trace)
                                SLog("Special case, rep = ", run[i]->p->rep, " ", run[i]->p->flags, "\n");

                        n->type = ast_node::Type::Token;
                        n->p = run[i]->p;
                        expressions.push_back({n, 1});

                        if (budget)
                                --budget;
                        return expressions;
                }

                // This may be handy. e.g for possibly generating composite terms
                const std::pair<uint32_t, uint32_t> runCtx(i, run.size());

                v.clear();
                v.push_back({{{token.data(), uint32_t(token.size())}, 0}, 1});

#ifndef _TRINITY_QRW_USECACHE
                altAllocator.reuse();
#endif

                for (size_t upto = std::min<size_t>(run.size(), i + normalizedMaxSpan), k{i}, n{0}; k != upto && run[k]->p->rep == 1; ++k) // mind reps
                {
                        tokens[n] = run[k]->p->terms[0].token;

#ifndef _TRINITY_QRW_USECACHE
                        // legacy, for validations only: don't use the cache
                        alts.clear();
                        l(runCtx, tokens, ++n, altAllocator, &alts);

                        if (trace)
                        {
                                SLog("Starting from ", run[i]->p->terms[0].token, " ", n, "\n");
                                for (const auto &it : alts)
                                        SLog("alt [", it.first, "] ", it.second, "\n");
                        }

                        for (const auto &it : alts)
                                v.push_back({{it.first, it.second}, n});
#else
                        // Caching can have a huge performance impact
                        // for a rather elaborate query, query rewriting dropped from 0.003s down to 971us
                        // and I suspect there's still room for improvemengt
                        ++n;

                        const range_base<uint32_t, uint8_t> key{i, uint8_t(n)};
                        range_base<uint32_t, uint16_t> value;

                        if (const auto res = genCtx.try_populate(key); res.offset != UINT32_MAX)
                        {
                                if (trace)
                                        SLog("From cache for ", key, " => ", res, "\n");

                                value = res;
                        }
                        else
                        {
                                const uint32_t saved = genCtx.allAlts.size();

                                l(runCtx, tokens, n, genCtx.allocator, &genCtx.allAlts);
                                value.Set(saved, uint16_t(genCtx.allAlts.size() - saved));
                                genCtx.insert(key, value);

                                if (trace)
                                {
                                        SLog("Caching ", key, " => ", value, "\n");
                                        for (uint32_t i{saved}; i != genCtx.allAlts.size(); ++i)
                                                SLog(genCtx.allAlts[i], "\n");
                                }
                        }

                        for (const auto i : value)
                                v.push_back({genCtx.allAlts[i], n});
#endif
                }

#if !defined(TRINITY_QUERIES_REWRITE_FILTER)
                std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) {
                        return a.second < b.second;
                });

                expressions.clear();
                for (const auto *p = v.data(), *const e = p + v.size(); p != e;)
                {
                        const auto span = p->second;
                        ast_node *node;

                        alts.clear();
                        do
                        {
                                alts.push_back({p->first.first, p->first.second});
                        } while (++p != e && p->second == span);

                        if (alts.size() > 1)
                        {
                                auto lhs = ast_parser(alts.front().first, allocator, tokensParser).parse();

                                if (const auto n = lhs->nodes_count(); budget >= n)
                                        budget -= n;
                                else
                                        budget = 0;

                                if (const auto flags = alts.front().second)
                                {
                                        if (trace)
                                                SLog("FLAGS:", flags, "\n");
                                        lhs->set_alltokens_flags(flags);
                                }

                                for (uint32_t i{1}; i != alts.size(); ++i)
                                {
                                        auto n = ast_node::make_binop(allocator);

                                        if (budget)
                                                --budget;

                                        n->binop.op = Operator::OR;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = ast_parser(alts[i].first, allocator, tokensParser).parse();
                                        lhs = n;

                                        if (const auto n = n->binop.rhs->nodes_count(); budget >= n)
                                                budget -= n;
                                        else
                                                budget = 0;

                                        if (const auto flags = alts[i].second)
                                        {
                                                if (trace)
                                                        SLog("FLAGS:", flags, "\n");
                                                n->binop.rhs->set_alltokens_flags(flags);
                                        }
                                }

                                node = lhs;
                        }
                        else
                        {
                                node = Trinity::ast_parser(alts.front().first, allocator, tokensParser).parse();

                                if (const auto n = node->nodes_count(); budget >= n)
                                        budget -= n;
                                else
                                        budget = 0;

                                if (const auto flags = alts.front().second)
                                {
                                        if (trace)
                                                SLog("FLAGS:", flags, "\n");
                                        node->set_alltokens_flags(flags);
                                }
                        }

                        expressions.push_back({node, span});
                }
#else
                std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) {
                        return b.second < a.second; // sort by span
                });

                expressions.clear();
                alts.clear();
                for (const auto *p = v.data(), *const e = p + v.size(); p != e;)
                {
                        const auto span = p->second;
                        ast_node *node;
                        const auto saved = alts.size();

                        if (trace)
                                SLog("For SPAN ", span, "\n");

                        do
                        {
                                uint32_t k{0};
                                const auto s = p->first.first;

                                while (k != alts.size() && alts[k].first != s)
                                        ++k;

                                if (k == alts.size())
                                {
                                        // ignore duplicates or already seen
                                        if (trace)
                                                SLog("Accepting Alt [", s, "]\n");

                                        alts.push_back({s, p->first.second});
                                }

                        } while (++p != e && p->second == span);

                        const auto n = alts.size() - saved;

                        if (0 == n)
                                continue;

                        if (n > 1)
                        {
                                auto lhs = ast_parser(alts[saved].first, allocator, tokensParser).parse();

                                if (unlikely(nullptr == lhs))
                                        throw Switch::data_error("Failed to parse [", alts[saved].first, "]");

                                if (const auto n = lhs->nodes_count(); budget >= n)
                                        budget -= n;
                                else
                                        budget = 0;

                                if (const auto flags = alts[saved].second)
                                {
                                        if (trace)
                                                SLog("FLAGS:", flags, "\n");

                                        lhs->set_alltokens_flags(flags);
                                }

                                for (uint32_t i{1}; i != n; ++i)
                                {
                                        auto n = ast_node::make_binop(allocator);

                                        if (budget)
                                                --budget;

                                        n->binop.op = Operator::OR;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = ast_parser(alts[i + saved].first, allocator, tokensParser).parse();
                                        lhs = n;

                                        if (unlikely(nullptr == n->binop.rhs))
                                                throw Switch::data_error("Failed to parse [", alts[i + saved].first, "]");

                                        if (const auto _n = n->binop.rhs->nodes_count(); budget >= _n)
                                                budget -= _n;
                                        else
                                                budget = 0;

                                        if (const auto flags = alts[i + saved].second)
                                        {
                                                if (trace)
                                                        SLog("FLAGS:", flags, "\n");

                                                n->binop.rhs->set_alltokens_flags(flags);
                                        }

                                        if (trace)
                                                SLog("CREATED:", *n, "\n");
                                }

                                node = lhs;
                        }
                        else
                        {
                                node = Trinity::ast_parser(alts[saved].first, allocator, tokensParser).parse();

                                if (unlikely(nullptr == node))
                                        throw Switch::data_error("Failed to parse [", alts[saved].first, "]");

                                if (trace)
                                        SLog("Parsed [", alts[saved], "] ", *node, "\n");

                                if (const auto n = node->nodes_count(); budget >= n)
                                        budget -= n;
                                else
                                        budget = 0;

                                if (const auto flags = alts[saved].second)
                                {
                                        if (trace)
                                                SLog("FLAGS:", flags, "\n");

                                        node->set_alltokens_flags(flags);
                                }
                        }

                        expressions.push_back({node, span});

                        if (trace)
                                SLog(ansifmt::color_brown, "<<<<<< [", *node, "] ", span, ansifmt::reset, "\n");
                }
#endif

                if (trace)
                {
                        SLog(ansifmt::bold, ansifmt::color_blue, "CAPTURED EXPRESSIONS", ansifmt::reset, "\n");
                        for (const auto &it : expressions)
                                SLog(it.second, ":", *it.first, "\n");
                }

                return expressions;
        }

	// TODO: all kinds of IMPLEMENT_ME(), also
	// not sure the use of replace_self() makes sense. Need to reproduce the problem though
        template <typename L>
        static std::pair<ast_node *, uint8_t> run_capture(size_t &budget, query &q, const std::vector<ast_node *> &run, const uint32_t i, L &&l, const uint8_t maxSpan, gen_ctx &genCtx)
        {
                static constexpr bool trace{false};
                [[maybe_unused]] auto &allocator = q.allocator;
                static thread_local std::vector<std::pair<range32_t, ast_node *>> list_tl;
                auto &list{list_tl};
                const auto baseIndex{i};

		genCtx.prepare_run_capture();

                // New faster, simpler, more efficient, and likely correct scheme
                // Beats all past attempts and alternative schemes that required all kind of heuristics and recursion.
                // 1. Collect all (range, tokens list)s in this run
                list.clear();
                for (uint32_t it{i}, cnt = run.size(); it != cnt; ++it)
                {
                        auto e = run_next(budget, q, run, it, maxSpan, l, genCtx);
                        const auto normalized = it - baseIndex;

                        for (const auto &eit : e)
                                list.push_back({{normalized, eit.second}, eit.first}); // range [offset, end) => ast_node
                }

                // This is what makes everything work.
                // 2. Sorting the list _properly_
                // Figuring out the right order is extremely important; it's what make this scheme design possible
                std::sort(list.begin(), list.end(), [](const auto &a, const auto &b) {

                        //return a.first.stop() < b.first.stop() || (a.first.stop() == b.first.stop() && a.first.offset < b.first.offset); 	// SORT METHOD #1
                        //return a.first.stop() < b.first.stop() || (a.first.stop() == b.first.stop() && b.first.offset < a.first.offset); 	// SORT METHOD #2

			// WORKS
                        //return a.first.offset < b.first.offset || (a.first.offset == b.first.offset && b.first.stop() < a.first.stop()); // SORT METHOD #3


                        return a.first.offset < b.first.offset || (a.first.offset == b.first.offset && a.first.stop() < b.first.stop()); // SORT METHOD #4

                });

                if (trace)
                {
                        for (uint32_t i{0}; i != list.size(); ++i)
                        {
                                const auto &it = list[i];

                                Print(i, ": ", it.first, ":", *it.second, "\n");
                        }
                }

                auto &flows = genCtx.flows;
                uint32_t maxStop{0};
                std::set<flow *> S[2], localSet;
                std::vector<flow *> localFlows;
                const auto find_flows_by_range = [&](const auto range, auto *const v1, auto *const v2) {
                        v1->clear();
                        v2->clear();

                        for (const auto f : flows)
                        {
                                f->validate();
                                if (f->range.offset == range.offset)
                                        v1->push_back(f);
                                else if (f->range.stop() == range.offset)
                                        v2->push_back(f);
                        }
                };
                [[maybe_unused]] const auto consider_stop = [&](const uint32_t stop) noexcept
                {
                        maxStop = std::max<uint32_t>(stop, maxStop);
                };
                // Fast-simple way to identify the common ancestor among a list of flows[]
                // TODO: we can probably avoid using std::set<> here which would make sense, but let's consider alternatives later
                [[maybe_unused]] const auto common_anchestor = [&](flow **const flows, const size_t cnt, const bool oneFlowUseParent = false) -> flow * {
                        if (cnt == 0)
                                return nullptr;
                        else if (cnt == 1)
                        {
                                if (oneFlowUseParent)
                                        return flows[0]->parent ?: flows[0];
                                else
                                        return flows[0];
                        }

                        uint16_t active{0};
                        const auto penultimate{cnt - 1};

                        flows[0]->validate();

                        S[0].clear();
                        for (auto it{flows[0]}; it; it = it->parent)
                                S[0].insert(it);

                        for (uint32_t i{1}; i != penultimate; ++i)
                        {
                                flows[i]->validate();
                                for (auto it{flows[i]}; it; it = it->parent)
                                {
                                        if (S[active].count(it))
                                                S[1 - active].insert(it);
                                }

                                S[active].clear();
                                active = 1 - active;
                        }

                        for (auto it{flows[penultimate]}; it; it = it->parent)
                        {
                                if (S[active].count(it))
                                        return it;
                        }

                        return nullptr;
                };

                flows.clear();

                // Create a root here
                // This is not strictly required(we could check if there is a root, and if not, assign the first flow to the root)
                // but this is a good idea, because we won't need to do that, and because we can rely on it to be the common ancestor for most flows/paths
                auto root = genCtx.new_flow();
                auto &atOffset = genCtx.flows_1;
                auto &atStop = genCtx.flows_2;

                root->range.Set(UINT32_MAX, 0);
                flows.push_back(root);

                for (const auto p : list)
                {
                        atOffset.clear();
                        atStop.clear();
                        find_flows_by_range(p.first, &atOffset, &atStop);

                        if (trace)
                        {
                                SLog("\n\n", ansifmt::bold, ansifmt::color_green, "Processing ", p.first, ansifmt::reset, " ", *p.second, " =>  maxStop = ", maxStop, " (", atOffset.size(), ", ", atStop.size(), ") (", flows.size(), " flows)\n");
                                SLog("root:", *root->materialize(genCtx.allocator), ": ", *root, "\n");

#if 0
				for (const auto f : flows)
					SLog("Registered ", f->range, ":(", f->op, ", parent:", ptr_repr(f->parent), ", self:", ptr_repr(f), ") ", *f->materialize(genCtx.allocator), "\n");
#endif
                        }

                        if (atOffset.empty())
                        {
                                if (atStop.empty())
                                {
                                        [[maybe_unused]] auto nf = flow_for_node(p, genCtx, flows, maxStop);

                                        root->push_back_flow(nf);

                                        if (trace)
                                        {
                                                SLog("Registered first\n");
                                        }
                                }
                                else
                                {
                                        if (atStop.size() == 1)
                                        {
                                                auto front = atStop.front();

                                                if (trace)
                                                {
                                                        SLog("front:", *front, "\n");

                                                        for (auto p = front->parent; p; p = p->parent)
                                                                SLog("up:", *p, "\n");
                                                }

                                                [[maybe_unused]] auto nf = flow_for_node(p, genCtx, flows, maxStop);

                                                if (trace)
                                                {
                                                        SLog("Will just append to a flow ", ptr_repr(atStop.front()), "\n");
                                                        SLog("Before:", *atStop.front(), "\n");
                                                }

                                                nf->op = Operator::AND;
                                                atStop.front()->push_back_flow(nf);

                                                if (trace)
                                                {
                                                        SLog("NOW:", *atStop.front(), "\n");
                                                        SLog("root:", *root->materialize(genCtx.allocator), "\n");

                                                        SLog("hierarchy after appended\n");
                                                        for (auto p = nf->parent; p; p = p->parent)
                                                                SLog("up:", *p, "\n");
                                                }
                                        }
                                        else
                                        {
                                                // atOffset.empty == true && atStop.size() > 1
                                                if (trace)
                                                {
                                                        SLog("Candidates\n");

                                                        for (auto f : atStop)
                                                        {
                                                                SLog(*f->materialize(genCtx.allocator), " ", *f, "\n");

                                                                for (auto p = f->parent; p; p = p->parent)
                                                                        SLog("up:", *p, "\n");
                                                        }
                                                }

                                                // this needs to be intelligent enoguh
                                                // i.e it should accept the common ancestor from: (MACBOOKPRO OR (MAC BOOK  OR MACBOOK)) 	-- for PRO
                                                // but not from: (WORLD ((OF (WAR OR WARCRAFT)) OR OFWAR))   -- for CRAFT
                                                // If we can figure this out, we are probably fine.
                                                // Maybe we should check common ancestor, if there is another flow with range.stop() >= p.stop()
                                                // XXX: breaks down for mac book pro lap top
                                                // need to consider overlap for each candidate
                                                if (auto ac = common_anchestor(atStop.data(), atStop.size(), true); ac && false == ac->overlaps(p.first))
                                                {
                                                        if (trace)
                                                        {
                                                                SLog("common:", *ac->materialize(genCtx.allocator), "\n");
                                                        }

                                                        [[maybe_unused]] auto nf = flow_for_node(p, genCtx, flows, maxStop);

                                                        nf->op = Operator::AND;
                                                        ac->push_back_flow(nf);

                                                        if (trace)
                                                        {
                                                                SLog("AC now:", *ac->materialize(genCtx.allocator), "\n");
                                                        }
                                                }
                                                else
                                                {
                                                        if (trace)
                                                        {
                                                                if (ac)
                                                                {
                                                                        SLog("Have common ancestor[", *ac->materialize(genCtx.allocator), "] but overlaps\n");
                                                                        Print("\n\n");
                                                                }
                                                                else
                                                                        SLog("No common ancestor\n");
                                                        }



                                                        for (auto f : atStop)
                                                        {
                                                                auto clone = p.second->shallow_copy(&genCtx.allocator);

                                                                f->push_back_node({p.first, clone}, genCtx, flows, maxStop, Operator::AND);
                                                        }
                                                }

                                                if (trace)
                                                {
                                                        SLog("root:", *root->materialize(genCtx.allocator), "\n");
                                                }
                                        }
                                }
                        }
                        else
                        {
                                if (atStop.empty()) // atOffset.empty() == false && atStop.empty() == true
                                {
                                        [[maybe_unused]] auto nf = flow_for_node(p, genCtx, flows, maxStop);
                                        auto ca = common_anchestor(atOffset.data(), atOffset.size(), true);

                                        if (ca)
                                        {
                                                if (trace)
                                                {
                                                        for (const auto f : atOffset)
                                                                SLog("Candidate:", *f->materialize(genCtx.allocator), "\n");

                                                        SLog("Will merge, common ancestor:", ptr_repr(ca), " ", *ca->materialize(genCtx.allocator), " ", ca->ents.size(), ": ", *ca, "\n");
                                                }
                                        }
                                        else
                                        {
                                                if (trace)
                                                {
                                                        SLog("no common ancestor\n");
                                                        for (const auto f : atOffset)
                                                                SLog(*f->materialize(genCtx.allocator), "\n");
                                                }

                                                IMPLEMENT_ME();
                                        }

                                        if (atOffset.size() == 1)
                                        {
                                                auto pg = genCtx.new_flow();
                                                auto g = genCtx.new_flow();
                                                auto first = atOffset.front();

                                                pg->push_back_flow(g);
                                                if (first->replace_self(pg))
                                                {
                                                }

                                                first->op = Operator::OR;
                                                nf->op = Operator::OR;
                                                g->push_back_flow(first);
                                                g->push_back_flow(nf);

                                                if (trace)
                                                {
                                                        SLog("Created container:", *pg->materialize(genCtx.allocator), "\n");
                                                        SLog("g = ", ptr_repr(g), ", pg = ", ptr_repr(pg), "\n");

                                                        for (auto f : atOffset)
                                                        {
                                                                SLog("flow of ", ptr_repr(f), " ", *f, "\n");
                                                                for (auto p = f->parent; p; p = p->parent)
                                                                        SLog(ptr_repr(p), ":", *p, "\n");
                                                        }
                                                }

                                                if (trace)
                                                {
                                                        auto ac = common_anchestor(atOffset.data(), atOffset.size(), true);

                                                        SLog("ac = ", ptr_repr(ac), "\n");
                                                        require(ac == g);
                                                }
                                        }
                                        else
                                        {
                                                if (trace)
                                                {
                                                        for (auto f : atOffset)
                                                        {
                                                                SLog("flow ", ptr_repr(f), ": ", *f, "\n");
                                                                for (auto p = f->parent; p; p = p->parent)
                                                                        SLog(ptr_repr(p), ":", *p, "\n");
                                                        }
                                                        Print("\n\n");
                                                }

                                                auto g = genCtx.new_flow();

                                                if (auto p = ca->parent)
                                                        p->replace_child_flow(ca, g);

                                                g->op = ca->op;
                                                g->push_back_flow(ca);
                                                g->push_back_flow(nf);
                                                ca->op = nf->op = Operator::OR;

                                                if (trace)
                                                {
                                                        for (auto f : atOffset)
                                                        {
                                                                SLog("flow ", ptr_repr(f), " ", *f, "\n");
                                                                for (auto p = f->parent; p; p = p->parent)
                                                                        SLog(ptr_repr(p), ":", *p, "\n");
                                                        }

                                                        auto ac = common_anchestor(atOffset.data(), atOffset.size(), true);

                                                        SLog("ac = ", ptr_repr(ac), ":", *ac, "\n");
                                                }
                                        }
                                }
                                else // false == atOffset.empty() && false == atStop.empty()
                                {
					// FIXME:
					// if (atOffset.size() > 1)
					// this breaks for e.g [play station 4 video games] and [key board micro soft]
                                        [[maybe_unused]] auto nf = flow_for_node(p, genCtx, flows, maxStop);

                                        if (trace)
                                        {
                                                for (auto f : atOffset)
                                                {
                                                        SLog("atOffset:", *f->materialize(genCtx.allocator), "\n");
                                                        for (auto p = f->parent; p; p = p->parent)
                                                                SLog("up:", *p, "\n");
                                                }
                                                for (auto f : atStop)
                                                {
                                                        SLog("atStop:", *f->materialize(genCtx.allocator), "\n");
                                                        for (auto p = f->parent; p; p = p->parent)
                                                                SLog("up:", *p, "\n");
                                                }
                                        }

					if (atOffset.size() > 1)
                                        {
                                                // This is really tricky
                                                // for now, we 'll just append, and this works fine
                                                for (auto it : atOffset)
                                                {
                                                        auto g = genCtx.new_flow();
                                                        auto nf = flow_for_node(p, genCtx, flows, maxStop);

                                                        if (auto p = it->parent)
                                                                p->replace_child_flow(it, g);

                                                        g->op = it->op;
                                                        g->push_back_flow(it);
                                                        g->push_back_flow(nf);
                                                        it->op = nf->op = Operator::OR;
                                                }

                                                if (trace)
                                                {
                                                        SLog("root now:", *root->materialize(genCtx.allocator), "\n");
                                                }
                                        }
                                        else
                                        {
                                                auto ca = common_anchestor(atOffset.data(), atOffset.size(), false);
                                                auto g = genCtx.new_flow();

                                                if (trace)
                                                {
                                                        if (ca)
                                                                SLog("CA = ", *ca->materialize(genCtx.allocator), " ", *ca, ", overlaps:", ca->overlaps(p.first), "\n");
                                                        else
                                                        {
                                                                SLog("ca = nullptr\n");
                                                                exit(1);
                                                        }
                                                }
                                                else if (!ca)
                                                {
                                                        IMPLEMENT_ME();
                                                }

                                                if (auto p = ca->parent)
                                                        p->replace_child_flow(ca, g);

                                                g->op = ca->op;
                                                g->push_back_flow(ca);
                                                g->push_back_flow(nf);
                                                ca->op = nf->op = Operator::OR;

                                                SLog("Now g = ", *g->materialize(genCtx.allocator), "\n");

                                                if (trace)
                                                {
                                                        for (auto f : atOffset)
                                                        {
                                                                SLog("atOffset:", *f->materialize(genCtx.allocator), "\n");
                                                                for (auto p = f->parent; p; p = p->parent)
                                                                        SLog("up:", *p, "\n");
                                                        }
                                                        for (auto f : atStop)
                                                        {
                                                                SLog("atStop:", *f->materialize(genCtx.allocator), "\n");
                                                                for (auto p = f->parent; p; p = p->parent)
                                                                        SLog("up:", *p, "\n");
                                                        }

                                                        SLog("root ", *root->materialize(genCtx.allocator), "\n");
                                                }
                                        }
                                }
                        }
                }

                auto res = root->materialize(genCtx.allocator);

                SLog(ansifmt::bold, ansifmt::color_green, "FINAL:", ansifmt::reset, *res, "  ", *root, "\n");

                return {res, run.size()}; // we process the whole run
        }

        // Very handy utility function that faciliaties query rewrites
        // It generates optimal structures, and it is optimised for performance.
        // `K` is the span of tokens you want to consider for each run. The lambda
        // `l` will be passed upto that many tokens.
        //
        // Example:
        /*
	 *

	Trinity::rewrite_query(inputQuery, 3, [](const auto runCtx, const auto tokens, const auto cnt, auto &allocator, auto out)
	{
		if (cnt == 1)
		{
			if (tokens[0].Eq(_S("PS4"))
				out->push_back({"PLAYSTATION AND 4", 1});
		}
		else if (cnt == 2)
		{
			if (tokens[0].Eq(_S("MAC")) && tokens[1].Eq(_S("BOOK")))
				out->push_back({"MACBOOK", 1});
		}
		else if (cnt == 3)
		{
			if (tokens[0].Eq(_S("WORLD"))  && tokens[1].Eq(_S("OF")) || tokens[2].Eq(_S("WARCRAFT")))
				out->push_back({"WOW", 1});
		}}
	}

		You probably want another runs pass in order to e.g convert all stop word nodes to
		<stopword> so that <the> becomes optional

  	*	
	*/
        template <typename L>
        void rewrite_query(Trinity::query &q, size_t budget, const uint8_t K, L &&l)
        {
                static constexpr bool trace{true};

                if (!q)
                        return;

                const auto before = Timings::Microseconds::Tick();
                auto &allocator = q.allocator;
                static thread_local gen_ctx genCtxTL;
                auto &genCtx = genCtxTL;

                // For now, explicitly to unlimited
                // See: https://github.com/phaistos-networks/Trinity/issues/1 ( FIXME: )
                budget = std::numeric_limits<size_t>::max();

                genCtx.clear(K);

                if (trace)
                        SLog("Initially budget: ", budget, "\n");

                if (const auto n = q.root->nodes_count(); n < budget)
                        budget -= n;
                else
                        budget = 0;

                if (trace)
                        SLog("Then budget ", budget, "\n");

                Dexpect(K > 1 && K < 16);

                if (trace)
                        SLog("REWRITING:", q, "\n");

                // If we are going to be processing lots of OR sub-expressions, where
                // each is a new run, we need to know the logical index across all tokens in the query
                // in order to properly cache alts
                genCtx.logicalIndex = 0;

                // Second argument now set to false
                // because otherwise for e.g [iphone +with]
                // will replace with with alternatives or with itself, and it won't preserve the operator
                // i.e it will be turned to [iphone with].
                // TODO: preserve operator
                q.process_runs(false, false, true, [&](const auto &run) {
                        ast_node *lhs{nullptr};

                        if (trace)
                        {
                                SLog("Processing run of ", run.size(), "\n");
                                for (const auto n : run)
                                        Print(*n->p, "\n");
                        }

                        for (uint32_t i{0}; i < run.size();)
                        {
                                const auto pair = run_capture(budget, q, run, i, l, K, genCtx);
                                auto expr = pair.first;

                                if (trace)
                                        SLog("last index ", pair.second, " ", run.size(), "\n");

                                if (!lhs)
                                        lhs = expr;
                                else
                                {
                                        auto n = ast_node::make_binop(allocator);

                                        n->binop.op = Operator::AND;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = expr;
                                        lhs = n;
                                }

                                // in practice, pair.second will be == run.size(), but allow for different values
                                i = pair.second;
                        }
                        genCtx.logicalIndex += run.size();

                        *run[0] = *lhs;
                        for (uint32_t i{1}; i != run.size(); ++i)
                                run[i]->set_dummy();

                        if (trace)
                                SLog("Final:", *lhs, "\n");
                });

                if (trace || true)
                {
                        SLog(duration_repr(Timings::Microseconds::Since(before)), " to rewrite the query\n");
                        exit(0);
                }

                q.normalize();
        }
}

Trinity::flow *Trinity::flow_for_node(std::pair<range32_t, ast_node *> p, Trinity::gen_ctx &ctx, std::vector<Trinity::flow *> &flows, uint32_t &maxStop)
{
        auto f = ctx.new_flow();

        maxStop = std::max<std::size_t>(maxStop, p.first.stop());
        f->range = p.first;
        f->ents.push_back({p.second});
        flows.push_back(f);
        return f;
}

void Trinity::flow::push_back_node(const std::pair<range32_t, ast_node *> p, Trinity::gen_ctx &ctx, std::vector<Trinity::flow *> &flows, uint32_t &maxStop, const Operator op)
{
        push_back_flow(flow_for_node(p, ctx, flows, maxStop));
}

Trinity::ast_node *Trinity::flow_ent::materialize(simple_allocator &a) const
{
        if (type == Type::Node)
                return n;
        else
                return f->materialize(a);
}

bool Trinity::flow::overlaps(const range32_t r) const noexcept
{
        if (range.offset < (UINT32_MAX / 2) && range.stop() >= r.stop())
                return true;

        for (const auto &it : ents)
        {
                if (it.type == flow_ent::Type::Flow && it.f->overlaps(r))
                        return true;
        }

        return false;
}

static void PrintImpl(Buffer &b, const Trinity::flow_ent &ent)
{
        if (ent.type == Trinity::flow_ent::Type::Flow)
                b.append(*ent.f);
        else
                b.append(*ent.n);
}

static void PrintImpl(Buffer &b, const Trinity::flow &f)
{
        b.append('[');
        if (f.ents.size())
        {
                for (const auto &it : f.ents)
                        b.append(it, ", "_s32);
                b.shrink_by(2);
        }
        b.append(']');
}
