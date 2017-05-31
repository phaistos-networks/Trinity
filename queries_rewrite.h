// Query rewrites is as important, maybe even more so, than a good ranking (precision) function.
// However, it's also important to consider the final query complexity. A simple query with say, 5-8 terms, can easily blow up to
// be extremely concpet, many 100s of nodes in size, so you need to compromise and accept tradeoffs.
//
// Having a "budget" helps, although its not optimal; query normalization may drop nodes count, by a large factor, and when the query is compiled the actual final
// nodes generated may be even fewe, by an even larger factor -- but we want rewrites to be fast, so short of normalizing the query after every rewrite, and/or accepting
/// many, many query nodes and somehow trimming during execution, this is a good compromise.
#pragma once
#include "queries.h"

// This is important so that we will disregard dupes, and also, if e.g
// (united, states, of, america) => [usa]
// and (united, states) => [usa]
// we will ignore the second (united, states) rule because we already matched it earlier (we process by match span descending)
#define TRINITY_QUERIES_REWRITE_FILTER 1
#define _TRINITY_QRW_USECACHE 1

namespace Trinity
{
        struct gen_ctx
        {
                // Implements
                // https://github.com/phaistos-networks/Trinity/issues/3
		//
		// TODO: we should probably cache the parsed node, not the tokens
		// so that we won't be parsing them again, and again, and only copy them once cached.
                std::vector<std::pair<str32_t, uint8_t>> allAlts;
                std::vector<std::pair<range_base<uint32_t, uint8_t>, range_base<uint32_t, uint16_t>>> cache;
                simple_allocator allocator;
		uint32_t logicalIndex;
		uint32_t K;

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



	// We are going to be processing the list from back to forth(in reverse)
	static ast_node *trail_of(std::vector<std::pair<range32_t, ast_node *>> &list, std::size_t stop, simple_allocator &a, const std::size_t depth)
        {
                static constexpr bool trace{false};

                if (0 == stop)
                        return nullptr;

                // Identify list range where it.first.stop() == stop
                // list has been sorted earlier so we can rely on that ordering scheme
                range32_t nodesRange;
                const auto listSize = list.size();

                for (uint32_t i{0}; i != listSize; ++i)
                {
                        if (list[i].first.stop() == stop)
                        {
                                nodesRange.offset = i;

                                do
                                {

                                } while (++i != list.size() && list[i].first.stop() == stop);
                                nodesRange.len = i - nodesRange.offset;
                                break;
                        }
                }

                if (nodesRange.empty())
                {
                        // how's that possible?
                        // XXX: not sure rteturning nullptr is what we should be doing here
                        return nullptr;
                }

                ast_node *nodes[nodesRange.size() + 1];
                std::size_t nodesSize{0};

                // Process range, one distinct offset/time(in case there are multiple, though there shouldn't be)
                for (uint32_t i = nodesRange.offset, e = nodesRange.stop(); i < e;)
                {
                        const auto base{i};
                        const auto offset = list[i].first.offset;
                        auto rhs = list[base].second->shallow_copy(&a);

                        do
                        {

                        } while (++i != e && list[i].first.offset == offset);

                        if (trace)
                                SLog("Dealing with offset ", offset, " for ", *list[base].second, "\n");

                        // Now, get the trail, and use stop this offset
                        const auto lhs = trail_of(list, offset, a, depth + 1);
                        ast_node *n;

                        if (!lhs)
                        {
                                // Likely because (0 == offset)
                                if (trace)
                                        SLog("NO lhs\n");

                                n = rhs;
                        }
                        else
                        {
                                // Join into a binary op
                                n = ast_node::make_binop(a);

                                n->binop.op = Operator::AND;
                                n->binop.lhs = lhs;
                                n->binop.rhs = rhs;
                        }

                        nodes[nodesSize++] = n;
                }

                if (trace)
                        SLog("nodes.size = ", nodesSize, " for ", stop, "\n");

                ast_node *res;

                if (nodesSize == 1)
                {
                        res = nodes[0];
                }
                else if (nodesSize)
                {
                        // Multiple for this stop
                        // group them using Operator::OR
                        auto lhs = nodes[0];

                        for (uint32_t i{1}; i != nodesSize; ++i)
                        {
                                auto rhs = nodes[i];
                                auto n = ast_node::make_binop(a);

                                n->binop.op = Operator::OR;
                                n->binop.lhs = lhs;
                                n->binop.rhs = rhs;
                                lhs = n;
                        }

                        res = lhs;
                }
                else
                        res = nullptr;

                if (trace)
                {
                        if (!res)
                                SLog("res = nullptr\n");
                        else
                                SLog("res = ", *res, "\n");
                }

                return res;
        }

        template <typename L>
        static std::pair<ast_node *, uint8_t> run_capture(size_t &budget, query &q, const std::vector<ast_node *> &run, const uint32_t i, L &&l, const uint8_t maxSpan, gen_ctx &genCtx)
        {
                static constexpr bool trace{false};
                [[maybe_unused]] auto &allocator = q.allocator;
                static thread_local std::vector<std::pair<range32_t, ast_node *>> list_tl;
		auto &list{list_tl};
                const auto baseIndex{i};
                std::size_t highest_stop{0};

                // Turns out, this is a simple matter of using a radix-tree like construction scheme
                // Instead of relying on the previously concienved and used heuristic which was somewhat complicated and it broke down in some weird edge cases
                // that wasn't worth investigating vs replacing with a simple and, likely, more efficient design.

	
		// 1. Collect all candidates for every index in run[] => (range, ast_node *)
                list.clear();
                for (uint32_t it{i}; it != run.size(); ++it)
                {
                        auto e = run_next(budget, q, run, it, maxSpan, l, genCtx);
                        const auto normalized = it - baseIndex;

                        for (const auto &eit : e)
                        {
                                highest_stop = std::max<size_t>(highest_stop, normalized + eit.second);
                                list.push_back({{normalized, eit.second}, eit.first}); // range [offset, end) => ast_node
                        }
                }

	
		// 2. Sort by range stop DESC, offset ASC
                std::sort(list.begin(), list.end(), [](const auto &a, const auto &b) {
                        return b.first.stop() < a.first.stop() || (b.first.stop() == a.first.stop() && a.first.offset < b.first.offset);
                });

                if (trace)
                {
                        for (const auto &it : list)
                                Print(it.first, ":", *it.second, "\n");

                        SLog("highest_stop = ", highest_stop, "\n");
                }

		// 3. Use trail_of() to recursvely process everything collected in list[]
                const auto output = trail_of(list, highest_stop, genCtx.allocator, 0);

		if (trace)
                {
                        if (!output)
                                SLog("NO OUTPUT\n");
                        else
                                SLog("output:", *output, "\n");
                }

                return {output, run.size()}; // we process the whole run
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
                static constexpr bool trace{false};

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

                if (trace)
                {
                        SLog(duration_repr(Timings::Microseconds::Since(before)), " to rewrite the query\n");
                        //exit(0);
                }

                q.normalize();
        }
}
