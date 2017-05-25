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

namespace Trinity
{
        struct gen_ctx
        {
                // Implements
                // https://github.com/phaistos-networks/Trinity/issues/3
                std::vector<std::pair<str32_t, uint8_t>> allAlts;
                std::vector<std::pair<range_base<uint32_t, uint8_t>, range_base<uint32_t, uint16_t>>> cache;
                simple_allocator allocator;

                void clear()
                {
                        allAlts.clear();
                        cache.clear();
                        allocator.reuse();
                }

                range_base<uint32_t, uint16_t> try_populate(const range_base<uint32_t, uint8_t> r)
                {
                        for (const auto &it : cache)
                        {
                                if (it.first == r)
                                        return it.second;
                        }

                        return {UINT32_MAX, 0};
                }

                void insert(const range_base<uint32_t, uint8_t> k, const range_base<uint32_t, uint16_t> v)
                {
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
                static thread_local simple_allocator altAllocatorInstance;
                static thread_local std::vector<std::pair<str32_t, uint8_t>> alts;
                auto &altAllocator = altAllocatorInstance;
                strwlen8_t tokens[maxSpan];
                std::vector<std::pair<Trinity::ast_node *, uint8_t>> expressions;
                auto tokensParser = q.tokensParser;
                auto &allocator = q.allocator;

                if (trace)
                        SLog(ansifmt::bold, ansifmt::color_green, "AT ", i, " ", token, ansifmt::reset, "\n");

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
                altAllocator.reuse();

                for (size_t upto = std::min<size_t>(run.size(), i + maxSpan), k{i}, n{0}; k != upto && run[k]->p->rep == 1; ++k) // mind reps
                {
                        tokens[n] = run[k]->p->terms[0].token;

#if 0 // legacy, for validations only: don't use the cache
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
					SLog("Caching ", key, " => ", value, "\n");
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
                                        auto n = allocator.Alloc<ast_node>();

                                        if (budget)
                                                --budget;

                                        n->type = ast_node::Type::BinOp;
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
                                        auto n = allocator.Alloc<ast_node>();

                                        if (budget)
                                                --budget;

                                        n->type = ast_node::Type::BinOp;
                                        n->binop.op = Operator::OR;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = ast_parser(alts[i + saved].first, allocator, tokensParser).parse();
                                        lhs = n;

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

        template <typename L>
        static std::pair<ast_node *, uint8_t> run_capture(size_t &budget, query &q, const std::vector<ast_node *> &run, const uint32_t i, L &&l, const uint8_t maxSpan, gen_ctx &genCtx)
        {
                static constexpr bool trace{false};
                auto &allocator = q.allocator;
                auto expressions = run_next(budget, q, run, i, maxSpan, l, genCtx);

                require(expressions.size());

                if (0 == budget)
                {
#if !defined(TRINITY_QUERIES_REWRITE_FILTER)
                        if (trace)
                                SLog("No budget, returning first ", *expressions.front().first, "\n");

                        return {expressions.front().first, i + 1};
#else
                        if (trace)
                                SLog("No budget, returning first ", *expressions.back().first, "\n");

                        return {expressions.back().first, i + 1};
#endif
                }

                if (expressions.size() == 1)
                {
                        if (trace)
                                SLog("SINGLE expression ", *expressions.front().first, "\n");

                        return {expressions.front().first, i + 1};
                }

                require(expressions.size());
// This is a bit complicated, but it produces optimal results in optimal amount of time
#if !defined(TRINITY_QUERIES_REWRITE_FILTER)
                const auto max = expressions.back().second;
                const auto upto = i + max;
                auto _lhs = expressions.back().first;
                size_t maxIdx{i + 1};

                expressions.pop_back();

                if (trace)
                        SLog("Last:", *_lhs, ", max = ", max, "\n");

                for (auto &it : expressions)
                {
                        const auto span = it.second;
                        auto lhs = it.first;

                        for (uint32_t k = i + span; k != upto; ++k)
                        {
                                const auto pair = run_capture(budget, q, run, k, l, maxSpan);
                                const auto expr = pair.first;
                                auto n = allocator.Alloc<ast_node>();

                                maxIdx = std::max<size_t>(maxIdx, pair.second);
                                n->type = ast_node::Type::BinOp;
                                n->binop.op = Operator::AND;
                                n->binop.lhs = lhs;
                                n->binop.rhs = expr;
                                lhs = n;

                                if (trace)
                                        SLog("GOT for expr [", *lhs, "] [", *expr, "], from ", k, "\n");
                        }

                        if (trace)
                                SLog("LHS:", *lhs, "\n");

                        auto n = allocator.Alloc<ast_node>();

                        n->type = ast_node::Type::BinOp;
                        n->binop.op = Operator::OR;
                        n->binop.lhs = _lhs;
                        n->binop.rhs = lhs;
                        _lhs = n;
                }
#else
                require(expressions.size());
                const auto max = expressions.front().second;
                auto _lhs = expressions.front().first;
                auto firstExpression = _lhs;
                ast_node *C{nullptr};
                size_t maxIdx{i + 1};

                require(_lhs);
                if (trace)
                        SLog("Last:", *_lhs, ", max = ", max, "\n");

                static constexpr bool traceFix{false};
                const bool _D = traceFix ? _lhs->p->terms[0].token.Eq(_S("foobar")) : false;

                for (uint32_t _i{1}; _i != expressions.size(); ++_i)
                {
                        auto &it = expressions[_i];
                        const auto upto = i + max;
                        const auto span = it.second;
                        auto lhs = it.first;

                        for (uint32_t k = i + span; k != upto; ++k)
                        {
                                const auto pair = run_capture(budget, q, run, k, l, maxSpan, genCtx);
                                const auto expr = pair.first;

                                if (!expr)
                                        return {nullptr, 0};

                                auto n = allocator.Alloc<ast_node>();

                                maxIdx = std::max<size_t>(maxIdx, pair.second);
                                n->type = ast_node::Type::BinOp;
                                n->binop.op = Operator::AND;
                                n->binop.lhs = lhs;
                                n->binop.rhs = expr;
                                lhs = n;

                                if (trace)
                                        SLog("GOT for expr [", *lhs, "] .rhs = [", *expr, "], from ", k, "\n");
                        }

                        if (trace)
                                SLog("LHS:", *lhs, "\n");

                        auto n = allocator.Alloc<ast_node>();

                        n->type = ast_node::Type::BinOp;
                        n->binop.op = Operator::OR;
                        n->binop.lhs = _lhs;
                        n->binop.rhs = lhs;

                        if (!C)
                                C = n;

                        _lhs = n;
                }

                // consider [foo bar ping]
                // and your callback for two tokens(t1,t2) will output (t1t2). e.g for (foo,bar) => foobar
                // Without this bit here, we 'll end up compiling to
                // foobar OR (foo barping OR bar AND ping)
                const auto lastspan = max;
                const auto rem = maxIdx - i - lastspan;

                if (traceFix)
                {
                        SLog(ansifmt::bold, ansifmt::color_brown, "maxIdx = ", maxIdx, ", i = ", i, ", lastspan = ", lastspan, ", _D = ", _D, ", rem = ", rem, ansifmt::reset, "\n");
                        SLog("OK FINAL:", *_lhs, "\n");
                }

                if (rem)
                {
                        const auto pair = run_capture(budget, q, run, i + lastspan, l, rem, genCtx);
                        const auto expr = pair.first;
                        auto n = allocator.Alloc<ast_node>();

                        if (traceFix)
                                SLog("Expr:", *expr, "\n");

                        n->type = ast_node::Type::BinOp;
                        n->binop.op = Operator::AND;
                        n->binop.lhs = firstExpression;
                        n->binop.rhs = expr;

                        if (traceFix)
                                SLog("WELL:", *n, "\n");

                        require(C);
                        C->binop.lhs = n;

                        if (traceFix)
                                SLog("FINAL:", *_lhs, "\n");
                }
                else if (traceFix)
                        SLog("Not needed\n");

//if (_D) { exit(0); }
#endif

                if (trace)
                {
                        require(_lhs);
                        SLog("OUTPUT:", *_lhs, "\n");
                }

                return {_lhs, maxIdx};
        }

        // Very handy utility function that faciliaties query rewrites
        // It generates optimal structures, and it is optimised for performance.
        // `K` is the span of tokens you want to consider for each run. The lambda
        // `l` will be passed upto that many tokens.
        //
        // Example:
        /*
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

		You probably want another runs pass in order to e.g convert all stop word nodes to
		<stopword> so that <the> becomes optional

		See RECIPES

		XXX: [foo bar   ping]
		This FAILS  if we e.g return [foobar] from [foo bar]
		ALSO: [foo bar 512    8192]
	*/
        template <typename L>
        void rewrite_query(Trinity::query &q, size_t budget, const uint8_t K, L &&l)
        {
                static constexpr bool trace{false};
                const auto before = Timings::Microseconds::Tick();
                auto &allocator = q.allocator;
		static thread_local gen_ctx genCtx;

                // For now, explicitly to unlimited
                // See: https://github.com/phaistos-networks/Trinity/issues/1 ( FIXME: )
                budget = std::numeric_limits<size_t>::max();

		genCtx.clear();

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
                                auto pair = run_capture(budget, q, run, i, l, K, genCtx);
                                auto expr = pair.first;

                                if (trace)
                                        SLog("last index ", pair.second, " ", run.size(), "\n");

                                if (!lhs)
                                        lhs = expr;
                                else
                                {
                                        auto n = allocator.Alloc<ast_node>();

                                        n->type = ast_node::Type::BinOp;
                                        n->binop.op = Operator::AND;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = expr;
                                        lhs = n;
                                }

                                i = pair.second;
                        }

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
