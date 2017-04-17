#pragma once
#include "queries.h"

namespace Trinity
{
        template <typename L>
        static auto run_next(simple_allocator &allocator, const std::vector<ast_node *> &run, const uint32_t i, const uint8_t maxSpan, L &&l)
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

                if (trace)
                        SLog(ansifmt::bold, ansifmt::color_green, "AT ", i, " ", token, ansifmt::reset, "\n");

                v.clear();
                v.push_back({{{token.data(), uint32_t(token.size())}, 0}, 1});
                altAllocator.reuse();

                for (size_t upto = std::min<size_t>(run.size(), i + maxSpan), k{i}, n{0}; k != upto; ++k)
                {
                        tokens[n] = run[k]->p->terms[0].token;
                        alts.clear();
                        l(tokens, ++n, altAllocator, &alts);

			if (trace)
			{
				SLog("Starting from ", run[i]->p->terms[0].token, " ", n, "\n");
				for (const auto &it : alts)
					SLog("[", it.first, "] ", it.second, "\n");
			}

                        for (const auto &it : alts)
                                v.push_back({{it.first, it.second}, n});
                }

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
                                auto lhs = ast_parser(alts.front().first, allocator).parse();

				if (const auto flags = alts.front().second)
				{
					if (trace)
						SLog("FLAGS:", flags, "\n");
					lhs->set_alltokens_flags(flags);
				}

                                for (uint32_t i{1}; i != alts.size(); ++i)
                                {
                                        auto n = allocator.Alloc<ast_node>();

                                        n->type = ast_node::Type::BinOp;
                                        n->binop.op = Operator::OR;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = ast_parser(alts[i].first, allocator).parse();
                                        lhs = n;

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
                                node = Trinity::ast_parser(alts.front().first, allocator).parse();

				if (const auto flags = alts.front().second)
				{
					if (trace)
						SLog("FLAGS:", flags, "\n");
					node->set_alltokens_flags(flags);
				}
                        }

                        expressions.push_back({node, span});
                }

                if (trace)
                {
                        SLog(ansifmt::bold, ansifmt::color_blue, "CAPTURED EXPRESSIONS", ansifmt::reset, "\n");
                        for (const auto &it : expressions)
                                SLog(it.second, ":", *it.first, "\n");
                }

                return expressions;
        }

        template <typename L>
        static std::pair<ast_node *, uint8_t> run_capture(simple_allocator &allocator, const std::vector<ast_node *> &run, const uint32_t i, L &&l, const uint8_t maxSpan)
        {
                static constexpr bool trace{false};
                auto expressions = run_next(allocator, run, i, maxSpan, l);

                if (expressions.size() == 1)
                {
                        if (trace)
                                SLog("SINGLE expression ", *expressions.front().first, "\n");
                        return {expressions.front().first, i + 1};
                }

                // This is a bit complicated, but it produces optimal results in optimal amount of time
                const auto max = expressions.back().second;
                auto _lhs = expressions.back().first;
                size_t maxIdx{i + 1};

                expressions.pop_back();

                if (trace)
                        SLog("Last:", *_lhs, "\n");

                for (auto &it : expressions)
                {
                        const auto upto = i + max;
                        auto lhs = it.first;

                        for (uint32_t k = i + 1; k != upto; ++k)
                        {
                                const auto pair = run_capture(allocator, run, k, l, maxSpan);
                                const auto expr = pair.first;
                                auto n = allocator.Alloc<ast_node>();

                                maxIdx = std::max<size_t>(maxIdx, pair.second);
                                n->type = ast_node::Type::BinOp;
                                n->binop.op = Operator::STRICT_AND;
                                n->binop.lhs = lhs;
                                n->binop.rhs = expr;
                                lhs = n;
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

                if (trace)
                        SLog("OUTPUT:", *_lhs, "\n");

                return {_lhs, maxIdx};
        }

	// Very handy utility function that faciliaties query rewrites
	// It generates optimal structures, and it is optimised for performance.
	// `K` is the span of tokens you want to consider for each run. The lambda
	// `l` will be passed upto that many tokens. 
	// 
	// Example:
	/*
	Trinity::rewrite_query(inputQuery, 3, [](const auto tokens, const auto cnt, auto &allocator, auto out)
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
	*/
        template <typename L>
        void rewrite_query(Trinity::query &q, const uint8_t K, L &&l)
        {
                static constexpr bool trace{false};
                const auto before = Timings::Microseconds::Tick();
                auto &allocator = q.allocator;

                Dexpect(K > 1 && K < 16);

                if (trace)
                        SLog("REWRITING:", q, "\n");

                q.process_runs(false, true, [&](const auto &run) {
                        ast_node *lhs{nullptr};

                        for (uint32_t i{0}; i < run.size();)
                        {
                                auto pair = run_capture(allocator, run, i, l, K);
                                auto expr = pair.first;

                                if (trace)
                                        SLog("last index ", pair.second, " ", run.size(), "\n");

                                if (!lhs)
                                        lhs = expr;
                                else
                                {
                                        auto n = allocator.Alloc<ast_node>();

                                        n->type = ast_node::Type::BinOp;
                                        n->binop.op = Operator::STRICT_AND;
                                        n->binop.lhs = lhs;
                                        n->binop.rhs = expr;
                                        lhs = n;
                                }

                                i = pair.second;
                        }

                        q.root = lhs;
                        if (trace)
                                SLog("Final:", *lhs, "\n");
                });

                SLog(duration_repr(Timings::Microseconds::Since(before)), " to rewrite the query\n");

                SLog("NOW:", ptr_repr(q.root), "\n");
                q.normalize();
                SLog("NOW:", ptr_repr(q.root), "\n");
        }
}
