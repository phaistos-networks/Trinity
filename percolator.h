// This is inspired by Elastic Search's Percolator functionality.
// You can register queries, and then you can feed documents to ES(that don't necessarily need to be indexed)
// and ES will index the document in-memory(temp.index) and run all registered queries against it, determining which match.
// This is useful for e.g alerts and subscriptions. See https://speakerdeck.com/javanna/whats-new-in-percolator
//
// Trinity provides a similar aux. class/interface for building a percolator like service or anything that requires matching a query
// against a 'document' quickly. 
// You can subclass Trinity::percolator_query, and override match_term() and match_phrase() to return true if the term or phrase respectivally match.
// The idea is that you can use it like so:
// {
// 	Trinity::query q("lucene OR trinity OR google NOT duckduckgo");
//
// 	struct MyServicePercolatorQuery final
// 		: public Trinity::percolator_query
// 	{
//		bool match_term(const uint16_t term) override final { ... };
//		bool match_phrase(const uint16_t *phraseTerms, const uint16_t phraseTermsCnt) override final { ... };
//		
//		MyServicePercolatorQuery(const Trinity::query &q)
//			: percolator_query{q}
//		{
//		}
//	};
//
//	MyServicePercolatorQuery percolatorQuery(q);
//
//	const auto matched = percolatorQuery.match();
// }
// 	
// Before invoking match(), you should, for example, 'reset' percolatorQuery to track the terms of a new document; either all terms, or
// all distinct terms involved in the query. You can access those via percolatorQuery.distinct_terms()
// your match_term() and match_phrase() impl. can then just use that information to return true or false.
// match() will in turn return true if the document is matched, false otherwise. That's it.
// 
// It relies on the compilation backend of Trinity and takes advantage of the optimizer which results in very fast query evaluation
#include "common.h"
#include "compilation_ctx.h"
#include "queries.h"
#include <ext/flat_hash_map.h>

namespace Trinity
{
        class percolator_query
        {
              protected:
                struct CCTX final
                    : public compilation_ctx
                {
                        ska::flat_hash_map<str8_t, uint16_t> localMap;
                        std::vector<str8_t> allTerms; // we need to keep track of those here

                        uint16_t resolve_query_term(const str8_t term) override final
                        {
                                const auto res = localMap.insert({term, 0});

                                if (res.second)
                                {
                                        res.first->second = localMap.size();
                                        res.first->first.Set(allocator.CopyOf(term.data(), term.size()), term.size());

                                        require(allTerms.size() == localMap.size() - 1);
                                        allTerms.push_back(res.first->first);
                                }

                                return res.first->second;
                        }

                } comp_ctx;

                exec_node root;

              protected:
                bool exec(const exec_node);

		auto term_by_index(const uint16_t idx)
		{
			return comp_ctx.allTerms[idx - 1];
		}

		auto &distinct_terms() noexcept
		{
			return comp_ctx.allTerms;
		}

		const auto &distinct_terms() const noexcept
		{
			return comp_ctx.allTerms;
		}

              public:
		// After compilation, you can access all distinct terms, i.e all distinct terms you may be
		// interested in, in a document, via distinct_terms()
                percolator_query(const Trinity::query &q)
                {
                        if (!q)
                        {
				SLog("here\n");
                                root.fp = ENT::constfalse;
                                return;
                        }

                        root = compile_query(q.root, comp_ctx);
                        if (root.fp == ENT::constfalse || root.fp == ENT::dummyop)
                                root.fp = ENT::constfalse;
                        else
                                group_execnodes(root, comp_ctx.allocator);
                }

                operator bool() const noexcept
                {
                        return root.fp != ENT::constfalse && root.fp != ENT::dummyop;
                }

                bool match();

		// Just override those two methods
		// You can access the actual term via term_by_index(idx)
		// 
		// You can e.g reset state, and then match()
                virtual bool match_term(const uint16_t term) = 0;

                virtual bool match_phrase(const uint16_t *, const uint16_t cnt) = 0;
        };
}
