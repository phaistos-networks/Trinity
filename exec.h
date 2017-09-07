#pragma once
#include "docidupdates.h"
#include "index_source.h"
#include "matches.h"
#include "queries.h"
#include <future>

namespace Trinity
{
        enum class ExecFlags : uint32_t
        {
                // If this set, then only the matchign document IDs will be provided in MatchedIndexDocumentsFilter's subclass consider(const docid_t) call
		// as opposed to when the default execution mode is selected where consider(matched_document &) is invoked instead, with rich information
		// about any and all matched tokens etc.
                //
                // This is very helpful if you want to e.g just count or collect documents matching a query,
                // or otherwise don't care for which of the terms (in case of ORs) matched the document, only for
                // the documents(IDs) that match the query (so you won't get a chance to e.g compute a trinity/query score based on the matched terms).
                //
                // It is also helpful if you want to e.g build a prefix-search people search system(like LinkedIn's) where you want 
		// to match all users matching the query, and you really don't care
                // for which of the terms (or their hits) to do so. 
                DocumentsOnly = 1,

		// This flag selects a query execution mode that matches Lucene's, and can be useful for
		// very specific use cases, like visual search, and in other cases where you prioritize faster execution over
		// higher relevancy, which would be computed by having access to rich information Trinity tracks and provides in matched_document
		// in the default execution mode.
		//
		// If this mode is selected, it will instead accumulate the scores of various iterators together into a "score" and invoke
		// MatchedIndexDocumentsFilter's subclass consider(const docid_t, const double score)
		AccumulatedScoreScheme = 2,

		// If set, this doesn't track unique (termID, toNextSpan, flags) for MatchedIndexDocumentsFilter::queryIndicesTerms
		// instead it tracks unique (termID, toNextSpan) -- that is, respects the older semantics.
		// If you are not interested for that unique tripplet, but instead of the unique (termID, toNextSpan), you should use
		// this flag. If set, query_index_term::flags will be set to 0.
		// This is really only relevant if the default exec. mode is selected
		// i.e neither DocumentsOnly nor AccumulatedScoreScheme are set in the passed flags to exec_query()
		DisregardTokenFlagsForQueryIndicesTerms = 4
        };

        void exec_query(const query &in, IndexSource *, masked_documents_registry *const maskedDocumentsRegistry, MatchedIndexDocumentsFilter *, IndexDocumentsFilter *const f = nullptr, const uint32_t flags = 0);

        // Handy utility function; executes query on all index sources in the provided collection in sequence and returns
        // a vector with the match filters/results of each execution.
        //
        // You are expected to merge/reduce/blend them.
        // It's trivial to do this in parallel using e.g std::async() or any other means of scheduling exec_query() for each index source in
        // a different thread. See exec_query_par() for a possible implementation.
        //
        // Note that execution of sources does not depend on state of other sources - they are isolated so parallel processing them requires
        // no coordination.
        template <typename T, typename... Arg>
        std::vector<std::unique_ptr<T>> exec_query(const query &in, IndexSourcesCollection *collection, IndexDocumentsFilter *f, const uint32_t flags, Arg &&... args)
        {
                static_assert(std::is_base_of<MatchedIndexDocumentsFilter, T>::value, "Expected a MatchedIndexDocumentsFilter subclass");
                const auto n = collection->sources.size();
                std::vector<std::unique_ptr<T>> out;

                for (uint32_t i{0}; i != n; ++i)
                {
                        auto source = collection->sources[i];
                        auto scanner = collection->scanner_registry_for(i);
                        auto filter = std::make_unique<T>(std::forward<Arg>(args)...);

                        exec_query(in, source, scanner.get(), filter.get(), f, flags);
                        out.push_back(std::move(filter));
                }

                return out;
        }

        // Parallel queries execution, using std::async()
        template <typename T, typename... Arg>
        std::vector<std::unique_ptr<T>> exec_query_par(const query &in, IndexSourcesCollection *collection, IndexDocumentsFilter *f, const uint32_t flags, Arg &&... args)
        {
                static_assert(std::is_base_of<MatchedIndexDocumentsFilter, T>::value, "Expected a MatchedIndexDocumentsFilter subclass");
                const auto n = collection->sources.size();
                std::vector<std::unique_ptr<T>> out;

                if (!n)
                        return out;
                else if (n == 1)
                {
                        // fast-path: single source
                        if (false == collection->sources[0]->index_empty())
                        {
                                auto source = collection->sources[0];
                                auto scanner = collection->scanner_registry_for(0);
                                auto filter = std::make_unique<T>(std::forward<Arg>(args)...);

                                exec_query(in, source, scanner.get(), filter.get(), f, flags);
                                out.push_back(std::move(filter));
                        }
                        return out;
                }

                std::vector<std::future<std::unique_ptr<T>>> futures;

                // Schedule all but the first via std::async()
                // we 'll handle the first here.
                for (uint32_t i{1}; i != n; ++i)
                {
                        if (false == collection->sources[i]->index_empty())
                        {
                                futures.push_back(
                                    std::async(std::launch::async, [&](const uint32_t i) {
                                            auto source = collection->sources[i];
                                            auto scanner = collection->scanner_registry_for(i);
                                            auto filter = std::make_unique<T>(std::forward<Arg>(args)...);

                                            exec_query(in, source, scanner.get(), filter.get(), f, flags);
                                            return filter;
                                    },
                                               i));
                        }
                }

                if (auto source = collection->sources[0]; false == source->index_empty())
                {
                        auto scanner = collection->scanner_registry_for(0);
                        auto filter = std::make_unique<T>(std::forward<Arg>(args)...);

                        exec_query(in, source, scanner.get(), filter.get(), f, flags);
                        out.push_back(std::move(filter));
                }

                while (futures.size())
                {
                        auto &f = futures.back();

                        out.push_back(std::move(f.get()));
                        futures.pop_back();
                }

                return out;
        }
};
