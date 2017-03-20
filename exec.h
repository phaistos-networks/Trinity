#pragma once
#include "queries.h"
#include "docidupdates.h"
#include "index_source.h"
#include "matches.h"

namespace Trinity
{
	void exec_query(const query &in, IndexSource *, masked_documents_registry *const maskedDocumentsRegistry, MatchedIndexDocumentsFilter *);

	// Handy utility function; executes query on all index sources in the provided collection in sequence and returns
	// a vector with the match filters/results of each execution.
	//
	// You are expected to merge/reduce/blend them.
	template<typename T, typename...Arg>
		std::vector<std::unique_ptr<MatchedIndexDocumentsFilter>> exec_query(const query &in, IndexSourcesCollection *collection, Arg&&...args)
		{
			static_assert(std::is_base_of<MatchedIndexDocumentsFilter, T>::value, "Expected a MatchedIndexDocumentsFilter subclass");
			const auto n = collection->sources.size();
			std::vector<std::unique_ptr<MatchedIndexDocumentsFilter>> out;

			for (uint32_t i{0}; i != n; ++i)
                        {
                                auto source = collection->sources[i];
                                auto scanner = collection->scanner_registry_for(i);
                                auto filter = std::make_unique<T>(std::forward<Arg>(args)...);

                                exec_query(in, source, scanner.get(), filter.get());
                                out.push_back(std::move(filter));
                        }

                        return out;
		}
};
