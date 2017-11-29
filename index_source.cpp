#include "index_source.h"

void Trinity::IndexSourcesCollection::commit()
{
        std::sort(sources.begin(), sources.end(), [](const auto a, const auto b) noexcept {
                return b->generation() < a->generation();
        });

        map.clear();
        all.clear();
        for (auto s : sources)
        {
                auto ud = s->masked_documents();

                map.push_back({s, all.size()});
                if (ud)
                        all.push_back(ud);
        }
}

Trinity::IndexSourcesCollection::~IndexSourcesCollection()
{
	while (sources.size())
	{
		sources.back()->Release();
		sources.pop_back();
	}
}

std::unique_ptr<Trinity::masked_documents_registry> Trinity::IndexSourcesCollection::scanner_registry_for(const uint16_t idx) 
{
	const auto n = map[idx].second;

	return masked_documents_registry::make(all.data(), n);
}
