#include "index_source.h"

void Trinity::IndexSourcesCollection::commit()
{
        std::sort(sources.begin(), sources.end(), [](const auto a, const auto b) {
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
