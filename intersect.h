#pragma once
#include "queries.h"
#include "docidupdates.h"
#include "index_source.h"
#include "matches.h"

namespace Trinity
{
        void intersect_impl(const str8_t *tokens, const uint8_t tokensCnt, IndexSource *src, masked_documents_registry *, std::vector<std::pair<uint64_t, uint32_t>> *);

        std::vector<std::pair<uint64_t, uint32_t>> intersect(const str8_t *tokens, const uint8_t tokensCnt, IndexSource *src, masked_documents_registry *reg)
        {
                std::vector<std::pair<uint64_t, uint32_t>> res;

                intersect_impl(tokens, tokensCnt, src, reg, &res);
                return res;
        }

        std::vector<std::pair<uint64_t, uint32_t>> intersect(const str8_t *tokens, const uint8_t tokensCnt, IndexSourcesCollection *collection);

	uint8_t intersection_indices(uint64_t, uint8_t *indices);
}
