#pragma once
#include "queries.h"
#include "docidupdates.h"
#include "index_source.h"
#include "matches.h"
#include <switch_bitops.h>

namespace Trinity
{
	// If you want to ignore intersections here (as opposed to later when you process the results), by checking if the first or last token
	// is either of the ones encoded in stopwordsMask, set stopwordsMask to a value != 0
	// Just set uint64_t(1) << index, where index is the token index in tokens specified in the method
	// You probably should use this mask because we ignore intersections that mask other intersections completely
	// TODO: consider intersections for [world of warcraft gears of war]
	// if we set 'of' as stop word in stopwordsMak, we 'll likely return (world of warcraft, gears, war)
	// if we don't, we 'll return (world of warcraft, of gears war, of war)
	// neither of them is optimal, but then again, there is not much we do around this except perhaps
	// consider past queries compsied of those tokens and figure out which is the most popular arrangement of tokens
        void intersect_impl(const uint64_t stopwordsMask, const str8_t *tokens, const uint8_t tokensCnt, IndexSource *src, masked_documents_registry *, std::vector<std::pair<uint64_t, uint32_t>> *);

        inline std::vector<std::pair<uint64_t, uint32_t>> intersect(const uint64_t stopwordsMask, const str8_t *tokens, const uint8_t tokensCnt, IndexSource *src, masked_documents_registry *reg)
        {
                std::vector<std::pair<uint64_t, uint32_t>> res;

                intersect_impl(stopwordsMask, tokens, tokensCnt, src, reg, &res);
                return res;
        }

        std::vector<std::pair<uint64_t, uint32_t>> intersect(const uint64_t stopwordsMask, const str8_t *tokens, const uint8_t tokensCnt, IndexSourcesCollection *collection);

	uint8_t intersection_indices(uint64_t, uint8_t *indices);

	static inline bool sort_intersections(const std::pair<uint64_t, uint32_t> &a, const std::pair<uint64_t, uint32_t> &b) noexcept
        {
                if (const auto ca = SwitchBitOps::PopCnt(a.first), cb = SwitchBitOps::PopCnt(b.first); cb < ca || (cb == ca && b.second < a.second))
                        return true;
                else
                        return false;
        }
}
