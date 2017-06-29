#pragma once
#include "docidupdates.h"
#include "index_source.h"
#include "matches.h"
#include "queries.h"
#include <set>
#include <switch_bitops.h>

namespace Trinity
{
        // tokens is a std::vector<> of std::set<> where each set represents the list of synonymous tokens (e.g ball, balls)
        // that std::vector<> size cannot exceed 64(because sizeof(uint64_t)*3 == 64)

        // If you want to ignore intersections here (as opposed to later when you process the results), by checking if the first or last token
        // is either of the ones encoded in stopwordsMask, set stopwordsMask to a value != 0
        // Just set uint64_t(1) << index, where index is the token index in tokens specified in the method
        // You probably should use this mask because we ignore intersections that mask other intersections completely
        //
        // TODO: consider intersections for [world of warcraft gears of war]
        // if we set 'of' as stop word in stopwordsMak, we 'll likely return (world of warcraft, gears, war)
        // if we don't, we 'll return (world of warcraft, of gears war, of war)
        // neither of them is optimal, but then again, there is not much we do around this except perhaps
        // consider past queries compsied of those tokens and figure out which is the most popular arrangement of tokens
        void intersect_impl(const uint64_t stopwordsMask, const std::vector<std::set<str8_t>> &tokens, IndexSource *src, masked_documents_registry *, std::vector<std::pair<uint64_t, uint32_t>> *);

        inline std::vector<std::pair<uint64_t, uint32_t>> intersect(const uint64_t stopwordsMask, const std::vector<std::set<str8_t>> &tokens, IndexSource *src, masked_documents_registry *reg)
        {
                std::vector<std::pair<uint64_t, uint32_t>> res;

                intersect_impl(stopwordsMask, tokens, src, reg, &res);
                return res;
        }

        // Should just merge from the collection and then return that
        std::vector<std::pair<uint64_t, uint32_t>> intersect(const uint64_t stopwordsMask,
                                                             const std::vector<std::set<str8_t>> &tokens,
                                                             IndexSourcesCollection *collection);

	// Returns the index (bits offsets in bitmap)
        uint8_t intersection_indices(uint64_t bitmap, uint8_t *indices);

        static inline bool sort_intersections(const std::pair<uint64_t, uint32_t> &a, const std::pair<uint64_t, uint32_t> &b) noexcept
        {
                if (const auto ca = SwitchBitOps::PopCnt(a.first), cb = SwitchBitOps::PopCnt(b.first); cb < ca || (cb == ca && b.second < a.second))
                        return true;
                else
                        return false;
        }

	// A handy function that generates alternatives, in the right order, by intersecting the rewrittenQuery(which is the result of
	// Trinity::rewrite_query(), and you need to use it even if you don't end up rewriting the query, because it will set 
	// phrase::rewrite_ctx for every phrase/token in the query), across the collection of index sources.
	//
	// You must set K = 1 when invoking rewrite_query(), and only accept single word token expansions.
	// e.g for [macbook], it's correct to expand to [macbooks] but not correct to expand to [mac book](two words expansion)
	// Those restrictions are required for interesections where query rewrites are applied .
	std::vector<std::pair<range_base<str8_t *, uint8_t>, std::size_t>> intersection_alternatives(const query &originalQuery, query &rewrittenQuery, IndexSourcesCollection &collection, simple_allocator *const a);
}
