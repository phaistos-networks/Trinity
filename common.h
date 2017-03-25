#pragma once
#include <switch.h>
#include <text.h>
#define TRINITY_ENABLE_PREFETCH 1
#ifdef TRINITY_ENABLE_PREFETCH
#include <emmintrin.h> // for _mm_prefetch() intrinsic . We could have also used __builtin_prefetch()
#endif

namespace Trinity
{
        // We will support unicode, so more appropriate string types will be better suited to the task
        using str8_t = strwlen8_t;
        using str32_t = strwlen32_t;

	// you should be able to set docid_t to uint64_t, recompile and get 64bit document identifiers - though it hasn't been tested and there may be edge cases
	// where this won't work but will likely be trivial to fix/implement whatever's required(Please file a GH issue)
	using docid_t = uint32_t;

	// magic value; signifies end of document
	static constexpr docid_t MaxDocIDValue{std::numeric_limits<docid_t>::max()};


	using tokenpos_t = uint16_t;

        static inline int32_t terms_cmp(const str8_t::value_type *a, const uint8_t aLen, const str8_t::value_type *b, const uint8_t bLen)
        {
                // Your impl. may ignore case completely so that you can
                // index and query without having to care for distinctions between lower and upper case (e.g use Text::StrnncasecmpISO88597() )
                // However, if you were to do that, you 'd need to account for that wherever in the codebase
                // you either track strings(tokens) or check for equality, e.g
                // - Trinity::IndexSource::resolve_term_ctx()
                // - query and parser_ctx
                // - exec.cpp caches etc
                return Trinity::str32_t(a, aLen).Cmp(b, bLen);
        }

	static inline uint32_t default_token_parser_impl(const str32_t content)
	{
		return Text::TermLengthWithEnd(content.p, content.end());
	}
}

#include "limits.h"
