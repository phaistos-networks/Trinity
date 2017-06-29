#pragma once
#include <switch.h>
#include <text.h>
#define TRINITY_ENABLE_PREFETCH 1
#ifdef TRINITY_ENABLE_PREFETCH
#include <emmintrin.h> // for _mm_prefetch() intrinsic . We could have also used __builtin_prefetch()
#endif


// Define if you want to read in the contents of the index instead of memory mapping it to the process space
// You probably don't want to do that though
//#define TRINITY_MEMRESIDENT_INDEX 1

namespace Trinity
{
        // We will support unicode, so more appropriate string types will be better suited to the task.
	// See: http://site.icu-project.org/
        using str8_t = strwlen8_t;
        using str32_t = strwlen32_t;
	using char_t = str8_t::value_type;

	// You should be able to set docid_t to uint64_t, recompile and get 64bit document identifiers - though it hasn't been tested and there may be edge cases
	// where this won't work but will likely be trivial to fix/implement whatever's required(Please file a GH issue)
	//
	// The bundled codecs(google, lucene) will not properly work if docid_it is 64bits or longer, so you 'd have to modify them or create a new codec
	// that supports longer document IDs in the index (should be simple enough).
	using docid_t = uint32_t;

	// magic value; signifies end of document
	static constexpr docid_t MaxDocIDValue{std::numeric_limits<docid_t>::max()};


	// Represents the position of a token(i.e word) in a document
	using tokenpos_t = uint16_t;

        static inline int32_t terms_cmp(const char_t *a, const uint8_t aLen, const char_t *b, const uint8_t bLen)
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

	// Returns how many characters(char_t) were parsed from `content`, and how many were stored into `out`
	//
	// You may want to translate e.g "spider-man" to "spiderman", which is why this is not simply expected to
	// return the number of characters consumed. Or you may want to translate from whatever case to lower-case.
	// Or, for example, you may want to consume 'I.B.M' as 'IBM', etc.
	//
	// It is possible to consume characters, but not actually store any in out. (i.e result.first to be != 0 and result.second to be == 0)
	//
	// This default implementation simply consumes a token based in very simple heuristics and return it as-is, with no translation.
	//
	// XXX: out must be at least (Limits::MaxTermLength + 1) in size, you can then check if 
	// return value.second > Limits::MaxTermLength, like parse_term() does.
	// Your alternative implementations must comply with this rule.
	std::pair<uint32_t, uint8_t> default_token_parser_impl(const str32_t content, char_t *out, const bool in_phrase);
}

#include "trinity_limits.h"
#ifdef LEAN_SWITCH
#include <compress.h>
#endif
