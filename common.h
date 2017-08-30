#pragma once
#include <switch.h>
#include <text.h>
//#define TRINITY_ENABLE_PREFETCH 1
#ifdef TRINITY_ENABLE_PREFETCH
#include <emmintrin.h> // for _mm_prefetch() intrinsic . We could have also used __builtin_prefetch()
#endif

#define TRINITY_VERSION (2 * 10 + 0) // 2.0

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
	using query_term_flags_t = uint16_t;

	// Index Source Document ID
	// It is specific to index sources and the execution engine (and by extension, to the various documents set iterators).
	//
	// Those can be translated to global docid_t via IndexSource::translate_docid() during query execution.
	//
	// When indexing, you are going to provide a meaningful isrc_docid. It can be the actual global ID of a document, or
	// a translated - and you e.g store in a file at sizeof(docid_t) the actual value of the indexed isrc_docid and
	// you consult it in translate_docid()
	using isrc_docid_t = uint32_t;


	// The global document ID
	using docid_t = uint32_t;

	// magic value; end of postinggs list or documents set
	// This is specific to index source document IDs and DocsSets iterators -- not related to global document IDs.
	static constexpr isrc_docid_t DocIDsEND{std::numeric_limits<isrc_docid_t>::max()};


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
