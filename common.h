#pragma once
#include <switch.h>
#include "limits.h"
#include <text.h>

namespace Trinity
{
        // We will support unicode, so more appropriate string types will be better suited to the task
        using str8_t = strwlen8_t;
        using str32_t = strwlen32_t;

        static inline int32_t terms_cmp(const char *a, const uint8_t aLen, const char *b, const uint8_t bLen)
        {
                // Your impl. may ignore case completely so that you can
                // index and query without having to care for distinctions between lower and upper case (e.g use Text::StrnncasecmpISO88597() )
                // However, if you wwere to do that, you 'd need to account for that wherever in the codebase
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
