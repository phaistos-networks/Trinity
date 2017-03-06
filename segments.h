#pragma once
#include "codecs.h"
#include <switch_dictionary.h>
#include "docidupdates.h"
#include "runtime.h"
#include <switch_refcnt.h>


namespace Trinity
{
	// A segment represents an self-contained index - though an actual Index will have 0+ segments
	// You are supposed to search all segments and respect their updated/ files.
	//
	// You can safely access a segment concurrently - it is effectively a wrapper for access to an AccessProxy and other segments data
        struct segment final
		: public RefCounted<segment>
        {

		segment(Trinity::Codecs::AccessProxy *const ap) 	// segment assumes ownership of the provided AccessProxy
			: accessProxy{ap}
		{
			// TODO: prepare for terms dictionary access
		}

		std::unique_ptr<Trinity::Codecs::AccessProxy> accessProxy;
                Switch::unordered_map<strwlen8_t, uint32_t> termIDsMap;
                Switch::unordered_map<uint32_t, term_segment_ctx> cache;

		// TODO: serialize access
		uint32_t resolve_term(const strwlen8_t term)
		{
			uint32_t *p;

			SLog("Resolving [", term, "]\n");

			if (termIDsMap.Add(term, 0, &p))
			{
				if (auto tctx = resolve_term_ctx(term); 0 == tctx.documents)
				{
					// Undefined in this segment
					SLog("UNDEFINED [", term, "]\n");
					*p = 0;
				}
				else
				{
					SLog("FOR [", term, "] ", tctx.documents, "\n");
					*p = termIDsMap.size();
					cache.insert({*p, tctx});
				}
			}

			SLog("For [", term, "] ", *p, "\n");
			return *p;
		}

		// TODO: serialize access
		term_segment_ctx term_ctx(const uint32_t termID /* segment space */)
		{
			return cache[termID];
		}

		Switch::unordered_map<strwlen8_t, term_segment_ctx> tctxMap; // for debugging

                // TODO: actual implementation needs to do the right Thing
                // TODO: serialize access
                term_segment_ctx resolve_term_ctx(const strwlen8_t term)
		{
			return tctxMap[term];
		}

		Trinity::Codecs::Decoder *new_postings_decoder(const term_segment_ctx ctx);
        };
}
