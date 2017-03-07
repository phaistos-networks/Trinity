#pragma once
#include <switch.h>
#include <switch_dictionary.h>
#include "codecs.h"

namespace Trinity
{
        // An index source provides term_index_ctx and decoders to the query execution runtime
        // It can be a RO wrapper to an index segment, a wrapper to a simple hashtable/list, anything
        // Lucene implements near real-time search by providing a segment wrapper(i.e index source) which accesses the indexer state directly
        // With index sources, we could accomplish that as well
	//
	// One could build a fairly impressive scheme, where custom IndexSource and Trinity::Codec::Decoder sub-classes would allow for very interesting use cases
        class IndexSource
        {
              protected:
                Switch::unordered_map<strwlen8_t, uint32_t> termIDsMap;
                Switch::unordered_map<uint32_t, term_index_ctx> cache;

              public:
                // Returns an INDEX SOURCE WORD SPACE integer identifier
                // You will need to translate to this words space. See exec.cpp
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
                term_index_ctx term_ctx(const uint32_t termID /* segment space */)
                {
                        return cache[termID];
                }

                // Subclasses only need to implement 2 methods
                virtual term_index_ctx resolve_term_ctx(const strwlen8_t term) = 0;

                // factory method
                virtual Trinity::Codecs::Decoder *new_postings_decoder(const term_index_ctx ctx) = 0;
        };
}
