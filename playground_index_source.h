#pragma once
#include "index_source.h"

namespace Trinity
{
	// A simple playground index source for monkey around
        class PlaygroundIndexSource final
            : public IndexSource
        {
              private:
                std::unique_ptr<Trinity::Codecs::AccessProxy> accessProxy;

              public:
                PlaygroundIndexSource(Trinity::Codecs::AccessProxy *const ap) // segment assumes ownership of the provided AccessProxy
                    : accessProxy{ap}
                {
                }

                Switch::unordered_map<strwlen8_t, term_index_ctx> tctxMap; // for debugging

                term_index_ctx resolve_term_ctx(const strwlen8_t term) override final
                {
                        return tctxMap[term];
                }

                Trinity::Codecs::Decoder *new_postings_decoder(const term_index_ctx ctx) override final
                {
                        return accessProxy->new_decoder(ctx, accessProxy.get());
                }
        };
}
