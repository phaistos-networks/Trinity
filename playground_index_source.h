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
			gen = Timings::Microseconds::SysTime();
                }

                Switch::unordered_map<str8_t, term_index_ctx> tctxMap; // for debugging

                term_index_ctx resolve_term_ctx(const str8_t term) override final
                {
                        return tctxMap[term];
                }

                Trinity::Codecs::Decoder *new_postings_decoder(const str8_t, const term_index_ctx ctx) override final
                {
                        return accessProxy->new_decoder(ctx);
                }
        };
}
