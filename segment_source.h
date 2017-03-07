#pragma once
#include "index_source.h"

namespace Trinity
{
        class SegmentIndexSource
            : public IndexSource,
              public RefCounted<SegmentIndexSource>
        {
              private:
                std::unique_ptr<Trinity::Codecs::AccessProxy> accessProxy;

              public:
                SegmentIndexSource(Trinity::Codecs::AccessProxy *const ap) // segment assumes ownership of the provided AccessProxy
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
                        return accessProxy->decoder(ctx, accessProxy.get(), nullptr);
                }
        };
}
