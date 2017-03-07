#pragma once
#include "index_source.h"
#include "terms.h"
#include "docidupdates.h"

namespace Trinity
{
        class SegmentIndexSource
            : public IndexSource,
              public RefCounted<SegmentIndexSource>
        {
              private:
                std::unique_ptr<Trinity::Codecs::AccessProxy> accessProxy;
                uint64_t gen;       // Timings::Microseconds::SysTime() when it was comitted
		std::unique_ptr<SegmentTerms> terms; // all terms for this segment
		range_base<const uint8_t *, uint32_t> index;

                struct masked_documents_struct
                {
                        updated_documents set;
                        range_base<const uint8_t *, uint32_t> fileData;

                        ~masked_documents_struct()
                        {
                                if (auto ptr = (void *)fileData.offset)
                                        munmap(ptr, fileData.size());
                        }

			masked_documents_struct()
				: set{}
			{
			}
                } maskedDocuments;

              public:
                SegmentIndexSource(const char *basePath);

                Switch::unordered_map<strwlen8_t, term_index_ctx> tctxMap; // for debugging

                term_index_ctx resolve_term_ctx(const strwlen8_t term) override final
                {
                        return terms->lookup(term);
                }

                Trinity::Codecs::Decoder *new_postings_decoder(const term_index_ctx ctx) override final
                {
                        return accessProxy->decoder(ctx, accessProxy.get(), nullptr);
                }

		~SegmentIndexSource()
		{
			if (auto ptr = (void *)index.offset)
				munmap(ptr, index.size());
		}
        };

}
