#pragma once
#include "index_source.h"
#include "terms.h"
#include "docidupdates.h"

namespace Trinity
{
	// You can use SegmentIndexSession to create a new segment
        class SegmentIndexSource final
            : public IndexSource
        {
              private:
                std::unique_ptr<Trinity::Codecs::AccessProxy> accessProxy;
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

                term_index_ctx resolve_term_ctx(const str8_t term) override final
                {
                        return terms->lookup(term);
                }

                Trinity::Codecs::Decoder *new_postings_decoder(const term_index_ctx ctx) override final
                {
                        return accessProxy->new_decoder(ctx);
                }

                updated_documents masked_documents() override final
                {
                        return maskedDocuments.set;
                }

                ~SegmentIndexSource()
		{
			if (auto ptr = (void *)index.offset)
				munmap(ptr, index.size());
		}
        };

}
