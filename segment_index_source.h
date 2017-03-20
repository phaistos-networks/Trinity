#pragma once
#include "index_source.h"
#include "terms.h"
#include "docidupdates.h"

namespace Trinity
{
	// You can use SegmentIndexSession to create a new segment
	// This is a utility class
        class SegmentIndexSource final
            : public IndexSource
        {
              private:
                std::unique_ptr<Trinity::Codecs::AccessProxy> accessProxy;
		std::unique_ptr<SegmentTerms> terms; // all terms for this segment
		range_base<const uint8_t *, uint32_t> index;

                struct masked_documents_struct final
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

		auto access_proxy()
		{
			return accessProxy.get();
		}

                term_index_ctx resolve_term_ctx(const str8_t term) override final
                {
                        return terms->lookup(term);
                }

		auto segment_terms() const
		{
			return terms.get();
		}

                Trinity::Codecs::Decoder *new_postings_decoder(const strwlen8_t, const term_index_ctx ctx) override final
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
