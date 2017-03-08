#pragma once
#include "docidupdates.h"
#include "terms.h"

namespace Trinity
{
        struct merge_candidate final
        {
                // generation of the index source
                // See IndexSource::gen
                uint64_t gen;

                // Access to all terms of the index source
                IndexSourceTermsView *terms;

                // Faciliates access to the index and other content
                // it allows us to create decoders etc
                Trinity::Codecs::AccessProxy *ap;

                // All documents masked in this index source
                // more recent candidates(i.e candidates where gen < this gen) will use it
                updated_documents maskedDocuments;

                merge_candidate &operator=(const merge_candidate &o)
                {
                        gen = o.gen;
                        terms = o.terms;
                        ap = o.ap;
                        new (&maskedDocuments) updated_documents(o.maskedDocuments);
                        return *this;
                }
        };

        // See IndexSourcesCollection
        class MergeCandidatesCollection final
        {
              private:
                std::vector<updated_documents> all;
                std::vector<std::pair<merge_candidate, uint16_t>> map;

              public:
                std::vector<merge_candidate> candidates;

              public:
                void insert(const merge_candidate c)
                {
                        candidates.push_back(c);
                }

                void commit();

                Trinity::dids_scanner_registry *scanner_registry_for(const uint16_t idx);

                // You may want to use
                // - Trinity::pack_terms() to build the terms files and then persist them
                // - Trinity::persist_segment() to persist the actual index
                void merge(Codecs::IndexSession *is, simple_allocator *, std::vector<std::pair<strwlen8_t, term_index_ctx>> *const terms);
        };
}
