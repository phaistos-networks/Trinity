#pragma once
#include "docidupdates.h"
#include "terms.h"
#include "index_source.h"

namespace Trinity {
        struct merge_candidate final {
                // generation of the index source
                // See IndexSource::gen
                uint64_t gen;

                // Access to all terms of the index source
                IndexSourceTermsView *terms;

                // Faciliates access to the index and other content
                Trinity::Codecs::AccessProxy *ap;

                // All documents masked in this index source
                // more recent candidates(i.e candidates where gen < this gen) will use it
                // see MergeCandidatesCollection::merge() impl.
                updated_documents maskedDocuments;

                merge_candidate &operator=(const merge_candidate &o) {
                        gen   = o.gen;
                        terms = o.terms;
                        ap    = o.ap;
                        new (&maskedDocuments) updated_documents(o.maskedDocuments);
                        return *this;
                }
        };

        // See IndexSourcesCollection
        class MergeCandidatesCollection final {
              private:
                std::vector<updated_documents>                    all;
                std::vector<std::pair<merge_candidate, uint16_t>> map;

              public:
                std::vector<merge_candidate> candidates;

              public:
                void insert(const merge_candidate c) {
                        candidates.push_back(c);
                }

                void commit();

                std::unique_ptr<Trinity::masked_documents_registry> scanner_registry_for(const uint16_t idx);

                // This method will merge all registered merge candidates into a new index session and will also output all
                // distinct terms and their term_index_ctx.
                // It will properly and optimally handle different input codecs and mismatches between output codec(i.e is->codec_identifier() )
                // and input codecs.
                //
                // You may want to use
                // - Trinity::pack_terms() to build the terms files and then persist them
                // - Trinity::persist_segment() to persist the actual index
                //
                // IMPORTANT:
                // You should use consider_tracked_sources() after you have merge()ed to figure out what to do with all tracked sources.
                //
                // You are expected to outIndexSess->begin() before you merge(), and outIndexSess->end() afterwards, though you may
                // want to use Trinity::persist_segment(outIndexSess) which will persist and invoke end() for you
                //
                // You may want to explicitly disable use of IndexSession::append_index_chunk() and IndexSession::merge(), even if it is supported by the outIndexSess's codec.
                // If you are going to use ExecFlags::AccumulatedScoreScheme, and your scorer depends on IndexSource::field_statistics, those are
                // only computed, during merge, for terms that are not handled by append_index_chunk(), so you may want to disable it, so that
                // statistics for those terms as well will be collected.
                void merge(Codecs::IndexSession *outIndexSess, simple_allocator *, std::vector<std::pair<str8_t, term_index_ctx>> *const outTerms, IndexSource::field_statistics *fs, const uint32_t flushFreq = 0, const bool disableOptimizations = false);

                enum class IndexSourceRetention : uint8_t {
                        RetainAll = 0,
                        RetainDocumentIDsUpdates,
                        Delete
                };

                // Once you have committed(), and merge(d), you should provide
                // all tracked sources, and this method will return another vector with a std::pair<IndexSourceRetention, uint64_t>
                // for each of the sources(identified by generation) that you should consider;
                // RetainAll:  don't delete anything, leave it alone
                // RetainDocumentIDsUpdates: delete all index files but retain document IDs updates files
                // Delete: wipe out the directory, retain nothing
                std::vector<std::pair<uint64_t, IndexSourceRetention>> consider_tracked_sources(std::vector<uint64_t> trackedSources);
        };
} // namespace Trinity

static inline void PrintImpl(Buffer &b, const Trinity::MergeCandidatesCollection::IndexSourceRetention r) {
        switch (r) {
                case Trinity::MergeCandidatesCollection::IndexSourceRetention::RetainAll:
                        b.append("ALL"_s32);
                        break;

                case Trinity::MergeCandidatesCollection::IndexSourceRetention::RetainDocumentIDsUpdates:
                        b.append("DocIDs"_s32);
                        break;

                case Trinity::MergeCandidatesCollection::IndexSourceRetention::Delete:
                        b.append("DELETE"_s32);
                        break;
        }
}
