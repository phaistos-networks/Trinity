#pragma once
#include "codecs.h"
#include <mutex>
#include <switch.h>
#include <switch_dictionary.h>
#include <switch_mallocators.h>
#include <switch_refcnt.h>

namespace Trinity {
        // An index source provides term_index_ctx and decoders to the query execution runtime
        // It can be a RO wrapper to an index segment, a wrapper to a simple hashtable/list, anything
        // Lucene implements near real-time search by providing a segment wrapper(i.e index source) which accesses the indexer state directly
        // With index sources, we could accomplish that as well
        //
        // One could build a fairly impressive scheme, where custom IndexSource and Trinity::Codec::Decoder sub-classes would allow for very interesting use cases
        // An Index Source doesn't need to be a segment, or use the terms infrastructure to store and access terms. It can be anything, as long as it implements the 3 methods and
        // a decoder doesn't need to be accessing a segment either. It can be accessing e.g dicts and lists or vectors or whatever.
        class IndexSource
            : public RefCounted<IndexSource> {
              protected:
                std::mutex                                 cacheLock;
                simple_allocator                           keysAllocator{512};
                std::unordered_map<str8_t, term_index_ctx> cache;
                uint64_t                                   gen{0}; // See IndexSourcesCollection

              public:
                // We currently don't support multiple fields
                // so we only track one(default) "field"'s worth of stats.
                // Those may be used by similarity models or for debugging internals.
                //
                // A problem here is that we need to account only for hits where (pos != 0)
                // but that'd require that we track two document counts
                // in terms, one for total documents -- needed for codescs, and another for
                // total documents that have at least one hit where position != 0, and that'd inflate terms
                // file and therefore slow down access to it.
                //
                // This is because we may index all kind of documents with no position or even payload
                //
                // For SegmentIndexSource we can include those metrics in the codec file that's used for storing the codec
                // of the index, and the default_field_stats() method would just return the deserialized SegmentIndexSource::fs
                // whereas say another IndexSource that's used for real time search would return something else
                //
                // TODO: figure this out later
                struct field_statistics {
                        // Sum of all hits for all terms for this "field"
                        uint64_t sumTermHits{0}; // lucene: Terms::getSumTotalTermFreq() sum of all TermsIndexEnum::ttoalTermFreq()
                        uint32_t totalTerms{0};  // lucene: Terms::size()
                                                 // Sum of all documents for all all terms for this "field"
                        // Not the intersection of them, the sum.
                        uint64_t sumTermsDocs{0}; // lucene: Terms##getSumDocFreq() sum of TermsIndexEnum::docFreq()
                                                  // Total distinct docments that have at least one term for this "field"
                        uint32_t docsCnt{0};
                };

              public:
                inline auto generation() const noexcept {
                        return gen;
                }

                term_index_ctx term_ctx(const str8_t term) {
                        [[maybe_unused]] static constexpr bool trace{false};
                        std::lock_guard<std::mutex>            g(cacheLock);

                        auto p = cache.insert({term, {}});

                        if (p.second) {
                                const_cast<str8_t *>(&p.first->first)->Set(keysAllocator.CopyOf(term.data(), term.size()), term.size());
                                p.first->second = resolve_term_ctx(term);
                        }

                        return p.first->second;
                }

#if 0 // This would probably be a good idea, but we don't need this, and it would make some optimisations in updated_documents_scanner::test() possible because                     \
      // of document IDs(global space) are always expected to be considered in ascending order would not work, and it would also require some effort to                             \
      // get this right everywhere we deal with document IDs (e.g merging documents).                                                                                               \
      //                                                                                                                                                                            \
      // It may be a good idea to support this in the future though, with the added benefit that returning 0 would                                                                  \
      // save the execution engine from eval()uating the query for the document, and that we could use e.g 32bit integers for index source-local IDs mapped to e.g 64bit integers   \
      // and that could have some impact in the index size (and it would allow for e.g LUCENE_ENCODE_FREQ1_DOCDELTA).                                                               \
      // Consider it for later.                                                                                                                                                     \
      //                                                                                                                                                                            \
      //                                                                                                                                                                            \
      // Theory: two docIDs spaces. One is global, and another local to each index source/segment and irrelevant outside that context.                                              \
      // By using 2 different document IDs spaces, we could use an e.g u32 integer for the document IDs of documents indexed in a segment, and translate them                       \
      // to e.g u64 global IDs during execution, prior to evaluating the query on the document. (e.g while indexing store in a file the global ID for each segment-local logical id \
      // and then mmap that file in your IndexSource and directly dereference it to get the global ID from the local ID).                                                           \
      //                                                                                                                                                                            \
      // By default, it's an identity method -- no translation between index source and global space                                                                                \
      // You can override it to return 0, if you want to ommit the document ID completely for whatever reason                                                                       \
      // or otherwise translate it to global space here
		virtual isrc_docid_t translate_docid(const isrc_docid_t indexSourceSpaceDocumentID)
		{
			return indexSourceSpaceDocumentID;
		}
#endif

                virtual term_index_ctx resolve_term_ctx(const str8_t term) = 0;

                // For performance reasons, if you are going to perform any kind of translation in your translate_docid()
                // then you should also implement and override this method, and return true, so that the exec.engine
                // will know if it needs to invoke the virtual method translate_docid() or not. This is for performance reasons.
                // Also, if we know that no translation is required, we can use masked_documents_registry or some other
                // datastructure that's optimised for in-order lookups, otherwise use e.g a SparseFixedBitSet which is
                // optimised for random access
                inline virtual bool require_docid_translation() const {
                        return false;
                }

                // The default impl. assumes you used the actual id during indexing.
                // See common.h for comments on relationship between the two different document IDs domains.
                inline virtual docid_t translate_docid(const isrc_docid_t localId) {
                        return docid_t(localId);
                }

                // factory method
                // see RECIPES.md for when you should perhaps make use of the passed `term`
                // See Codecs::Decoder::init() for execCtxTermID
                virtual Trinity::Codecs::Decoder *new_postings_decoder(const str8_t term, const term_index_ctx ctx) = 0;

                // Override if you have any masked documents
                virtual updated_documents masked_documents() {
                        return {};
                }

                // Returns the maximum position expected
                // you may want to override to provide a more accurate value
                // This is used by the execution engine when creating a new DocWordsSpace
                // It cannot be higher than Limits::MaxPosition or lower than 1
                //
                // For example, if you are only indexing titles, you should probably override this method
                // in your IndexSource variant to return a low max indexed position (e.g 100) because you likely don't
                // have any title term indexed at a higher position.
                virtual tokenpos_t max_indexed_position() const {
                        return 8192;
                }

                // Return something meaningful if you have it, and if you intent to use the AccumulatedScoreScheme
                virtual field_statistics default_field_stats() {
                        return {};
                }

                // After we merge, we may, depending on which indices we decided to merge, be left with
                // 1+ indices that may have masked documents, but no index data(i.e they exist simply
                // to hold the masked documents.
                // If that's the case, subclasses should override this method and return true
                //
                // This is not necessary; it helps the execution engine to avoid compiling queries and initializing itself. See exec.h
                virtual bool index_empty() const {
                        return true;
                }

                virtual ~IndexSource() {
                }
        };

        // A simple IndexSource that only masks documents
        // You may want to use it to quickly mask documents without having to index any documents
        // Create one with an updated_documents and insert it into an IndexSourcesCollection and you are done
        class TrivialMaskedDocumentsIndexSource
            : public IndexSource {
              private:
                const updated_documents maskedDocuments;

              public:
                TrivialMaskedDocumentsIndexSource(const updated_documents ud)
                    : maskedDocuments{ud} {
                        gen = Timings::Microseconds::SysTime();
                }

                term_index_ctx resolve_term_ctx(const str8_t term) override final {
                        // fails for every term because we are only blocking here
                        return {};
                }

                // Currently, term is not really useful, but see RECIPES.md for how it could really help with some specific designs
                Trinity::Codecs::Decoder *new_postings_decoder(const str8_t term, const term_index_ctx) override final {
                        return nullptr;
                }

                updated_documents masked_documents() override final {
                        return maskedDocuments;
                }

                bool index_empty() const noexcept override final {
                        // this is just about masked documents really; there's no index
                        return true;
                }
        };

        // A collection of IndexSource; an index of segments or other sources
        // Each index source is identified by a generation, and no two sources can share the same generation
        // The generation represents the order of the sources in relation to each other; a higher generation means that
        // a source with that generation has been created after another with a lower generation.
        // In practice those are likely Timings::Microseconds::SysTime() at the time of the source creation and
        // for segments thats when they were persisted to disk. (in fact for segments, their name is their
        // generation)
        //
        // Each source is also associated with an `updated_documents` instance which tracked all documents updated or deleted when the source was created.
        // This is not used directly by the source itself, but when executing a query on a source(see exec_query() API), we need to consider the source's generation
        // and, for any other sources that will be involved in the search session that have generation HIGHER than the source's generation, we need to check that
        // a document is not set in any of their `updated_documents` instances (because that would mean that there is more recent information about that document
        // in another source that will be considered in this search session).
        //
        // IndexSourcesCollection facilitates that arrangement.
        // It represents a `search session` collection of index sources, and for each such source, it creates a masked_documents_registry that contains scanners
        // for all more recent sources.
        //
        // It also retains all sources.
        // See Trinity::exec_query(const query&, IndexSourcesCollection *) for how to do this in sequence, but you can and should do
        // this in paralle using multple threads and collecting the top-k results from every exec() and then use merge/reduce to come up with the final set of top-k results
        //
        // A great use case would be to have one IndexSourcesCollection which retains many sources and whenever you want to reload segments/sources etc, create
        // a new IndexSourcesCollection for them and atomically exchange pointers (old, new IndexSourcesCollection)
        //
        // It is very important you don't forget to invoke commit() otherwise updated/masked documents state will not be built
        class IndexSourcesCollection final {
              private:
                std::vector<updated_documents> all;
                // for each source, we track how many of the first update_documents in all[]
                // we should consider for masking documents
                std::vector<std::pair<IndexSource *, uint16_t>> map;

              public:
                std::vector<IndexSource *> sources;

              public:
                void insert(IndexSource *is) {
                        is->Retain();
                        sources.push_back(is);
                }

                ~IndexSourcesCollection();

                void commit();

                std::unique_ptr<Trinity::masked_documents_registry> scanner_registry_for(const uint16_t idx);
        };
} // namespace Trinity
