#pragma once
#include "codecs.h"
#include "index_source.h"
#include <buffer.h>
#include <sparsefixedbitset.h>
#include <switch_bitops.h>
#include <switch_dictionary.h>
#include <switch_mallocators.h>

namespace Trinity {
        // Persists an index sesion as a segment
        // The application is responsible for persisting the terms (see SegmentIndexSession::commit()
        // for example, and IndexSession::persist_terms())
        void persist_segment(const IndexSource::field_statistics &,
                             Trinity::Codecs::IndexSession *const sess,
                             std::vector<uint32_t> &              updatedDocumentIDs,
                             int                                  fd);

        // Wrapper for persist_segment(); opens the index file and passes it to persist_segment()
        void persist_segment(const IndexSource::field_statistics &,
                             Trinity::Codecs::IndexSession *const sess,
                             std::vector<uint32_t> &              updatedDocumentIDs);

        // A utility class suitable for indexing document terms and persisting the index and other codec specifc data into a directory
        // It offers a simple API for adding, replacing and erasing documents.
        // You can use SegmentIndexSource to load the segment(and use it for search)
        //
        // It now also may track state that may be used with statistical models implementations
        // see Trinity::Similarity NS.
        class SegmentIndexSession final {
              private:
                // We needed a faster way for tracking committed document IDs
                struct bank final {
                        static constexpr std::size_t SPAN{1 << 20};

                        SparseFixedBitSet bs{SPAN};
                        isrc_docid_t      base;
                };

                std::vector<bank *> banks;
                bank *              curBank{nullptr};

              private:
                // Trinity doesn't support fields, unlike Lucene and others, because
                // they are mostly useless for most tasks, though we may support them eventually, if only
                // because it should be easy to do so.
                //
                // For future-proofing reasons, we 'll name this field_doc_state, even if there is only
                // one implicit "field"/namespace for all tokens.
                // Also, this is about tracking positions hits only.
                // This is a per-document state
                struct field_doc_stats final {
                        // this is just for terms where position is specified
                        std::uint16_t distinctTermsCnt{0};

                        // the maximum number of hits with a specified position
                        // for any term.
                        std::uint16_t maxTermFreq{0};

                        // total hits where their position is shared among 2+ of them
                        // i.e there's an overlap
                        std::uint16_t overlapsCnt;

                        std::uint16_t positionHitsCnt{0};

                        void reset() {
                                distinctTermsCnt = 0;
                                maxTermFreq      = 0;
                                overlapsCnt      = 0;
                                positionHitsCnt  = 0;
                        }
                };

                IndexSource::field_statistics defaultFieldStats;

              private:
                IOBuffer b;
                IOBuffer hitsBuf;
                int      backingFileFD{-1};
                // by partitioning hits based on term we save about 2s in a previous runtime of 39s down to 37s
                // maybe we can use radix sort there?
                std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> hits[16];
                std::vector<isrc_docid_t>                                                            updatedDocumentIDs;
                simple_allocator                                                                     dictionaryAllocator;

                // flat_hash_map<> is about 11% faster than alternative dictionaries
                // so we are using it now here
                // UPDATE: issues discovered with flat_hash_map<>; will switch to it again when
                // we can be certain that it is no longer broken
                std::unordered_map<str8_t, uint32_t> dictionary;
                std::unordered_map<uint32_t, str8_t> invDict;

                //See IndexSession::indexOutFlushed comments
                uint32_t flushFreq{0}, intermediateStateFlushFreq{0};

              public:
                // Check https://www.ebayinc.com/stories/blogs/tech/making-e-commerce-search-faster/
                // for an alternative ordering scheme, based on grouping and other semantics
                struct document_proxy final {
                        SegmentIndexSession &                                                                 sess;
                        const isrc_docid_t                                                                    did;
                        std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> *hits;
                        IOBuffer &                                                                            hitsBuf;
                        tokenpos_t                                                                            lastPos;
                        uint16_t                                                                              positionOverlapsCnt;

                        uint32_t term_id(const str8_t term) {
                                return sess.term_id(term);
                        }

                        document_proxy(SegmentIndexSession &s, isrc_docid_t documentID, std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> *h, IOBuffer &hb)
                            : sess{s}, did{documentID}, hits{h}, hitsBuf{hb}, lastPos{0}, positionOverlapsCnt{0} {
                        }

                        void insert(const uint32_t termID, const tokenpos_t position, range_base<const uint8_t *, const uint8_t> payload);

                        // new `term` hit at `position` with `payload`(attrs.)
                        void insert(const str8_t term, const tokenpos_t position, range_base<const uint8_t *, const uint8_t> payload) {
                                DEXPECT(term.size() <= Limits::MaxTermLength);
                                insert(term_id(term), position, payload);
                        }

                        void insert(const uint32_t termID, const tokenpos_t position) {
                                insert(termID, position, {});
                        }

                        // new `term` hit at `position`
                        void insert(const str8_t term, const tokenpos_t position) {
                                DEXPECT(term.size() <= Limits::MaxTermLength);
                                insert(term_id(term), position, {});
                        }

                        template <typename T>
                        void insert(const uint32_t termID, const tokenpos_t position, const T &v) {
                                insert(termID, position, {reinterpret_cast<const uint8_t *>(&v), uint32_t(sizeof(T))});
                        }

                        template <typename T>
                        void insert(const str8_t term, const tokenpos_t position, const T &v) {
                                DEXPECT(term.size() <= Limits::MaxTermLength);
                                insert(term_id(term), position, {static_cast<const uint8_t *>(&v), uint32_t(sizeof(T))});
                        }

                        void insert_var_payload(const uint32_t termID, const tokenpos_t pos, const uint64_t payload) {
                                const uint8_t requiredBytes = ((64 - SwitchBitOps::LeadingZeros(payload)) + 7) >> 3;

                                insert(termID, pos, {reinterpret_cast<const uint8_t *>(&payload), requiredBytes});
                        }
                };

              private:
                void commit_document_impl(const document_proxy &proxy, const bool replace);

                bool track(const isrc_docid_t);

                void consider_update(const isrc_docid_t);

              public:
                uint32_t term_id(const str8_t term);

                str8_t term(const uint32_t id);

                void clear() {
                        b.clear();
                        while (banks.size()) {
                                delete banks.back();
                                banks.pop_back();
                        }
                }

                // When != 0, whenever the session's indexOut size exceeds that value, the index
                // will be flushed.
                void set_flush_freq(const size_t n) {
                        flushFreq = n;
                }

                void set_intermediate_state_flush_freq(const size_t n) {
                        intermediateStateFlushFreq = n;
                }

                void erase(const isrc_docid_t documentID);

                // After you have obtained a document_proxy, you can use its insert methods to register term hits
                // and when you are done indexing a document, use insert() or update() to insert as a NEW document or UPDATE an existing
                // document in case it is already indexed in another segments.
                document_proxy begin(const isrc_docid_t documentID);

                // In the past we 'd store those in reverse, so that if we were to erase a document and then index it
                // We 'd read the document's terms first (because we 'd read in reverse from the buffer) and would ignore
                // the erase (document, 0) pair
                // It should be easy to implement that again later
                void insert(const document_proxy &proxy) {
                        commit_document_impl(proxy, false);
                }

                // Use this method instead of insert() when you are updating a document.
                // If you are not, you should (but are not required to) use insert()
                // XXX: update is a misnomer, should have been replace.  Please use replace()
                [[deprecated("please use replace()")]] void update(const document_proxy &proxy) {
                        commit_document_impl(proxy, true);
                }

                void replace(const document_proxy &proxy) {
                        commit_document_impl(proxy, true);
                }

                // Persist index and masked products into the directory s->basePath
                // See also SegmentIndexSource::SegmentIndexSource()
                void commit(Trinity::Codecs::IndexSession *const s);

                auto any_indexed() const noexcept {
                        return backingFileFD != -1 || hitsBuf.size() || b.size() || updatedDocumentIDs.size();
                }

                ~SegmentIndexSession() {
                        if (backingFileFD != -1) {
                                close(backingFileFD);
                        }
                        while (banks.size()) {
                                delete banks.back();
                                banks.pop_back();
                        }
                }
        };
} // namespace Trinity
