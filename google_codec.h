// A codec based on Google's "Challenges in Building Large-Scale Information Retrieval Systems"
// Not using rice/gamma encoding here, though it should be trivial to support that encoding
// See https://github.com/powturbo/TurboPFor for Elias Fano encoding (for other Elias encoding, Rice, gamma etc, the impl. is trivial)
#pragma once
#include "codecs.h"

#define TRINITY_CODECS_GOOGLE_AVAILABLE 1

static_assert(sizeof(Trinity::isrc_docid_t) <= sizeof(uint32_t));

namespace Trinity
{
        namespace Codecs
        {
                namespace Google
                {
                        static constexpr bool TRACK_PAYLOADS{true};
                        static constexpr size_t N{32};                  // block size (Google's block size is 32)
                        static constexpr size_t SKIPLIST_STEP{256 / N}; // generate a new skiplist entry every that many blocks
                        static constexpr bool CONSTRUCT_SKIPLIST{true};

                        struct IndexSession final
                            : public Trinity::Codecs::IndexSession
                        {
                                void begin() override final;

                                void end() override final;

                                Trinity::Codecs::Encoder *new_encoder() override final;

                                IndexSession(const char *bp)
                                    : Trinity::Codecs::IndexSession{bp}
                                {
                                }

                                strwlen8_t codec_identifier() override final
                                {
                                        return "GOOGLE"_s8;
                                }

                                range32_t append_index_chunk(const Trinity::Codecs::AccessProxy *, const term_index_ctx srcTCTX) override final;

                                void merge(merge_participant *, const uint16_t, Trinity::Codecs::Encoder *) override final;
                        };

                        class Encoder final
                            : public Trinity::Codecs::Encoder
                        {
                              private:
                                IOBuffer skipListData;
                                IOBuffer block, hitsData;
                                isrc_docid_t prevBlockLastDocumentID{0}, curDocID{0}, lastCommitedDocID;
                                uint8_t curBlockSize, curPayloadSize;
                                uint32_t lastPos;
                                isrc_docid_t docDeltas[N];
                                uint32_t blockFreqs[N];
                                uint32_t skiplistEntryCountdown{SKIPLIST_STEP};
                                uint32_t curTermOffset;
                                uint32_t termDocuments;

                              private:
                                void commit_block();

                              public:
                                Encoder(Trinity::Codecs::IndexSession *s)
                                    : Trinity::Codecs::Encoder{s}
                                {
                                }

                                auto total_documents() const noexcept
                                {
                                        return termDocuments;
                                }

                                void begin_term() override final;

                                void begin_document(const isrc_docid_t documentID) override final;

                                void new_hit(const uint32_t pos, const range_base<const uint8_t *, const uint8_t> payload) override final;

                                inline void new_position(const tokenpos_t pos)
                                {
                                        new_hit(pos, {});
                                }

                                void end_document() override final;

                                void end_term(term_index_ctx *tctx) override final;
                        };

                        struct AccessProxy final
                            : public Trinity::Codecs::AccessProxy
                        {
                                AccessProxy(const char *bp, const uint8_t *p)
                                    : Trinity::Codecs::AccessProxy{bp, p}
                                {
                                }

                                strwlen8_t codec_identifier() override final
                                {
                                        return "GOOGLE"_s8;
                                }

                                Trinity::Codecs::Decoder *new_decoder(const term_index_ctx &tctx) override final;
                        };

			struct PostingsListIterator
				: public Trinity::Codecs::PostingsListIterator
                        {
                                friend class Decoder;

				private:
                                uint8_t blockDocIdx;
                                isrc_docid_t documents[N];
                                isrc_docid_t blockLastDocID{0};
                                uint32_t freqs[N];
                                uint32_t skipListIdx;
				const uint8_t *p;

                              public:
                                inline isrc_docid_t next() override final;

                                inline isrc_docid_t advance(const isrc_docid_t) override final;

                                inline void materialize_hits(DocWordsSpace *dwspace, term_hit *out) override final;

                                PostingsListIterator(Decoder *const d)
                                    : Trinity::Codecs::PostingsListIterator{reinterpret_cast<Trinity::Codecs::Decoder *>(d)}
                                {
                                }
                        };

                        // We used to keep track of remDocsInBlocks
                        // which was decremented whenever we 'd unpack a block, so we didn't need to
                        // know the chunk size (thereby having to store this in the terms dictionary)
                        // however that meant:
                        // 1. chunk_size() was needed to compute this for when we had to merge a term's postlist
                        // that didnt' need to be merged
                        // 2. we either had to make the skiplists larger, encoding documents_count_upto there, or not
                        // use skiplists at all, because we 'd need to adjust remDocsInBlocks when we were using them and that
                        // meant we need to how many documents to adjust it with
                        //
                        // Tracking the postings chunk size solves all those problems, and the cost is 4 bytes to track that (we could use varints)
                        // size in the terms dictionary. Sounds like a good compromise (for 1 million terms, we need 4 extra MBs)
                        class Decoder final
                            : public Trinity::Codecs::Decoder
                        {
				friend struct PostingsListIterator;

                                const uint8_t *chunkEnd;
                                const uint8_t *base;
                                std::vector<std::pair<isrc_docid_t, uint32_t>> skiplist;

                              protected:
                                void next(PostingsListIterator *);

                                void advance(PostingsListIterator *, const isrc_docid_t);

                                void materialize_hits(PostingsListIterator *, DocWordsSpace *, term_hit *);

                              private:
                                uint32_t skiplist_search(PostingsListIterator *, const isrc_docid_t target) const noexcept;

                                void skip_block_doc(PostingsListIterator *);

                                void unpack_block(PostingsListIterator *, const isrc_docid_t thisBlockLastDocID, const uint8_t n);

                                void seek_block(PostingsListIterator *, const isrc_docid_t target);

                                void unpack_next_block(PostingsListIterator *);

                                void finalize(PostingsListIterator *const it)
                                {
                                        it->blockDocIdx = 0;
                                        it->blockLastDocID = DocIDsEND; // magic value; signifies end of documents
                                        it->documents[0] = DocIDsEND;
                                        it->p = chunkEnd;
                                        it->curDocument.id = DocIDsEND;
                                }

                                // see skip_block_doc()
                                void skip_remaining_block_documents(PostingsListIterator *);

                              public:
                                void init(const term_index_ctx &tctx, Trinity::Codecs::AccessProxy *access) override final;

				Trinity::Codecs::PostingsListIterator *new_iterator() override final;
                        };

                        isrc_docid_t PostingsListIterator::next()
                        {
                                static_cast<Codecs::Google::Decoder *>(dec)->next(this);
                                return curDocument.id;
                        }

                        isrc_docid_t PostingsListIterator::advance(const isrc_docid_t target)
                        {
                                static_cast<Codecs::Google::Decoder *>(dec)->advance(this, target);
                                return curDocument.id;
                        }

                        void PostingsListIterator::materialize_hits(DocWordsSpace *dwspace, term_hit *out)
                        {
                                static_cast<Codecs::Google::Decoder *>(dec)->materialize_hits(this, dwspace, out);
                        }
                }
        }
}
