// A codec based on Google's "Challenges in Building Large-Scale Information Retrieval Systems"
#pragma once
#include "codecs.h"

namespace Trinity
{
        namespace Codecs
        {
                namespace Google
                {
			static constexpr bool TRACK_PAYLOADS{true};
                        static constexpr size_t N{32};             // block size (Google's block size is 32)
                        static constexpr size_t SKIPLIST_STEP{512}; // generate a new skiplist entry every that many blocks

                        struct IndexSession final
                            : public Trinity::Codecs::IndexSession
                        {
                                void begin() override final;

                                void end() override final;

                                Trinity::Codecs::Encoder *new_encoder(Trinity::Codecs::IndexSession *) override final;

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
                              public:
                                IOBuffer skipListData;

                              private:
                                IOBuffer block, hitsData;
                                uint32_t prevBlockLastDocumentID{0}, curDocID{0}, lastCommitedDocID;
                                uint8_t curBlockSize, curPayloadSize;
                                uint32_t lastPos;
                                uint32_t docDeltas[N];
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

                                void begin_term() override final
                                {
                                        auto out{&sess->indexOut};

                                        curBlockSize = 0;
                                        lastCommitedDocID = 0;
                                        prevBlockLastDocumentID = 0;
                                        hitsData.clear();
                                        termDocuments = 0;
                                        curTermOffset = out->size();
                                }

                                void begin_document(const uint32_t documentID, const uint16_t hitsCnt) override final
                                {
					if (unlikely(documentID <= lastCommitedDocID))
					{
						SLog("Unexpected documentID(", documentID, ") <= lastCommitedDocID(", lastCommitedDocID, ")\n");
						std::abort();
					}

                                        curDocID = documentID;
                                        lastPos = 0;
					curPayloadSize = 0;
                                        blockFreqs[curBlockSize] = 0;
                                }

                                void new_hit(const uint32_t pos, const range_base<const uint8_t *, const uint8_t> payload) override final
                                {
                                        static constexpr bool trace{false};
                                        const auto delta = pos - lastPos;
					const uint8_t payloadSize = payload.size();

					require(pos >= lastPos);

                                        if (trace)
                                                SLog("HIT ", pos, " => ", delta, "\n");

                                        ++blockFreqs[curBlockSize];

					if (TRACK_PAYLOADS)
                                        {
                                                if (payloadSize != curPayloadSize)
                                                {
                                                        hitsData.encode_varuint32((delta << 1) | 1);
                                                        hitsData.Serialize(payloadSize);
                                                        curPayloadSize = payloadSize;
                                                }
                                                else
                                                        hitsData.encode_varuint32(delta << 1);
                                                if (payloadSize)
                                                        hitsData.serialize(payload.offset, payloadSize);
                                        }
                                        else
                                                hitsData.encode_varuint32(delta);

                                        lastPos = pos;
                                }

                                inline void new_position(const uint32_t pos)
                                {
                                        new_hit(pos, {});
                                }

                                void end_document() override final
                                {
                                        static constexpr bool trace{false};

                                        if (trace)
                                                SLog("end document ", curDocID, " ", lastCommitedDocID, " ", curBlockSize, "\n");

                                        docDeltas[curBlockSize++] = curDocID - lastCommitedDocID;
                                        if (curBlockSize == N)
                                                commit_block();

                                        lastCommitedDocID = curDocID;
                                        ++termDocuments;
                                }

                                void end_term(term_index_ctx *tctx) override final
                                {
                                        static constexpr bool trace{false};
                                        auto out{&sess->indexOut};

                                        if (curBlockSize)
                                                commit_block();

                                        if (trace)
                                                SLog("ENDING term ", curTermOffset, "\n");

                                        tctx->indexChunk.Set(curTermOffset, out->size() - curTermOffset);
                                        tctx->documents = termDocuments;
                                }
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

                                Trinity::Codecs::Decoder *new_decoder(const term_index_ctx &tctx, Trinity::Codecs::AccessProxy *access) override final;
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
                                uint32_t documents[N];
                                const uint8_t *p, *chunkEnd;
                                uint8_t blockDocIdx;
                                uint32_t blockLastDocID{0};
                                uint32_t freqs[N];
                                uint32_t skipListIdx;
                                const uint8_t *base;
                                std::vector<std::pair<uint32_t, uint32_t>> skiplist;

                              private:
                                uint32_t skiplist_search(const uint32_t target) const noexcept;

                                void skip_block_doc();

                                void unpack_block(const uint32_t thisBlockLastDocID, const uint8_t n);

                                void seek_block(const uint32_t target);

                                void unpack_next_block();

                                void finalize()
                                {
                                        blockDocIdx = 0;
                                        blockLastDocID = UINT32_MAX; // magic value; signifies end of documents
                                        documents[0] = UINT32_MAX;
                                        p = chunkEnd;

					curDocument.id = UINT32_MAX;
                                }

                                // see skip_block_doc()
                                void skip_remaining_block_documents();

                              public:
                                uint32_t begin() override final;

                                Decoder &operator++()
                                {
                                        next();
                                        return *this;
                                }

                                [[gnu::always_inline]] auto operator*() const noexcept
                                {
                                        return documents[blockDocIdx];
                                }

                                // XXX: make sure you check if (cur_document() != UINT32_MAX)
                                bool next() override final;

                                bool seek(const uint32_t target) override final;

                                void materialize_hits(const exec_term_id_t termID, DocWordsSpace *dwspace, term_hit *out) override final;

                                void init(const term_index_ctx &tctx, Trinity::Codecs::AccessProxy *access) override final;
                        };
                }
        }
}
