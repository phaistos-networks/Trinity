#pragma once
#include "codecs.h"
#include <ext/FastPFor/headers/fastpfor.h>

namespace Trinity
{
	namespace Codecs
	{
		namespace Lucene
		{
			static constexpr size_t BLOCK_SIZE{128};

                        struct IndexSession final
                            : public Trinity::Codecs::IndexSession
                        {
				FastPForLib::FastPFor<4> forUtil;	 // handy for merge()

				IOBuffer positionsOut;

                                void begin() override final;

                                void end() override final;

                                Trinity::Codecs::Encoder *new_encoder() override final;

                                IndexSession(const char *bp)
                                    : Trinity::Codecs::IndexSession{bp}
                                {
                                }

                                strwlen8_t codec_identifier() override final
                                {
                                        return "LUCENE"_s8;
                                }

                                range32_t append_index_chunk(const Trinity::Codecs::AccessProxy *, const term_index_ctx srcTCTX) override final;

                                void merge(merge_participant *, const uint16_t, Trinity::Codecs::Encoder *) override final;
                        };

                        class Encoder final
                            : public Trinity::Codecs::Encoder
                        {
                              private:
			      	docid_t lastDocID;
				uint32_t docDeltas[BLOCK_SIZE], docFreqs[BLOCK_SIZE], hitPayloadSizes[BLOCK_SIZE], hitPosDeltas[BLOCK_SIZE];
				uint32_t buffered, totalHits, sumHits;
				uint32_t termDocuments;
				uint16_t lastPosition;
				uint32_t termIndexOffset, termPositionsOffset;
				FastPForLib::FastPFor<4> forUtil;
				IOBuffer payloadsBuf;

                              private:
                                void commit_block();

                              public:
                                Encoder(Trinity::Codecs::IndexSession *s)
                                    : Trinity::Codecs::Encoder{s}
                                {
                                }

                                void begin_term() override final;

                                void begin_document(const docid_t documentID, const uint16_t hitsCnt) override final;

                                void new_hit(const uint32_t pos, const range_base<const uint8_t *, const uint8_t> payload) override final;
                                
                                inline void new_position(const uint32_t pos)
                                {
                                        new_hit(pos, {});
                                }

                                void end_document() override final;

                                void end_term(term_index_ctx *tctx) override final;
                        };

                        struct AccessProxy final
                            : public Trinity::Codecs::AccessProxy
                        {
				const uint8_t *hitsDataPtr;

                                AccessProxy(const char *bp, const uint8_t *p, const uint8_t *hd = nullptr);

                                strwlen8_t codec_identifier() override final
                                {
                                        return "LUCENE"_s8;
                                }

                                Trinity::Codecs::Decoder *new_decoder(const term_index_ctx &tctx) override final;
                        };


                        class Decoder final
                            : public Trinity::Codecs::Decoder
                        {
                              private:
                                const uint8_t *p;
                                const uint8_t *chunkEnd;
                                const uint8_t *hdp;
				uint32_t docDeltas[BLOCK_SIZE], docFreqs[BLOCK_SIZE], hitsPositionDeltas[BLOCK_SIZE], hitsPayloadLengths[BLOCK_SIZE];
				const uint8_t *payloadsIt, *payloadsEnd;
				docid_t lastDocID;
				uint32_t lastPosition;
				FastPForLib::FastPFor<4> forUtil;
				uint32_t docsLeft, hitsLeft;
				uint16_t docsIndex, hitsIndex;
				uint16_t bufferedDocs, bufferedHits;
				uint32_t skippedHits;

                              private:
                                bool next_impl();

                                void refill_hits();

                                void refill_documents();

                                inline void update_curdoc()
				{
					curDocument.id = lastDocID + docDeltas[docsIndex];
					curDocument.freq = docFreqs[docsIndex];
				}

                                void finalize()
                                {
                                        curDocument.id = UINT32_MAX;
                                }

				void decode_next_block();

				void skip_hits(const uint32_t);

                              public:
                                docid_t begin() override final;

                                inline bool next() override final
				{
					return next_impl();
				}

                                bool seek(const docid_t target) override final;

                                void materialize_hits(const exec_term_id_t termID, DocWordsSpace *dwspace, term_hit *out) override final;

                                void init(const term_index_ctx &tctx, Trinity::Codecs::AccessProxy *access) override final;
                        };
                }
	}
}
