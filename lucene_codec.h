#pragma once
#include "codecs.h"
#include <ext/FastPFor/headers/fastpfor.h>

static_assert(sizeof(Trinity::docid_t) <= sizeof(uint32_t), "This codec implementation does not support 64bit document IDs. You can duplicate this file and update it to support larget document identifiers, or use another codec");
namespace Trinity
{
	namespace Codecs
	{
		namespace Lucene
		{
// We can't use this encoding idea because we can't handle deltas >= 
// (MaxDocIDValue>>1), so that if e.g the first document ID for a term
// is (1<<31) + 15, this will fail. However, see IndexSource::translate_docid() for how that would work with docIDs translations
//#define LUCENE_ENCODE_FREQ1_DOCDELTA 1

// this doesn't seem to be helping much
// however, we are not using masked_vbyte_select_delta()/masked_vbyte_search_delta() which could been useful
//#define LUCENE_USE_MASKEDVBYTE 1
#ifdef LUCENE_USE_MASKEDVBYTE
			static constexpr size_t BLOCK_SIZE{64};
#else
			static constexpr size_t BLOCK_SIZE{128};
#endif
			static constexpr size_t SKIPLIST_STEP{1};	 // every (SKIPLIST_STEP * BLOCK_SIZE) documents

                        struct IndexSession final
                            : public Trinity::Codecs::IndexSession
                        {
				FastPForLib::FastPFor<4> forUtil;	 // handy for merge()

				// TODO: support for periodic flushing
				// i.e in either Encoder::end_term() or Encoder::end_document()
				IOBuffer positionsOut;
				uint32_t positionsOutFlushed;
				int positionsOutFd;
				uint32_t flushFreq;

				// private
				void flush_positions_data();




                                IndexSession(const char *bp)
                                    : Trinity::Codecs::IndexSession{bp}, positionsOutFlushed{0}, positionsOutFd{-1}, flushFreq{0}
                                {
                                }

				~IndexSession()
				{
					if (positionsOutFd != -1)
						close(positionsOutFd);
				}

				constexpr void set_flush_freq(const uint32_t f)
				{
					flushFreq = f;
				}
                                void begin() override final;

                                void end() override final;

                                Trinity::Codecs::Encoder *new_encoder() override final;

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
                                  struct skiplist_entry final
                                  {
                                          // offset to the index relative to the term base offset
                                          uint32_t indexOffset;
                                          docid_t lastDocID; // previous to the first document id in the block
					  // offset to the hits relative to the term base offset
                                          uint32_t lastHitsBlockOffset;
					  uint32_t totalDocumentsSoFar;
                                          uint32_t lastHitsBlockTotalHits;
					  uint16_t curHitsBlockHits;
                                  };

                                private:
                                  std::vector<skiplist_entry> skiplist;
                                  docid_t lastDocID;
                                  uint32_t docDeltas[BLOCK_SIZE], docFreqs[BLOCK_SIZE], hitPayloadSizes[BLOCK_SIZE], hitPosDeltas[BLOCK_SIZE];
                                  uint32_t buffered, totalHits, sumHits;
                                  uint32_t termDocuments;
                                  tokenpos_t lastPosition;
                                  uint32_t termIndexOffset, termPositionsOffset;
                                  FastPForLib::FastPFor<4> forUtil;
                                  IOBuffer payloadsBuf;
                                  uint32_t skiplistCountdown, lastHitsBlockOffset, lastHitsBlockTotalHits;
				  skiplist_entry cur_block;

                              private:
				void output_block();


                              public:
                                Encoder(Trinity::Codecs::IndexSession *s)
                                    : Trinity::Codecs::Encoder{s}
                                {
                                }

                                void begin_term() override final;

                                void begin_document(const docid_t documentID) override final;

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
                                  struct skiplist_entry final
                                  {
                                          uint32_t indexOffset;
                                          docid_t lastDocID;
                                          uint32_t lastHitsBlockOffset;
					  uint32_t totalDocumentsSoFar;
                                          uint32_t totalHitsSoFar;
					  uint16_t curHitsBlockHits;
                                  };

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
                                std::vector<skiplist_entry> skiplist;
                                uint32_t skipListIdx;
                                const uint8_t *postingListBase, *hitsBase;
				uint32_t totalDocuments, totalHits;

                              private:
			      	uint32_t skiplist_search(const docid_t) const noexcept;

                                bool next_impl();

                                void refill_hits();

                                void refill_documents();

                                inline void update_curdoc() noexcept
				{
					curDocument.id = lastDocID + docDeltas[docsIndex];
					curDocument.freq = docFreqs[docsIndex];
				}

                                inline void finalize() noexcept
                                {
                                        curDocument.id = MaxDocIDValue;
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
