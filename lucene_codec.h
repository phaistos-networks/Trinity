#pragma once
#include "codecs.h"
#include <ext/FastPFor/headers/fastpfor.h>

static_assert(sizeof(Trinity::isrc_docid_t) <= sizeof(uint32_t), "This codec implementation does not support 64bit document IDs. You can duplicate this file and update it to support larget document identifiers, or use another codec");

namespace Trinity
{
        namespace Codecs
        {
                namespace Lucene
                {

#define LUCENE_SKIPLIST_SEEK_EARLY 1
#define LUCENE_LAZY_SKIPLIST_INIT 1

// We can't use this encoding idea because we can't handle deltas >=
// (DocIDsEND>>1), so that if e.g the first document ID for a term
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
                        static constexpr size_t SKIPLIST_STEP{1}; // every (SKIPLIST_STEP * BLOCK_SIZE) documents

                        struct IndexSession final
                            : public Trinity::Codecs::IndexSession
                        {
                                FastPForLib::FastPFor<4> forUtil; // handy for merge()

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
                                        // previous to the first document id in the block
                                        // i.e last document ID in the previous block
                                        isrc_docid_t lastDocID;
                                        // offset to the hits relative to the term base offset
                                        uint32_t lastHitsBlockOffset;
                                        uint32_t totalDocumentsSoFar;
                                        uint32_t lastHitsBlockTotalHits;
                                        uint16_t curHitsBlockHits;
                                };

                              private:
                                std::vector<skiplist_entry> skiplist;
                                isrc_docid_t lastDocID;
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

                                void begin_document(const isrc_docid_t documentID) override final;

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

                        class Decoder;

                        struct PostingsListIterator final
                            : public Trinity::Codecs::PostingsListIterator
                        {
                                friend class Decoder;

                              protected:
                                const uint8_t *p;
                                const uint8_t *hdp;
                                const uint8_t *payloadsIt, *payloadsEnd;
                                isrc_docid_t lastDocID;
                                uint32_t lastPosition{0};
                                uint32_t docsLeft, hitsLeft;
                                uint16_t docsIndex, hitsIndex;
                                uint16_t bufferedDocs, bufferedHits;
                                uint32_t skippedHits;
                                uint32_t docDeltas[BLOCK_SIZE], docFreqs[BLOCK_SIZE], hitsPositionDeltas[BLOCK_SIZE], hitsPayloadLengths[BLOCK_SIZE];
                                uint32_t skipListIdx;
                                isrc_docid_t curSkipListLastDocID{DocIDsEND};

                              public:
                                inline isrc_docid_t next() override final;

                                inline isrc_docid_t advance(const isrc_docid_t) override final;

                                inline void materialize_hits(DocWordsSpace *dwspace, term_hit *out) override final;

                                PostingsListIterator(Decoder *const d)
                                    : Trinity::Codecs::PostingsListIterator{reinterpret_cast<Trinity::Codecs::Decoder *>(d)}
                                {
                                }
                        };

                        class Decoder final
                            : public Trinity::Codecs::Decoder
                        {
                                friend struct PostingsListIterator;

                              private:
                                // Pretty much the only shared state among iterators created by
                                // this decoder is the skiplist, which may be initialized once and owned by the Decoder.
                                struct skiplist_entry final
                                {
                                        uint32_t indexOffset;
                                        isrc_docid_t lastDocID;
                                        uint32_t lastHitsBlockOffset;
                                        uint32_t totalDocumentsSoFar;
                                        uint32_t totalHitsSoFar;
                                        uint16_t curHitsBlockHits;
                                };

                              protected:
                                void next(PostingsListIterator *);

                                void advance(PostingsListIterator *, const isrc_docid_t);

                                void materialize_hits(PostingsListIterator *, DocWordsSpace *, term_hit *);

                              private:
                                const uint8_t *chunkEnd;
#ifdef LUCENE_LAZY_SKIPLIST_INIT
                                uint16_t skiplistSize;
#endif
                                FastPForLib::FastPFor<4> forUtil;
                                struct skiplist_struct
                                {
                                        skiplist_entry *data;
                                        uint16_t size{0};

                                        ~skiplist_struct()
                                        {
                                                if (size)
                                                        std::free(data);
                                        }

                                } skiplist;
                                const uint8_t *postingListBase, *hitsBase;
                                uint32_t totalDocuments, totalHits;

                              private:
                                void init_skiplist(const uint16_t);

                                uint32_t skiplist_search(PostingsListIterator *, const isrc_docid_t) const noexcept;

                                void refill_hits(PostingsListIterator *);

                                void refill_documents(PostingsListIterator *);

                                [[gnu::always_inline]] void update_curdoc(PostingsListIterator *const __restrict__ it) noexcept
                                {
                                        const auto docsIndex{it->docsIndex};
                                        auto &curDocument{it->curDocument};

                                        curDocument.id = it->lastDocID + it->docDeltas[docsIndex];
                                        it->freq = it->docFreqs[docsIndex];
                                }

                                inline void finalize(PostingsListIterator *const it) noexcept
                                {
                                        it->curDocument.id = DocIDsEND;
                                }

                                void decode_next_block(PostingsListIterator *);

                                void skip_hits(PostingsListIterator *, const uint32_t);

                              public:
                                void init(const term_index_ctx &tctx, Trinity::Codecs::AccessProxy *access) override final;

                                Trinity::Codecs::PostingsListIterator *new_iterator() override final;
                        };

                        isrc_docid_t PostingsListIterator::next()
                        {
                                static_cast<Codecs::Lucene::Decoder *>(dec)->next(this);
                                return curDocument.id;
                        }

                        isrc_docid_t PostingsListIterator::advance(const isrc_docid_t target)
                        {
                                static_cast<Codecs::Lucene::Decoder *>(dec)->advance(this, target);
                                return curDocument.id;
                        }

                        void PostingsListIterator::materialize_hits(DocWordsSpace *dwspace, term_hit *out)
                        {
                                static_cast<Codecs::Lucene::Decoder *>(dec)->materialize_hits(this, dwspace, out);
                        }
                }
        }
}
