#include "lucene_codec.h"
#include "utils.h"
#include <ansifmt.h>
#include <switch_bitops.h>
#ifdef LUCENE_USE_MASKEDVBYTE
#include <ext/MaskedVByte/include/varintdecode.h>
#include <ext/MaskedVByte/include/varintencode.h>
#endif

static constexpr bool trace{false};

static bool all_equal(const uint32_t *const values, const size_t n) noexcept
{
        const auto v = values[0];

        for (uint32_t i{1}; i < n; ++i)
        {
                if (values[i] != v)
                        return false;
        }
        return true;
}

static void pfor_encode(FastPForLib::FastPFor<4> &forUtil, const uint32_t *values, const size_t n, IOBuffer &out)
{
        if (all_equal(values, n))
        {
                if (trace)
                        SLog("ENCODING all equal ", values[0], "\n");
                out.pack(uint8_t(0));
                out.encode_varbyte32(values[0]);
                return;
        }

#ifdef LUCENE_USE_MASKEDVBYTE
        out.reserve(n * 8);
        out.pack(uint8_t(1));

        const auto len = vbyte_encode(const_cast<uint32_t *>(values), n, (uint8_t *)out.end());

        out.advance_size(len);
#else
        const auto offset = out.size();

        out.RoomFor(sizeof(uint8_t));
        out.reserve((n + n) * sizeof(uint32_t));
        auto l = out.capacity() / sizeof(uint32_t);
        forUtil.encodeArray(values, n, (uint32_t *)out.end(), l);
        out.advance_size(l * sizeof(uint32_t));
        *(out.data() + offset) = l; // this is great, means we can skip ahead n * sizeof(uint32_t) bytes to get to the next block
#endif
}

static const uint8_t *pfor_decode(FastPForLib::FastPFor<4> &forUtil, const uint8_t *p, uint32_t *values)
{
        if (const auto blockSize = *p++; blockSize == 0)
        {
                // all equal values
                uint32_t value;

                varbyte_get32(p, value);

                if (trace)
                        SLog("All equal values of ", value, "\n");

// the compiler is likely clever enough to unroll or whatever it is that it needs to do
// UPDATE: well, it's not
// with -Ofast -msse4  -ftree-vectorize -Wno-sign-compare
// this is compiled down to
/*
		*
			.LBB0_1:
			movl    %eax, (%rsp,%rbx,4)
			movl    %eax, 4(%rsp,%rbx,4)
			movl    %eax, 8(%rsp,%rbx,4)
			movl    %eax, 12(%rsp,%rbx,4)
			movl    %eax, 16(%rsp,%rbx,4)
			movl    %eax, 20(%rsp,%rbx,4)
			movl    %eax, 24(%rsp,%rbx,4)
			movl    %eax, 28(%rsp,%rbx,4)
			addq    $8, %rbx
			cmpq    $128, %rbx
			jne     .LBB0_1

		*
		*/
#if 1
                // this is still faster than the optimised alternative below, for no good reason
                for (uint32_t i{0}; i != Trinity::Codecs::Lucene::BLOCK_SIZE; ++i)
                        values[i] = value;
#else
                // this loop is compiled to
                // it is unrolled as expected but its still not optimal
                /*
		 *

			movq    8(%rsp), %rcx
			movq    %rax, (%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 8(%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 16(%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 24(%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 32(%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 40(%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 48(%rcx,%rbx)
			movq    8(%rsp), %rcx
			movq    %rax, 56(%rcx,%rbx)
			addq    $64, %rbx
			cmpq    $512, %rbx              # imm = 0x200
			jne     .LBB0_1
		*
		*/

                const uint32_t pair[] = {value, value};
                const uint64_t u64 = *(uint64_t *)pair;
                auto *const out = (uint64_t *)values;

                for (uint32_t i{0}; i != Trinity::Codecs::Lucene::BLOCK_SIZE / 2; ++i)
                        out[i] = u64;
#endif
        }
        else
        {
#if defined(LUCENE_USE_MASKEDVBYTE)
                p += masked_vbyte_decode(p, values, Trinity::Codecs::Lucene::BLOCK_SIZE);
#else
                size_t n{Trinity::Codecs::Lucene::BLOCK_SIZE};
                const auto *ptr = reinterpret_cast<const uint32_t *>(p);

                ptr = forUtil.decodeArray(ptr, blockSize, values, n);
                p = reinterpret_cast<const uint8_t *>(ptr);
#endif
        }

        return p;
}

void Trinity::Codecs::Lucene::IndexSession::begin()
{
        // We will need two extra/additional buffers, one for documents, another for the hits
}

void Trinity::Codecs::Lucene::IndexSession::flush_positions_data()
{
        if (positionsOutFd == -1)
        {
                positionsOutFd = open(Buffer{}.append(basePath, "/hits.data.t").c_str(), O_WRONLY | O_LARGEFILE | O_CREAT, 0775);

                if (positionsOutFd == -1)
                        throw Switch::data_error("Failed to persist hits.data");
        }

        if (Utilities::to_file(positionsOut.data(), positionsOut.size(), positionsOutFd) == -1)
                throw Switch::data_error("Failed to persist hits.data");

        positionsOutFlushed += positionsOut.size();
        positionsOut.clear();
}

void Trinity::Codecs::Lucene::IndexSession::end()
{
        if (positionsOut.size())
                flush_positions_data();

        if (positionsOutFd != -1)
        {
                if (close(positionsOutFd) == -1)
                        throw Switch::data_error("Failed to persist hits.data");

                positionsOutFd = -1;

                if (rename(Buffer{}.append(basePath, "/hits.data.t").c_str(), Buffer{}.append(basePath, "/hits.data").c_str()) == -1)
                {
                        unlink(Buffer{}.append(basePath, "/hits.data.t").c_str());
                        throw Switch::data_error("Failed to persist hits.data");
                }
        }
}

range32_t Trinity::Codecs::Lucene::IndexSession::append_index_chunk(const Trinity::Codecs::AccessProxy *src_, const term_index_ctx srcTCTX)
{
        const auto src = static_cast<const Trinity::Codecs::Lucene::AccessProxy *>(src_);
        const auto o = indexOut.size() + indexOutFlushed;

        require(srcTCTX.indexChunk.size());

        auto *p = src->indexPtr + srcTCTX.indexChunk.offset, *const end = p + srcTCTX.indexChunk.size();
        const auto hitsDataOffset = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const auto sumHits = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const auto positionsChunkSize = *(uint32_t *)p;
        p += sizeof(uint32_t);
        [[maybe_unused]] const auto skiplistSize = *(uint16_t *)p;
        p += sizeof(uint16_t);
        const auto newHitsDataOffset = positionsOut.size() + positionsOutFlushed;

        positionsOut.serialize(src->hitsDataPtr + hitsDataOffset, positionsChunkSize);
        indexOut.pack(uint32_t(newHitsDataOffset), sumHits, positionsChunkSize, skiplistSize);
        indexOut.serialize(p, end - p);

        return {uint32_t(o), srcTCTX.indexChunk.size()};
}


void Trinity::Codecs::Lucene::IndexSession::merge(merge_participant *const __restrict__ participants, const uint16_t participantsCnt, Trinity::Codecs::Encoder *const __restrict__ enc_)
{
        // This is somewhat complicated
        static constexpr bool trace{false};

        struct candidate
        {
                docid_t lastDocID;
                docid_t docDeltas[BLOCK_SIZE];
                uint32_t docFreqs[BLOCK_SIZE];
                uint32_t hitsPayloadLengths[BLOCK_SIZE];
                uint32_t hitsPositionDeltas[BLOCK_SIZE];
                masked_documents_registry *maskedDocsReg;

                uint32_t documentsLeft;
                uint32_t hitsLeft;
                uint32_t skippedHits;
                uint16_t bufferedHits;
                uint16_t hitsIndex;

                struct
                {
                        const uint8_t *p;
                        const uint8_t *e;
                } index_chunk;

                struct
                {
                        const uint8_t *p;
                        const uint8_t *e;
                } positions_chunk;

                struct
                {
                        uint8_t i;
                        uint8_t size;
                } cur_block;

                const uint8_t *payloadsIt, *payloadsEnd;

                void refill_hits(FastPForLib::FastPFor<4> &forUtil)
                {
                        uint32_t payloadsChunkLength;
                        auto hdp = positions_chunk.p;

                        if (trace)
                                SLog(ansifmt::bold, "REFILLING NOW, hitsLeft = ", hitsLeft, ", hitsIndex = ", hitsIndex, ", bufferedHits = ", bufferedHits, ansifmt::reset, "\n");

                        require(hitsIndex == 0 || hitsIndex == BLOCK_SIZE);

                        if (hitsLeft >= BLOCK_SIZE)
                        {
                                hdp = pfor_decode(forUtil, hdp, hitsPositionDeltas);
                                hdp = pfor_decode(forUtil, hdp, hitsPayloadLengths);

                                varbyte_get32(hdp, payloadsChunkLength);

                                payloadsIt = hdp;
                                hdp += payloadsChunkLength;
                                payloadsEnd = hdp;

                                bufferedHits = BLOCK_SIZE;
                                hitsLeft -= BLOCK_SIZE;
                        }
                        else
                        {
                                uint32_t v;
                                uint8_t payloadLen{0};

                                payloadsChunkLength = 0;
                                for (uint32_t i{0}; i != hitsLeft; ++i)
                                {
                                        varbyte_get32(hdp, v);

                                        if (v & 1)
                                                payloadLen = *hdp++;

                                        hitsPositionDeltas[i] = v >> 1;
                                        hitsPayloadLengths[i] = payloadLen;
                                        payloadsChunkLength += payloadLen;
                                }

                                payloadsIt = hdp;
                                hdp += payloadsChunkLength;
                                payloadsEnd = hdp;

                                bufferedHits = hitsLeft;
                                hitsLeft = 0;
                        }

                        positions_chunk.p = hdp;
                        hitsIndex = 0;
                }

                void refill_documents(FastPForLib::FastPFor<4> &forUtil)
                {

                        if (trace)
                                SLog("Refilling documents ", documentsLeft, "\n");

                        if (documentsLeft >= BLOCK_SIZE)
                        {
                                index_chunk.p = pfor_decode(forUtil, index_chunk.p, docDeltas);
                                index_chunk.p = pfor_decode(forUtil, index_chunk.p, docFreqs);

                                cur_block.size = BLOCK_SIZE;
                                documentsLeft -= BLOCK_SIZE;
                        }
                        else
                        {
                                uint32_t v;
                                auto p = index_chunk.p;

                                for (uint32_t i{0}; i != documentsLeft; ++i)
                                {
                                        varbyte_get32(p, v);

#if defined(LUCENE_ENCODE_FREQ1_DOCDELTA)
                                        docDeltas[i] = v >> 1;
                                        if (v & 1)
                                                docFreqs[i] = 1;
                                        else
                                        {
                                                varbyte_get32(p, v);
                                                docFreqs[i] = v;
                                        }
#else
                                        docDeltas[i] = v;
                                        varbyte_get32(p, v);
                                        docFreqs[i] = v;
#endif
                                }
                                index_chunk.p = p;

                                cur_block.size = documentsLeft;
                                documentsLeft = 0;
                        }
                        cur_block.i = 0;

                        if (trace)
                                SLog(cur_block.i, " ", cur_block.size, "\n");
                }

                void skip_ommitted_hits(FastPForLib::FastPFor<4> &forUtil)
                {

                        if (trace)
                                SLog("Skipping omitted hits ", skippedHits, ", bufferedHits = ", bufferedHits, "\n");

                        if (!skippedHits)
			{
				// There was a silly fast-path optimization here
				//
				// if (!skipppedHits) return;
				// else if (bufferedHits == skippedHits){ skippedHits = 0; bufferedHits = 0; hitsIndex = 0; payloadsIt = payloadsEnd; return;}
				// else { ... }
				//
				// which was causing all kinds of random problems(missing positions/payloads etc)
				// I should be able to figure out exactly what's wrong here, but just dropping this fixed everything and it's also simpler to reason about the state anyway. Better off without it
                                return;
			}
                        else
                        {
#if 0
				// This is somewhat faster, but let's go withthe somewhat slower impl. because
				// it matches Lucene::Decoder::skip_hits() and it's simpler to understand
                                if (trace)
                                        SLog("Slow path (bufferedHits = ", bufferedHits, ", hitsIndex = ", hitsIndex, ", rem = ", bufferedHits - hitsIndex, ") \n");

                                require(hitsIndex <= bufferedHits);

                                if (const auto rem = bufferedHits - hitsIndex; skippedHits >= rem)
                                {
					// to end of the block
                                        skippedHits -= rem;
                                        bufferedHits = 0;
                                        hitsIndex = 0;

					// whichever other we can skip
                                        const auto n = skippedHits / BLOCK_SIZE;

                                        for (uint32_t i{0}; i != n; ++i)
                                                refill_hits(forUtil);

                                        skippedHits -= n * BLOCK_SIZE;
                                        payloadsIt = payloadsEnd;
                                }

                                if (skippedHits)
                                {
                                        uint32_t sum{0};

                                        if (!bufferedHits)
                                                refill_hits(forUtil);

                                        require(hitsIndex + skippedHits <= bufferedHits);

                                        for (uint32_t i{0}; i != skippedHits; ++i)
                                                sum += hitsPayloadLengths[hitsIndex++];

                                        skippedHits = 0;
                                        payloadsIt += sum;
                                }
#else

				do
				{
					if (hitsIndex == bufferedHits)
						refill_hits(forUtil);

					const auto step = std::min<uint32_t>(skippedHits, bufferedHits - hitsIndex);

					for (uint32_t i{0}; i != step; ++i)
					{
						const auto pl = hitsPayloadLengths[hitsIndex++];

						payloadsIt += pl;
					}

					skippedHits -= step;
				} while (skippedHits);
#endif
                        }
                }

                void output_hits(FastPForLib::FastPFor<4> &forUtil, Trinity::Codecs::Lucene::Encoder *__restrict__ enc)
                {
                        auto freq = docFreqs[cur_block.i];
                        uint64_t payload;
                        tokenpos_t pos{0};

                        if (trace)
                                SLog("Will output hits for ", cur_block.i, " ", freq, ", skippedHits = ", skippedHits, "\n");

                        skip_ommitted_hits(forUtil);

                        if (const auto upto = hitsIndex + freq; upto <= bufferedHits)
                        {
                                if (trace)
                                        SLog("fast-path hitsIndex = ", hitsIndex, ", upto = ", upto, "\n");

                                while (hitsIndex != upto)
                                {
                                        pos += hitsPositionDeltas[hitsIndex];

                                        const auto pl = hitsPayloadLengths[hitsIndex];

                                        if (pl)
                                        {
                                                memcpy(&payload, payloadsIt, pl);
                                                payloadsIt += pl;
                                        }
                                        else
                                                payload = 0;

                                        enc->new_hit(pos, {(uint8_t *)&payload, uint8_t(pl)});

                                        ++hitsIndex;
                                }
                        }
                        else
                        {
                                if (trace)
                                        SLog("SLOW path\n");

                                for (;;)
                                {
                                        const auto n = std::min<uint32_t>(bufferedHits - hitsIndex, freq);
                                        const auto upto = hitsIndex + n;

                                        if (trace)
                                                SLog("upto = ", upto, ", bufferedHits = ", bufferedHits, ", hitsIndex = ", hitsIndex, "\n");

                                        while (hitsIndex != upto)
                                        {
                                                pos += hitsPositionDeltas[hitsIndex];

                                                const auto pl = hitsPayloadLengths[hitsIndex];

                                                if (pl)
                                                {
                                                        memcpy(&payload, payloadsIt, pl);
                                                        payloadsIt += pl;
                                                }
                                                else
                                                        payload = 0;

                                                enc->new_hit(pos, {(uint8_t *)&payload, uint8_t(pl)});

                                                ++hitsIndex;
                                        }

                                        freq -= n;

                                        if (freq)
                                        {
                                                if (trace)
                                                        SLog("Will refill hits (Freq now = ", freq, ")\n");

                                                refill_hits(forUtil);
                                        }
                                        else
                                                break;
                                }
                        }

                        docFreqs[cur_block.i] = 0; // simplifies processing logic (See next().)
                }

                bool next(FastPForLib::FastPFor<4> &forUtil)
                {
                        skippedHits += docFreqs[cur_block.i];
                        lastDocID += docDeltas[cur_block.i++];

                        if (cur_block.i == cur_block.size)
                        {
                                if (trace)
                                        SLog("End of block, documentsLeft = ", documentsLeft, "\n");

                                if (!documentsLeft)
                                        return false;

				// this is important, because refill_documents()
				// will update cur_block
                                skip_ommitted_hits(forUtil);

                                refill_documents(forUtil);
                        }
                        else
                        {
                                if (trace)
                                        SLog("NOW at ", cur_block.i, "\n");
                        }

                        return true;
                }

                constexpr auto current() noexcept
                {
                        return lastDocID + docDeltas[cur_block.i];
                }

                constexpr auto current_freq() noexcept
                {
                        return docFreqs[cur_block.i];
                }
        };

        candidate candidates[participantsCnt];
        uint16_t rem{participantsCnt};
        uint16_t toAdvance[participantsCnt];
        auto encoder = static_cast<Trinity::Codecs::Lucene::Encoder *>(enc_);

        for (uint32_t i{0}; i != participantsCnt; ++i)
        {
                auto c = candidates + i;
                const auto ap = static_cast<const Trinity::Codecs::Lucene::AccessProxy *>(participants[i].ap);
                const auto *p = ap->indexPtr + participants[i].tctx.indexChunk.offset;

                c->index_chunk.e = p + participants[i].tctx.indexChunk.size();
                c->maskedDocsReg = participants[i].maskedDocsReg;
                c->documentsLeft = participants[i].tctx.documents;
                c->lastDocID = 0;
                c->skippedHits = 0;
                c->hitsIndex = 0;
                c->bufferedHits = 0;
                c->payloadsIt = c->payloadsEnd = nullptr;

                const auto hitsDataOffset = *(uint32_t *)p;
                p += sizeof(uint32_t);
                const auto sumHits = *(uint32_t *)p;
                p += sizeof(uint32_t);
                const auto posChunkSize = *(uint32_t *)p;
                p += sizeof(uint32_t);
                [[maybe_unused]] const auto skiplistSize = *(uint16_t *)p;
                p += sizeof(uint16_t);

                c->index_chunk.p = p;
                c->positions_chunk.p = ap->hitsDataPtr + hitsDataOffset;
                c->positions_chunk.e = c->positions_chunk.p + posChunkSize;
                c->hitsLeft = sumHits;

                if (trace)
                        SLog("participant ", i, " ", c->documentsLeft, " ", c->hitsLeft, ", skiplistSize = ", skiplistSize, "\n");

                // Skip past skiplist
                if (skiplistSize)
                {
                        static constexpr size_t skiplistEntrySize{sizeof(uint32_t) * 5 + sizeof(uint16_t)};

                        c->index_chunk.e -= skiplistSize * skiplistEntrySize;
                }

                c->refill_documents(forUtil);
        }

        for (docid_t prev{0};;)
        {
                uint16_t toAdvanceCnt{1};
                auto did = candidates[0].current();

                toAdvance[0] = 0;
                for (uint32_t i{1}; i != rem; ++i)
                {
                        if (const auto id = candidates[i].current(); id == did)
                                toAdvance[toAdvanceCnt++] = i;
                        else if (id < did)
                        {
                                did = id;
                                toAdvanceCnt = 1;
                                toAdvance[0] = i;
                        }
                }

                require(did > prev);
                prev = did;

                const auto c = candidates + toAdvance[0]; // always choose the first because they are sorted in-order

                if (!c->maskedDocsReg->test(did))
                {
                        [[maybe_unused]] const auto freq = c->current_freq();

                        encoder->begin_document(did);
                        c->output_hits(forUtil, encoder);
                        encoder->end_document();
                }

                do
                {
                        const auto idx = toAdvance[--toAdvanceCnt];
                        auto c = candidates + idx;

                        if (!c->next(forUtil))
                        {
                                if (!--rem)
                                        goto l1;

                                memmove(candidates + idx, candidates + idx + 1, (rem - idx) * sizeof(candidates[0]));
                        }

                } while (toAdvanceCnt);
        }

l1:;
}

void Trinity::Codecs::Lucene::Encoder::begin_term()
{
        const auto s = static_cast<Trinity::Codecs::Lucene::IndexSession *>(sess);

        lastDocID = 0;
        totalHits = 0;
        sumHits = 0;
        buffered = 0;
        termDocuments = 0;
        termIndexOffset = sess->indexOut.size() + sess->indexOutFlushed;
        termPositionsOffset = s->positionsOut.size() + s->positionsOutFlushed;
        lastHitsBlockOffset = 0;
        lastHitsBlockTotalHits = 0;
        skiplistCountdown = SKIPLIST_STEP;
        skiplist.clear();

        sess->indexOut.pack(uint32_t(termPositionsOffset), uint32_t(0), uint32_t(0), uint16_t(0)); // will fill in later. Will also track positions chunk size for efficient merge
}

void Trinity::Codecs::Lucene::Encoder::output_block()
{
        if (trace)
                SLog("<< BLOCK\n");

        require(buffered == BLOCK_SIZE);

        if (--skiplistCountdown == 0)
        {
                if (likely(skiplist.size() < UINT16_MAX))
                {
                        // keep it sane
                        skiplist.push_back(cur_block);
                }
                skiplistCountdown = SKIPLIST_STEP;
        }

        auto indexOut = &sess->indexOut;

        pfor_encode(forUtil, docDeltas, buffered, *indexOut);
        pfor_encode(forUtil, docFreqs, buffered, *indexOut);
        buffered = 0;

        if (trace)
                SLog("Encoded now ", indexOut->size() + sess->indexOutFlushed, "\n");
}

void Trinity::Codecs::Lucene::Encoder::begin_document(const uint32_t documentID)
{
        require(documentID > lastDocID);

        if (trace)
                SLog("INDEXING document ", documentID, "\n");

        if (unlikely(buffered == BLOCK_SIZE))
                output_block();

        if (!buffered)
        {
                cur_block.indexOffset = (sess->indexOut.size() + sess->indexOutFlushed) - termIndexOffset;
                cur_block.lastDocID = lastDocID; // last block's last document ID
                cur_block.totalDocumentsSoFar = termDocuments;

                // Last positions/hits block when this new block is captured
                cur_block.lastHitsBlockOffset = lastHitsBlockOffset;
                cur_block.lastHitsBlockTotalHits = lastHitsBlockTotalHits;

                // total hits of the current position/hits block
                cur_block.curHitsBlockHits = totalHits;
        }

        docDeltas[buffered] = documentID - lastDocID;
        docFreqs[buffered] = 0;
        ++termDocuments;

        lastDocID = documentID;
        lastPosition = 0;
}

void Trinity::Codecs::Lucene::Encoder::new_hit(const uint32_t pos, const range_base<const uint8_t *, const uint8_t> payload)
{
	static constexpr bool trace{false};
	//const bool trace = lastDocID== 2151228176 || lastDocID== 2152925656 || lastDocID==  2154895013;

	if (trace)
		SLog("New hit (", pos, ", ", payload, ")\n");

        if (!pos && !payload)
        {
                // This is perfectly fine
                return;
        }

        require(pos >= lastPosition);

        const auto delta = pos - lastPosition;

        ++docFreqs[buffered];
        hitPosDeltas[totalHits] = delta;
        hitPayloadSizes[totalHits] = payload.size();
        lastPosition = pos;

        if (const auto size = payload.size())
        {
                require(size <= sizeof(uint64_t));
                payloadsBuf.serialize(payload.offset, size);
        }

        ++totalHits;
        if (unlikely(totalHits == BLOCK_SIZE))
        {
                auto s = static_cast<Trinity::Codecs::Lucene::IndexSession *>(sess);
                auto positionsOut = &s->positionsOut;

                sumHits += totalHits;

                pfor_encode(forUtil, hitPosDeltas, totalHits, *positionsOut);
                pfor_encode(forUtil, hitPayloadSizes, totalHits, *positionsOut);

                {
                        size_t s{0};

                        for (uint32_t i{0}; i != totalHits; ++i)
                                s += hitPayloadSizes[i];

                        require(s == payloadsBuf.size());
                }

                if (trace)
                        SLog("<< pyaloads length:", payloadsBuf.size(), "\n");

                positionsOut->encode_varbyte32(payloadsBuf.size());
                positionsOut->serialize(payloadsBuf.data(), payloadsBuf.size());
                payloadsBuf.clear();

                lastHitsBlockTotalHits = sumHits;
                lastHitsBlockOffset = (positionsOut->size() + s->positionsOutFlushed) - termPositionsOffset;

                totalHits = 0;
        }
}

void Trinity::Codecs::Lucene::Encoder::end_document()
{
        ++buffered;
}

void Trinity::Codecs::Lucene::Encoder::end_term(term_index_ctx *out)
{
        auto indexOut = &sess->indexOut;
        auto s = static_cast<Trinity::Codecs::Lucene::IndexSession *>(sess);

        sumHits += totalHits;

        if (trace)
                SLog("Remaining ", buffered, " (sumHits = ", sumHits, ")\n");

        if (buffered == BLOCK_SIZE)
                output_block();
        else
        {
                for (uint32_t i{0}; i != buffered; ++i)
                {
                        const auto delta = docDeltas[i];
                        const auto freq = docFreqs[i];

#if defined(LUCENE_ENCODE_FREQ1_DOCDELTA)
                        if (freq == 1)
                                indexOut->encode_varbyte32((delta << 1) | 1);
                        else
                        {
                                indexOut->encode_varbyte32(delta << 1);
                                indexOut->encode_varbyte32(freq);
                        }
#else
                        indexOut->encode_varbyte32(delta);
                        indexOut->encode_varbyte32(freq);
#endif
                }
        }

        *(uint32_t *)(sess->indexOut.data() + (termIndexOffset - sess->indexOutFlushed) + sizeof(uint32_t)) = sumHits;

        if (totalHits)
        {
                uint8_t lastPayloadLen{0x0};
                auto positionsOut = &s->positionsOut;
                size_t sum{0};

                for (uint32_t i{0}; i != totalHits; ++i)
                {
                        const auto posDelta = hitPosDeltas[i];
                        const auto payloadLen = hitPayloadSizes[i];

                        if (payloadLen != lastPayloadLen)
                        {
                                lastPayloadLen = payloadLen;
                                positionsOut->encode_varbyte32((posDelta << 1) | 1);
                                positionsOut->pack(uint8_t(payloadLen));
                        }
                        else
                                positionsOut->encode_varbyte32(posDelta << 1);

                        sum += payloadLen;
                }

                // we don't need to encode as varbyte the payloadsBuf.size() because
                // we can just sum those individual hit payload lengths
                require(sum == payloadsBuf.size());
                positionsOut->serialize(payloadsBuf.data(), payloadsBuf.size());
                payloadsBuf.clear();
        }

        const uint16_t skiplistSize = skiplist.size();

        *(uint32_t *)(sess->indexOut.data() + (termIndexOffset - sess->indexOutFlushed) + sizeof(uint32_t) + sizeof(uint32_t)) = (s->positionsOut.size() + s->positionsOutFlushed) - termPositionsOffset;
        *(uint16_t *)(sess->indexOut.data() + (termIndexOffset - sess->indexOutFlushed) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t)) = skiplistSize;

        if (skiplistSize)
        {
                // serialize skiplist here
                auto *const __restrict__ b = &sess->indexOut;

                for (const auto &it : skiplist)
                        b->pack(it.indexOffset, it.lastDocID, it.lastHitsBlockOffset, it.totalDocumentsSoFar, it.lastHitsBlockTotalHits, it.curHitsBlockHits);

                skiplist.clear();
        }

        out->documents = termDocuments;
        out->indexChunk.Set(termIndexOffset, uint32_t((sess->indexOut.size() + sess->indexOutFlushed) - termIndexOffset));

        if (const auto f = s->flushFreq; f && s->positionsOut.size() > f)
                s->flush_positions_data();
}

Trinity::Codecs::Encoder *Trinity::Codecs::Lucene::IndexSession::new_encoder()
{
        return new Trinity::Codecs::Lucene::Encoder(this);
}

Trinity::Codecs::Decoder *Trinity::Codecs::Lucene::AccessProxy::new_decoder(const term_index_ctx &tctx)
{
        auto d = std::make_unique<Trinity::Codecs::Lucene::Decoder>();

        d->init(tctx, this);
        return d.release();
}

void Trinity::Codecs::Lucene::Decoder::refill_hits()
{
        uint32_t payloadsChunkLength;

        if (trace)
                SLog(ansifmt::color_green, "Refilling hits, hitsLeft = ", hitsLeft, ansifmt::reset, "\n");

        if (hitsLeft >= BLOCK_SIZE)
        {
                hdp = pfor_decode(forUtil, hdp, hitsPositionDeltas);
                hdp = pfor_decode(forUtil, hdp, hitsPayloadLengths);

                varbyte_get32(hdp, payloadsChunkLength);
                payloadsIt = hdp;
                hdp += payloadsChunkLength;
                payloadsEnd = hdp;

                bufferedHits = BLOCK_SIZE;
                hitsLeft -= BLOCK_SIZE;
        }
        else
        {
                uint32_t v;
                uint8_t payloadLen{0};

                payloadsChunkLength = 0;
                for (uint32_t i{0}; i != hitsLeft; ++i)
                {
                        varbyte_get32(hdp, v);

                        if (v & 1)
                        {
                                payloadLen = *hdp++;
                        }

                        if (trace)
                                SLog("GOT ", v >> 1, " ", payloadLen, "\n");

                        hitsPositionDeltas[i] = v >> 1;
                        hitsPayloadLengths[i] = payloadLen;
                        payloadsChunkLength += payloadLen;
                }
                payloadsIt = hdp;
                hdp += payloadsChunkLength;
                payloadsEnd = hdp;
                bufferedHits = hitsLeft;
                hitsLeft = 0;
        }
        hitsIndex = 0;

        if (trace)
                SLog("bufferedHits now = ", bufferedHits, ", hitsIndex  = ", hitsIndex, "\n");
}

void Trinity::Codecs::Lucene::Decoder::skip_hits(const uint32_t n)
{
	static constexpr bool trace{false}; 
	
        if (trace)
        {
                if (!n)
                        return;

                SLog("SKIPPING ", n, " hits, hitsIndex = ", hitsIndex, ", bufferedHits = ", bufferedHits, " (for ", curDocument.id, ")\n");
	}

        if (n)
        {
                auto rem{n};

                if (trace)
                        SLog(ansifmt::bold, "NOW skippedHits = ", skippedHits, ", bufferedHits = ", bufferedHits, ", n = ", n, "\n");

                do
                {
                        if (hitsIndex == bufferedHits)
                        {
                                if (trace)
                                        SLog("Need to refill\n");

                                refill_hits();

                                if (trace)
                                        SLog("DID refill hitsIndex = ", hitsIndex, "\n");
                        }

                        const auto step = std::min<uint32_t>(rem, bufferedHits - hitsIndex);

                        if (trace)
                                SLog("skippedHits = ", skippedHits, ", hitsIndex = ", hitsIndex, ", step = ", step, ", bufferedHits  = ", bufferedHits, "\n");

#if defined(TRINITY_ENABLE_PREFETCH)
                        {
                                const size_t prefetchIterations = step / 16; // 64/4
                                const auto end = hitsIndex + step;

                                for (uint32_t i{0}; i != prefetchIterations; ++i)
                                {
                                        _mm_prefetch(hitsPayloadLengths + hitsIndex, _MM_HINT_NTA);

                                        for (const auto upto = hitsIndex + 16; hitsIndex != upto; ++hitsIndex)
                                                payloadsIt += hitsPayloadLengths[hitsIndex];
                                }

                                while (hitsIndex != end)
                                        payloadsIt += hitsPayloadLengths[hitsIndex++];
                        }
#else
                        for (uint32_t i{0}; i != step; ++i)
                        {
                                const auto pl = hitsPayloadLengths[hitsIndex++];

                                payloadsIt += pl;
                        }
#endif

                        skippedHits -= step;
                        rem -= step;

                        if (trace)
                                SLog("hitsIndex now = ", hitsIndex, ", bufferedHits = ", bufferedHits, ", rem ", rem, "\n");

                } while (rem);
        }
}

void Trinity::Codecs::Lucene::Decoder::refill_documents()
{
        if (trace)
                SLog("Refilling documents docsLeft = ", docsLeft, "\n");

        if (docsLeft >= BLOCK_SIZE)
        {
                if (trace)
                        SLog(ansifmt::bold, ansifmt::color_brown, "REFILL ", docsLeft, ansifmt::reset, "\n");

                p = pfor_decode(forUtil, p, docDeltas);
                p = pfor_decode(forUtil, p, docFreqs);
                bufferedDocs = BLOCK_SIZE;
                docsLeft -= BLOCK_SIZE;
        }
        else
        {
                uint32_t v;

                for (uint32_t i{0}; i != docsLeft; ++i)
                {
                        varbyte_get32(p, v);

#if defined(LUCENE_ENCODE_FREQ1_DOCDELTA)
                        docDeltas[i] = v >> 1;
                        if (v & 1)
                                docFreqs[i] = 1;
                        else
                        {
                                varbyte_get32(p, v);
                                docFreqs[i] = v;
                        }
#else
                        docDeltas[i] = v;
                        varbyte_get32(p, v);
                        docFreqs[i] = v;
#endif

                        if (trace)
                                SLog("deltas ", docDeltas[i], " ", docFreqs[i], "\n");
                }
                bufferedDocs = docsLeft;
                docsLeft = 0;
        }
        docsIndex = 0;
        update_curdoc();
}

void Trinity::Codecs::Lucene::Decoder::decode_next_block()
{
        // this is important
        skip_hits(skippedHits);
        refill_documents();
}

uint32_t Trinity::Codecs::Lucene::Decoder::begin()
{
        if (p != chunkEnd)
        {
                decode_next_block();
                update_curdoc();
        }
        else
        {
                finalize();
        }

        return curDocument.id;
}

bool Trinity::Codecs::Lucene::Decoder::next_impl()
{
        // see skip_hits() impl.
        if (trace)
                SLog(ansifmt::color_brown, "NEXT: docsIndex = ", docsIndex, " / ", bufferedDocs, " (skippedHits increment by ", docFreqs[docsIndex], ") ", skippedHits, ansifmt::reset, "\n");

        skippedHits += docFreqs[docsIndex];
        lastDocID += docDeltas[docsIndex];

        if (unlikely(++docsIndex == bufferedDocs))
        {
                if (trace)
                        SLog("End of buffered documents, ", chunkEnd - p, "\n");

                if (p != chunkEnd)
                        decode_next_block();
                else
                {
                        if (trace)
                                SLog(ansifmt::bold, ansifmt::color_cyan, "FINALIZING", ansifmt::reset, "\n");

                        finalize();
                        return false;
                }
        }

        update_curdoc();
        return true;
}

uint32_t Trinity::Codecs::Lucene::Decoder::skiplist_search(const docid_t target) const noexcept
{
        // See Google::Decoder::skiplist_search()
        static constexpr bool trace{false};
        uint32_t idx{UINT32_MAX};

        for (int32_t top{int32_t(skiplist.size()) - 1}, btm{int32_t(skipListIdx)}; btm <= top;)
        {
                const auto mid = (btm + top) / 2;
                const auto v = skiplist[mid].lastDocID;

                if (trace)
                        SLog("mid = ", mid, " ", v, " ", target, " ", TrivialCmp(target, v), "\n");

                if (target < v)
                        top = mid - 1;
                else
                {
                        if (v != target)
                                idx = mid;
                        else if (mid != skipListIdx)
                        {
                                // we need this
                                idx = mid - 1;
                        }

                        btm = mid + 1;
                }
        }

        return idx;
}

bool Trinity::Codecs::Lucene::Decoder::seek(const uint32_t target)
{
        // TODO: if we store (block freq, last docID in block) we can perhaps skip
        // the whole block ?
	// https://github.com/phaistos-networks/Trinity/issues/7
        if (trace)
                SLog(ansifmt::bold, ansifmt::color_blue, "SKIPPING TO ", target, ansifmt::reset, "\n");

        for (;;)
        {
                if (trace)
                        SLog("docsIndex = ", docsIndex, " ", bufferedDocs, ", curDocument.id = ", curDocument.id, "\n");

                if (unlikely(docsIndex == bufferedDocs))
                {
                        if (unlikely(p == chunkEnd))
                        {
                                if (trace)
                                        SLog("At the end already\n");

                                finalize();
                                return false;
                        }
                        else
                        {
#if 1
                                if (trace)
                                        SLog("skipListIdx = ", skipListIdx, " ", skiplist.size(), " ", target, "\n");

                                if (skipListIdx != skiplist.size())
                                {
                                        // see if we can determine where to seek to here
                                        if (const auto index = skiplist_search(target); index != UINT32_MAX)
                                        {
                                                const auto &it = skiplist[index];

                                                if (trace)
                                                        SLog("index now = ", index, "\n");
//SLog("YES to lastDocID = ", it.lastDocID, ", documentsLeft = ", totalDocuments - it.totalDocumentsSoFar, ", ", totalHits - it.totalHitsSoFar, ", ", it.curHitsBlockHits, "\n");

#if 1
                                                // XXX: what if we point into the _current_ hits block?
                                                const auto blockPtr = postingListBase + it.indexOffset;
                                                const auto hitsBlockPtr = hitsBase + it.lastHitsBlockOffset;

                                                p = blockPtr;
                                                hdp = hitsBlockPtr;

                                                lastDocID = it.lastDocID;
                                                docsLeft = totalDocuments - it.totalDocumentsSoFar;
                                                hitsLeft = totalHits - it.totalHitsSoFar;

                                                skippedHits = 0;
                                                bufferedHits = 0;

                                                refill_documents();
                                                refill_hits();
                                                update_curdoc();

                                                if (trace)
                                                        SLog("SKIPPING ", it.curHitsBlockHits, "\n");

                                                skippedHits = it.curHitsBlockHits;
                                                skip_hits(skippedHits);

                                                // we can advance here; we will only attempt to skiplist search
                                                // next time we are done with a block
                                                skipListIdx = index + 1;
                                                goto l10;
#endif
                                        }
                                }
#endif

                                if (trace)
                                        SLog("Will decode next block\n");
                                decode_next_block();
                        }
                }
                else
                {
                l10:
                        if (curDocument.id == target)
                        {
                                if (trace)
                                        SLog("Found it\n");

                                return true;
                        }
                        else if (curDocument.id > target)
                        {
                                if (trace)
                                        SLog("Not Here, now past target\n");

                                return false;
                        }
                        else
                        {
                                skippedHits += docFreqs[docsIndex];
                                lastDocID += docDeltas[docsIndex];

                                ++docsIndex;

                                // see: update_curdoc();
                                curDocument.id = lastDocID + docDeltas[docsIndex];
                                curDocument.freq = docFreqs[docsIndex];
                        }
                }
        }
}

void Trinity::Codecs::Lucene::Decoder::materialize_hits(const exec_term_id_t termID, DocWordsSpace *__restrict__ dws, term_hit *__restrict__ out)
{
	static constexpr bool trace{false};
	//const bool trace = curDocument.id == 2151228176 || curDocument.id == 2152925656 || curDocument.id == 2154895013 || curDocument.id == 2151228176;
        auto freq = docFreqs[docsIndex];
        auto outPtr = out;

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_blue, "materializing, skippedHits = ", skippedHits, ", hitsLeft = ", hitsLeft, ansifmt::reset, "\n");

        skip_hits(skippedHits);

        if (trace)
                SLog("hitsIndex = ", hitsIndex, ", freq = ", freq, ", bufferedHits = ", bufferedHits, "\n");

        // fast-path; can satisfy directly from the current hits block
        if (const auto upto = hitsIndex + freq; upto <= bufferedHits)
        {
                tokenpos_t pos{0};

                if (trace)
                        SLog("fast-path\n");

                while (hitsIndex != upto)
                {
                        const auto pl = hitsPayloadLengths[hitsIndex];

                        pos += hitsPositionDeltas[hitsIndex];
                        outPtr->pos = pos;
                        outPtr->payloadLen = pl;

                        if (trace)
                                SLog(" POS = ", pos, ", length = ", pl, "\n");

                        if (pos)
                                dws->set(termID, pos);

                        if (pl)
                        {
                                memcpy(&outPtr->payload, payloadsIt, pl);
                                payloadsIt += pl;
                        }
                        else
                                outPtr->payload = 0;

                        ++outPtr;
                        ++hitsIndex;
                }
        }
        else
        {
                tokenpos_t pos{0};

                if (trace)
                        SLog("slow path, freq = ", freq, ", hitsIndex =", hitsIndex, ", bufferedHits = ", bufferedHits, "\n");

                for (;;)
                {
                        const auto n = std::min<uint32_t>(bufferedHits - hitsIndex, freq);
                        const auto upto = hitsIndex + n;

                        if (trace)
                                SLog("UPTO = ", upto, ", bufferedHits = ", bufferedHits, ", hitsIndex = ", hitsIndex, ", freq = ", freq, ", n = ", n, "\n");

                        while (hitsIndex != upto)
                        {
                                const auto pl = hitsPayloadLengths[hitsIndex];

                                pos += hitsPositionDeltas[hitsIndex];
                                outPtr->pos = pos;
                                outPtr->payloadLen = pl;

                                if (pos)
                                        dws->set(termID, pos);

                                if (trace)
                                        SLog("FROM ", hitsIndex, ": pos = ", pos, " payload size =  ", pl, "\n");

                                if (pl)
                                {
                                        memcpy(&outPtr->payload, payloadsIt, pl);
                                        payloadsIt += pl;
                                }
                                else
                                        outPtr->payload = 0;

                                ++outPtr;
                                ++hitsIndex;
                        }
                        freq -= n;

                        if (trace)
                                SLog("freq now = ", freq, "\n");

                        if (freq)
                                refill_hits();
                        else
                                break;
                }
        }

        docFreqs[docsIndex] = 0; // simplifies processing logic
}

void Trinity::Codecs::Lucene::Decoder::init(const term_index_ctx &tctx, Trinity::Codecs::AccessProxy *access)
{
        auto ap = static_cast<Trinity::Codecs::Lucene::AccessProxy *>(access);
        const auto indexPtr = ap->indexPtr;
        const auto ptr = indexPtr + tctx.indexChunk.offset;
        const auto chunkSize = tctx.indexChunk.size();

        postingListBase = ptr;
        p = ptr;
        chunkEnd = ptr + chunkSize;
        lastDocID = 0;
        lastPosition = 0;
        totalDocuments = docsLeft = tctx.documents;
        docsIndex = hitsIndex = 0;
        bufferedDocs = bufferedHits = 0;
        skippedHits = 0;
        // important:
        docFreqs[0] = 0;
        docDeltas[0] = 0;

        const auto hitsDataOffset = *(uint32_t *)p;
        p += sizeof(uint32_t);
        totalHits = hitsLeft = *(uint32_t *)p;
        p += sizeof(uint32_t);
        p += sizeof(uint32_t); // positions chunk size
        [[maybe_unused]] const auto skiplistSize = *(uint16_t *)p;
        p += sizeof(uint16_t);

        if (skiplistSize)
        {
                // deserialize the skiplist and maybe use it
                static constexpr size_t skiplistEntrySize{sizeof(uint32_t) * 5 + sizeof(uint16_t)};
                const auto *sit = (ptr + chunkSize) - (skiplistSize * skiplistEntrySize);
                struct skiplist_entry e;

                chunkEnd = sit;
                skiplist.reserve(skiplistSize);
                for (uint32_t i{0}; i != skiplistSize; ++i, sit += skiplistEntrySize)
                {
                        const auto it = reinterpret_cast<const uint32_t *>(sit);

                        e.indexOffset = it[0];
                        e.lastDocID = it[1];
                        e.lastHitsBlockOffset = it[2];
                        e.totalDocumentsSoFar = it[3];
                        e.totalHitsSoFar = it[4];
                        e.curHitsBlockHits = *(uint16_t *)(sit + skiplistEntrySize - sizeof(uint16_t));
                        skiplist.push_back(e);
                }
        }

        skipListIdx = 0;
        hitsBase = hdp = ap->hitsDataPtr + hitsDataOffset;

        if (trace)
                SLog("skiplist.size = ", skiplist.size(), ", docsLeft = ", docsLeft, ", hitsLeft = ", hitsLeft, "\n");
}

Trinity::Codecs::Lucene::AccessProxy::AccessProxy(const char *bp, const uint8_t *p, const uint8_t *hd)
    : Trinity::Codecs::AccessProxy{bp, p}, hitsDataPtr{hd}
{
        if (hd == nullptr)
        {
                int fd = open(Buffer{}.append(basePath, "/hits.data").c_str(), O_RDONLY | O_LARGEFILE);

                if (fd == -1)
                {
                        if (errno != ENOENT)
                                throw Switch::data_error("Unable to access hits.data");
                }
                else if (const auto fileSize = lseek64(fd, 0, SEEK_END))
                {
                        hitsDataPtr = reinterpret_cast<const uint8_t *>(mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0));

                        close(fd);
                        expect(hitsDataPtr != MAP_FAILED);
                }
                else
                        close(fd);
        }
}
