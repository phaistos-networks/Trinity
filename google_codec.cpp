#include "google_codec.h"
#include "docidupdates.h"
#include <compress.h>
#include <memory>


#pragma mark ENCODER

void Trinity::Codecs::Google::Encoder::commit_block()
{
	const auto delta = curDocID - prevBlockLastDocumentID;
	const auto n = curBlockSize - 1;
	auto out{&sess->indexOut};

	SLog("Commiting block, curBlockSize = ", curBlockSize, ", curDocID = ", curDocID, ", prevBlockLastDocumentID = ", prevBlockLastDocumentID, ", delta = ", delta, "  ", out->size(), "\n");

	// build the new block
	block.clear();
	for (uint32_t i{0}; i != n; ++i) // exclude that last one because it will be referenced in the header
	{
		SLog("<< ", docDeltas[i], "\n");
		block.SerializeVarUInt32(docDeltas[i]);
	}

	for (uint32_t i{0}; i != curBlockSize; ++i)
	{
		SLog("<< freq ", blockFreqs[i], "\n");
		block.SerializeVarUInt32(blockFreqs[i]);
	}

	const auto blockLength = block.size() + hitsData.size();

	if (--skiplistEntryCountdown == 0)
	{
		SLog("NEW skiplist record for ", prevBlockLastDocumentID, "\n");

		skipListData.pack(prevBlockLastDocumentID, uint32_t(out->size()));
		skiplistEntryCountdown = SKIPLIST_STEP;
	}

	out->SerializeVarUInt32(delta);       // delta to last docID in block from previous block's last document ID
	out->SerializeVarUInt32(blockLength); // block length in bytes, excluding this header
	out->pack(curBlockSize);              // one byte will suffice

	out->Serialize(block.data(), block.size());
	out->Serialize(hitsData.data(), hitsData.size());
	hitsData.clear();

	prevBlockLastDocumentID = curDocID;
	curBlockSize = 0;

#if 0
	if (out->size() > 64)
	{
		if (write(indexFD, out->data(), out->size()) != out->size())
			throw Switch::data_error("Failed to update index");

		indexFileSize+=out->size();
		out->clear();
	}
#endif

	SLog("Commited Block ", out->size(), "\n");
}

void Trinity::Codecs::Google::IndexSession::merge(range_base<const uint8_t *, uint32_t> *in, const uint32_t chunksCnt, Trinity::Codecs::Encoder *const encoder_, dids_scanner_registry *maskedDocuments)
{
	// if it's just one chunk we can copy it as is because we now track
	// the chunk size
	//
	// need to be provided by more recent to the least recent
	//require(chunksCnt > 1);

	struct chunk
	{
		const uint8_t *p;
		const uint8_t *e;

		struct
		{
			uint32_t documents[N];
			uint32_t freqs[N];
			uint8_t size;
			uint8_t idx;
		} cur_block;

		constexpr size_t size() noexcept
		{
			return e - p;
		}

		bool skip_current()
		{
			for (auto n = cur_block.freqs[cur_block.idx]; n; --n)
				Compression::UnpackUInt32(p);

			return ++cur_block.idx == cur_block.size;
		}
	};

	chunk chunks[32];
	uint32_t toAdvance[32];
	uint32_t rem{chunksCnt};
	auto encoder = static_cast<Trinity::Codecs::Google::Encoder *>(encoder_);

	require(chunksCnt <= sizeof_array(chunks));

	for (uint32_t i{0}; i != chunksCnt; ++i)
	{
		auto c = chunks + i;

		c->p = in[i].start();
		c->e = in[i].stop();

		// Simplifies refill()
		c->cur_block.size = 1;
		c->cur_block.documents[0] = 0;
	}

	const auto refill = [](auto c) {
		auto p = c->p;
		const auto prevBlockLastID = c->cur_block.documents[c->cur_block.size - 1];
		const auto thisBlockLastDocID = prevBlockLastID + Compression::UnpackUInt32(p);
		[[maybe_unused]] const auto blockLength = Compression::UnpackUInt32(p);
		const auto n = *p++;
		auto id{prevBlockLastID};
		const auto k = n - 1;
		SLog("prevBlockLastID = ", prevBlockLastID, ", thisBlockLastDocID = ", thisBlockLastDocID, " ", n, "\n");

		for (uint8_t i{0}; i != k; ++i)
		{
			id += Compression::UnpackUInt32(p);
			SLog("<< ", id, "\n");
			c->cur_block.documents[i] = id;
		}

		c->cur_block.documents[k] = thisBlockLastDocID;

		for (uint8_t i{0}; i != n; ++i)
		{
			c->cur_block.freqs[i] = Compression::UnpackUInt32(p);
			SLog("<< freq(", c->cur_block.freqs[i], ")\n");
		}

		c->cur_block.size = n;
		c->cur_block.idx = 0;
		c->p = p;
	};

	const auto append_from = [encoder](auto c) {
		const auto idx = c->cur_block.idx;
		const auto did = c->cur_block.documents[idx];
		// TODO: if `did` is ignore, skip the hits but don't forward to the encoder
		auto freq = c->cur_block.freqs[idx];
		auto p = c->p;

		encoder->begin_document(did, freq);

		SLog("APENDING ", did, " ", freq, "\n");

		for (uint32_t i{0}, pos{0}; i != freq; ++i)
		{
			pos += Compression::UnpackUInt32(p);
			encoder->new_position(pos);
		}

		encoder->end_document();

		c->p = p;
		c->cur_block.freqs[idx] = 0; // this is important, otherwise skip_current() will skip those hits we just consumed
	};

	for (uint32_t i{0}; i != chunksCnt; ++i)
		refill(chunks + i);

	for (;;)
	{
		uint32_t toAdvanceCnt = 1;
		uint32_t lowestDID = chunks[0].cur_block.documents[chunks[0].cur_block.idx];

		toAdvance[0] = 0;
		for (uint32_t i{1}; i < rem; ++i)
		{
			const auto id = chunks[i].cur_block.documents[chunks[i].cur_block.idx];

			if (id < lowestDID)
			{
				lowestDID = id;
				toAdvanceCnt = 1;
				toAdvance[0] = i;
			}
			else if (id == lowestDID)
			{
				toAdvance[toAdvanceCnt++] = i;
			}
		}

		// We use the first chunk
		SLog("To advance ", toAdvanceCnt, " ", toAdvance[0], " ", lowestDID, "\n");

		if (!maskedDocuments->test(lowestDID))
		{
			const auto src = chunks + toAdvance[0];

			append_from(src);
		}

		do
		{
			auto idx = --toAdvanceCnt;
			auto c = chunks + idx;

			if (c->skip_current()) // end of the block
			{
				if (c->p != c->e)
				{
					// more blocks available
					SLog("No more block documents\n");
					refill(c);
				}
				else
				{
					// exhausted
					if (--rem == 0)
					{
						// no more chunks to process
						SLog("No More Chunks\n");
						goto l1;
					}

					memmove(c, c + 1, (rem - idx) * sizeof(chunk));
				}
			}

		} while (toAdvanceCnt);
	}

l1:;
}









#pragma mark DECODER

uint32_t Trinity::Codecs::Google::Decoder::skiplist_search(const uint32_t target) const noexcept
{
	// we store {previous block's last ID, block's offset}
	// in skiplist[], because when we unpack a block, we need to know
	// the previous block last document ID.
	//
	// So we need to use binary search to look for the last skiplist entry where
	// target > entry.first
	// We could use std::lower_bound() twice(if returned iterator points to an entry where entry.first == target)
	// but we 'll just roll out own here
	uint32_t idx{UINT32_MAX};

	for (int32_t top{int32_t(skiplist.size()) - 1}, btm{int32_t(skipListIdx)}; btm <= top;)
	{
		const auto mid = (btm + top) / 2;
		const auto v = skiplist[mid].first;

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


void Trinity::Codecs::Google::Decoder::skip_block_doc()
{
	// just advance to the next document in the current block
	// skip current document's hits/positions first

	SLog("skipping document index ", blockDocIdx, ", freq = ", freqs[blockDocIdx], "\n");

	const auto freq = freqs[blockDocIdx++];

	for (uint32_t i{0}; i != freq; ++i)
		Compression::UnpackUInt32(p);

	// p now points to the positions/attributes for the current document
	// current document is documents[blockDocIdx]
	// and its frequency is freqs[blockDocIdx]
	// You can access the current document at documents[blockDocIdx], freq at freqs[blockDocIdx] and you can materialize
	// the document attributes with materialize_attributes()
}

void Trinity::Codecs::Google::Decoder::unpack_block(const uint32_t thisBlockLastDocID, const uint8_t n)
{
	const auto k{n - 1};
	auto id{blockLastDocID};

	SLog("Now unpacking block contents, n = ", n, ", blockLastDocID = ", blockLastDocID, "\n");

	for (uint8_t i{0}; i != k; ++i)
	{
		id += Compression::UnpackUInt32(p);
		SLog("<< ", id, "\n");
		documents[i] = id;
	}

	for (uint32_t i{0}; i != n; ++i)
	{
		freqs[i] = Compression::UnpackUInt32(p);
		SLog("Freq ", i, " ", freqs[i], "\n");
	}

	blockLastDocID = thisBlockLastDocID;
	documents[k] = blockLastDocID;

	// We don't need to track current block documents cnt, because
	// we can just check if (documents[blockDocIdx] == blockLastDocID)
	blockDocIdx = 0;
}

void Trinity::Codecs::Google::Decoder::seek_block(const uint32_t target)
{
	SLog("SEEKING ", target, "\n");

	for (;;)
	{
		const auto thisBlockLastDocID = blockLastDocID + Compression::UnpackUInt32(p);
		const auto blockSize = Compression::UnpackUInt32(p);
		const auto blockDocsCnt = *p++;

		SLog("thisBlockLastDocID = ", thisBlockLastDocID, ", blockSize = ", blockSize, ", blockDocsCnt, ", blockDocsCnt, "\n");

		if (target > thisBlockLastDocID)
		{
			SLog("Target(", target, ") past this block (thisBlockLastDocID = ", thisBlockLastDocID, ")\n");

			p += blockSize;

			if (p == chunkEnd)
			{
				// exchausted all blocks
				SLog("Finalizing\n");
				finalize();
				return;
			}

			blockLastDocID = thisBlockLastDocID;
			SLog("Skipped past block\n");
		}
		else
		{
			SLog("Found potential block\n");
			unpack_block(thisBlockLastDocID, blockDocsCnt);
			break;
		}
	}
}

void Trinity::Codecs::Google::Decoder::unpack_next_block()
{
	const auto thisBlockLastDocID = blockLastDocID + Compression::UnpackUInt32(p);
	const auto blockSize = Compression::UnpackUInt32(p);
	const auto blockDocsCnt = *p++;

	SLog("UNPACKING next block, thisBlockLastDocID = ", thisBlockLastDocID, ", blockSize = ", blockSize, ", blockDocsCnt = ", blockDocsCnt, ", blockLastDocID = ", blockLastDocID, "\n");

	unpack_block(thisBlockLastDocID, blockDocsCnt);
}


void Trinity::Codecs::Google::Decoder::skip_remaining_block_documents()
{
	SLog("Skipping current block\n");
	for (;;)
	{
		auto freq = freqs[blockDocIdx];

		SLog("Skipping ", documents[blockDocIdx], " ", freq, "\n");

		while (freq)
		{
			Compression::UnpackUInt32(p);
			--freq;
		}

		if (documents[blockDocIdx] == blockLastDocID)
			break;
		else
			++blockDocIdx;
	}
}

void Trinity::Codecs::Google::Decoder::materialize_attributes()
{
	const auto freq = freqs[blockDocIdx];

	for (uint32_t i{0}; i != freq; ++i)
		Compression::UnpackUInt32(p);

	// need to reset freqs[blockDocIdx] to 0 because
	// we have already avanced ptr.
	// We could have used a local ptr = p and advanced that instead
	// but for performance reasons we will reset freqs[blockDocIdx] so that
	// skip_block_doc() won't need to unpack varints again
	freqs[blockDocIdx] = 0;
}


uint32_t Trinity::Codecs::Google::Decoder::begin()
{
	SLog("Resetting\n");

	if (p != chunkEnd)
	{
		unpack_next_block();
	}
	else
	{
		// ODD, not a single document for this term
		// doesn't make much sense, but we can handle it
		finalize();
	}

	return documents[blockDocIdx];
}

bool Trinity::Codecs::Google::Decoder::next()
{
	SLog("NEXT\n");

	if (documents[blockDocIdx] == blockLastDocID)
	{
		SLog("done with block\n");

		skip_block_doc();

		if (p != chunkEnd)
		{
			// we are at the last document in the block
			SLog("Yes, have more blocks\n");

			// more blocks available
			unpack_next_block();
		}
		else
		{
			SLog("Exhausted all documents\n");
			// exhausted all documents
			finalize();
			return false;
		}
	}
	else
	{
		SLog("Just skipping block\n");
		skip_block_doc();
	}

	return true;
}

bool Trinity::Codecs::Google::Decoder::seek(const uint32_t target)
{
	SLog(ansifmt::bold, ansifmt::color_green, "SKIPPING to ", target, ansifmt::reset, "\n");

	if (target > blockLastDocID)
	{
		// we can safely assume (p != chunkEnd)
		// because in that case we 'd have finalize() and
		// blockLastDocID would have been UINT32_MAX
		// and (target > blockLastDocID) would have been false

		skip_remaining_block_documents();

		if (unlikely(p == chunkEnd))
		{
			SLog("Exhausted documents\n");
			finalize();
			return false;
		}

		SLog("Skipped remaining block documents, skipListIdx = ", skipListIdx, " ", skiplist.size(), "\n");

		if (skipListIdx != skiplist.size())
		{
			const auto idx = skiplist_search(target);

			SLog("idx = ", idx, ", target = ", target, "\n");
			for (const auto &it : skiplist)
				Print(it, "\n");

			if (idx != UINT32_MAX)
			{
				// there is a skiplist entry we can use
				blockLastDocID = skiplist[idx].first;
				p = base + skiplist[idx].second;

				SLog("Skipping ahead to past ", blockLastDocID, "\n");
				skipListIdx = idx + 1;
			}
		}

		seek_block(target);
	}

	// If it's anywhere, it must be in this current block
	for (;;)
	{
		const auto docID = documents[blockDocIdx];

		SLog("Scannning current block blockDocIdx = ", blockDocIdx, ", docID = ", docID, "\n");

		if (docID > target)
		{
			SLog("Not in this block or maybe any block\n");
			return false;
		}
		else if (docID == target)
		{
			// got it
			SLog("Got target\n");
			return true;
		}
		else if (docID == blockLastDocID)
		{
			// exhausted block documents and still not here
			// we determined we don't have this document
			SLog("Exhausted block\n");
			return false;
		}
		else
		{
			SLog("Skipping block document\n");
			skip_block_doc();
		}
	}

	return false;
}

void Trinity::Codecs::Google::Decoder::init(const term_segment_ctx &tctx,  Trinity::Codecs::AccessProxy *proxy, const uint8_t *skiplistData)
{
	[[maybe_unused]] auto access = static_cast<Trinity::Codecs::Google::AccessProxy *>(proxy);
	auto indexPtr = access->indexPtr;
        auto ptr = indexPtr + tctx.offsets[0];
        const auto chunkSize = tctx.chunkSize;

	SLog("INITIALIZED ", tctx.offsets[0], "\n");

        chunkEnd = ptr + chunkSize;
        blockLastDocID = 0;
        blockDocIdx = 0;
        documents[0] = 0;
        freqs[0] = 0;
        base = p = ptr;
        skipListIdx = 0;

        if (unlikely(!chunkSize))
                finalize();
        else if (auto it = skiplistData)
        {
                // That many skiplist entries(deterministic)
                const auto n = ((tctx.documents + (N - 1)) / N) / SKIPLIST_STEP;

                for (uint32_t i{0}; i != n; ++i)
                {
                        const auto firstBlockID = *(uint32_t *)it;
                        it += sizeof(uint32_t);
                        const auto offset = *(uint32_t *)it;
                        it += sizeof(uint32_t);

                        skiplist.push_back({firstBlockID, offset});
                }

                SLog(skiplist.size(), " skiplist entries\n");
        }
}

Trinity::Codecs::Decoder *Trinity::Codecs::Google::AccessProxy::decoder(const term_segment_ctx &tctx,  Trinity::Codecs::AccessProxy *access, const uint8_t *skiplistData)
{
	auto d = std::make_unique<Trinity::Codecs::Google::Decoder>();

	d->init(tctx,  access, skiplistData);
	return d.release();
}



void Trinity::Codecs::Google::IndexSession::begin()
{

}

void Trinity::Codecs::Google::IndexSession::end()
{

}

Trinity::Codecs::Encoder *Trinity::Codecs::Google::IndexSession::new_encoder(Trinity::Codecs::IndexSession *s)
{
	return new Trinity::Codecs::Google::Encoder(s);
}
