#include "indexer.h"
#include "docidupdates.h"
#include "terms.h"
#include "utils.h"
#include <fcntl.h>
#include <future>
#include <sparsefixedbitset.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <text.h>

using namespace Trinity;


void SegmentIndexSession::document_proxy::insert(const uint32_t termID, const tokenpos_t position, range_base<const uint8_t *, const uint8_t> payload)
{
        require(termID);
        Dexpect(position < Limits::MaxPosition);

        if (const auto size = payload.size())
        {
                const auto l = hitsBuf.size();

                Drequire(size <= sizeof(uint64_t));
                hitsBuf.serialize(payload.start(), size);
                hits[termID & 15].push_back({termID, {position, {l, size}}});
        }
        else
                hits[termID & 15].push_back({termID, {position, {0, 0}}});
}

void SegmentIndexSession::commit_document_impl(const document_proxy &proxy, const bool replace)
{
        uint32_t terms{0};
        const auto all_hits = reinterpret_cast<const uint8_t *>(hitsBuf.data());

        // we can't update the same document more than once in the same session
        if (unlikely(!track(proxy.did)))
                throw Switch::data_error("Already committed document ", proxy.did);

        b.pack(proxy.did);

        if (replace)
        {
                updatedDocumentIDs.push_back(proxy.did);
        }

        const auto offset = b.size();

        b.pack(uint16_t(0)); // XXX: should be u32 if possible, or use varint

	for (auto &v : hits)
        {
                std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) noexcept {
                        return a.first < b.first || (a.first == b.first && a.second.first < b.second.first);
                });

                for (const auto *p = v.data(), *const e = p + v.size(); p != e;)
                {
                        const auto term = p->first;
                        uint32_t termHits{0};
                        uint32_t prev{0};
                        uint8_t prevPayloadSize{0xff};

                        require(term);
                        b.pack(term);

                        const auto o = b.size();

                        b.pack(uint16_t(0)); // XXX: should be u32 or use varint

                        do
                        {
                                const auto &it = p->second;
                                const auto delta = it.first - prev;
                                const auto payloadSize = it.second.size();

                                prev = it.first;
                                if (payloadSize != prevPayloadSize)
                                {
                                        b.encode_varbyte32((delta << 1) | 0);
                                        b.encode_varbyte32(payloadSize);
                                        prevPayloadSize = payloadSize;
                                }
                                else
                                {
                                        // Same paload size
                                        b.encode_varbyte32((delta << 1) | 1);
                                }

                                if (payloadSize)
                                        b.serialize(all_hits + it.second.start(), payloadSize);

                                ++termHits;
                        } while (++p != e && p->first == term);

                        require(termHits <= UINT16_MAX);
                        *(uint16_t *)(b.data() + o) = termHits; // total hits for (document, term): TODO use varint?

                        ++terms;
                }

                v.clear();
        }

        *(uint16_t *)(b.data() + offset) = terms; // total distinct terms for (document) XXX: see earlier comments

        if (intermediateStateFlushFreq && unlikely(b.size() > intermediateStateFlushFreq))
        {
                if (backingFileFD == -1)
                {
                        Buffer path;

                        path.append("/tmp/trinity-index-intermediate.", Timings::Microseconds::SysTime(), ".", uint32_t(getpid()), ".tmp");
                        backingFileFD = open(path.c_str(), O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC | O_EXCL, 0755);

                        if (backingFileFD == -1)
                                throw Switch::data_error("Failed to persist state");

                        // Unlink it here; won't need it
                        unlink(path.c_str());
                }

                if (write(backingFileFD, b.data(), b.size()) != b.size())
                        throw Switch::data_error("Failed to persist state");

                b.clear();
        }
}

str8_t SegmentIndexSession::term(const uint32_t id)
{
        return invDict[id];
}

uint32_t SegmentIndexSession::term_id(const str8_t term)
{
        // Indexer words space
        // Each segment has its own terms and there is no need to maintain a global(index) or local(segment) (term=>id) dictionary
        // but we use transient term IDs (integers) for simplicity and performance
        // SegmentIndexSession::commit() will store actual terms, not their transient IDs.
        // See CONCEPTS.md
        auto it = dictionary.insert({term, 0});

        if (it.second)
        {
                // got to abuse it because..well, whatever
                auto key = (str8_t *)&it.first->first;

                key->Set(dictionaryAllocator.CopyOf(term.data(), term.size()), term.size());

                const auto k = dictionary.size();

		it.first->second = k;
                invDict.insert({k, *key});
                return k;
        }
        else
                return it.first->second;
}

bool SegmentIndexSession::track(const isrc_docid_t documentID)
{
	// This currently disabled, but if you need to make sure that
	// you won't accidently or otherwise attempt to index/remove a document
	// that you have already committed(will throw an exception), enable this code block.
	// The cost is neglible anwyay
#if 1
        static_assert(0 == (bank::SPAN & 1));
        const auto base = documentID & (~(bank::SPAN - 1));
        const auto normalized = documentID - base;

        if (likely(curBank) && curBank->base == base)
                return curBank->bs.try_set(normalized);

        for (auto it : banks)
        {
                if (it->base == base)
                {
                        curBank = it;
                        return it->bs.try_set(normalized);
                }
        }

        auto b = new bank();

        banks.push_back(b);
        b->base = base;
        b->bs.set(normalized);
        curBank = b;
        return true;
#else
	return true;
#endif
}

void SegmentIndexSession::erase(const isrc_docid_t documentID)
{
        if (!track(documentID))
                throw Switch::data_error("Already committed document ", documentID);

        updatedDocumentIDs.push_back(documentID);
}

Trinity::SegmentIndexSession::document_proxy SegmentIndexSession::begin(const isrc_docid_t documentID)
{
	// XXX: hits[] is clear()ed in commit_document_impl()
	// so if you don't commit, you may end up with garbage.
        hitsBuf.clear();
        return {*this, documentID, hits, hitsBuf};
}

// You are expected to have invoked sess->begin() and built the index in sess->indexOut
// see SegmentIndexSession::commit()
// Callee is responsible for clos()ing indexFd
//
// Please note that it will invoke sess->end() for you
void Trinity::persist_segment(Trinity::Codecs::IndexSession *const sess, std::vector<isrc_docid_t> &updatedDocumentIDs, int indexFd)
{
        if (sess->indexOut.size())
        {
                if (Trinity::Utilities::to_file(sess->indexOut.data(), sess->indexOut.size(), indexFd) == -1)
                        throw Switch::system_error("Failed to persist index");

                sess->indexOut.clear();
        }

        IOBuffer maskedDocumentsBuf;

        // Persist masked documents if any
        pack_updates(updatedDocumentIDs, &maskedDocumentsBuf);

        if (maskedDocumentsBuf.size())
        {
                if (Trinity::Utilities::to_file(maskedDocumentsBuf.data(), maskedDocumentsBuf.size(), Buffer{}.append(sess->basePath, "/updated_documents.ids").c_str()) == -1)
                        throw Switch::system_error("Failed to persist masked documents");
        }

        // Persist codec info
        int fd = open(Buffer{}.append(sess->basePath, "/codec").c_str(), O_WRONLY | O_LARGEFILE | O_TRUNC | O_CREAT, 0775);

        if (fd == -1)
                throw Switch::system_error("Failed to persist codec id");

        const auto codecID = sess->codec_identifier();

        if (write(fd, codecID.data(), codecID.size()) != codecID.size())
        {
                close(fd);
                throw Switch::system_error("Failed to persist codec id");
        }
        else
                close(fd);

        sess->end();
}

void Trinity::persist_segment(Trinity::Codecs::IndexSession *const sess, std::vector<isrc_docid_t> &updatedDocumentIDs)
{
        auto path = Buffer{}.append(sess->basePath, "/index.t");
        int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_LARGEFILE | O_TRUNC, 0775);

        if (fd == -1)
                throw Switch::system_error("Failed to persist index ", path.AsS32(), ":", strerror(errno));

        Defer({
                close(fd);
        });

        persist_segment(sess, updatedDocumentIDs, fd);

        if (rename(path.c_str(), Buffer{}.append(strwlen32_t(path.data(), path.size() - 2)).c_str()) == -1)
                throw Switch::system_error("Failed to persist index");
}

/*
<indexer.cpp:346 operator()>2.163s to collect them
<indexer.cpp:373 operator()>1.351s to sort them
<indexer.cpp:426 operator()>11.787s to encode
<indexer.cpp:494 commit>1.826s to persist segment

We clearly need to optimize the encoding process, the remaining time can be reduced but won't make a difference if manage to do it anyway
Need to consider means to optimize the encoder impl.
*/
void SegmentIndexSession::commit(Trinity::Codecs::IndexSession *const sess)
{
        struct segment_data
        {
                uint32_t termID;
                isrc_docid_t documentID;
                uint32_t hitsOffset;
                uint16_t hitsCnt; // XXX: see comments earlier
                uint8_t rangeIdx;
        };

        static constexpr bool trace{false};
        std::vector<uint32_t> allOffsets;
        ska::flat_hash_map<uint32_t, term_index_ctx> map;
        std::unique_ptr<Trinity::Codecs::Encoder> enc_(sess->new_encoder());
        auto path = Buffer{}.append(sess->basePath, "/index.t");
        int indexFd = open(path.c_str(), O_WRONLY | O_CREAT | O_LARGEFILE | O_TRUNC, 0775);

        if (indexFd == -1)
                throw Switch::system_error("Failed to persist index: ", path.AsS32());

        Defer({
                if (indexFd != -1)
                        close(indexFd);
        });

        const auto scan = [ flushFreq = this->flushFreq, indexFd, enc = enc_.get(), &map, sess ](const auto &ranges)
        {
                uint8_t payloadSize;
                std::vector<segment_data> all[32];
                term_index_ctx tctx;
                const auto R = ranges.data();
                uint64_t before;

                for (auto &v : all)
                        v.reserve(64 * 1024);

                require(ranges.size() < sizeof(uint8_t) << 3);
                before = Timings::Microseconds::Tick();
                for (uint8_t i{0}; i != ranges.size(); ++i)
                {
                        const auto range = R[i];
                        const auto data = range.offset;
                        const auto dataSize = range.size();
                        uint32_t _t;

                        for (const auto *p = data, *const e = p + dataSize; p != e;)
                        {
                                const auto documentID = *(isrc_docid_t *)p;
                                p += sizeof(isrc_docid_t);
                                auto termsCnt = *(uint16_t *)p; // XXX: see earlier comments
                                p += sizeof(uint16_t);

                                if (!termsCnt)
                                {
                                        // deleted?
                                        continue;
                                }

                                do
                                {
                                        const auto term = *(uint32_t *)p;
                                        p += sizeof(uint32_t);
                                        auto hitsCnt = *(uint16_t *)p; // XXX: see earlier comments
                                        const auto saved{hitsCnt};

                                        p += sizeof(hitsCnt);

                                        const auto base{p};
                                        do
                                        {
                                                varbyte_get32(p, _t);
                                                const auto deltaMask{_t};

                                                if (0 == (deltaMask & 1))
                                                {
                                                        varbyte_get32(p, payloadSize);
                                                }

                                                p += payloadSize;
                                        } while (--hitsCnt);

                                        all[term & (sizeof_array(all) - 1)].push_back({term, documentID, uint32_t(base - data), saved, i});
                                } while (--termsCnt);
                        }
                }
                if (trace)
                        SLog(duration_repr(Timings::Microseconds::Since(before)), " to collect them\n");

                {
                        // can sort those in parallel
                        // can't rely on std::execution::par, not available yet
                        // down to 2s from 10s, just by partitioning them and sorting them in parallel
                        std::vector<std::future<void>> futures;

                        before = Timings::Microseconds::Tick();
                        for (auto &v : all)
                        {
                                futures.push_back(
                                    std::async(std::launch::async, [](auto v) {
                                            std::sort(v->begin(), v->end(), [](const auto &a, const auto &b) noexcept {
                                                    return a.termID < b.termID || (a.termID == b.termID && a.documentID < b.documentID);
                                            });

                                    },
                                               &v));
                        }

                        while (futures.size())
                        {
                                futures.back().get();
                                futures.pop_back();
                        }

                        if (trace)
                                SLog(duration_repr(Timings::Microseconds::Since(before)), " to sort them\n");
                }

                before = Timings::Microseconds::Tick();
                for (auto &v : all)
                {
                        for (const auto *it = v.data(), *const e = it + v.size(); likely(it != e);)
                        {
                                const auto term = it->termID;
                                isrc_docid_t prevDID{0};
                                uint32_t _t;

				// TODO:
				// Maybe we need a new API which would allow us to encode terms individually, i.e use begin_term() to get
				// hold of some identifier and buffer, and then begin_document() and new_hit() on that buffer, end then use end_term() with that buffer
				// on the encoder to flush that term into the encoder. By doing so, and by serializing access to the encoding on end_term(), we would have been able
				// to process/encode terms indepedently and take advantage of multiple cores.
				// This is important because most the time's spent in encoding PFOR arrays

                                enc->begin_term();

                                do
                                {
                                        const auto documentID = it->documentID;
                                        const auto hitsCnt = it->hitsCnt;
                                        const auto *p = R[it->rangeIdx].offset + it->hitsOffset;
                                        uint32_t pos{0};

                                        require(documentID > prevDID);

                                        enc->begin_document(documentID);
                                        for (uint32_t i{0}; i != hitsCnt; ++i)
                                        {
                                                varbyte_get32(p, _t);
                                                const auto deltaMask{_t};

                                                if (0 == (deltaMask & 1))
                                                {
                                                        //payloadSize = Compression::decode_varuint32(p);
                                                        varbyte_get32(p, payloadSize);
                                                }

                                                pos += deltaMask >> 1;

                                                enc->new_hit(pos, {p, payloadSize});

                                                p += payloadSize;
                                        }
                                        enc->end_document();

                                        prevDID = documentID;
                                } while (likely(++it != e) && it->termID == term);

                                enc->end_term(&tctx);
                                map.insert({term, tctx});

                                if (flushFreq && unlikely(sess->indexOut.size() > flushFreq))
                                        sess->flush_index(indexFd);
                        }
                }
                if (trace)
                        SLog(duration_repr(Timings::Microseconds::Since(before)), " to encode\n");
        };

        // basepath already set for IndexSession
        // begin() could open files, etc
        sess->begin();

        std::vector<range_base<const uint8_t *, size_t>> ranges;

        if (b.size())
                ranges.push_back({reinterpret_cast<const uint8_t *>(b.data()), b.size()});

        if (backingFileFD != -1)
        {
                const auto fileSize = lseek64(backingFileFD, 0, SEEK_END);

                if (fileSize == off64_t(-1))
                        throw Switch::data_error("Failed to access backing file");

                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, backingFileFD, 0);

                if (fileData == MAP_FAILED)
                        throw Switch::data_error("Failed to access backing file");

                Defer({
                        munmap(fileData, fileSize);
                });

                madvise(fileData, fileSize, MADV_SEQUENTIAL);
                ranges.push_back({reinterpret_cast<const uint8_t *>(fileData), size_t(fileSize)});

                scan(ranges);

                close(backingFileFD);
                backingFileFD = -1;
        }
        else if (ranges.size())
                scan(ranges);

        // Persist terms dictionary
        std::vector<std::pair<str8_t, term_index_ctx>> v;
        size_t sum{0};
        uint64_t before;

        v.reserve(map.size() + 512);
        for (const auto &it : map)
        {
                const auto termID = it.first;
                const auto term = invDict[termID]; // actual term

                sum += it.second.indexChunk.size();
                v.push_back({term, it.second});
        }

        // TODO: move this out to another method (persist), so that
        // if we want to keep those resident in-memory
        // If you don't set flush frequence(by default, set to 0), and you don't persist here, you can
        // use this handy class to build a memory resident index, without
        // having to directly use the various codec classes.
        before = Timings::Microseconds::Tick();

        sess->persist_terms(v);
        persist_segment(sess, updatedDocumentIDs, indexFd);

        if (trace)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to persist segment\n");

        if (fsync(indexFd) == -1)
                throw Switch::data_error("Failed to persist index");

        if (const auto res = lseek64(indexFd, 0, SEEK_END); res != sum)
        {
                // Sanity check
                throw Switch::data_error("Unexpected state");
        }

        if (close(indexFd) == -1)
                throw Switch::data_error("Failed to persist index");

        indexFd = -1;
        if (rename(path.c_str(), Buffer{}.append(strwlen32_t(path.data(), path.size() - 2)).c_str()) == -1)
                throw Switch::system_error("Failed to persist index");
}
