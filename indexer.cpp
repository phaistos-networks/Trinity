#include "indexer.h"
#include "docidupdates.h"
#include "terms.h"
#include "utils.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <text.h>

using namespace Trinity;

void SegmentIndexSession::document_proxy::insert(const uint32_t termID, const uint32_t position, range_base<const uint8_t *, const uint8_t> payload)
{
        require(termID);

        if (const auto size = payload.size())
        {
                const auto l = hitsBuf.size();

                Drequire(size <= sizeof(uint64_t));
                hitsBuf.serialize(payload.start(), size);
                hits.push_back({termID, {position, {l, size}}});
        }
        else
                hits.push_back({termID, {position, {0, 0}}});
}

void SegmentIndexSession::commit_document_impl(const document_proxy &proxy, const bool isUpdate)
{
        uint32_t terms{0};
        const auto all_hits = reinterpret_cast<const uint8_t *>(hitsBuf.data());

        std::sort(hits.begin(), hits.end(), [](const auto &a, const auto &b) {
                return a.first < b.first || (a.first == b.first && a.second.first < b.second.first);
        });

        b.pack(proxy.did);

        if (isUpdate)
        {
                updatedDocumentIDs.push_back(proxy.did);
        }

        const auto offset = b.size();

        b.pack(uint16_t(0));	 // XXX: should be u32 if possible, or use varint

        for (const auto *p = hits.data(), *const e = p + hits.size(); p != e;)
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
                                b.encode_varuint32((delta << 1) | 0);
                                b.encode_varuint32(payloadSize);
                                prevPayloadSize = payloadSize;
                        }
                        else
                        {
                                // Same paload size
                                b.encode_varuint32((delta << 1) | 1);
                        }

                        if (payloadSize)
                                b.serialize(all_hits + it.second.start(), payloadSize);

                        ++termHits;
                } while (++p != e && p->first == term);

                require(termHits <= UINT16_MAX);
                *(uint16_t *)(b.data() + o) = termHits; // total hits for (document, term): TODO use varint?

                ++terms;
        }

        *(uint16_t *)(b.data() + offset) = terms; // total distinct terms for (document) XXX: see earlier comments

        if (b.size() > 16 * 1024 * 1024)
        {
                // flush buffer?
        }
}

uint32_t SegmentIndexSession::term_id(const str8_t term)
{
	// Indexer words space
	// Each segment has its own terms and there is no need to maintain a global(index) or local(segment) (term=>id) dictionary
	// but we use transient term IDs (integers) for simplicity and performance
	// SegmentIndexSession::commit() will store actual terms, not their transient IDs.
	// See CONCEPTS.md
        uint32_t *idp;
        str8_t *keyptr;

#ifndef LEAN_SWITCH
        if (dictionary.AddNeedKey(term, 0, keyptr, &idp))
#else
	const auto p = dictionary.insert({term, 0});
	
	idp = &p.first->second;
	if (p.second)
#endif
        {
                keyptr->Set(dictionaryAllocator.CopyOf(term.data(), term.size()), term.size());
                *idp = dictionary.size();
		invDict.insert({*idp, *keyptr});
        }
        return *idp;
}

void SegmentIndexSession::erase(const docid_t documentID)
{
        updatedDocumentIDs.push_back(documentID);
}

Trinity::SegmentIndexSession::document_proxy SegmentIndexSession::begin(const docid_t documentID)
{
        hits.clear();
        hitsBuf.clear();
        return {*this, documentID, hits, hitsBuf};
}

// You are expected to have invoked sess->begin() and built the index in sess->indexOut
// see SegmentIndexSession::commit()
void Trinity::persist_segment(Trinity::Codecs::IndexSession *const sess, std::vector<docid_t> &updatedDocumentIDs)
{
	if (Trinity::Utilities::to_file(sess->indexOut.data(), sess->indexOut.size(), Buffer{}.append(sess->basePath, "/index").c_str()) == -1)
                throw Switch::system_error("Failed to persist index");


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

        Dexpect(fd != -1);

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

void SegmentIndexSession::commit(Trinity::Codecs::IndexSession *const sess)
{
        struct segment_data
        {
                uint32_t termID;
                docid_t documentID;
                uint32_t hitsOffset;
                uint16_t hitsCnt; 	// XXX: see comments earlier
        };

        std::vector<uint32_t> allOffsets;
        Switch::unordered_map<uint32_t, term_index_ctx> map;
        std::unique_ptr<Trinity::Codecs::Encoder> enc_(sess->new_encoder());

        const auto scan = [ enc = enc_.get(), &map ](const auto data, const auto dataSize)
        {
                uint8_t payloadSize;
                std::vector<segment_data> all;
                term_index_ctx tctx;

                for (const auto *p = data, *const e = p + dataSize; p != e;)
                {
                        const auto documentID = *(docid_t *)p;
                        p += sizeof(docid_t);
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
                                        const auto deltaMask = Compression::decode_varuint32(p);

                                        if (0 == (deltaMask & 1))
                                                payloadSize = Compression::decode_varuint32(p);

                                        p += payloadSize;
                                } while (--hitsCnt);

                                all.push_back({term, documentID, uint32_t(base - data), saved});
                        } while (--termsCnt);
                }

                std::sort(all.begin(), all.end(), [](const auto &a, const auto &b) noexcept {
                        return a.termID < b.termID || (a.termID == b.termID && a.documentID < b.documentID);
                });


                for (const auto *it = all.data(), *const e = it + all.size(); it != e;)
                {
                        const auto term = it->termID;
			docid_t prevDID{0};

                        enc->begin_term();

                        do
                        {
                                const auto documentID = it->documentID;
                                const auto hitsCnt = it->hitsCnt;
                                const auto *p = data + it->hitsOffset;
                                uint32_t pos{0};

				require(documentID > prevDID);

                                enc->begin_document(documentID);
                                for (uint32_t i{0}; i != hitsCnt; ++i)
                                {
                                        const auto deltaMask = Compression::decode_varuint32(p);

                                        if (0 == (deltaMask & 1))
                                                payloadSize = Compression::decode_varuint32(p);

                                        pos += deltaMask >> 1;

                                        enc->new_hit(pos, {p, payloadSize});

                                        p += payloadSize;
                                }
                                enc->end_document();

				prevDID = documentID;
                        } while (++it != e && it->termID == term);

                        enc->end_term(&tctx);
                        map.insert({term, tctx});
                }
        };

        // basepath already set for IndexSession
        // begin() could open files, etc
        sess->begin();


        // IF we flushed b earlier, mmap() and scan() that mmmap()ed region first
        scan(reinterpret_cast<const uint8_t *>(b.data()), b.size());


        // Persist terms dictionary
        std::vector<std::pair<str8_t, term_index_ctx>> v;

        for (const auto &it : map)
        {
#ifdef LEAN_SWITCH
                const auto termID = it.first;
                const auto term = invDict[termID]; // actual term

                v.push_back({term, it.second});
#else
                const auto termID = it.key();
                const auto term = invDict[termID]; // actual term

                v.push_back({term, it.value()});
#endif
        }

        sess->persist_terms(v);
	persist_segment(sess, updatedDocumentIDs);
}
