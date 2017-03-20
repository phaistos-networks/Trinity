#include <text.h>
#include <compress.h>
#include "terms.h"

Trinity::term_index_ctx Trinity::lookup_term(range_base<const uint8_t *, uint32_t> termsData, const str8_t q, const Switch::vector<Trinity::terms_skiplist_entry> &skipList)
{
        int32_t top{int32_t(skipList.size()) - 1};
        const auto skipListData = skipList.data();

        // skiplist search for the appropriate block
        // we can't use lower_bound
        for (int32_t btm{0}; btm <= top;)
        {
                const auto mid = (btm + top) / 2;
                const auto t = skipListData + mid;

                if (t->term == q)
                {
#ifdef TRINITY_TERMS_FAT_INDEX
                        // found in the index/skiplist
                        return t->tctx;
#else
			top = mid;
			goto l100;
#endif
                }
                else if (terms_cmp(q.data(), q.size(), t->term.data(), t->term.size()) < 0)
                        top = mid - 1;
                else
                        btm = mid + 1;
        }

        if (top == -1)
                return {};

l100:
        const auto &it = skipList[top];
        const auto o = it.blockOffset;
        auto prev = it.term;
	str8_t::value_type termStorage[Limits::MaxTermLength];

        memcpy(termStorage, prev.data(), prev.size() * sizeof(str8_t::value_type));

        for (const auto *p = termsData.offset + o, *const e = termsData.offset + termsData.size(); p != e;)
        {
                const auto commonPrefixLen = *p++;
                const auto suffixLen = *p++;

                memcpy(termStorage + commonPrefixLen * sizeof(str8_t::value_type), p, suffixLen * sizeof(str8_t::value_type));
                p += suffixLen * sizeof(str8_t::value_type);

                const auto curTermLen = commonPrefixLen + suffixLen;
                const auto r = terms_cmp(q.data(), q.size(), termStorage, curTermLen);

                if (r < 0)
                {
                        // definitely not here
                        break;
                }
                else if (r == 0)
                {
                        term_index_ctx tctx;

                        tctx.documents = Compression::decode_varuint32(p);
                        tctx.indexChunk.len = Compression::decode_varuint32(p);
                        tctx.indexChunk.offset = *(uint32_t *)p;

                        return tctx;
                }
                else
                {
                        Compression::decode_varuint32(p);
                        Compression::decode_varuint32(p);
                        p += sizeof(uint32_t);
                }
        }

        return {};
}

void Trinity::unpack_terms_skiplist(const range_base<const uint8_t *, const uint32_t> termsIndex, Switch::vector<Trinity::terms_skiplist_entry> *skipList, simple_allocator &allocator)
{
	for (const auto *p = reinterpret_cast<const uint8_t *>(termsIndex.start()), *const e = p + termsIndex.size(); p != e;)
	{
		auto t = skipList->PushEmpty();
		const str8_t term((char *)p + 1, *p);
		p += (term.size() * sizeof(str8_t::value_type)) + sizeof(uint8_t);
#ifdef TRINITY_TERMS_FAT_INDEX
		{
			t->tctx.documents = Compression::decode_varuint32(p);
			t->tctx.indexChunk.len = Compression::decode_varuint32(p);
			t->tctx.indexChunk.offset = *(uint32_t *)p; p+=sizeof(uint32_t);
		}
#endif
		t->blockOffset = Compression::decode_varuint32(p);
		t->term.Set(allocator.CopyOf(term.data(), term.size()), term.size());
	}
}

void Trinity::pack_terms(std::vector<std::pair<str8_t, term_index_ctx>> &terms, IOBuffer *const data, IOBuffer *const index)
{
        static constexpr uint32_t SKIPLIST_INTERVAL{128};	 // 128 or 64 is more than fine
        uint32_t nextSkipListEntry{1}; 	// so that we will output for the first term (required)
        str8_t prev;

        std::sort(terms.begin(), terms.end(), [](const auto &a, const auto &b) {
                return terms_cmp(a.first.data(), a.first.size(), b.first.data(), b.first.size()) < 0;
        });

	for (const auto &it : terms)
        {
                const auto cur = it.first;

                if (--nextSkipListEntry == 0)
                {
                        // store (term, terms file offset, terminfo) in terms index
                        // skip that term, will be in the index
                        nextSkipListEntry = SKIPLIST_INTERVAL;

                        index->pack(uint8_t(cur.size()));
                        index->serialize(cur.data(), cur.size() * sizeof(str8_t::value_type));
#ifdef TRINITY_TERMS_FAT_INDEX
                        {
                                index->encode_varuint32(it.second.documents);
                                index->encode_varuint32(it.second.indexChunk.len);
                                index->pack(it.second.indexChunk.offset);
                        }
#endif
                        index->encode_varuint32(data->size()); // offset in the terms data file

                }
#ifdef TRINITY_TERMS_FAT_INDEX
                else
#endif
                {
                        const auto commonPrefix = cur.CommonPrefixLen(prev);
                        const auto suffix = cur.SuffixFrom(commonPrefix);

                        data->pack(uint8_t(commonPrefix), uint8_t(suffix.size()));
                        data->serialize(suffix.data(), suffix.size() * sizeof(str8_t::value_type));
                        {
                                data->encode_varuint32(it.second.documents);
                                data->encode_varuint32(it.second.indexChunk.len);
                                data->pack(it.second.indexChunk.offset);
                        }
                }

                prev = cur;
        }
}

Trinity::SegmentTerms::SegmentTerms(const char *segmentBasePath)
{
        int fd;

        fd = open(Buffer{}.append(segmentBasePath, "/terms.idx").c_str(), O_RDONLY | O_LARGEFILE);
        if (fd == -1)
                throw Switch::system_error("Failed to access terms.idx");
        else if (const auto fileSize = lseek64(fd, 0, SEEK_END))
        {
                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                close(fd);
                expect(fileData != MAP_FAILED);
                madvise(fileData, fileSize, MADV_SEQUENTIAL);

                unpack_terms_skiplist({static_cast<const uint8_t *>(fileData), uint32_t(fileSize)}, &skiplist, allocator);
        }
        else
                close(fd);

        fd = open(Buffer{}.append(segmentBasePath, "/terms.data").c_str(), O_RDONLY | O_LARGEFILE);
        if (fd == -1)
                throw Switch::system_error("Failed to access terms.data");

        const auto fileSize = lseek64(fd, 0, SEEK_END);
        auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

        close(fd);
        expect(fileData != MAP_FAILED);

        termsData.Set(reinterpret_cast<const uint8_t *>(fileData), fileSize);
}

void Trinity::terms_data_view::iterator::decode_cur()
{
	if (!cur.term)
	{
		const auto commonPrefixLen = *p++;
		const auto suffixLen = *p++;

		memcpy(termStorage + commonPrefixLen * sizeof(str8_t::value_type), p, suffixLen * sizeof(str8_t::value_type));
		p += suffixLen * sizeof(str8_t::value_type);

		cur.term.len = commonPrefixLen + suffixLen;
		cur.tctx.documents = Compression::decode_varuint32(p);
		cur.tctx.indexChunk.len = Compression::decode_varuint32(p);
		cur.tctx.indexChunk.offset = *(uint32_t *)p;
		p += sizeof(uint32_t);
	}
}
