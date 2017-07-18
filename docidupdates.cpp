#include "docidupdates.h"
#include <ansifmt.h>
#include <switch_bitops.h>

// packs a list of updated/delete documents into a buffer that also contains
// a skiplist for random access to the bitmaps
void Trinity::pack_updates(std::vector<docid_t> &updatedDocumentIDs, IOBuffer *const buf)
{
        if (updatedDocumentIDs.size())
        {
                static constexpr size_t BANK_SIZE{32 * 1024};
                static_assert((BANK_SIZE & 63) == 0, "Not divisable by 64");
                IOBuffer skiplist;

                std::sort(updatedDocumentIDs.begin(), updatedDocumentIDs.end());

#if 0 	// We are now throwing an exception if we attempt to erase or update the same document more than once in Trinity::SegmentIndexSession
		// We could have filtered dupes int he main loop, but keep it simple
		// We will throw an exception if we attempt to update a document twice, but we can safely erase a document however many times
                updatedDocumentIDs.resize(std::unique(updatedDocumentIDs.begin(), updatedDocumentIDs.end()) - updatedDocumentIDs.begin());
#endif

                for (const auto *p = updatedDocumentIDs.data(), *const e = p + updatedDocumentIDs.size(); p != e;)
                {
                        // We can create a bitmap
                        // which will come down to about 4k that can hold 32k documents
                        // which allows for O(1) access
                        // For 10million IDs, this comes down to about 2MBs which is not much, considering
                        // how we are going to be advancing one range/time
                        const auto id = *p, base = id;
                        const auto upto = id + BANK_SIZE;

                        buf->reserve(BANK_SIZE / 8);

                        auto *const bm = (uint64_t *)buf->end();

                        memset(bm, 0, BANK_SIZE / 8);

                        skiplist.pack(id);
                        do
                        {
                                const auto rel = *p - base;

                                SwitchBitOps::Bitmap<uint64_t>::Set(bm, rel);
                        } while (++p != e && *p < upto);

                        buf->advance_size(BANK_SIZE / 8);
                }

                buf->pack(uint8_t(log2(BANK_SIZE)));                              // 1 byte will suffice
                buf->serialize(skiplist.data(), skiplist.size());                 // skiplist
                buf->pack(uint32_t(skiplist.size() / sizeof(docid_t)));           // TODO: use varint encoding here
                buf->pack(updatedDocumentIDs.front(), updatedDocumentIDs.back()); //lowest, highest
        }
}

// see pack_updates()
// use this function to unpack the represetnation we need to access the packed (into bitmaps)
// updated documents
Trinity::updated_documents Trinity::unpack_updates(const range_base<const uint8_t *, uint32_t> content)
{
        if (content.size() <= sizeof(uint32_t) + sizeof(uint8_t))
                return {};

        const auto *const b = content.start();
        const auto *p = b + content.size();

        p -= sizeof(docid_t);
        const auto highest = *(docid_t *)p;
        p -= sizeof(docid_t);
        const auto lowest = *(docid_t *)p;

        p -= sizeof(uint32_t);
        const auto skiplistSize = *(uint32_t *)p;
        p -= skiplistSize * sizeof(uint32_t);

        const auto skiplist = reinterpret_cast<const docid_t *>(p);
        const uint32_t bankSize = 1 << (*(--p));

        require(p - content.start() == bankSize / 8 * skiplistSize);
        return {skiplist, skiplistSize, bankSize, b, lowest, highest};
}

bool Trinity::updated_documents_scanner::test(const docid_t id) noexcept
{
        static constexpr bool trace{false};

        if (trace)
                SLog(ansifmt::bold, "Check for ", id, ", curBankRange = ", curBankRange, ", contains ", curBankRange.Contains(id), ansifmt::reset, "\n");

        if (id >= curBankRange.start())
        {
                if (id < curBankRange.stop())
                {
                        if (trace)
                                SLog("In bank range ", id - curBankRange.offset, "\n");

                        return SwitchBitOps::Bitmap<uint64_t>::IsSet((uint64_t *)curBank, id - curBankRange.offset);
                }
                else if (id > maxDocID)
                {
                        reset();
                        return false;
                }
                else
                {
                        // skip ahead using binary search
                        // Look for the first element in the skiplist that is <= `id`
                        int32_t top{int32_t(end - skiplistBase) - 1};

                        for (int32_t btm{0}; btm <= top;)
                        {
                                const auto mid = (btm + top) / 2;
                                const auto v = skiplistBase[mid];

                                if (id < v)
                                        top = mid - 1;
                                else if (id == v)
                                {
                                        top = mid;
                                        break;
                                }
                                else
                                        btm = mid + 1;
                        }

                        if (unlikely(top == -1))
                        {
                                if (trace)
                                        SLog("Definitely not here because top = -1\n");

                                reset();
                                return false;
                        }
                        else
                        {
                                skiplistBase += top;
                                curBankRange.Set(*skiplistBase, bankSize);
                                curBank = udBanks + ((skiplistBase - udSkipList) * (bankSize / 8));

                                if (trace)
                                        SLog("Now at ", skiplistBase - udSkipList, " => ", curBankRange, "\n");

                                if (curBankRange.Contains(id))
                                {
                                        // this is is important
                                        // imagine if cur bank range is [1, 65) and second bank is [150, 215)
                                        // and id is 68
                                        // then we 'd skip to [150, 215) bank but
                                        // that's ahead of our target(68)
                                        if (trace)
                                                SLog("REL = ", id - curBankRange.offset, "\n");

                                        return SwitchBitOps::Bitmap<uint64_t>::IsSet((uint64_t *)curBank, id - curBankRange.offset);
                                }
                                else
                                {
                                        if (trace)
                                                SLog("id ", id, " out of range of ", curBankRange, "\n");

                                        return false;
                                }
                        }
                }
        }
        else
        {
                if (trace)
                        SLog("Not Even\n");

                return false;
        }
}
