#include "docidupdates.h"
#include <ansifmt.h>
#include <switch_bitops.h>
#include <boost/sort/spreadsort/spreadsort.hpp>

// packs a list of updated/delete documents into a buffer that also contains
// a skiplist for random access to the bitmaps
void Trinity::pack_updates(std::vector<docid_t> &updatedDocumentIDs, IOBuffer *const buf) {
        if (updatedDocumentIDs.size()) {
                static constexpr size_t BANK_SIZE{32 * 1024};
                IOBuffer                skiplist;
                static_assert((BANK_SIZE & 63) == 0, "Not divisable by 64");
                auto bf = reinterpret_cast<uint64_t *>(calloc(updated_documents::K_bloom_filter_size / 64 + 1, sizeof(uint64_t)));

                boost::sort::spreadsort::spreadsort(updatedDocumentIDs.begin(), updatedDocumentIDs.end());

#if 0 // We are now throwing an exception if we attempt to erase or update the same document more than once in Trinity::SegmentIndexSession \
		// We could have filtered dupes int he main loop, but keep it simple                                                                  \
		// We will throw an exception if we attempt to update a document twice, but we can safely erase a document however many times
		updatedDocumentIDs.resize(std::unique(updatedDocumentIDs.begin(), updatedDocumentIDs.end()) - updatedDocumentIDs.begin());
#endif

                for (const auto *p = updatedDocumentIDs.data(), *const e = p + updatedDocumentIDs.size(); p != e;) {
                        // We can create a bitmap
                        // which will come down to about 4k that can hold 32k documents
                        // which allows for O(1) access
                        // For 10million IDs, this comes down to about 2MBs which is not much, considering
                        // how we are going to be advancing one range/time
                        const auto id = *p, base = id;
                        const auto upto = id + BANK_SIZE;

                        buf->reserve(BANK_SIZE / 8);

                        auto *const bm = reinterpret_cast<uint64_t *>(buf->end());

                        memset(bm, 0, BANK_SIZE / 8);

                        skiplist.pack(id);
                        do {
                                const auto id  = *p;
                                const auto rel = id - base;
                                const auto h   = id & (updated_documents::K_bloom_filter_size - 1);

                                bf[h / 64] |= static_cast<uint64_t>(1) << (h & 63);

                                SwitchBitOps::Bitmap<uint64_t>::Set(bm, rel);
                        } while (++p != e && *p < upto);

                        buf->advance_size(BANK_SIZE / 8);
                }

                buf->serialize(bf, updated_documents::K_bloom_filter_size / 8);
                free(bf);

                buf->pack(uint8_t(log2(BANK_SIZE)));                              // 1 byte will suffice
                buf->pack(static_cast<uint8_t>(0));                               // 0 if bloom filter is included
                buf->serialize(skiplist.data(), skiplist.size());                 // skiplist
                buf->pack(uint32_t(skiplist.size() / sizeof(docid_t)));           // TODO: use varint encoding here
                buf->pack(updatedDocumentIDs.front(), updatedDocumentIDs.back()); //lowest, highest
        }
}

// see pack_updates()
// use this function to unpack the represetnation we need to access the packed (into bitmaps)
// updated documents
Trinity::updated_documents Trinity::unpack_updates(const range_base<const uint8_t *, uint32_t> content) {
        if (content.size() <= sizeof(uint32_t) + sizeof(uint8_t)) {
                return {};
        }

        const auto *const b = content.start();
        const auto *      p = b + content.size();

        p -= sizeof(docid_t);
        const auto highest = *reinterpret_cast<const docid_t *>(p);
        p -= sizeof(docid_t);
        const auto lowest = *reinterpret_cast<const docid_t *>(p);

        p -= sizeof(uint32_t);
        const auto skiplistSize = *reinterpret_cast<const uint32_t *>(p);
        p -= skiplistSize * sizeof(uint32_t);

        const auto      skiplist = reinterpret_cast<const docid_t *>(p);
        const uint64_t *bloom_filter;
        uint32_t        bank_size;

        if (*(--p) == 0) {
                // we have a bloom filter
                bank_size = 1u << *(--p);

                p -= updated_documents::K_bloom_filter_size / 8;
                bloom_filter = reinterpret_cast<const uint64_t *>(p);
        } else {
                bank_size    = 1u << *p;
                bloom_filter = nullptr;
        }

        EXPECT(p - content.start() == bank_size / 8 * skiplistSize);
        return {skiplist, skiplistSize, bank_size, b, lowest, highest, bloom_filter};
}

bool Trinity::updated_documents_scanner::test(const docid_t id) noexcept {
        static constexpr bool trace{false}, traceAdvances{false};

        if constexpr (trace) {
                SLog(ansifmt::bold, "Check for ", id, ", curBankRange = ", curBankRange, ", contains ", curBankRange.Contains(id), ansifmt::reset, "\n");
        }


	if (unlikely(id > maxDocID)) {
		// fast-path; flag it as drained
		curBankRange.offset = UINT32_MAX;
		return false;
	} else if (id < low_doc_id) {
		// fast-path: definitely not here
		return false;
	} else if (const auto m = bf) {
                const uint64_t h = id & (updated_documents::K_bloom_filter_size - 1);

                if (0 == (m[h / 64] & (static_cast<uint64_t>(1) << (h & 63)))) {
                        // fast-path: definitely not here
                        return false;
                }
        }

        if (id >= curBankRange.start()) {
                if (id < curBankRange.stop()) {
                        if constexpr (trace) {
                                SLog("In bank range ", id - curBankRange.offset, "\n");
                        }

                        return SwitchBitOps::Bitmap<uint64_t>::IsSet((uint64_t *)curBank, id - curBankRange.offset);
                } else if (id > maxDocID) {
                        reset();
                        return false;
                } else {
                        int32_t btm{0};

                        if constexpr (traceAdvances) {
                                SLog("Binary search FOR ", id, " ", curBankRange, " ", curBankRange.size(), ", maxDocID = ", maxDocID, "\n");
                        }

                        // binary search highest bank, where id < bank.end
                        // There's no need to check for success, we already checked for (id > maxDocID)
                        for (int32_t top{static_cast<int32_t>(end - skiplistBase) - 1}; btm <= top;) {
                                const auto mid = (btm + top) / 2;
                                const auto end = skiplistBase[mid] + bankSize;

                                if (id < end) {
                                        top = mid - 1;
                                } else {
                                        btm = mid + 1;
                                }
                        }

                        skiplistBase += btm;
                        curBankRange.Set(*skiplistBase, bankSize);
                        curBank = udBanks + ((skiplistBase - udSkipList) * (bankSize / 8));

                        if constexpr (trace || traceAdvances) {
                                SLog("Now at ", skiplistBase - udSkipList, " => ", curBankRange, " ", curBankRange.Contains(id), "\n");
                        }

                        if (curBankRange.Contains(id)) {
                                if constexpr (trace) {
                                        SLog("REL = ", id - curBankRange.offset, "\n");
                                }

                                return SwitchBitOps::Bitmap<uint64_t>::IsSet((uint64_t *)curBank, id - curBankRange.offset);
                        } else {
                                if constexpr (trace) {
                                        SLog("id ", id, " out of range of ", curBankRange, "\n");
                                }

                                return false;
                        }
                }
        } else {
                if constexpr (trace) {
                        SLog("Not Even\n");
                }

                return false;
        }
}
