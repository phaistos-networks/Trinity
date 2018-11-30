#pragma once
#include "common.h"
#include <memory>
#include <switch.h>

// Efficient, lean, fixed-size bitmaps based document IDs tracking
// You are expected to test for document IDs in ascending order, but if you need a different behavior, it should be easy to modify
// the implementation to accomplish it.
//
// Note that it operates on docit_t, not on isrc_docid_t. Because we store isrc_docid in ascending order in
// postings lists, but that doesn't guarantee that the translated global document IDs will also be
// in ascending order, we should either account for that, or use e.g a SparseFixedBitSet which is
// almost as fast, takes up less memory and is great for random access
namespace Trinity {
        struct updated_documents final {
                static constexpr size_t K_bloom_filter_size{256 * 1024};
                static_assert(0 == (K_bloom_filter_size & 1));
                // Each bitmaps bank can be accessed by a skiplist via binary search
                const docid_t *skiplist;
                const uint32_t skiplistSize;

                // Fixed size bitmap banks
                const uint32_t bankSize;
                const uint8_t *banks;

                docid_t lowestID;
                docid_t highestID;

                const uint64_t *bf;

                inline operator bool() const {
                        return banks;
                }
        };

        // Facilitates fast set test operations for updated/deleted documents packed
        // as bitmaps using pack_updates()
        struct updated_documents_scanner final {
                const docid_t *const end;
                const uint32_t       bankSize;

                range_base<docid_t, docid_t> curBankRange;
                const docid_t *              skiplistBase;
                const uint8_t *              curBank;
                docid_t                      low_doc_id;
                docid_t                      maxDocID;
                const docid_t *const         udSkipList;
                const uint8_t *const         udBanks;
                const uint64_t *const        bf;

                void reset() {
                        maxDocID   = std::numeric_limits<docid_t>::max();
                        low_doc_id = 0;
                        curBankRange.Set(maxDocID, 0);
                }

                updated_documents_scanner(const updated_documents &ud)
                    : end{ud.skiplist + ud.skiplistSize}, bankSize{ud.bankSize}, skiplistBase{ud.skiplist}, udSkipList{ud.skiplist}, udBanks{ud.banks}, low_doc_id{ud.lowestID}, maxDocID{ud.highestID}, bf{ud.bf} {
                        if (skiplistBase != end) {
                                curBankRange.Set(*skiplistBase, ud.bankSize);
                                curBank = udBanks;
                        }
                }

                updated_documents_scanner(const updated_documents_scanner &o)
                    : end{o.end}, bankSize{o.bankSize}, skiplistBase{o.skiplistBase}, udSkipList{o.udSkipList}, udBanks{o.udBanks}, bf{o.bf} {
                        low_doc_id   = o.low_doc_id;
                        maxDocID     = o.maxDocID;
                        curBankRange = o.curBankRange;
                        curBank      = o.curBank;
                }

                constexpr bool drained() const noexcept {
                        return curBankRange.offset == UINT32_MAX;
                }

                // You are expected to test monotonically increasing document IDs
                bool test(const docid_t id) noexcept;

                inline bool operator==(const updated_documents_scanner &o) const noexcept {
                        return end == o.end && bankSize == o.bankSize && curBankRange == o.curBankRange && skiplistBase == o.skiplistBase && curBank == o.curBank && udSkipList == o.udSkipList && udBanks == o.udBanks;
                }
        };

        void pack_updates(std::vector<docid_t> &updatedDocumentIDs, IOBuffer *const buf);

        updated_documents unpack_updates(const range_base<const uint8_t *, uint32_t> content);

        // manages multiple scanners and tests among all of them, and if any of them is exchausted, it is removed from the collection
        struct masked_documents_registry final {
                bool test(const docid_t id) {
                        // O(1) checks first
                        // if we have 10s or 100s of scanner to iterate, we really
                        // want to be able to check here before we get to consider them all
                        if (id < min_doc_id || id > max_doc_id) {
                                return false;
                        } else if (const auto m = bf) {
                                const uint64_t h = id & (updated_documents::K_bloom_filter_size - 1);

                                if (0 == (m[h / 64] & (static_cast<uint64_t>(1) << (h & 63)))) {
                                        // fast-path: definitely not here
                                        return false;
                                }
                        }

                        for (uint8_t i{0}; i < rem;) {
                                auto it = scanners + i;

                                if (it->test(id)) {
                                        return true;
                                } else if (it->drained()) {
                                        new (it) updated_documents_scanner(scanners[--rem]);
                                } else {
                                        ++i;
                                }
                        }

                        return false;
                }

                uint8_t                   rem;
                docid_t                   min_doc_id, max_doc_id;
                uint64_t *                bf{nullptr};
                updated_documents_scanner scanners[0];

                masked_documents_registry()
                    : rem{0} {
                }

                ~masked_documents_registry() noexcept {
                        if (bf) {
                                free(bf);
                        }
                }

                inline auto size() const noexcept {
                        return rem;
                }

                inline auto empty() const noexcept {
                        return 0 == rem;
                }

                // we no longer build a BF here
                // because this registry is materialized in every query and it's expensive-ish
                // we will instead rely on the per scanner bf
                static std::unique_ptr<Trinity::masked_documents_registry> make(const updated_documents *ud_list, const std::size_t n, const bool use_bf = false) {
                        // ASAN will complain that about alloc-dealloc-mismatch
                        // because we are using placement new operator and apparently there is no way to tell ASAN that this is fine
                        // I need to figure this out
                        // TODO: do whatever makes sense here later
                        EXPECT(n <= std::numeric_limits<uint8_t>::max());

                        auto      ptr = new (malloc(sizeof(masked_documents_registry) + sizeof(updated_documents_scanner) * n)) masked_documents_registry();
                        docid_t   min_doc_id{std::numeric_limits<docid_t>::max()}, max_doc_id{std::numeric_limits<docid_t>::min()};
                        uint64_t *bf;

                        if (use_bf) {
                                bf = reinterpret_cast<uint64_t *>(calloc(sizeof(uint64_t), updated_documents::K_bloom_filter_size / 64));
                        } else {
                                bf = nullptr;
                        }

                        ptr->rem = n;

                        for (uint32_t i{0}; i != n; ++i) {
                                const auto ud    = ud_list + i;
                                const auto ud_bf = ud->bf;

                                new (&ptr->scanners[i]) updated_documents_scanner(*ud);

                                max_doc_id = std::max(max_doc_id, ud->highestID);
                                min_doc_id = std::min(min_doc_id, ud->lowestID);
                                if (bf) {
                                        if (!ud_bf) {
                                                free(bf);
                                                bf = nullptr;
                                        } else {
                                                for (size_t i{0}; i < updated_documents::K_bloom_filter_size / 64; ++i) {
                                                        bf[i] |= ud_bf[i];
                                                }
                                        }
                                }
                        }

                        ptr->min_doc_id = min_doc_id;
                        ptr->max_doc_id = max_doc_id;
                        ptr->bf         = bf;
                        return std::unique_ptr<Trinity::masked_documents_registry>(ptr);
                }
        };
} // namespace Trinity
