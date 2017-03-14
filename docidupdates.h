#pragma once
#include <switch.h>
#include <memory>
#include "common.h"

// Efficient, lean, fixed-size bitmaps based document IDs tracking
// You are expected to test for document IDs in ascending order
namespace Trinity
{
        struct updated_documents final
        {
		// Each bitmaps bank can be accessed by a skiplist via binary search
                const docid_t *skiplist;
                const uint32_t skiplistSize;

		// Fixed size bitmap banks
                const uint32_t bankSize;
                const uint8_t *banks;

		docid_t lowestID;
		docid_t highestID;
		
		inline operator bool() const
		{
			return banks;
		}
        };

        // Facilitates fast set test operations for updated/deleted documents packed
        // as bitmaps using pack_updates()
        struct updated_documents_scanner final
        {
                const docid_t *const end;
                const uint32_t bankSize;

                range32_t curBankRange;
                const docid_t *skiplistBase;
                const uint8_t *curBank;

                const docid_t *const udSkipList;
                const uint8_t *const udBanks;

		const range_base<docid_t, docid_t> documentsRange;


                void reset()
                {
                        curBankRange.Set(UINT32_MAX, 0);
                }

                updated_documents_scanner(const updated_documents &ud)
                    : end{ud.skiplist + ud.skiplistSize}, bankSize{ud.bankSize}, skiplistBase{ud.skiplist}, udSkipList{ud.skiplist}, udBanks{ud.banks}, documentsRange{ud.lowestID, ud.highestID - ud.lowestID + 1}
                {
                        if (skiplistBase != end)
                        {
                                curBankRange.Set(*skiplistBase, ud.bankSize);
                                curBank = udBanks;
                        }
                }

		updated_documents_scanner(const updated_documents_scanner &o)
			: end{o.end}, bankSize{o.bankSize}, skiplistBase{o.skiplistBase}, udSkipList{o.udSkipList}, udBanks{o.udBanks}
		{
			curBankRange = o.curBankRange;
			curBank = o.curBank;
		}


                constexpr bool drained() noexcept
                {
                        return curBankRange.offset == UINT32_MAX;
                }


                // You are expected to test monotonically increasing document IDs
                bool test(const docid_t id) noexcept;

		inline bool operator==(const updated_documents_scanner &o) const noexcept
                {
                        return end == o.end && bankSize == o.bankSize && curBankRange == o.curBankRange && skiplistBase == o.skiplistBase && curBank == o.curBank && udSkipList == o.udSkipList && udBanks == o.udBanks;
                }
        };

	void pack_updates(std::vector<docid_t> &updatedDocumentIDs, IOBuffer *const buf);

	updated_documents unpack_updates(const range_base<const uint8_t *, docid_t> content);


	// manages multiple scanners and tests among all of them, and if any of them is exchausted, it is removed from the collection
	struct masked_documents_registry final
	{
		bool test(const docid_t id)
                {
                        for (uint8_t i{0}; i < rem;)
                        {
				auto it = scanners + i;

                                if (it->test(id))
                                        return true;
                                else if (it->drained())
				{
                                        new (it) updated_documents_scanner(scanners[--rem]);
					require(*it == scanners[rem]);
				}	
                                else
                                        ++i;
                        }

                        return false;
                }

                uint8_t rem;
		updated_documents_scanner scanners[0];		

		masked_documents_registry()
			: rem{0}
		{
		}


		static std::unique_ptr<Trinity::masked_documents_registry> make(const updated_documents *ud, const uint8_t n)
                {
			// ASAN will complain that about alloc-dealloc-mismatch
			// because we are using placement new operator and apparently there is no way to tell ASAN that this is fine
			// I need to figure this out
			// TODO: do whatever makes sense here later
                        auto ptr = new (malloc(sizeof(masked_documents_registry) + sizeof(updated_documents_scanner) * n)) masked_documents_registry();

                        ptr->rem = n;

                        for (uint32_t i{0}; i != n; ++i)
                                new (&ptr->scanners[i]) updated_documents_scanner(ud[i]);

                        return std::unique_ptr<Trinity::masked_documents_registry>(ptr);
                }
        };
}
