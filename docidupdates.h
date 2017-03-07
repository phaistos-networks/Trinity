#pragma once
#include <switch.h>


namespace Trinity
{
        struct updated_documents final
        {
                const uint32_t *skiplist;
                const uint32_t skiplistSize;
                const uint32_t bankSize;
                const uint8_t *banks;
        };

        // Facilitates fast set test operations for updated/deleted documents packed
        // as bitmaps using pack_updates()
        struct updated_documents_scanner final
        {
                const uint32_t *const end;
                const uint32_t bankSize;

                range32_t curBankRange;
                const uint32_t *skiplistBase;
                const uint8_t *curBank;

                const uint32_t *const udSkipList;
                const uint8_t *const udBanks;


                void reset()
                {
                        curBankRange.Set(UINT32_MAX, 0);
                }

                updated_documents_scanner(const updated_documents &ud)
                    : end{ud.skiplist + ud.skiplistSize}, bankSize{ud.bankSize}, skiplistBase{ud.skiplist}, udSkipList{ud.skiplist}, udBanks{ud.banks}
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
                bool test(const uint32_t id) noexcept;

		inline bool operator==(const updated_documents_scanner &o) const noexcept
                {
                        return end == o.end && bankSize == o.bankSize && curBankRange == o.curBankRange && skiplistBase == o.skiplistBase && curBank == o.curBank && udSkipList == o.udSkipList && udBanks == o.udBanks;
                }
        };

	void pack_updates(std::vector<uint32_t> &updatedDocumentIDs, IOBuffer *const buf);

	updated_documents unpack_updates(const range_base<const uint8_t *, uint32_t> content);

	struct dids_scanner_registry final
	{
		bool test(const uint32_t id)
                {
                        for (uint8_t i{0}; i < rem;)
                        {
				auto it = scanners + i;

                                if (it->test(id))
                                        return true;
                                else if (it->drained())
				{
                                        new (it) updated_documents_scanner(scanners[--rem]);
					require(*it == scanners[rem + 1]);
				}	
                                else
                                        ++i;
                        }

                        return false;
                }

                uint8_t rem;
		updated_documents_scanner scanners[0];		

		static dids_scanner_registry *make(const updated_documents *ud, const uint8_t n)
		{
			auto ptr = (dids_scanner_registry *)malloc(sizeof(dids_scanner_registry) + sizeof(updated_documents_scanner) * n);

			ptr->rem = n;

			for (uint32_t i{0}; i != n; ++i)
				new (&ptr->scanners[i])updated_documents_scanner(ud[i]);

			return ptr;
		}
	};
}
