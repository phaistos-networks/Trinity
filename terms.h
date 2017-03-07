#pragma once
#include <switch.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include "codecs.h"

namespace Trinity
{
	struct terms_skiplist_entry
        {
                strwlen8_t term;
                uint32_t blockOffset; 	// offset in the terms datafile
                term_index_ctx tctx;	// payload
        };

        term_index_ctx lookup_term(range_base<const uint8_t *, uint32_t> termsData, const strwlen8_t term, const Switch::vector<terms_skiplist_entry> &skipList);

        void unpack_terms_skiplist(const range_base<const uint8_t *, const uint32_t> termsIndex, Switch::vector<terms_skiplist_entry> *skipList, simple_allocator &allocator);

        void pack_terms(std::vector<std::pair<strwlen8_t, term_index_ctx>> &terms, IOBuffer *const data, IOBuffer *const index);

	class SegmentTerms
	{
		private:
			Switch::vector<terms_skiplist_entry> skiplist;
			simple_allocator allocator;
			range_base<const uint8_t *, uint32_t> termsData;


		public:
			SegmentTerms(const char *segmentBasePath);

			~SegmentTerms()
			{
                                if (auto ptr = (void *)(termsData.offset))
                                        munmap(ptr, termsData.size());
			}

			term_index_ctx lookup(const strwlen8_t term)
			{
				return lookup_term(termsData, term, skiplist);
			}
	};
};
