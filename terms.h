#pragma once
#include <switch.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include <compress.h>
#include "codecs.h"


// Prefic compressed terms dictionary
// Maps from str8_t=>term_index_ctx
namespace Trinity
{

// We can no longer ommit (term, term_index_ctx) from the terms data file and keep
// that just in the index, beause while it works great for lookups, it means we can't trivially iterate
// over all terms in the terms data file (see terms_data_view struct), and this is important for merging segments.
// For other applications that do not need to access to all terms, one couild get those structures, make sure TRINITY_TERMS_FAT_INDEX is defined
// and use it .
//#define TRINITY_TERMS_FAT_INDEX 
	struct terms_skiplist_entry final
        {
                str8_t term;
#ifdef TRINITY_TERMS_FAT_INDEX
                uint32_t blockOffset; 	// offset in the terms datafile
                term_index_ctx tctx;	// payload
#else
                uint32_t blockOffset; 	// offset in the terms datafile
#endif
        };

        term_index_ctx lookup_term(range_base<const uint8_t *, uint32_t> termsData, const str8_t term, const Switch::vector<terms_skiplist_entry> &skipList);

        void unpack_terms_skiplist(const range_base<const uint8_t *, const uint32_t> termsIndex, Switch::vector<terms_skiplist_entry> *skipList, simple_allocator &allocator);

        void pack_terms(std::vector<std::pair<str8_t, term_index_ctx>> &terms, IOBuffer *const data, IOBuffer *const index);

	// An abstract index source terms access wrapper
	// For segments, you will likely use the prefix-compressed terms infra. but you may have
	// an index source that is e.g storing all those terms in an in-memory std::unordered_map<> or whatever else
	// for some reason and you can just write an IndexSourceTermsView subclass to access that.
	//
	// IndexSourceTermsView subclasses are used while merging index sources
	//
	// see merge.h
	struct IndexSourceTermsView
	{
		virtual std::pair<str8_t, term_index_ctx> cur() = 0;

		virtual void next() = 0;

		virtual bool done() = 0;
	};


	// iterator access to the terms data
	// this is very useful for merging terms dictionaries (see IndexSourcePrefixCompressedTermsView)
	struct terms_data_view final
        {
              public:
                struct iterator final
                {
                        friend struct terms_data_view;

                      public:
                        const uint8_t *p;
                        str8_t::value_type termStorage[Limits::MaxTermLength];
                        struct
                        {
                                str8_t term;
                                term_index_ctx tctx;
                        } cur;

                        iterator(const uint8_t *ptr)
                            : p{ptr}
                        {
                                cur.term.p = termStorage;
				cur.term.len = 0;
                        }

                        inline bool operator==(const iterator &o) const noexcept
                        {
                                return p == o.p;
                        }

                        inline bool operator!=(const iterator &o) const noexcept
                        {
                                return p != o.p;
                        }

                        str8_t term() noexcept
                        {
                                decode_cur();
                                return cur.term;
                        }

                        term_index_ctx tctx() noexcept
                        {
                                decode_cur();
                                return cur.tctx;
                        }

                        inline iterator &operator++()
                        {
                                cur.term.len = 0;
                                return *this;
                        }

                        inline std::pair<str8_t, term_index_ctx> operator*() noexcept
                        {
                                decode_cur();
                                return {cur.term, cur.tctx};
                        }

                      protected:
                        void decode_cur();
                };

              private:
                const range_base<const uint8_t *, uint32_t> termsData;

              public:
                iterator begin() const
                {
                        return {termsData.start()};
                }

                iterator end() const
                {
                        return {termsData.stop()};
                }

                terms_data_view(const range_base<const uint8_t *, uint32_t> d)
                    : termsData{d}
                {
                }
        };

	// A specialised IndexSourceTermsView for accessing prefix-encoded terms dictionaries
	struct IndexSourcePrefixCompressedTermsView final
		: public IndexSourceTermsView
        {
              private:
                terms_data_view::iterator it, end;

              public:
                IndexSourcePrefixCompressedTermsView(const range_base<const uint8_t *, uint32_t> termsData)
                    : it{termsData.start()}, end{termsData.stop()}
                {
                }

                std::pair<str8_t, term_index_ctx> cur() override final
                {
                        return *it;
                }

                void next() override final
                {
                        ++it;
                }

                bool done() override final
                {
                        return it == end;
                }
        };

	//A handy wrapper for memory mapped terms data and a skiplist from the terms index
        class SegmentTerms final
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

			term_index_ctx lookup(const str8_t term)
			{
				return lookup_term(termsData, term, skiplist);
			}

			auto terms_data_access() const
			{ 
				return terms_data_view(termsData);
			}

			auto new_terms_view() const
			{
				return new IndexSourcePrefixCompressedTermsView(termsData);
			}
	};
}
