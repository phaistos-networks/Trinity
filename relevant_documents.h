#pragma once
#include "common.h"

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundefined-inline"
#endif

namespace Trinity
{
	// We need to support an execution mode which matches Lucene's, which
	// can be useful in specific applications, like Visual Search, where you
	// care a lot about performance because you may be dealing with 'queries' made up of
	// 1000s of "terms"(for this example, those would be image features extracted by e.g OpenCV),
	// but you also want some sort of score based on a statistical model.
	//
	// We need to inflate Iterator by making it a subclass of relevant_document_provider and also
	// make MatchesProxy::process() accept a relevant_document_provider* as opposed to a simple isrc_docid_t
	// which does have a slight impact on runtime perf., but it's worth it to support the semantics we need.

//#define RDP_NEED_TOTAL_MATCHES 1
	struct relevant_document_provider
	{
		virtual isrc_docid_t document() const noexcept = 0;

#ifdef RDP_NEED_TOTAL_MATCHES
		virtual uint32_t total_matches() = 0;
#endif
		inline double score();
	};


	namespace DocsSetIterators
	{
		struct Iterator;
	}

	struct IteratorWrapper
		: public relevant_document_provider
	{
		DocsSetIterators::Iterator *const it;

		IteratorWrapper(DocsSetIterators::Iterator *const it_)
			: it{it_}
		{
		}

		virtual ~IteratorWrapper()
		{
		}

#ifdef RDP_NEED_TOTAL_MATCHES
		inline uint32_t total_matches() override final;
#endif

		inline isrc_docid_t document() const noexcept override final;

		// Will return the wrapped iterator, except for e.g Filter and Optional
		// where we only really want to advance the required or main respectively iterator
		// For now, this is not supported, but it may be in the future as an optimization.
		// TODO: consider this
		virtual DocsSetIterators::Iterator *iterator() 
		{
			return it;
		}

		virtual double iterator_score() = 0;
	};

        double relevant_document_provider::score()
	{
		// if you tried to get a score, then this is a IteratorWrapper
		// we used to virtual double score() = 0 here
		// but we really don't want to inflate Iterator's vtable with it
		return static_cast<IteratorWrapper *>(this)->iterator_score();
	}
}


#ifdef __clang__
#pragma GCC diagnostic pop
#endif
