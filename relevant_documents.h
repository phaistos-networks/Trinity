#pragma once
#include "common.h"

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
        struct relevant_document_provider
        {
                virtual isrc_docid_t document() const noexcept = 0;

                virtual double score() = 0;
        };

        struct relevant_document final
            : public relevant_document_provider
        {
                isrc_docid_t id;
		double score_;

                inline isrc_docid_t document() const noexcept override final
                {
                        return id;
                }

		inline double score() override final
		{
			return score_;
		}
        };

	// Lucene's Scorer creates/owns an iterator and provides a score()
	// as opposed to what we do where the iterator provides the score and is itself a relevant document provider (i.e something like
	// a Scorer)
	// This makes sense because scoring is not important to Trinity, it's another query execution mode, but not the default.
}
