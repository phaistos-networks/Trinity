#pragma once
#include "common.h"

// Whatever's here is specific to the "accumuluated score scheme" execution mode, where
// we just aggregate a similarity score for each iterator on the "current" document, similar to what Lucene's doing.
namespace Trinity
{
        namespace Similarity
        {
                class Scorer
                {
			public:
			// `cnt` is the number of matches of a term, or a phrase, in the document `id`
                        virtual float score(const isrc_docid_t id, const uint16_t cnt) = 0;

                        virtual ~Scorer()
			{

			}
                };

                class TrivialScorer final
                    : public Scorer
                {
			public:
                        inline float score(const isrc_docid_t, const uint16_t cnt) override final
                        {
                                return cnt;
                        }
                };
        }
}
