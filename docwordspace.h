#pragma once
#include "runtime.h"

namespace Trinity
{
	class DocWordsSpace final
        {
              private:
	      	// just 4 bytes/position
		// we could have separated docSeq and termID into different arrays(as in, not in the same struct) so that reset()
		// would only memset() the docSeq array(2 bytes vs 4 bytes) * maxPos
		// but that 'd make access somewhat slower for set() and test() because we 'd get more cache misses so we optimise for it
                struct position
                {
                        uint16_t docSeq;
                        exec_term_id_t termID;  // See IMPL.md
                };

                position *const positions;
                const uint32_t maxPos;
                uint16_t curSeq{0};

              public:
                DocWordsSpace(const uint32_t max)
			: positions((position *)calloc(sizeof(position), max)), maxPos{max}
		{

		}

		~DocWordsSpace()
		{
                        free(positions);
		}

		void reset(const uint32_t did)
		{
			// in order to avoid resetting/clearing positions[] for every other document
			// we track a document-specific identifier in positions[] so if positions[idx].docSeq != curDocSeq
			// then whatever is in positions[] is stale and should be considered unset.
			// In a previous Trinity design we stored the document ID as u32 but that is excessive, we care about cache-misses
			// so now we instead use a `uint16_t curSeq` and periodically clear. This should be more efficient.
                        if (curSeq == UINT16_MAX)
                        {
				// we reset every 65k documents
				// this is preferrable to using uint32_t to encode the actual document in position{}
                                curSeq = 0;
                                memset(positions, 0, sizeof(position) * maxPos);
                        }
                        else
                                ++curSeq;
		}

		[[gnu::always_inline]] void set(const exec_term_id_t termID, const uint16_t pos) noexcept
		{
                        positions[pos] = {curSeq, termID};
		}

		bool set_checkprev_set(const exec_term_id_t termID, const uint16_t pos, const uint16_t phrasePrevTermID) noexcept
		{
                        positions[pos] = {curSeq, termID};
                        return positions[pos - 1].docSeq == curSeq && positions[pos - 1].termID == phrasePrevTermID;
		}

		inline bool test(const exec_term_id_t termID, const uint16_t pos) const noexcept
		{
			return positions[pos].docSeq == curSeq && positions[pos].termID == termID;
		}

		// We can probably just sort all phrase terms by freq asc
		// and iterate across all hits, and for each hit, see if it is set
		// in the adjacement position for the next phrase term, and the next, and so on, but 
		// we would need to track the offset(relative index in the phrase)
		// This is an example/reference implementation
		bool test_phrase(const std::vector<exec_term_id_t> &phraseTerms, const uint16_t *phraseFirstTokenPositions, const uint16_t phraseFirstTokenPositionsCnt) const;
        };
}
