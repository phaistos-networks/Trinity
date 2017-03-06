#pragma once
#include "runtime.h"

namespace Trinity
{
	class DocWordsSpace final
        {
              private:
	      	// just 4 bytes/position
                struct position
                {
                        uint16_t docSeq;
                        exec_term_id_t termID; // term index into a runtime_ctx tokens
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

		void set(const exec_term_id_t termID, const uint16_t pos) noexcept
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
		// this is an example impl.
		bool test_phrase(const std::vector<exec_term_id_t> &phraseTerms, const uint16_t *phraseFirstTokenPositions, const uint16_t phraseFirstTokenPositionsCnt) const;
        };
}
