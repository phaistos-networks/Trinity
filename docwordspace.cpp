#include "docwordspace.h"

bool Trinity::DocWordsSpace::test_phrase(const std::vector<exec_term_id_t> &phraseTerms, const tokenpos_t *phraseFirstTokenPositions, const tokenpos_t phraseFirstTokenPositionsCnt) const
{
	for (uint32_t i{0}; i != phraseFirstTokenPositionsCnt; ++i)
	{
		const auto pos = phraseFirstTokenPositions[i];

		for (tokenpos_t k{1};;++k)
		{
			if (k == phraseTerms.size())
				return true;
			else if (!test(phraseTerms[k], pos + k))
				break;
		}
	}
	return false;
}
