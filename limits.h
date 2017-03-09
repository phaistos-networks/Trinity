#pragma once
#include <stdint.h>
#include <stddef.h>

namespace Trinity
{
	namespace Limits
	{
		static constexpr size_t MaxPhraseSize{16};
		static constexpr size_t MaxQueryTokens{640};
		static constexpr size_t MaxTermLength{64};


		// Sanity check
		static_assert(MaxTermLength < 250 && MaxTermLength > 8);
		static_assert(MaxPhraseSize < 128);
		static_assert(MaxQueryTokens < 2048);
	}
}
