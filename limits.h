#pragma once
#include <stdint.h>
#include <stddef.h>

namespace Trinity
{
	namespace Limits
	{
		static constexpr size_t MaxPhraseSize{16};
		static constexpr size_t MaxQueryTokens{1024};
		static constexpr size_t MaxTermLength{64};
	}
}
