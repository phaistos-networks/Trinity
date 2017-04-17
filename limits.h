#pragma once
#include "common.h"

namespace Trinity
{
	// Google allows upto 32 different tokens in the query
	// e.g https://www.google.gr/?gfe_rd=cr&ei=hmzBWJJ3qd3wB427g_AP#q=apple+OR+samsung+OR+nokia+OR+iphone+OR+ipad+OR+microsoft+OR+the+OR+of+OR+in+OR+playstation+OR+pc+OR+xbox+OR+macbook+OR+case+OR+box+OR+mp3+OR+player+OR+tv+OR++panasonic+OR+windows+OR+out+OR+with+OR+over+OR+under+OR+soccer+OR+pro+OR+fifa+OR+ea+OR+ps2+OR+playstation+OR+nintendo+OR+fast+OR+imac+OR+pro+OR+lg+OR+adidas+OR+nike+OR+stan+OR+black+OR+white+OR+dpf+OR+air+OR+force+OR+indesit+OR+morris+OR+watch+OR+galaxy+OR+2016+OR+2017+OR+2105+OR+32gb++OR+1+OR+2+OR+10+OR+20+OR+50+OR+100+OR+a+OR+b+OR+c+OR+d+OR+foo+OR+ba++OR+greece+OR+UK+OR+US+OR+tea+OR+coffee+OR+water+OR+air+OR+space+OR+star+OR+sun+OR+foobarjuibar&*
	// > "nike" (and any subsequent words) was ignored because we limit queries to 32 words.  In fact, those are not 32 _distinct_ tokens, its 32 tokens, period. ( I checked )
	//
	// so if we want to support upto 32 query tokens and say upto 10 different synonyms for each of them, then we max out at 320 tokens/query. Just to be on the safe side though and because we want to
	// make sure we can support a few more, we set MaxQueryTokens to 640. In practice, we can set it to however many but we need to keep things sane
	namespace Limits
	{
		static constexpr size_t MaxPhraseSize{16};
		static constexpr size_t MaxQueryTokens{1024};
		static constexpr size_t MaxTermLength{64};
		static constexpr size_t MaxPosition{8192};


		// Sanity check
		static_assert(MaxTermLength < 250 && MaxTermLength > 8);
		static_assert(MaxPhraseSize < 128);
		static_assert(MaxQueryTokens < 2048);
		static_assert(MaxPosition <= std::numeric_limits<tokenpos_t>::max());
	}
}
