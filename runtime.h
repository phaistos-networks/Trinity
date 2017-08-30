#pragma once
#include "common.h"

namespace Trinity
{
	using exec_term_id_t = uint16_t;
 
	// a materialized document term hit
	struct term_hit final
        {
                uint64_t payload;
                tokenpos_t pos;
                uint8_t payloadLen;

		inline auto bytes() const noexcept
		{
			return reinterpret_cast<const uint8_t *>(&payload);
		}

		inline auto bytes() noexcept
		{
			return reinterpret_cast<uint8_t *>(&payload);
		}
        };
}

