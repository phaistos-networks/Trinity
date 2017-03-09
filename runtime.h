#pragma once
#include <switch.h>

namespace Trinity
{
	using exec_term_id_t = uint16_t;
 
	// a materialized document term hit
	struct term_hit final
        {
                uint64_t payload;
                uint16_t pos;
                uint8_t payloadLen;
        };
}

