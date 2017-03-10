#pragma once
#include <switch.h>
#include "limits.h"

namespace Trinity
{
	// We will support unicode, so more appropriate string types will be better suited to the task
	using str8_t = strwlen8_t;
	using str32_t = strwlen32_t;
}
