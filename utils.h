#pragma once
#include <switch.h>
#include <compress.h>

namespace Trinity
{
	namespace Utilities
	{
		int8_t to_file(const char *p, uint64_t len, const char *path);
	}
}
