#pragma once
#include <compress.h>
#include <switch.h>

namespace Trinity {
        namespace Utilities {
                int8_t to_file(const char *p, uint64_t len, const char *path);

                int8_t to_file(const char *p, uint64_t len, int fd);
        } // namespace Utilities
} // namespace Trinity
