#include "utils.h"
#include "common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int8_t Trinity::Utilities::to_file(const char *p, uint64_t len, int fd)
{
	// can't write more than sizeof(ssize_t) bytes/time (EINVAL)
        static constexpr uint64_t MaxSpan{(2ul * 1024 * 1024 * 1024) - 1};
        const auto *ptr{p};

        for (auto n = len / MaxSpan; n; len -= MaxSpan, ptr += MaxSpan)
        {
                if (write(fd, ptr, MaxSpan) != MaxSpan)
                {
                        close(fd);
                        return -1;
                }
        }

        if (len && write(fd, ptr, len) != len)
        {
                close(fd);
                return -1;
        }

        return 0;
}

int8_t Trinity::Utilities::to_file(const char *p, uint64_t len, const char *path)
{
        int fd = open(path, O_WRONLY | O_TRUNC | O_CREAT | O_LARGEFILE, 0775);

        if (fd == -1)
                return -1;
        else if (const auto res = to_file(p, len, fd); res == -1)
        {
                close(fd);
                return -1;
        }

        // http://www.jeffplaisance.com/2013/10/how-to-write-file.html
        // fdatasync() is insufficient; it does not sync the file size
        if (fsync(fd) == -1)
        {
                close(fd);
                return -1;
        }
        else if (close(fd) == -1)
                return -1;
        else
                return 0;
}
