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
	else
        {
                // TODO: as per Jeff Praisance's recommendation, we should open() the directory, fsync() its fd, and close() it
                close(fd);
                return 0;
        }
}

std::pair<uint32_t, uint8_t> Trinity::default_token_parser_impl(const Trinity::str32_t content, Trinity::char_t *out)
{
        const auto *p = content.begin(), *const e = content.end(), *const b{p};

	if (p + 5 < e && isalpha(*p) && p[1] == '.' && isalnum(p[2]) && p[3] == '.' && isalpha(p[4]))
	{
		// is it e.g I.B.M ?
		const auto threshold = out + 0xff;
		auto o{out};

		*o++ = p[0];
		*o++ = p[2];
		*o++ = p[4];

                for (p += 5;;)
                {
                        if (p == e)
				break;
                        else if (*p == '.')
                        {
				++p;
				if (p == e)
					break;
				else if (isalpha(*p))
				{
					if (unlikely(o !=threshold))
						*o++ = *p;
					++p;
					continue;
				}
				else if (!isalnum(*p))
					break;
				else
					goto l20;
                        }
                        else if (isalnum(*p))
                                goto l20;
			else
				break;
                }

                return {p - content.data(), o - out};
        }

l20:
        if (p != e && isalnum(*p))
        {
		// e.g site:google.com, or video|games
                while (p != e && isalpha(*p))
			++p;

                if (p + 1 < e && *p == ':' && isalnum(p[1]))
                {
                        for (p += 2; p != e && (isalnum(*p) || *p == '.'); ++p)
                                continue;
                        goto l10;
                }
        }

        for (;;)
        {
		if (*p == '|' && p + 1 < e && isalnum(p[1]))
		{
			for (p+=2; p != e; )
			{
				if (*p == '|' && p + 1 < e && isalnum(p[1]))
					p+=2;
				else if (isalnum(*p))
					++p;
				else
					break;
			}
			break;
		}

                while (p != e && isalnum(*p))
                        ++p;

                if (p != b && p != e)
                {
                        if (*p == '-')
                        {
                                ++p;
                                continue;
                        }
                        else if (*p == '+' && isalpha(p[-1]) && (p + 1 == e || !isalnum(p[1])))
                        {
                                // C++
                                for (++p; p != e && *p == '+'; ++p)
                                        continue;
                                continue;
                        }
                }

                break;
        }

l10:
        const uint32_t consumed = p - b;
        const uint8_t stored = std::min<uint32_t>(consumed, 0xff);

        memcpy(out, b, stored * sizeof(char_t));
        return {consumed, stored};
}
