#include "segment_index_source.h"
#include "google_codec.h"

Trinity::SegmentIndexSource::SegmentIndexSource(const char *basePath)
{
        int fd;
        char path[PATH_MAX];

        Snprintf(path, sizeof(path), "%s/updated_documents.ids");
        fd = open(path, O_RDONLY | O_LARGEFILE);

        if (fd == -1)
        {
                if (errno != ENOENT)
                        throw Switch::system_error("open() failed for updated_documents.ids");
        }
        else if (const auto fileSize = lseek64(fd, 0, SEEK_END))
        {
                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                close(fd);
                Dexpect(fileData != MAP_FAILED);

                maskedDocuments.fileData.Set((uint8_t *)fileData, fileSize);
                new (&maskedDocuments.set) updated_documents(unpack_updates(maskedDocuments.fileData));
        }
        else
                close(fd);

        terms.reset(new SegmentTerms(basePath));

        Snprintf(path, sizeof(path), "%s/index");
        fd = open(path, O_RDONLY | O_LARGEFILE);
        Dexpect(fd != -1);

	const auto fileSize = lseek64(fd, 0, SEEK_END);
	auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

	close(fd);
	Dexpect(fileData != MAP_FAILED);

	index.Set(static_cast<const uint8_t *>(fileData), uint32_t(fileSize));

	// TODO: read codec from e.g codec.desc file
	// for now assume its google
	accessProxy.reset(new Trinity::Codecs::Google::AccessProxy(basePath, index.start()));
}
