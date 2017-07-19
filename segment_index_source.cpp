#include "segment_index_source.h"
#include "google_codec.h"
#include "lucene_codec.h"

Trinity::SegmentIndexSource::SegmentIndexSource(const char *basePath)
{
        int fd;
        char path[PATH_MAX];
	strwlen32_t bp(basePath);

	bp.StripTrailingCharacter('/');

	if (auto p = bp.SearchR('/'))
		bp = bp.SuffixFrom(p + 1);

	if (!bp.IsDigits())
		throw Switch::data_error("Expected segment name to be a generation(digits)");

	gen = bp.AsUint64();

        snprintf(path, sizeof(path), "%s/updated_documents.ids", basePath);
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
		if (unlikely(fileData == MAP_FAILED))
			throw Switch::data_error("Failed to access ", path, ":" , strerror(errno));
			
                maskedDocuments.fileData.Set((uint8_t *)fileData, fileSize);
                new (&maskedDocuments.set) updated_documents(unpack_updates(maskedDocuments.fileData));
        }
        else
                close(fd);

        terms.reset(new SegmentTerms(basePath));

        snprintf(path, sizeof(path), "%s/index", basePath);
        fd = open(path, O_RDONLY | O_LARGEFILE);
	if (fd == -1)
	{
		if (errno != ENOENT)
			throw Switch::data_error("Failed to access ", path);
		else
		{

		}
	}


        auto fileSize = lseek64(fd, 0, SEEK_END);

	if (0 == fileSize)
	{
		// just updated documents
	}
	else
	{
#ifdef TRINITY_MEMRESIDENT_INDEX
	auto p = (uint8_t *)malloc(fileSize + 1);

	if (pread64(fd, p, fileSize, 0) != fileSize)
	{
		free(p);
		close(fd);
		throw Switch::data_error("Failed to acess ", path);
	}
	
	close(fd);
	index.Set(p, fileSize);
#else
        auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

        close(fd);
	if (unlikely(fileData == MAP_FAILED))
		throw Switch::data_error("Failed to acess ", path);
		
        index.Set(static_cast<const uint8_t *>(fileData), uint32_t(fileSize));
#endif
	}


        snprintf(path, sizeof(path), "%s/codec", basePath);
        fd = open(path, O_RDONLY | O_LARGEFILE);
	if (unlikely(fd == -1))
		throw Switch::data_error("Failed to acess ", path);

        fileSize = lseek64(fd, 0, SEEK_END);

        if (!IsBetweenRange<size_t>(fileSize, 3, 128))
        {
                close(fd);
                throw Switch::data_error("Invalid segment codec file");
        }

        char codecStorage[128];

        if (pread64(fd, codecStorage, fileSize, 0) != fileSize)
        {
                close(fd);
                throw Switch::system_error("Failed to read codec");
        }
        else
                close(fd);

        const strwlen8_t codec(codecStorage, fileSize);

        if (codec.Eq(_S("GOOGLE")))
                accessProxy.reset(new Trinity::Codecs::Google::AccessProxy(basePath, index.start()));
        else if (codec.Eq(_S("LUCENE")))
                accessProxy.reset(new Trinity::Codecs::Lucene::AccessProxy(basePath, index.start()));
        else
                throw Switch::data_error("Unknown codec");
}
