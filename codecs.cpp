#include "codecs.h"
#include "terms.h"
#include "utils.h"

void Trinity::Codecs::IndexSession::flush_index(int fd)
{
        if (indexOut.size())
        {
                if (Utilities::to_file(indexOut.data(), indexOut.size(), fd) == -1)
                        throw Switch::data_error("Failed to flush index");
                else
                {
                        indexOutFlushed += indexOut.size();
                        indexOut.clear();
                }
        }
}

void Trinity::Codecs::IndexSession::persist_terms(std::vector<std::pair<str8_t, term_index_ctx>> &v)
{
        IOBuffer data, index;

        pack_terms(v, &data, &index);

        if (Utilities::to_file(data.data(), data.size(), Buffer{}.append(basePath, "/terms.data"_s32).c_str()) == -1)
                throw Switch::system_error("Failed to persist terms.data");

        if (Utilities::to_file(index.data(), index.size(), Buffer{}.append(basePath, "/terms.idx"_s32).c_str()) == -1)
                throw Switch::system_error("Failed to persist terms.idx");
}

double Trinity::Codecs::PostingsListIterator::score()
{
	[[maybe_unused]] auto *const rctx = dec->rctx; 
	// whatever we need here
	// e.g use rctx->similarityProxy etc
	// we will initialize scorer to a subclass of Scorer or whatever we will call it
	// e.g return rctx->scorer->score(curDocument.id, freq);
	return 0;
}
