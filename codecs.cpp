#include "codecs.h"
#include "terms.h"
#include "utils.h"

void Trinity::Codecs::IndexSession::persist_terms(std::vector<std::pair<str8_t, term_index_ctx>> &v)
{
        IOBuffer data, index;

        pack_terms(v, &data, &index);

        if (Utilities::to_file(data.data(), data.size(), Buffer{}.append(basePath, "/terms.data").c_str()) == -1)
                throw Switch::system_error("Failed to persist terms.data");

        if (Utilities::to_file(index.data(), index.size(), Buffer{}.append(basePath, "/terms.idx").c_str()) == -1)
                throw Switch::system_error("Failed to persist terms.idx");
}
