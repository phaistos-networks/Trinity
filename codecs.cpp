#include "codecs.h"
#include "terms.h"

void Trinity::Codecs::IndexSession::persist_terms(std::vector<std::pair<str8_t, term_index_ctx>> &v)
{
        IOBuffer data, index;

        pack_terms(v, &data, &index);

        if (data.SaveInFile(Buffer{}.append(basePath, "/terms.data").c_str()) != data.size())
                throw Switch::system_error("Failed to persist terms.data");

        if (index.SaveInFile(Buffer{}.append(basePath, "/terms.idx").c_str()) != index.size())
                throw Switch::system_error("Failed to persist terms.idx");
}
