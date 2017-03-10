#pragma once
#include "queries.h"
#include "docidupdates.h"
#include "index_source.h"
#include "matches.h"

namespace Trinity
{
	bool exec_query(const query &in, IndexSource *, masked_documents_registry *const maskedDocumentsRegistry);

        bool exec_query(const query &in, IndexSourcesCollection *collection);
};
