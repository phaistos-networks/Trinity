#pragma once
#include "queries.h"
#include "docidupdates.h"
#include "index_source.h"

namespace Trinity
{
	bool exec_query(const query &in, IndexSource *, dids_scanner_registry *const maskedDocumentsRegistry);
};
