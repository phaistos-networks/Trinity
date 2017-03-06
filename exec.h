#pragma once
#include "queries.h"
#include "segments.h"
#include "docidupdates.h"

namespace Trinity
{
	bool exec_query(const query &in, segment &, dids_scanner_registry *const maskedDocumentsRegistry);
};
