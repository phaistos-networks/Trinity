#include "similarity.h"

static bool init()
{
	auto t{Trinity::Similarity::IndexSourcesCollectionBM25Scorer::Scorer::normalizationTable};

	for (uint32_t i{0}; i != 256; ++i)
        {
                const float f(i);

                t[i] = 1.0 / (f * f);
        }

	return true;
}

float Trinity::Similarity::IndexSourcesCollectionBM25Scorer::Scorer::normalizationTable[256];
bool Trinity::Similarity::IndexSourcesCollectionBM25Scorer::Scorer::initializer = init();
