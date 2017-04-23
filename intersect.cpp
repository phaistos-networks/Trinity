#include "intersect.h"

using namespace Trinity;

void Trinity::intersect_impl(const str8_t *const tokens, const uint8_t tokensCnt, IndexSource *__restrict__ const src, masked_documents_registry *const __restrict__ maskedDocumentsRegistry, std::vector<std::pair<uint64_t, uint32_t>> *const out)
{
        Dexpect(tokensCnt < 64);

        struct tracked
        {
                Codecs::Decoder *dec;
                uint8_t tokenIdx;
        } remaining[64];
        uint8_t rem{0};
        uint64_t origMask{0};

        for (uint8_t i{0}; i != tokensCnt; ++i)
        {
                if (const auto tctx = src->term_ctx(tokens[i]); tctx.documents)
                {
                        auto dec = src->new_postings_decoder(tokens[i], tctx);

                        dec->begin();
                        remaining[rem++] = {dec, i};
                        origMask |= uint64_t(1u) << i;
                }
                else
                        SLog("Unknown for [", tokens[i], "]\n");
        }

        if (!rem)
                return;

        struct ctx
        {
                uint64_t mapPrev{0};
                uint8_t indexPrev{0};
                uint8_t matchesCnt{0};

                struct match
                {
                        uint64_t v;
                        uint32_t cnt;
                } matches[64];

                void consider(const uint64_t map)
                {
                        if (map == mapPrev)
                                ++matches[indexPrev].cnt;
                        else
                        {
                                mapPrev = map;
                                for (uint8_t i{0}; i < matchesCnt;)
                                {
                                        if (const auto v = matches[i].v; (v & map) == map)
                                        {
                                                // [wars jedi] [star wars jedi]
                                                if (map == v)
                                                        ++matches[i].cnt;
                                                indexPrev = i;
                                                return;
                                        }
                                        else if ((map & v) == v)
                                        {
                                                // [star wars return jedi] [wars jedi]
                                                matches[i] = matches[--matchesCnt];
                                        }
                                        else
                                                ++i;
                                }

                                indexPrev = matchesCnt;
                                matches[matchesCnt++] = {map, 1};
                        }
                }

                void finalize()
                {
                        std::sort(matches, matches + matchesCnt, [](const auto &a, const auto &b) noexcept {
                                const auto r = int8_t(SwitchBitOps::PopCnt(b.v)) - int8_t(SwitchBitOps::PopCnt(a.v));

                                return r < 0 || (!r && b.cnt < a.cnt);
                        });
                }
        };

        uint8_t selected[64];
        ctx c;
        const auto before = Timings::Microseconds::Tick();

        for (;;)
        {
                uint8_t cnt{1};
                docid_t lowest;
                const auto &it = remaining[0];
                uint64_t mask = uint64_t(1u) << it.tokenIdx;

                selected[0] = 0;
                lowest = it.dec->curDocument.id;
                for (uint32_t i{1}; i != rem; ++i)
                {
                        const auto &it = remaining[i];

                        // SLog("For [", tokens[it.tokenIdx], "] ", it.dec->curDocument.id, "\n");

                        if (const auto docID = it.dec->curDocument.id; docID == lowest)
                        {
                                mask |= uint64_t(1u) << it.tokenIdx;
                                selected[cnt++] = i;
                        }
                        else if (docID < lowest)
                        {
                                mask = uint64_t(1u) << it.tokenIdx;
                                selected[0] = i;
                                cnt = 1;
                                lowest = docID;
                        }
                }

                if (mask != origMask)
		{
                        if (!maskedDocumentsRegistry->test(lowest))
                                c.consider(mask);
                }

                do
                {
                        const auto idx = selected[--cnt];

                        if (!remaining[idx].dec->next())
                        {
                                delete remaining[idx].dec;
                                if (--rem == 0)
                                        goto l10;
                                else
                                        remaining[idx] = remaining[rem];
                        }
                } while (cnt);
        }

l10:
        c.finalize();
        SLog(duration_repr(Timings::Microseconds::Since(before)), " to intersect\n");

#if 0
	uint8_t v[64];

	for (uint32_t i{0}; i != c.matchesCnt; )
	{
		auto mask = c.matches[i].v;
		uint8_t shift{0};
		uint8_t collected{0};

		SLog("Collecting ", c.matches[i].cnt, "\n");
		do
                {
                        auto idx = SwitchBitOps::LeastSignificantBitSet(mask);
                        const auto translated = idx + shift - 1;

                        SLog("idx = ", idx, ", shift = ", shift, ", translated = ", translated, " ", tokens[translated], "\n");
                        v[collected] = translated;

                        mask >>= idx;
                        shift += idx;
                } while (mask);

                ++i;
	}
#else
        for (uint32_t i{0}; i != c.matchesCnt; ++i)
                out->push_back({c.matches[i].v, c.matches[i].cnt});
#endif
}

std::vector<std::pair<uint64_t, uint32_t>> intersect_impl(const str8_t *tokens, const uint8_t tokensCnt, IndexSourcesCollection *collection)
{
	std::vector<std::pair<uint64_t, uint32_t>> out;
	const auto n = collection->sources.size();

	for (uint32_t i{0}; i != n; ++i)
	{
		auto source = collection->sources[i];
		auto scanner = collection->scanner_registry_for(i);

		intersect_impl(tokens, tokensCnt, source, scanner.get(), &out);
	}

	std::sort(out.begin(), out.end(), [](const auto &a, const auto &b) { return a.first < b.first; });

	auto o = out.data();
	for (const auto *p = out.data(), *const e = p + out.size(); p != e;)
	{
		const auto mask = p->first;
		auto cnt = p->second;

		for (++p; p != e && p->first == mask; ++p)
			continue;

		*o++ = {mask, cnt};
	}

	out.resize(o - out.data());
	return out;
}

uint8_t Trinity::intersection_indices(uint64_t mask, uint8_t *const out)
{
        uint8_t shift{0}, collected{0};

        do
        {
                auto idx = SwitchBitOps::LeastSignificantBitSet(mask);
                const auto translated = idx + shift - 1;

                out[collected++] = translated;

                mask >>= idx;
                shift += idx;
        } while (mask);

        return collected;
}

