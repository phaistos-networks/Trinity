#include "docset_spans.h"
#include "runtime_ctx.h"
#include <switch_bitops.h>

using namespace Trinity;
extern thread_local Trinity::runtime_ctx *curRCTX;


Trinity::DocsSetSpanForDisjunctionsWithSpans::DocsSetSpanForDisjunctionsWithSpans(std::vector<DocsSetSpan *> &its, const bool root)
	: DocsSetSpan{root}, matching((uint64_t *)calloc(SET_SIZE, sizeof(uint64_t))), pq(its.size() + 16), collected((span_ctx *)malloc(sizeof(span_ctx) * (its.size() + 1))), tracker(matching, curRCTX)
{
	for (auto it : its)
		pq.push({it, 0});

	require(pq.size());
}

Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctions::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        isrc_docid_t id{DocIDsEND};

        // while (pq.size())  // we are no longer dropping anything from pq
        for (;;)
        {
                auto it = pq.top();

                id = it->current();
                if (id >= max)
                        break;

                const isrc_docid_t windowBase = id & ~MASK; // fast round down to SIZE(works because SIZE is a power of two int.). Identifies the window the next match belongs to.
                [[maybe_unused]] const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
                const auto windowMax = std::min<isrc_docid_t>(max, windowBase + SIZE);
                uint16_t collectedCnt{1};

                collected[0] = it;
                for (pq.pop(); likely(pq.size()) && (it = pq.top())->current() < windowMax; pq.pop())
                {
                        collected[collectedCnt++] = it;
                }

                if (collectedCnt == 1)
                {
                        // fast-path: one iterator can match in this window
                        auto *const it = collected[0];
                        auto id = it->current();

                        // no need for
                        // if (id < windowMin) id = it->advance(windowMin);
                        // because id has been rounded down to SIZE, and windowMin is std::min(min, windowBase)

                        while (id < windowMax)
                        {
                                mp->process(id);
                                id = it->next();
                        }

                        pq.push(it);
                }
                else
                {
                        // Encode document presence(matched) in bitmap for this window
                        uint32_t m{0};

                        for (uint32_t i_{0}; i_ != collectedCnt; ++i_)
                        {
                                auto *const it = collected[i_];
                                auto id = it->current();

                                // no need for if (id < windowMin) id = it->advance(windowMin);
                                // see comments above

                                while (id < windowMax)
                                {
                                        const auto i = id & MASK;
                                        const auto mi = i >> 6;

                                        // std::max() is at least as fast as the branchless alt.
                                        // m = m ^ ((m ^ mi) & -(m < mi));
                                        m = std::max<uint32_t>(m, mi);
                                        matching[mi] |= uint64_t(1) << (i & 63);

#ifdef TRACK_DOCTERMS
                                        tracker[i].push_back({t, 0});
#endif

                                        id = it->next();
                                }

                                pq.push(it);
                        }

                        // Process the bitmap
                        for (uint32_t idx{0}; idx <= m; ++idx)
                        {
                                const uint64_t _b = uint64_t(idx) << 6;

                                for (auto b = matching[idx]; b;)
                                {
                                        const auto bidx = SwitchBitOps::TrailingZeros(b);
                                        const auto translated = _b + bidx;
                                        const auto id = windowBase + translated;

                                        b ^= uint64_t(1) << bidx;

#ifdef TRACK_DOCTERMS
                                        auto &v = tracker[id & MASK];

                                        require(v.size());
                                        v.clear();
#endif

                                        mp->process(id);
                                }
                        }

                        //memset(matching, 0, sizeof(matching)); // this will reset 256bytes. Can we do better?
                        memset(matching, 0, (m + 1) * sizeof(matching[0])); // yes, we can
                }
        }

        // XXX: Shouldn't we return (id + 1) if (id == max && id != DocIDsEND) ?
        return id;
}

Trinity::DocsSetSpanForDisjunctionsWithSpans::span_ctx Trinity::DocsSetSpanForDisjunctionsWithSpans::advance(const isrc_docid_t min)
{
        auto *top = &pq.top();

        while (top->next < min)
        {
                top->advance(min);

                pq.update_top();
                top = &pq.top();
        }

        return *top;
}

// both mp->process() and Tracker::process() are expected to rctx.cds_drain_STM() so
// we won't be doing it manually here
Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctionsWithSpans::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        isrc_docid_t id;

        for (auto top = advance(min); (id = top.next) < max; top = pq.top())
        {
                const isrc_docid_t windowBase = id & ~MASK;
                const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
                const auto windowMax = std::min<isrc_docid_t>(max, windowBase + SIZE);
                uint16_t collectedCnt{1};

                collected[0] = top;
                for (pq.pop(); likely(pq.size()) && (top = pq.top()).next < windowMax; pq.pop())
                {
                        collected[collectedCnt++] = top;
                }

                if (collectedCnt == 1)
                {
                        // fast-path: one iterator can match in this window
                        top = collected[0];
                        top.process(mp, windowMin, windowMax);

                        pq.push(top);
                        continue;
                }

                for (uint32_t i_{0}; i_ != collectedCnt; ++i_)
                {
                        auto &ctx = collected[i_];

                        ctx.process(&tracker, windowMin, windowMax);
                        pq.push(ctx);
                }

                const auto m = tracker.m;

                for (uint32_t idx{0}; idx <= m; ++idx)
                {
                        const uint64_t _b = uint64_t(idx) << 6;

                        for (auto b = matching[idx]; b;)
                        {
                                const auto bidx = SwitchBitOps::TrailingZeros(b);
                                const auto translated = _b + bidx;
                                const auto id = windowBase + translated;

                                b ^= uint64_t(1) << bidx;
                                mp->process(id);
                        }
                }

                memset(matching, 0, (m + 1) * sizeof(matching[0]));
                tracker.reset();
        }

        return id;
}

Trinity::isrc_docid_t Trinity::FilteredDocsSetSpan::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        auto upto{min};
        auto id = exclIt->current();

        while (upto < max)
        {
                if (id < upto)
                        id = exclIt->advance(upto);

                if (id == upto)
                {
                        ++upto;
                        id = exclIt->next();
                }
                else
                {
                        const auto end = std::min(id, max);

                        upto = req->process(mp, upto, end);
                }
        }

        if (upto == max)
                upto = req->process(mp, upto, upto);

        return upto;
}

Trinity::isrc_docid_t Trinity::GenericDocsSetSpan::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        auto id = it->current();

        if (id == 0 && min == 0 && max == DocIDsEND)
        {
                for (id = it->next(); likely(id != DocIDsEND); id = it->next())
                        mp->process(id);

                return DocIDsEND;
        }
        else
        {
                if (id < min)
                        id = it->advance(min);

                while (id < max)
                {
                        mp->process(id);
                        id = it->next();
                }

                return id;
        }
}

void Trinity::DocsSetSpanForDisjunctionsWithSpans::Tracker::process(const isrc_docid_t id)
{
        const auto i = id & MASK;
        const auto mi = i >> 6;

        m = std::max<uint32_t>(m, mi);
        matching[mi] |= uint64_t(1) << (i & 63);
}
