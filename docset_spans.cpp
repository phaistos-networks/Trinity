#include "docset_spans.h"
#include "queryexec_ctx.h"
#include <switch_bitops.h>


using namespace Trinity;
extern thread_local Trinity::queryexec_ctx *curRCTX;

#pragma mark DocsSetSpanForPartialMatch
Trinity::isrc_docid_t Trinity::DocsSetSpanForPartialMatch::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        isrc_docid_t id{DocIDsEND};
        relevant_document relDoc;

        for (;;)
        {
                auto it = pq.top();

                id = it->current();
                if (id >= max)
                        break;

                const isrc_docid_t windowBase = id & ~MASK;
                //[[maybe_unused]] const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
                const auto windowMax = std::min<isrc_docid_t>(max, windowBase + SIZE);
                uint16_t collectedCnt{1};

                collected[0] = it;
                for (pq.pop(); likely(pq.size()) && (it = pq.top())->current() < windowMax; pq.pop())
                {
                        collected[collectedCnt++] = it;
                }

                if (collectedCnt == 1)
                {
                        auto *const it = collected[0];
			const auto rdp{it->rdp};

                        for (auto id = it->current(); id < windowMax; id = it->next())
                                mp->process(rdp);

                        pq.push(it);
                }
                else
                {
                        uint32_t m{0};

                        for (uint32_t i_{0}; i_ != collectedCnt; ++i_)
                        {
                                auto *const it = collected[i_];

                                for (auto id = it->current(); id < windowMax; id = it->next())
                                {
                                        const auto i = id - windowBase;
                                        const auto mi = i >> 6;

                                        m = std::max<uint32_t>(m, mi);
                                        matching[mi] |= uint64_t(1) << (i & 63);

                                        tracker[i].second++;
                                }

                                pq.push(it);
                        }

                        for (uint32_t idx{0}; idx <= m; ++idx)
                        {
                                const uint64_t _b = uint64_t(idx) << 6;

                                for (auto b = matching[idx]; b;)
                                {
                                        const auto bidx = SwitchBitOps::TrailingZeros(b);
                                        const auto translated = _b + bidx;
                                        const auto id = windowBase + translated;
                                        auto &trackInfo = tracker[translated]; 

                                        b ^= uint64_t(1) << bidx;

                                        if (trackInfo.second >= matchThreshold)
                                        {
                                                relDoc.set_document(id);
                                                mp->process(&relDoc);
                                        }

                                        trackInfo.first = 0;
                                        trackInfo.second = 0;
                                }
                        }

                        memset(matching, 0, (m + 1) * sizeof(matching[0])); // yes, we can
                }
        }

        // XXX: Shouldn't we return (id + 1) if (id == max && id != DocIDsEND) ?
        return id;
}

#pragma mark DocsSetSpanForDisjunctionsWithSpans
Trinity::DocsSetSpanForDisjunctionsWithSpans::DocsSetSpanForDisjunctionsWithSpans(std::vector<DocsSetSpan *> &its)
    : matching((uint64_t *)calloc(SET_SIZE, sizeof(uint64_t))), pq(its.size() + 16), collected((span_ctx *)malloc(sizeof(span_ctx) * (its.size() + 1))), tracker(matching, curRCTX)
{
        for (auto it : its)
	{
		// XXX: do we need to it->process(nullptr, 1,1) here for the eqivalent of it->next() we 
		// do in other pq population loops in other Spans?
                pq.push({it, 0});
	}

        require(pq.size());
}

Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctions::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        isrc_docid_t id{DocIDsEND};
        relevant_document relDoc;

        for (;;)
        {
                auto it = pq.top();

                id = it->current();
                if (unlikely(id >= max))
                        break;


                // fast round down to SIZE(works because SIZE is a power of two int.). Identifies the window the next match belongs to.
                const isrc_docid_t windowBase = id & ~MASK;
                //[[maybe_unused]] const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
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

                        for (auto id = it->current(); id < windowMax; id = it->next())
			{
				relDoc.set_document(id);
                                mp->process(&relDoc);
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

                                for (auto id = it->current(); id < windowMax; id = it->next())
                                {
                                        const auto i = id - windowBase;
                                        const auto mi = i >> 6;


                                        // std::max() is at least as fast as the branchless alt.
                                        // m = m ^ ((m ^ mi) & -(m < mi));
                                        m = std::max<uint32_t>(m, mi);
                                        matching[mi] |= uint64_t(1) << (i & 63);
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

                                        relDoc.set_document(id); 
					mp->process(&relDoc);
                                }
                        }

                        //memset(matching, 0, sizeof(uint64_t) * SET_SIZE); 
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

Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctionsWithSpans::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        isrc_docid_t id;
        relevant_document relDoc;

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

                                relDoc.set_document(id);
                                mp->process(&relDoc);
                        }
                }

                memset(matching, 0, (m + 1) * sizeof(matching[0]));
                tracker.reset();
        }

        return id;
}

#pragma mark FilteredDocsSetSpan
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

#pragma mark GenericDocsSetSpan
Trinity::isrc_docid_t Trinity::GenericDocsSetSpan::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        auto id = it->current();
        const auto rdp{it->rdp};

        if (id == 0 && min == 1 && max == DocIDsEND)
        {
                for (id = it->next(); likely(id != DocIDsEND); id = it->next())
                        mp->process(rdp);

                return DocIDsEND;
        }
        else
        {
                if (id < min)
                        id = it->advance(min);

                while (likely(id < max))
                {
                        mp->process(rdp);
                        id = it->next();
                }

                return id;
        }
}

#pragma mark DocsSetSpanForDisjunctionsWithSpans
void Trinity::DocsSetSpanForDisjunctionsWithSpans::Tracker::process(relevant_document_provider *const relDoc)
{
        //TODO: considering reset() setting windowBase for this object so that
        // we can compute i = (id - windowBase) which is likely faster than (id & MASK)
        const auto id = relDoc->document();
        const auto i = id & MASK;
        const auto mi = i >> 6;

        m = std::max<uint32_t>(m, mi);
        matching[mi] |= uint64_t(1) << (i & 63);
}

Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::DocsSetSpanForDisjunctionsWithSpansAndCost(const uint16_t min, std::vector<DocsSetSpan *> &its)
    :  matchesTracker((std::pair<double, uint32_t> *)calloc(SIZE, sizeof(std::pair<double, uint32_t>))), leads((span_ctx **)malloc(sizeof(span_ctx *) * (its.size() + 1))), head(its.size() - min + 1), tail(min - 1), matching((uint64_t *)calloc(SET_SIZE, sizeof(uint64_t))), matchThreshold{min}, storage((span_ctx *)malloc(sizeof(span_ctx) * (its.size() + 1))), tracker(matching, matchesTracker, curRCTX)
{
        EXPECT(min && min <= its.size());
        EXPECT(its.size() > 1);

        for (uint32_t i{0}; i != its.size(); ++i)
        {
                auto t = storage + i;

                t->span = its[i];
                t->cost = t->span->cost();
                t->next = 0;

                if (span_ctx * evicted; !tail.try_push(t, evicted))
                        head.push(evicted);
        }

        {
                Switch::priority_queue<uint64_t, std::greater<uint64_t>> pq(its.size() - matchThreshold + 1);
                uint64_t evicted;

                for (uint32_t i{0}; i != its.size(); ++i)
                {
                        auto it = storage + i;

                        pq.try_push(it->span->cost(), evicted);
                }

                cost_ = 0;
                for (const auto it : pq)
                        cost_ += it;
        }
}

#pragma mark DocsSetSpanForDisjunctionsWithSpansAndCost
Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::span_ctx *Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::advance(const isrc_docid_t min)
{
        auto headTop = head.top();
        auto tailTop = tail.size() ? tail.top() : nullptr;

        while (headTop->next < min)
        {
                if (!tailTop || headTop->cost <= tailTop->cost)
                {
                        headTop->advance(min);
                        head.update_top();
                        headTop = head.top();
                }
                else
                {
                        auto previousHeadTop{headTop};

                        tailTop->advance(min);
                        head.update_top(tailTop);
                        headTop = head.top();

                        tail.update_top(previousHeadTop);
                        tailTop = tail.top();
                }
        }

        return headTop;
}

void Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::score_window_many(MatchesProxy *const mp, const isrc_docid_t windowBase, const isrc_docid_t windowMin, const isrc_docid_t windowMax, uint16_t leadsCnt)
{
        while (leadsCnt < matchThreshold && leadsCnt + tail.size() >= matchThreshold)
        {
                auto candidate = tail.pop();

                candidate->advance(windowMin);
                if (candidate->next < windowMax)
                        leads[leadsCnt++] = candidate;
                else
                        head.push(candidate);
        }

        if (leadsCnt >= matchThreshold)
        {
                auto all = tail.data();
                const auto cnt = tail.size();
                const auto min{windowMin};
                const auto max{windowMax};

                for (uint16_t i{0}; i != cnt; ++i)
                        leads[leadsCnt++] = all[i];

                tail.clear();

                for (uint16_t i{0}; i != leadsCnt; ++i)
                {
                        auto it = leads[i];

                        it->process(&tracker, min, max);
                }

                const auto m{tracker.m};
                relevant_document relDoc;

                for (uint32_t idx{0}; idx <= m; ++idx)
                {
                        const uint64_t _b = uint64_t(idx) << 6;

                        for (auto b = matching[idx]; b;)
                        {
                                const auto bidx = SwitchBitOps::TrailingZeros(b);
                                const auto translated = _b + bidx;
                                const auto id = windowBase + translated;
                                auto &trackInfo = matchesTracker[translated];

                                b ^= uint64_t(1) << bidx;

                                if (trackInfo.second >= matchThreshold)
                                {
                                        relDoc.set_document(id);
                                        mp->process(&relDoc);
                                }

                                trackInfo.first = 0;
                                trackInfo.second = 0;
                        }
                }

                memset(matching, 0, (m + 1) * sizeof(matching[0])); // yes, we can
                tracker.reset();
        }

        for (uint32_t i{0}; i != leadsCnt; ++i)
        {
                if (span_ctx * evicted; !head.try_push(leads[i], evicted))
                        tail.push(evicted);
        }
}

void Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::score_window_single(span_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t windowMin, const isrc_docid_t windowMax, const isrc_docid_t max)
{
        const auto nextWindowBase = head.top()->next & ~MASK;
        const auto end = std::max<isrc_docid_t>(windowMax, std::min<isrc_docid_t>(max, nextWindowBase));

        sctx->process(mp, windowMin, end);
}

Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::span_ctx *Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::score_window(span_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        const auto id{sctx->next};
        const isrc_docid_t windowBase = id & ~MASK;
        [[maybe_unused]] const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
        const auto windowMax = std::min<isrc_docid_t>(max, windowBase + SIZE);
        uint16_t leadsCnt{1};

        leads[0] = head.pop();
        while (head.size() && head.top()->next < windowMax)
                leads[leadsCnt++] = head.pop();

        if (matchThreshold == 1 && leadsCnt == 1)
        {
                auto *const it = leads[0];

                score_window_single(it, mp, windowMin, windowMax, max);
                head.push(it);
        }
        else
        {
                score_window_many(mp, windowBase, windowMin, windowMax, leadsCnt);
        }

        return head.top();
}

Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        auto top = advance(min ?: 1);

        while (top->next < max)
                top = score_window(top, mp, min, max);

        return top->next;
}

void Trinity::DocsSetSpanForDisjunctionsWithSpansAndCost::Tracker::process(relevant_document_provider *const relDoc)
{
        const auto id = relDoc->document();
        const auto i = id & MASK;
        const auto mi = i >> 6;

        m = std::max<uint32_t>(m, mi);
        matching[mi] |= uint64_t(1) << (i & 63);

        matchesTracker[i].second++;
}

#pragma mark DocsSetSpanForDisjunctionsWithThresholdAndCost
Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::DocsSetSpanForDisjunctionsWithThresholdAndCost(const uint16_t min, std::vector<DocsSetIterators::Iterator *> &its, const bool ns)
    :  matchesTracker((std::pair<double, uint32_t> *)calloc(SIZE, sizeof(std::pair<double, uint32_t>))), leads((it_ctx **)malloc(sizeof(it_ctx *) * (its.size() + 1))), head(its.size() - min + 1), tail(min - 1), matching((uint64_t *)calloc(SET_SIZE, sizeof(uint64_t))), needScores{ns}, matchThreshold{min}, storage((it_ctx *)malloc(sizeof(it_ctx) * (its.size() + 1)))
{
        EXPECT(min && min <= its.size());
        EXPECT(its.size() > 1);

        for (uint32_t i{0}; i != its.size(); ++i)
        {
                auto t = storage + i;

                t->it = its[i];
                t->cost = t->it->cost();
                // we 'll not set next to 0, instead we 'll set it to t->it->next()
                // so that we won't need to use:
                // auto top = advance(min ?: 1);
                // in process()
                // we 'll just use:
                // auto top = advance(min);
                //
                // We won't use this tick in DocsSetSpanForDisjunctionsWithSpansAndCost
                t->next = t->it->next();

                if (it_ctx * evicted; !tail.try_push(t, evicted))
                        head.push(evicted);
        }

        {
                Switch::priority_queue<uint64_t, std::greater<uint64_t>> pq(its.size() - matchThreshold + 1);
                uint64_t evicted;

                for (uint32_t i{0}; i != its.size(); ++i)
                {
                        auto it = storage + i;

                        pq.try_push(it->it->cost(), evicted);
                }

                cost_ = 0;
                for (const auto it : pq)
                        cost_ += it;
        }
}

Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::it_ctx *Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::advance(const isrc_docid_t min)
{
        auto headTop = head.top();
        auto tailTop = tail.size() ? tail.top() : nullptr;

        while (headTop->next < min)
        {
                if (!tailTop || headTop->cost <= tailTop->cost)
                {
                        headTop->advance(min);
                        head.update_top();
                        headTop = head.top();
                }
                else
                {
                        auto previousHeadTop{headTop};

                        tailTop->advance(min);
                        head.update_top(tailTop);
                        headTop = head.top();

                        tail.update_top(previousHeadTop);
                        tailTop = tail.top();
                }
        }

        return headTop;
}

void Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::score_window_many(MatchesProxy *const mp, const isrc_docid_t windowBase, const isrc_docid_t windowMin, const isrc_docid_t windowMax, uint16_t leadsCnt)
{
        while (leadsCnt < matchThreshold && leadsCnt + tail.size() >= matchThreshold)
        {
                auto candidate = tail.pop();

                candidate->advance(windowMin);
                if (candidate->next < windowMax)
                        leads[leadsCnt++] = candidate;
                else
                        head.push(candidate);
        }

        if (leadsCnt >= matchThreshold)
        {
                auto all = tail.data();
                const auto cnt = tail.size();
                const auto min{windowMin};
                const auto max{windowMax};
                uint32_t m{0};
                relevant_document relDoc;

                for (uint16_t i{0}; i != cnt; ++i)
                        leads[leadsCnt++] = all[i];

                tail.clear();

                if (needScores)
                {
                        for (uint16_t i{0}; i != leadsCnt; ++i)
                        {
                                auto *const it = leads[i]->it;
				const auto rdp{it->rdp};

                                if (it->current() < min)
                                        it->advance(min);

                                for (auto id = it->current(); id < max; id = it->next())
                                {
                                        const auto i = id - windowBase;
                                        const auto mi = i >> 6;

                                        m = std::max<uint32_t>(m, mi);
                                        matching[mi] |= uint64_t(1) << (i & 63);

                                        matchesTracker[i].second++;
                                        matchesTracker[i].first += rdp->score();
                                }
                                leads[i]->next = it->current();
                        }

                }
                else
                {
                        for (uint16_t i{0}; i != leadsCnt; ++i)
                        {
                                auto *const it = leads[i]->it;

                                if (it->current() < min)
                                        it->advance(min);

                                for (auto id = it->current(); id < max; id = it->next())
                                {
                                        const auto i = id - windowBase;
                                        const auto mi = i >> 6;

                                        m = std::max<uint32_t>(m, mi);
                                        matching[mi] |= uint64_t(1) << (i & 63);

                                        matchesTracker[i].second++;
                                }
                                leads[i]->next = it->current();
                        }
                }

		for (uint32_t idx{0}; idx <= m; ++idx)
		{
			const uint64_t _b = uint64_t(idx) << 6;

			for (auto b = matching[idx]; b;)
			{
				const auto bidx = SwitchBitOps::TrailingZeros(b);
				const auto translated = _b + bidx;
				const auto id = windowBase + translated;
				auto &trackInfo = matchesTracker[translated];

				b ^= uint64_t(1) << bidx;

				if (trackInfo.second >= matchThreshold)
				{
					relDoc.set_document(id);
					relDoc.score_ = trackInfo.first;
					mp->process(&relDoc);
				}

				trackInfo.first = 0;
				trackInfo.second = 0;
			}
		}


                memset(matching, 0, (m + 1) * sizeof(matching[0]));
        }

        for (uint32_t i{0}; i != leadsCnt; ++i)
        {
                if (it_ctx * evicted; !head.try_push(leads[i], evicted))
                        tail.push(evicted);
        }
}

void Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::score_window_single(it_ctx *const ictx, MatchesProxy *const mp, const isrc_docid_t windowMin, const isrc_docid_t windowMax, const isrc_docid_t max)
{
        const auto nextWindowBase = head.top()->next & ~MASK;
        const auto end = std::max<isrc_docid_t>(windowMax, std::min<isrc_docid_t>(max, nextWindowBase));
        auto it = ictx->it;


        if (it->current() < windowMin)
                it->advance(windowMin);
        for (auto id = it->current(); id < end; id = it->next())
        {
                mp->process(it);
        }

        ictx->next = it->current();
}

Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::it_ctx *Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::score_window(it_ctx *const sctx, MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        const auto id{sctx->next};
        const isrc_docid_t windowBase = id & ~MASK;
        [[maybe_unused]] const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
        const auto windowMax = std::min<isrc_docid_t>(max, windowBase + SIZE);
        uint16_t leadsCnt{1};

        leads[0] = head.pop();
        while (head.size() && head.top()->next < windowMax)
                leads[leadsCnt++] = head.pop();

        if (matchThreshold == 1 && leadsCnt == 1)
        {
                auto *const it = leads[0];

                score_window_single(it, mp, windowMin, windowMax, max);
                head.push(it);
        }
        else
        {
                score_window_many(mp, windowBase, windowMin, windowMax, leadsCnt);
        }

        return head.top();
}

Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctionsWithThresholdAndCost::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        auto top = advance(min);

        while (top->next < max)
                top = score_window(top, mp, min, max);

        return top->next;
}

#pragma mark DocsSetSpanForDisjunctionsWithThreshold
Trinity::isrc_docid_t Trinity::DocsSetSpanForDisjunctionsWithThreshold::process(MatchesProxy *const mp, const isrc_docid_t min, const isrc_docid_t max)
{
        isrc_docid_t id{DocIDsEND};
        relevant_document relDoc;

        for (;;)
        {
                auto it = pq.top();

                id = it->current();
                if (id >= max)
                        break;

                const isrc_docid_t windowBase = id & ~MASK;
                [[maybe_unused]] const auto windowMin = std::max<isrc_docid_t>(min, windowBase);
                const auto windowMax = std::min<isrc_docid_t>(max, windowBase + SIZE);
                uint16_t collectedCnt{1};
	
                collected[0] = it;
                for (pq.pop(); likely(pq.size()) && (it = pq.top())->current() < windowMax; pq.pop())
                {
			// there's no need for this now
                        // if (it->current() < windowMin) it->advance(windowMin);
			// as long as we make sure to advance iterators pushed into the PQ
			// in the constructor. This is really only necessary once, when current() == 0
			// It's not particularly expensive to check and advance() here, but simply
			// next() in the ctor means we don't need to do any of that here.
			// 
			// This is true for other similar Spans designs.
                        // if (it->current() < windowMin) it->advance(windowMin);

                        collected[collectedCnt++] = it;
                }

                if (collectedCnt == 1 && matchThreshold == 1)
                {
                        auto *const it = collected[0];
			const auto rdp{it->rdp};

                        for (auto id = it->current(); id < windowMax; id = it->next())
                                mp->process(rdp);

                        pq.push(it);
                }
                else
                {
                        uint32_t m{0};

                        if (needScores)
                        {
                                for (uint32_t i_{0}; i_ != collectedCnt; ++i_)
                                {
                                        auto *const it = collected[i_];
                                        const auto rdp{it->rdp};

                                        for (auto id = it->current(); id < windowMax; id = it->next())
                                        {
                                                const auto i = id - windowBase;
                                                const auto mi = i >> 6;

                                                m = std::max<uint32_t>(m, mi);
                                                matching[mi] |= uint64_t(1) << (i & 63);

                                                tracker[i].first += rdp->score();
                                                tracker[i].second++;
                                        }

                                        pq.push(it);
                                }
                        }
                        else
                        {
                                for (uint32_t i_{0}; i_ != collectedCnt; ++i_)
                                {
                                        auto *const it = collected[i_];

                                        if (it->current() < windowMin)
                                                it->advance(windowMax);

                                        for (auto id = it->current(); id < windowMax; id = it->next())
                                        {
                                                const auto i = id - windowBase;
                                                const auto mi = i >> 6;

                                                m = std::max<uint32_t>(m, mi);
                                                matching[mi] |= uint64_t(1) << (i & 63);

                                                tracker[i].second++;
                                        }

                                        pq.push(it);
                                }
                        }

                        for (uint32_t idx{0}; idx <= m; ++idx)
                        {
                                const uint64_t _b = uint64_t(idx) << 6;

                                for (auto b = matching[idx]; b;)
                                {
                                        const auto bidx = SwitchBitOps::TrailingZeros(b);
                                        const auto translated = _b + bidx;
                                        const auto id = windowBase + translated;
                                        auto &trackInfo = tracker[translated];

                                        b ^= uint64_t(1) << bidx;

                                        if (trackInfo.second >= matchThreshold)
                                        {
                                                relDoc.set_document(id);
                                                relDoc.score_ = trackInfo.first;
                                                mp->process(&relDoc);
                                        }

                                        trackInfo.first = 0;
                                        trackInfo.second = 0;
                                }
                        }

                        memset(matching, 0, (m + 1) * sizeof(matching[0]));
                }
        }

        // XXX: Shouldn't we return (id + 1) if (id == max && id != DocIDsEND) ?
        return id;
}
