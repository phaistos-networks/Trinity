#include "docset_iterators.h"
#include "codecs.h"
#include "runtime_ctx.h"

// see reorder_execnode_impl()
uint64_t Trinity::DocsSetIterators::cost(const Iterator *it)
{
        switch (it->type)
        {
                case Type::AppIterator:
                        std::abort();

                case Type::DisjunctionSome:
                        return static_cast<const DisjunctionSome *>(it)->cost_;

                case Type::Filter:
                {
                        const auto self = static_cast<const Filter *>(it);

                        return cost(self->req);
                }
                break;

                case Type::VectorIDs:
                        return static_cast<const VectorIDs *>(it)->ids.size();

                case Type::Optional:
                {
                        [[maybe_unused]] const auto self = static_cast<const Optional *>(it);

                        return UINT64_MAX - 1;
                }
                break;

                case Type::OptionalOptPLI:
                {
                        [[maybe_unused]] const auto self = static_cast<const OptionalOptPLI *>(it);

                        return UINT64_MAX - 1;
                }
                break;

                case Type::OptionalAllPLI:
                {
                        [[maybe_unused]] const auto self = static_cast<const OptionalOptPLI *>(it);

                        return UINT64_MAX - 1;
                }
                break;

                case Type::Disjunction:
                {
                        const auto self = static_cast<const Disjunction *>(it);
                        uint64_t sum{0};

                        for (uint32_t i{0}; i != self->pq.size(); ++i)
                                sum += cost(self->pq.data()[i]);
                        return sum;
                }
                break;

                case Type::DisjunctionAllPLI:
                {
                        const auto self = static_cast<const DisjunctionAllPLI *>(it);
                        uint64_t sum{0};

                        for (uint32_t i{0}; i != self->pq.size(); ++i)
                                sum += cost(self->pq.data()[i]);
                        return sum;
                }
                break;

                case Type::Conjuction:
                case Type::ConjuctionAllPLI:
                {
                        const auto self = static_cast<const Conjuction *>(it);

                        return cost(self->its[0]);
                }
                break;

                case Type::Phrase:
                {
                        const auto self = static_cast<const Phrase *>(it);

                        // XXX: see phrase_cost()
                        return cost(self->its[0]) + UINT32_MAX + UINT16_MAX * self->size;
                }
                break;

                case Type::PostingsListIterator:
                {
                        const auto self = static_cast<const Codecs::PostingsListIterator *>(it);
                        const auto dec = self->decoder();

                        return dec->indexTermCtx.documents;
                }
                break;

                case Type::Dummy:
                        return 0;
        }
}

#if 0
uint16_t Trinity::DocsSetIterators::reset_depth(Iterator *const it, const uint16_t d)
{
        switch (it->type)
        {
                case Type::Filter:
                {
                        auto self = static_cast<Filter *>(it);

                        self->depth = d;
                        reset_depth(self->filter, std::numeric_limits<uint16_t>::max() / 2);
                        return reset_depth(self->req, d + 1);
                }

                case Type::Optional:
                {
                        auto self = static_cast<Optional *>(it);

                        self->depth = d;
                        reset_depth(self->opt, std::numeric_limits<uint16_t>::max() / 2);
                        return reset_depth(self->main, d + 1);
                }

                case Type::Disjunction:
                case Type::DisjunctionAllPLI:
                {
                        auto self = static_cast<Disjunction *>(it);
                        uint16_t max{0};

                        self->depth = d;
                        for (uint32_t i{0}; i != self->pq.size(); ++i)
                                max = std::max(max, reset_depth(self->pq.data()[i], d + 1));
                        return max;
                }

                case Type::Conjuction:
                {
                        auto self = static_cast<Conjuction *>(it);
                        uint16_t max{0};

                        self->depth = d;
                        for (uint32_t i{0}; i != self->size; ++i)
                                max = std::max(max, reset_depth(self->its[i], d + 1));
                        return max;
                }

                case Type::ConjuctionAllPLI:
                {
                        auto self = static_cast<ConjuctionAllPLI *>(it);
                        uint16_t max{0};

                        self->depth = d;
                        for (uint32_t i{0}; i != self->size; ++i)
                                max = std::max(max, reset_depth(self->its[i], d + 1));
                        return max;
                }

                default:
                        return it->depth = d;
        }
}
#endif

bool Trinity::DocsSetIterators::Phrase::consider_phrase_match()
{
        [[maybe_unused]] static constexpr bool trace{false};
        const auto did = curDocument.id;
        auto &rctx = *rctxRef;
        auto *const doc = rctx.document_by_id(did);
        const auto n = size;
        auto it = its[0];
        const auto firstTermID = it->decoder()->exec_ctx_termid();
        auto *const __restrict__ th = doc->materialize_term_hits(&rctx, it, firstTermID); // will create and initialize dws if not created
        auto *const __restrict__ dws = doc->matchedDocument.dws;
        const auto firstTermFreq = th->freq;
        const auto firstTermHits = th->all;

        //require(did == static_cast<const Codecs::PostingsListIterator *>(its[0])->current());

        // On one hand, we care for documents where we have CAPTURED terms, and so we only need to bind
        // this phrase to a document if all terms match.
        // On the other hand though, we don't want to materialize a term's hits more than once.
        //
        // Consider the phrase "world of warcraft". If we dematerialize the terms (world,of,warcraft) for document 10
        // but they do not form a phrase, so we advance to document 15 where we dematerialize the same terms again and now
        // they do form a phrase. Now that a phrase is matched, we rightly bind_document(&docTracker, doc), but what about document's 10
        // materialized hits for those terms? Because this phrase is no longer bound to document 10 (assuming no other iterators are bound to it either), and
        // another iterator advances to document 10 and needs to access the same terms, it means we 'll need to dematerialize them again.
        // Maybe this is not a big deal though?
        for (uint16_t i{1}; i != n; ++i)
        {
                auto it = its[i];

                //require(did == it->current());
                doc->materialize_term_hits(&rctx, it, it->decoder()->exec_ctx_termid());
        }

        if (trace)
                SLog("firstTermFreq = ", firstTermFreq, "\n");

        for (uint32_t i{0}; i != firstTermFreq; ++i)
        {
                if (const auto pos = firstTermHits[i].pos)
                {
                        if (trace)
                                SLog("For POS ", pos, "\n");

                        for (uint8_t k{1};; ++k)
                        {
                                if (k == n)
                                {
                                        // matched seq
                                        if (trace)
                                                SLog("MATCHED\n");

                                        rctx.cds_release(doc);
                                        return true;
                                }

                                const auto termID = static_cast<const Codecs::PostingsListIterator *>(its[k])->decoder()->exec_ctx_termid();

                                if (trace)
                                        SLog("Check for ", termID, " at ", pos + k, ": ", dws->test(termID, pos + k), "\n");

                                if (!dws->test(termID, pos + k))
                                        break;
                        }
                }
        }

        rctx.cds_release(doc);
        return false;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Phrase::next_impl(isrc_docid_t id)
{
restart:
        for (uint32_t i{1}; i != size; ++i)
        {
                auto it = its[i];

                if (it->current() != id)
                {
                        const auto next = it->advance(id);

                        if (next > id)
                        {
                                if (unlikely(next == DocIDsEND))
                                        return DocIDsEND;

                                id = its[0]->advance(next);

                                if (unlikely(id == DocIDsEND))
                                        return DocIDsEND;

                                goto restart;
                        }
                }
        }

        return curDocument.id = id; // we need to set curDocument to id here; required by Phrase::consider_phrase_match()
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Phrase::advance(const isrc_docid_t target)
{
        if (size)
        {
                auto id = its[0]->advance(target);

                if (unlikely(id == DocIDsEND))
                {
                        size = 0;

                        if (boundDocument)
                                rctxRef->unbind_document(boundDocument);

                        return curDocument.id = DocIDsEND;
                }

                for (id = next_impl(id);; id = next_impl(its[0]->next()))
                {
                        if (unlikely(id == DocIDsEND))
                        {
                                size = 0;

                                if (boundDocument)
                                        rctxRef->unbind_document(boundDocument);

                                return curDocument.id = DocIDsEND;
                        }
                        else if (consider_phrase_match())
                                return id;
                }
        }
        else
                return DocIDsEND; // already reset curDocument.id to DocIDsEND
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Phrase::next()
{
        if (size)
        {
                auto id = its[0]->next();

                if (unlikely(id == DocIDsEND))
                {
                        size = 0;

                        if (boundDocument)
                                rctxRef->unbind_document(boundDocument);

                        return curDocument.id = DocIDsEND;
                }

                for (id = next_impl(id);; id = next_impl(its[0]->next()))
                {
                        if (unlikely(id == DocIDsEND))
                        {
                                size = 0;

                                if (boundDocument)
                                        rctxRef->unbind_document(boundDocument);

                                return curDocument.id = DocIDsEND;
                        }
                        else if (consider_phrase_match())
                                return curDocument.id = id;
                }
        }
        else
                return DocIDsEND; // already reset curDocument.id to DocIDsEND
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::ConjuctionAllPLI::advance(const isrc_docid_t target)
{
        if (size)
        {
                const auto id = its[0]->advance(target);

                if (unlikely(id == DocIDsEND))
                {
                        size = 0;
                        return curDocument.id = DocIDsEND;
                }
                else
                        return next_impl(id);
        }
        else
                return DocIDsEND; // already reset curDocument.id to DocIDsEND
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::ConjuctionAllPLI::next()
{
        if (size)
        {
                const auto id = its[0]->next();

                if (unlikely(id == DocIDsEND))
                {
                        size = 0;
                        return curDocument.id = DocIDsEND;
                }
                else
                        return next_impl(id);
        }
        else
                return DocIDsEND; // already reset curDocument.id to DocIDsEND
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::ConjuctionAllPLI::next_impl(isrc_docid_t id)
{
restart:
        for (uint32_t i{1}; i != size; ++i)
        {
                auto it = its[i];

                if (it->current() != id)
                {
                        const auto next = it->advance(id);

                        if (next > id)
                        {
                                if (unlikely(next == DocIDsEND))
                                {
                                        // draining either of the iterators means we always need to return DocIDsEND from now on
                                        size = 0;
                                        return curDocument.id = DocIDsEND;
                                }

                                id = its[0]->advance(next);

                                if (unlikely(id == DocIDsEND))
                                {
                                        size = 0;
                                        return curDocument.id = DocIDsEND;
                                }

                                goto restart;
                        }
                }
        }

        return curDocument.id = id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Conjuction::advance(const isrc_docid_t target)
{
        if (size)
        {
                const auto id = its[0]->advance(target);

                if (unlikely(id == DocIDsEND))
                {
                        size = 0;
                        return curDocument.id = DocIDsEND;
                }
                else
                        return next_impl(id);
        }
        else
                return DocIDsEND; // already reset curDocument.id to DocIDsEND
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Conjuction::next()
{
        if (size)
        {
                const auto id = its[0]->next();

                if (unlikely(id == DocIDsEND))
                {
                        size = 0;
                        return curDocument.id = DocIDsEND;
                }
                else
                        return next_impl(id);
        }
        else
                return DocIDsEND; // already reset curDocument.id to DocIDsEND
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Conjuction::next_impl(isrc_docid_t id)
{
        static constexpr bool trace{false};
        const auto localSize{size}; // alias just in case the compiler can't do it itself

restart:
        for (uint32_t i{1}; i != localSize; ++i)
        {
                auto it = its[i];

                if (trace)
                        SLog(i, "/", size, " id = ", id, ", it->current = ", it->current(), "\n");

                if (it->current() != id)
                {
                        const auto next = it->advance(id);

                        if (trace)
                                SLog("Advanced it to ", next, "\n");

                        if (next > id)
                        {
                                if (unlikely(next == DocIDsEND))
                                {
                                        // draining either of the iterators means we always need to return DocIDsEND from now on
                                        size = 0;
                                        return curDocument.id = DocIDsEND;
                                }

                                id = its[0]->advance(next);

                                if (trace)
                                        SLog("After advancing lead to ", next, " ", id, "\n");

                                if (unlikely(id == DocIDsEND))
                                {
                                        // see earlier
                                        size = 0;
                                        return curDocument.id = DocIDsEND;
                                }
                                goto restart;
                        }
                }
        }

        return curDocument.id = id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::DisjunctionAllPLI::next()
{
        if (pq.empty())
                return DocIDsEND;

        auto top = pq.top();
        const auto doc = top->current();

        do
        {
                if (likely(top->next() != DocIDsEND))
                {
                        pq.update_top();
                        top = pq.top();
                }
                else
                {
                        pq.erase(top);
                        if (unlikely(pq.empty()))
                                return curDocument.id = DocIDsEND;
                        else
                                top = pq.top();
                }

        } while ((curDocument.id = top->current()) == doc);

        return curDocument.id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::DisjunctionAllPLI::advance(const isrc_docid_t target)
{
        if (pq.empty())
                return DocIDsEND;

        auto top = pq.top();

#if 0
        if (top->current() == target)
        {
                // already there
                return curDocument.id = target;
        }
#endif

        do
        {
                const auto res = top->advance(target);

                if (likely(res != DocIDsEND))
                {
                        pq.update_top();
                        top = pq.top();
                }
                else
                {
                        pq.erase(top);
                        if (unlikely(pq.empty()))
                                return curDocument.id = DocIDsEND;
                        else
                                top = pq.top();
                }

        } while ((curDocument.id = top->current()) < target);

        return curDocument.id;
}

#if 0
// This is a nifty idea; we don't need to first check which to advance and then advance
// we can use `lastRoundMin` to accomplish this in one step
//
// Leaving it here for posterity
Trinity::isrc_docid_t Trinity::DocsSetIterators::Disjunction::advance(const isrc_docid_t target)
{
        static constexpr bool trace{false};
        isrc_docid_t min{DocIDsEND};

        if (trace)
                SLog(ansifmt::color_blue, "advance to ", target, ", lastRoundMin = ", lastRoundMin, ansifmt::reset, "\n");

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
        capturedCnt = 0;
#endif

        auto &rctx = *rctxRef;
        const auto base = rctx.cdSTM.size - (target != DocIDsEND);

        for (uint32_t i{0}; i < size;)
        {
                auto it = its[i];

                if (trace)
                        SLog("IT.current = ", it->current(), "\n");

                if (const auto cur = it->current(); cur < target)
                {
                        if (trace)
                                SLog("cur < lastRoundMin\n");

                        if (const auto id = it->advance(target); id == DocIDsEND)
                        {
                                // drained
                                if (trace)
                                        SLog("Drained it\n");

                                its[i] = its[--size];
                        }
                        else
                        {
                                if (trace)
                                        SLog("OK, id from next = ", id, "\n");

                                if (id < min)
                                {

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                        capturedCnt = 1;
                                        captured[0] = it;
#endif

                                        min = id;
                                }
                                else if (id == min)
                                {

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                        captured[capturedCnt++] = it;
#endif
                                }

                                ++i;
                        }
                }
                else
                {
                        if (trace)
                                SLog("cur(", cur, ") > lastRoundMin\n");

                        if (cur < min)
                        {

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                capturedCnt = 1;
                                captured[0] = it;
#endif
                                min = cur;
                        }
                        else if (cur == min)
                        {

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                captured[capturedCnt++] = it;
#endif
                        }

                        ++i;
                }
        }

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
        if (trace)
                SLog("OK min ", min, ", ", capturedCnt, "\n");
#endif

        lastRoundMin = min;

        return curDocument.id = min;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Disjunction::next()
{
        // This is a nifty idea; we don't need to first check which to advance and then advance
        // we can use `lastRoundMin` to accomplish this in one step
        //
        // Problem with this and the CDS short-term-memory idea is that because
        static constexpr bool trace{false};
        isrc_docid_t min{DocIDsEND};
        auto &rctx = *curRCTX;
        const auto base{rctx.cdSTM.size};

        if (trace)
                SLog(ansifmt::color_blue, "NEXT, lastRoundMin = ", lastRoundMin, ansifmt::reset, " ", rctx.cdSTM.size, "\n");

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
        capturedCnt = 0;
#endif

        for (uint32_t i{0}; i < size;)
        {
                auto it = its[i];

                if (trace)
                        SLog("IT.current = ", it->current(), "   ", rctx.cdSTM.size, "\n");

                if (const auto cur = it->current(); cur <= lastRoundMin)
                {
                        if (trace)
                                SLog("cur < lastRoundMin\n");

                        if (const auto id = it->next(); id == DocIDsEND)
                        {
                                // drained
                                if (trace)
                                        SLog("Drained it\n");

                                its[i] = its[--size];
                        }
                        else
                        {
                                if (trace)
                                        SLog("OK, id from next = ", id, " (", rctx.cdSTM.size, "), min = ", min, "\n");

                                if (id < min)
                                {

                                        if (trace)
                                                SLog("id(", id, ") < min(", min, ") now ", rctx.cdSTM.size, "\n");

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                        capturedCnt = 1;
                                        captured[0] = it;
#endif
                                        min = id;
                                }
                                else if (id == min)
                                {

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                        captured[capturedCnt++] = it;
#endif
                                }

                                ++i;
                        }
                }
                else
                {
                        if (trace)
                                SLog("cur(", cur, ") > lastRoundMin, min = ", min, "\n");

                        if (cur < min)
                        {
                                if (trace)
                                        SLog("Yes now cur < min ", rctx.cdSTM.size, "\n");

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                capturedCnt = 1;
                                captured[0] = it;
#endif
                                min = cur;
                        }
                        else if (cur == min)
                        {
// didn't get to push anything here

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
                                captured[capturedCnt++] = it;
#endif
                        }
                        else
                        {
                                // didn't get to push anything here
                        }

                        ++i;
                }
        }

#ifdef ITERATORS_DISJUNCTION_TRACK_CAPTURED
        if (trace)
                SLog("OK min ", min, ", ", capturedCnt, ", base = ", base, " ", rctx.cdSTM.size, "\n");
#else
        if (trace)
                SLog("OK min ", min, ", base = ", base, " ", rctx.cdSTM.size, "\n");
#endif

        lastRoundMin = min;

        return curDocument.id = min;
}
#endif

Trinity::isrc_docid_t Trinity::DocsSetIterators::Disjunction::next()
{
        if (pq.empty())
                return DocIDsEND;

        auto top = pq.top();
        const auto doc = top->current();

        do
        {
                if (likely(top->next() != DocIDsEND))
                {
                        pq.update_top();
                        top = pq.top();
                }
                else
                {
                        pq.erase(top);
                        if (unlikely(pq.empty()))
                                return curDocument.id = DocIDsEND;
                        else
                                top = pq.top();
                }

        } while ((curDocument.id = top->current()) == doc);

        return curDocument.id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Disjunction::advance(const isrc_docid_t target)
{
        if (pq.empty())
                return DocIDsEND;

        auto top = pq.top();

#if 0
        if (top->current() == target)
        {
                // already there
                return curDocument.id = target;
        }
#endif

        do
        {
                const auto res = top->advance(target);

                if (likely(res != DocIDsEND))
                {
                        pq.update_top();
                        top = pq.top();
                }
                else
                {
                        pq.erase(top);
                        if (unlikely(pq.empty()))
                                return curDocument.id = DocIDsEND;
                        else
                                top = pq.top();
                }

        } while ((curDocument.id = top->current()) < target);

        return curDocument.id;
}

bool Trinity::DocsSetIterators::Filter::matches(const isrc_docid_t id)
{
        auto excl = filter->current();

        if (excl < id)
                excl = filter->advance(id);

        return excl != id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Filter::next()
{
        for (auto id = req->next();; id = req->next())
        {
                if (id == DocIDsEND)
                        return curDocument.id = DocIDsEND;
                else if (matches(id))
                        return curDocument.id = id;
        }
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::Filter::advance(const isrc_docid_t target)
{
        for (auto id = req->advance(target);; id = req->next())
        {
                if (id == DocIDsEND)
                        return curDocument.id = DocIDsEND;
                else if (matches(id))
                        return curDocument.id = id;
        }
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::OptionalOptPLI::next()
{
        const auto id = main->next();

        opt->advance(id); // It's OK if we drain it
        return curDocument.id = id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::OptionalOptPLI::advance(const isrc_docid_t target)
{
        const auto id = main->advance(target);

        opt->advance(id);
        return curDocument.id = id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::OptionalAllPLI::next()
{
        const auto id = main->next();

        opt->advance(id); // It's OK if we drain it
        return curDocument.id = id;
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::OptionalAllPLI::advance(const isrc_docid_t target)
{
        const auto id = main->advance(target);

        opt->advance(id);
        return curDocument.id = id;
}

void Trinity::DocsSetIterators::DisjunctionSome::update_current()
{
        // the top of head defines the next potential match
        // pop all documents which are on that document
        lead = head.pop();
        lead->next = nullptr;
        curDocMatchedItsCnt = 1;

        curDocument.id = lead->id;
        while (head.size() && head.top()->id == curDocument.id)
                add_lead(head.pop());
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::DisjunctionSome::next_impl()
{
        while (curDocMatchedItsCnt < matchThreshold)
        {
                if (curDocMatchedItsCnt + tail.size() >= matchThreshold)
                {
			// we may still be able to match, advance tail.top()
                        advance_tail();
                }
                else
                {
			// Match impossibl for this document
			// Advance to the next potential document that may match
                        for (auto it{lead}; it; it = it->next)
                                tail.push(it);

                        update_current();
                }
        }

        return curDocument.id;
}

Trinity::DocsSetIterators::DisjunctionSome::DisjunctionSome(Trinity::DocsSetIterators::Iterator **const iterators, const uint16_t cnt, const uint16_t minMatch)
    : Iterator{Type::DisjunctionSome}, matchThreshold{minMatch}, head{uint32_t(cnt - minMatch + 1)}, tail{uint32_t(minMatch - 1)}
{
        expect(minMatch <= cnt);
        expect(minMatch);

        trackersStorage = (it_tracker *)malloc(sizeof(it_tracker) * cnt);

        for (uint32_t i{0}; i != cnt; ++i)
        {
                auto t = trackersStorage + i;

                t->it = iterators[i];
                t->cost = cost(t->it);
                add_lead(t);
        }

        {
                // Idea: a query c1,c2,..cn with minMatch = m
                // can be rewritten to:
                // (c1 AND (c2 ..cn | msm = n -1)) OR (!c1 AND (c2 .. cn|msm = m))
                // if we assume the iterators are provided in ascending cost, then the cost
                // for the first part is the cost of c1 (because the cost of a conjuction is the cost of the least costly clause)
                // the cost of the second part is the cost of finding m matches among the c2 .. cn remaining queries
                // if we recurse infinitely, we find out what the cost of a msm query is the sum of the costs of all (cnt - minMatch + 1)
                //
                // we can just sort that and be done with it anyway, no need to use a pq, but let's use it anyway
                Switch::priority_queue<uint64_t, std::greater<uint64_t>> pq{uint32_t(cnt - minMatch + 1)};
                uint64_t evicted;

                for (auto it{lead}; it; it = it->next)
                        pq.try_push(it->cost, evicted);

                cost_ = 0;
                for (uint32_t i{0}; i != pq.size(); ++i)
                        cost_ += pq.data()[i];
        }
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::DisjunctionSome::next()
{
        it_tracker *evicted;

        for (auto it{lead}; it; it = it->next)
        {
                if (!tail.try_push(it, evicted))
                {
                        evicted->id = (evicted->id == curDocument.id)
                                          ? evicted->it->next()
                                          : evicted->it->advance(curDocument.id + 1);

                        head.push(evicted);
                }
        }

        update_current();
        return next_impl();
}

Trinity::isrc_docid_t Trinity::DocsSetIterators::DisjunctionSome::advance(const isrc_docid_t target)
{
        it_tracker *evicted;

        for (auto it{lead}; it; it = it->next)
        {
                if (!tail.try_push(it, evicted))
                {
                        evicted->id = evicted->it->advance(target);
                        head.push(evicted);
                }
        }

        for (auto top = head.top();
             top->id < target; top = head.top())
        {
                // We know the tail is full, because it contains at most
                // (matchThreshold - 1) entries, and we have moved at least matchThreshold entries to it, so try_push()
                // would return false
                tail.try_push(top, evicted);

                evicted->id = evicted->it->advance(target);
                head.update_top(evicted);
        }

        update_current();
        return next_impl();
}

void Trinity::DocsSetIterators::DisjunctionSome::advance_tail(it_tracker *const top)
{
        top->id = top->it->advance(curDocument.id);

        if (top->id == curDocument.id)
                add_lead(top);
        else
                head.push(top);
}

void Trinity::DocsSetIterators::DisjunctionSome::update_matched_cnt()
{
	// We return the next document when there are matchThreshold matching iterators
	// but some of the iterators in tail might match as well.
	// In general, we want to advance least-costly iterators first in order to skip over non-matching
	// documents as fast as possible.
	// Here however we are advancing every iterator anyway, so iterating ovedr iterators in (roughly) cost-descending
	// order might help avoid some permutations in the head heap.
	for (int32_t i = int32_t(tail.size())  - 1; i >= 0; --i)
		advance_tail(tail.data()[i]);
	tail.clear();
}
