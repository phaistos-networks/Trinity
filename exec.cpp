#include "exec.h"
#include "docset_spans.h"
#include "docwordspace.h"
#include "matches.h"
#include "queryexec_ctx.h"
#include "similarity.h"
#include <prioqueue.h>

#include <memory>

using namespace Trinity;
thread_local Trinity::queryexec_ctx *curRCTX;

namespace // static/local this module
{
        [[maybe_unused]] static constexpr bool traceExec{false};
        static constexpr bool                  traceCompile{false};
} // namespace

#pragma mark    execution specific optimizations
static uint64_t reorder_execnode(exec_node &n, bool &updates, queryexec_ctx &);

//WAS: return rctx.term_ctx(p->termIDs[0]).documents;
//Now it's proprortional to the number of terms of the phrase
//because we 'd like to avoid avoid materializing hits for un-necessary documents if possible, and we
//need to materialize for processing phrases
static uint64_t phrase_cost(queryexec_ctx &rctx, const compilation_ctx::phrase *const p) {
        // boost it because its's a phrase so we will need to deserialize hits
        //
        // XXX: we only consider the lead here but that's perhaps not a great idea
        // We should take the phrase size into account, so that a phrase "a b c" should be more expensive than "a b"
        // where they have the same lead, but size is shorter. Conversely "a b" should be more expensive than "a c" if
        // b is more popular than rc
        return rctx.term_ctx(p->termIDs[0]).documents + UINT32_MAX + UINT16_MAX * p->size;
}

static uint64_t reorder_execnode_impl(exec_node &n, bool &updates, queryexec_ctx &rctx) {
        if (n.fp == ENT::matchterm)
                return rctx.term_ctx(n.u16).documents;
        else if (n.fp == ENT::matchphrase) {
                const auto p = static_cast<const compilation_ctx::phrase *>(n.ptr);

                return phrase_cost(rctx, p);
        } else if (n.fp == ENT::logicaland) {
                auto       ctx     = static_cast<compilation_ctx::binop_ctx *>(n.ptr);
                const auto lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                if (rhsCost < lhsCost) {
                        std::swap(ctx->lhs, ctx->rhs);
                        updates = true;
                        return rhsCost;
                } else
                        return lhsCost;
        } else if (n.fp == ENT::logicalnot) {
                auto *const                 ctx     = static_cast<compilation_ctx::binop_ctx *>(n.ptr);
                const auto                  lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                [[maybe_unused]] const auto rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                return lhsCost;
        } else if (n.fp == ENT::logicalor) {
                auto *const ctx     = static_cast<compilation_ctx::binop_ctx *>(n.ptr);
                const auto  lhsCost = reorder_execnode(ctx->lhs, updates, rctx);
                const auto  rhsCost = reorder_execnode(ctx->rhs, updates, rctx);

                return lhsCost + rhsCost;
        } else if (n.fp == ENT::unaryand || n.fp == ENT::unarynot) {
                auto *const ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                return reorder_execnode(ctx->expr, updates, rctx);
        } else if (n.fp == ENT::consttrueexpr) {
                auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                // it is important to return UINT64_MAX - 1 so that it will not result in a binop's (lhs, rhs) swap
                // we need to special-case the handling of those nodes
                reorder_execnode(ctx->expr, updates, rctx);
                return UINT64_MAX - 1;
        } else if (n.fp == ENT::matchsome) {
                auto pm = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);

                for (size_t i{0}; i != pm->size; ++i)
                        reorder_execnode(pm->nodes[i], updates, rctx);
                return UINT64_MAX - 1;
        } else if (n.fp == ENT::matchallnodes || n.fp == ENT::matchanynodes) {
                auto g = static_cast<compilation_ctx::nodes_group *>(n.ptr);

                for (size_t i{0}; i != g->size; ++i)
                        reorder_execnode(g->nodes[i], updates, rctx);
                return UINT64_MAX - 1;
        } else if (n.fp == ENT::matchallterms) {
                const auto run = static_cast<const compilation_ctx::termsrun *>(n.ptr);

                return rctx.term_ctx(run->terms[0]).documents;
        } else if (n.fp == ENT::matchanyterms) {
                const auto run = static_cast<const compilation_ctx::termsrun *>(n.ptr);
                uint64_t   sum{0};

                for (size_t i{0}; i != run->size; ++i)
                        sum += rctx.term_ctx(run->terms[i]).documents;
                return sum;
        } else if (n.fp == ENT::matchallphrases) {
                const auto *const __restrict__ run = static_cast<const compilation_ctx::phrasesrun *>(n.ptr);

                return phrase_cost(rctx, run->phrases[0]) * run->size; // This may or may not make sense
        } else if (n.fp == ENT::matchanyphrases) {
                const auto *const __restrict__ run = static_cast<compilation_ctx::phrasesrun *>(n.ptr);
                uint64_t sum{0};

                for (size_t i{0}; i != run->size; ++i)
                        sum += phrase_cost(rctx, run->phrases[i]);
                return sum;
        } else {
                SLog("Unexpected:", n, "\n");
                std::abort();
        }
}

uint64_t reorder_execnode(exec_node &n, bool &updates, queryexec_ctx &rctx) {
        return n.cost = reorder_execnode_impl(n, updates, rctx);
}

static exec_node reorder_execnodes(exec_node n, queryexec_ctx &rctx) {
        bool updates;

        do {
                updates = false;
                reorder_execnode(n, updates, rctx);
        } while (updates);

        return n;
}

static exec_node prepare_tree(exec_node root, queryexec_ctx &rctx) {
        static constexpr bool                            traceMetrics{false};
        size_t                                           totalNodes{0};
        uint64_t                                         before;
        std::vector<exec_node>                           stack;
        std::vector<exec_node *>                         stackP;
        std::vector<std::pair<exec_term_id_t, uint32_t>> v;

        before = Timings::Microseconds::Tick();
        stackP.clear();
        stackP.push_back(&root);

        do {
                auto ptr = stackP.back();
                auto n   = *ptr;

                stackP.pop_back();
                require(n.fp != ENT::constfalse);
                require(n.fp != ENT::dummyop);

                ++totalNodes;
                if (n.fp == ENT::matchallterms) {
                        auto ctx = static_cast<compilation_ctx::termsrun *>(n.ptr);

                        v.clear();
                        for (size_t i{0}; i != ctx->size; ++i) {
                                const auto termID = ctx->terms[i];

                                if constexpr (traceCompile)
                                        SLog("AND ", termID, " ", rctx.term_ctx(termID).documents, "\n");

                                v.emplace_back(termID, rctx.term_ctx(termID).documents);
                        }

                        std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) noexcept { return a.second < b.second; });

                        for (size_t i{0}; i != ctx->size; ++i)
                                ctx->terms[i] = v[i].first;
                } else if (n.fp == ENT::matchanyterms) {
                        // There are no real benefits to sorting terms for ENT::matchanyterms but we 'll do it anyway because its cheap
                        // This is actually useful, for leaders(deprecated)
                        auto ctx = static_cast<compilation_ctx::termsrun *>(n.ptr);

                        v.clear();
                        for (size_t i{0}; i != ctx->size; ++i) {
                                const auto termID = ctx->terms[i];

                                if constexpr (traceCompile)
                                        SLog("OR ", termID, " ", rctx.term_ctx(termID).documents, "\n");

                                v.emplace_back(termID, rctx.term_ctx(termID).documents);
                        }

                        std::sort(v.begin(), v.end(), [](const auto &a, const auto &b) noexcept { return a.second < b.second; });

                        for (size_t i{0}; i != ctx->size; ++i)
                                ctx->terms[i] = v[i].first;
                } else if (n.fp == ENT::logicaland || n.fp == ENT::logicalor || n.fp == ENT::logicalnot) {
                        auto ctx = static_cast<compilation_ctx::binop_ctx *>(n.ptr);

                        stackP.push_back(&ctx->lhs);
                        stackP.push_back(&ctx->rhs);
                } else if (n.fp == ENT::unaryand || n.fp == ENT::unarynot || n.fp == ENT::consttrueexpr) {
                        auto ctx = static_cast<compilation_ctx::unaryop_ctx *>(n.ptr);

                        stackP.push_back(&ctx->expr);
                } else if (n.fp == ENT::matchsome) {
                        auto ctx = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);

                        for (size_t i{0}; i != ctx->size; ++i) {
                                stackP.emplace_back(&ctx->nodes[i]);
                        }
                }
        } while (!stackP.empty());

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to sort runs, ", dotnotation_repr(totalNodes), " exec_nodes\n");

        // Fourth Pass
        // Reorder ENT::logicaland nodes (lhs, rhs) so that the least expensive to evaluate is always found in the lhs branch
        // This also helps with moving tokens before phrases
        before = Timings::Microseconds::Tick();

        root = reorder_execnodes(root, rctx);

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to reorder exec nodes\n");

        // JIT:
        // Now that are done building the execution plan (a tree of exec_nodes), it should be fairly simple to
        // perform JIT and compile it down to x86-64 code.
        // Please see: https://github.com/phaistos-networks/Trinity/wiki/JIT-compilation

        // NOW, prepare decoders
        // No need to have done so if we could have determined that the query would have failed anyway
        // This could take some time - for 52 distinct terms it takes 0.002s (>1ms)
        before = Timings::Microseconds::Tick();
        for (const auto &kv : rctx.tctxMap) {
                const auto termID = kv.first;

                rctx.prepare_decoder(termID);
        }

        if (traceMetrics)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " ", Timings::Microseconds::ToMillis(Timings::Microseconds::Since(before)), " ms  to initialize all decoders ", rctx.tctxMap.size(), "\n");

        return root;
}

#pragma mark iterators builder
static bool  all_pli(const std::vector<DocsSetIterators::Iterator *> &its) noexcept {
        for (const auto it : its) {
                if (it->type != DocsSetIterators::Type::PostingsListIterator)
                        return false;
        }
        return true;
}

void PrintImpl(Buffer &b, const exec_node &n); // compilation_ctx.cpp

DocsSetIterators::Iterator *queryexec_ctx::build_iterator(const exec_node n, const uint32_t execFlags) {
        if (n.fp == ENT::matchallterms) {
                const auto                  run = static_cast<const compilation_ctx::termsrun *>(n.ptr);
                DocsSetIterators::Iterator *decoders[run->size];

                for (size_t i{0}; i != run->size; ++i) {
                        auto pli = reg_pli(decode_ctx.decoders[run->terms[i]]->new_iterator());

                        decoders[i] = pli;
                }

                return reg_docset_it(new DocsSetIterators::Conjuction(decoders, run->size));
        } else if (n.fp == ENT::matchanyterms) {
                const auto                  run = static_cast<const compilation_ctx::termsrun *>(n.ptr);
                DocsSetIterators::Iterator *decoders[run->size];

                for (size_t i{0}; i != run->size; ++i) {
                        auto pli = reg_pli(decode_ctx.decoders[run->terms[i]]->new_iterator());

                        decoders[i] = pli;
                }

                return reg_docset_it(new DocsSetIterators::DisjunctionAllPLI(decoders, run->size));
        } else if (n.fp == ENT::matchsome) {
                const auto                  g = static_cast<const compilation_ctx::partial_match_ctx *>(n.ptr);
                DocsSetIterators::Iterator *its[g->size];

                for (size_t i{0}; i != g->size; ++i)
                        its[i] = build_iterator(g->nodes[i], execFlags);

                return reg_docset_it(new DocsSetIterators::DisjunctionSome(its, g->size, g->min));
        } else if (n.fp == ENT::matchphrase) {
                const auto                    p = static_cast<const compilation_ctx::phrase *>(n.ptr);
                Codecs::PostingsListIterator *its[p->size];

                for (size_t i{0}; i != p->size; ++i) {
                        const auto info = tctxMap[p->termIDs[i]];

                        require(info.first.documents);
                        require(info.second);

                        its[i] = reg_pli(decode_ctx.decoders[p->termIDs[i]]->new_iterator());
                }

                return reg_docset_it(new DocsSetIterators::Phrase(this, its, p->size, execFlags & unsigned(ExecFlags::AccumulatedScoreScheme), execFlags &unsigned(ExecFlags::DocumentsOnly)));
        } else if (n.fp == ENT::matchanyphrases) {
                const auto                  run = static_cast<const compilation_ctx::phrasesrun *>(n.ptr);
                DocsSetIterators::Iterator *its[run->size];

                for (uint32_t pit{0}; pit != run->size; ++pit) {
                        const auto                    p = run->phrases[pit];
                        Codecs::PostingsListIterator *tits[p->size];

                        for (size_t i{0}; i != p->size; ++i)
                                tits[i] = reg_pli(decode_ctx.decoders[p->termIDs[i]]->new_iterator());

                        its[pit] = reg_docset_it(new DocsSetIterators::Phrase(this, tits, p->size, execFlags & unsigned(ExecFlags::AccumulatedScoreScheme), execFlags &unsigned(ExecFlags::DocumentsOnly)));
                }

                return reg_docset_it(new DocsSetIterators::Disjunction(its, run->size));
        } else if (n.fp == ENT::matchallphrases) {
                const auto                  run = static_cast<const compilation_ctx::phrasesrun *>(n.ptr);
                DocsSetIterators::Iterator *its[run->size];

                for (uint32_t pit{0}; pit != run->size; ++pit) {
                        const auto                    p = run->phrases[pit];
                        Codecs::PostingsListIterator *tits[p->size];

                        for (size_t i{0}; i != p->size; ++i)
                                tits[i] = reg_pli(decode_ctx.decoders[p->termIDs[i]]->new_iterator());

                        its[pit] = reg_docset_it(new DocsSetIterators::Phrase(this, tits, p->size, execFlags & unsigned(ExecFlags::AccumulatedScoreScheme), execFlags &unsigned(ExecFlags::DocumentsOnly)));
                }

                return reg_docset_it(new DocsSetIterators::Conjuction(its, run->size));
        } else if (n.fp == ENT::logicalor) {
                // <foo> | bar => (foo | bar)
                const auto                                e = static_cast<const compilation_ctx::binop_ctx *>(n.ptr);
                std::vector<DocsSetIterators::Iterator *> its;
                DocsSetIterators::Iterator *              v[2] = {build_iterator(e->lhs, execFlags), build_iterator(e->rhs, execFlags)};

                if constexpr (traceCompile)
                        SLog("Compiling logical OR\n");

                // Pulling Iterators from (lhs, rhs) to this disjunction when possible is extremely important
                // Over 50% perf.improvement
                for (size_t i{0}; i != 2; ++i) {
                        auto it = v[i];

                        if (it->type == DocsSetIterators::Type::Disjunction) {
                                auto internal = static_cast<DocsSetIterators::Disjunction *>(it);

                                while (internal->pq.size()) {
                                        its.emplace_back(internal->pq.top());
                                        internal->pq.pop();
                                }
                        } else if (it->type == DocsSetIterators::Type::DisjunctionAllPLI) {
                                auto internal = static_cast<DocsSetIterators::DisjunctionAllPLI *>(it);

                                while (internal->pq.size()) {
                                        its.emplace_back(internal->pq.top());
                                        internal->pq.pop();
                                }
                        } else
                                its.push_back(it);
                }

                if constexpr (traceCompile)
                        SLog("Final ", its.size(), " ", execFlags & unsigned(ExecFlags::DocumentsOnly), ": ", all_pli(its), "\n");

                return reg_docset_it(all_pli(its)
                                         ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::DisjunctionAllPLI(its.data(), its.size()))
                                         : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Disjunction(its.data(), its.size())));
        } else if (n.fp == ENT::logicaland) {
                const auto e = static_cast<const compilation_ctx::binop_ctx *>(n.ptr);

		// specialize
                if (e->lhs.fp == ENT::consttrueexpr) {
                        const auto op = static_cast<const compilation_ctx::unaryop_ctx *>(e->lhs.ptr);

                        return reg_docset_it(new DocsSetIterators::Optional(build_iterator(e->rhs, execFlags), build_iterator(op->expr, execFlags)));
                } else if (e->rhs.fp == ENT::consttrueexpr) {
                        const auto op = static_cast<const compilation_ctx::unaryop_ctx *>(e->rhs.ptr);

                        return reg_docset_it(new DocsSetIterators::Optional(build_iterator(e->lhs, execFlags), build_iterator(op->expr, execFlags)));
                } else {
                        std::vector<DocsSetIterators::Iterator *> its;
                        Trinity::DocsSetIterators::Iterator *     v[2] = {build_iterator(e->lhs, execFlags), build_iterator(e->rhs, execFlags)};

                        for (size_t i{0}; i != 2; ++i) {
                                auto it = v[i];

				// see ENT::logicalor optimization
                                if (it->type == DocsSetIterators::Type::Conjuction || it->type == DocsSetIterators::Type::ConjuctionAllPLI) {
                                        const auto internal = static_cast<DocsSetIterators::Conjuction *>(it);

                                        for (size_t i{0}; i != internal->size; ++i)
                                                its.emplace_back(internal->its[i]);
                                } else
                                        its.push_back(it);
                        }

                        if constexpr (traceCompile)
                                SLog("final ", its.size(), "\n");

                        return reg_docset_it(all_pli(its)
                                                 ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::ConjuctionAllPLI(its.data(), its.size()))
                                                 : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Conjuction(its.data(), its.size())));
                }
        } else if (n.fp == ENT::matchallnodes) {
                std::vector<DocsSetIterators::Iterator *> its;
                const auto                                g = static_cast<const compilation_ctx::nodes_group *>(n.ptr);

                its.reserve(g->size);
                for (size_t i{0}; i != g->size; ++i)
                        its.emplace_back(build_iterator(g->nodes[i], execFlags));

                return reg_docset_it(all_pli(its)
                                         ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::ConjuctionAllPLI(its.data(), its.size()))
                                         : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Conjuction(its.data(), its.size())));
        } else if (n.fp == ENT::matchanynodes) {
                std::vector<DocsSetIterators::Iterator *> its;
                const auto                                g = static_cast<const compilation_ctx::nodes_group *>(n.ptr);

                its.reserve(g->size);
                for (size_t i{0}; i != g->size; ++i)
                        its.emplace_back(build_iterator(g->nodes[i], execFlags));

                return reg_docset_it(all_pli(its)
                                         ? static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::DisjunctionAllPLI(its.data(), its.size()))
                                         : static_cast<DocsSetIterators::Iterator *>(new DocsSetIterators::Disjunction(its.data(), its.size())));
        } else if (n.fp == ENT::logicalnot) {
                const auto e = static_cast<const compilation_ctx::binop_ctx *>(n.ptr);

                return reg_docset_it(new DocsSetIterators::Filter(build_iterator(e->lhs, execFlags), build_iterator(e->rhs, execFlags)));
        } else if (n.fp == ENT::matchterm) {
                return reg_pli(decode_ctx.decoders[n.u16]->new_iterator());
        } else if (n.fp == ENT::unaryand) {
                auto *const ctx = static_cast<const compilation_ctx::unaryop_ctx *>(n.ptr);

                return build_iterator(ctx->expr, execFlags);
        } else if (n.fp == ENT::consttrueexpr) {
                // not part of a binary op.
                const auto op = static_cast<const compilation_ctx::unaryop_ctx *>(n.ptr);

                if constexpr (traceCompile)
                        SLog("ConstTrueExpr not part of a binary op?\n");

                return build_iterator(op->expr, execFlags);
        } else {
                if (traceCompile || traceExec) {
                        SLog("Not supported\n");
                        exit(1);
                } else
                        std::abort();
        }
}

#pragma mark                        docsset spans builder
static std::unique_ptr<DocsSetSpan> build_span(DocsSetIterators::Iterator *root, queryexec_ctx *const rctx) {
        if (root->type == DocsSetIterators::Type::DisjunctionSome && (rctx->documentsOnly || rctx->accumScoreMode)) {
                auto                                               d = static_cast<DocsSetIterators::DisjunctionSome *>(root);
                std::vector<Trinity::DocsSetIterators::Iterator *> its;

                for (auto it{d->lead}; it; it = it->next)
                        its.emplace_back(it->it);

                // Either DocsSetSpanForDisjunctionsWithThresholdAndCost or DocsSetSpanForDisjunctionsWithThreshold
                // take the same time if we are dealing with iterators that are just PostingsListIterator
                // though if we have phrases and other complex binary ops, cost makes more sense, so we 'll settle for
                // DocsSetSpanForDisjunctionsWithThresholdAndCost
                return std::make_unique<DocsSetSpanForDisjunctionsWithThresholdAndCost>(d->matchThreshold, its, rctx->accumScoreMode);
                //return std::make_unique<DocsSetSpanForDisjunctionsWithThreshold>(d->matchThreshold, its, rctx->accumScoreMode);
        } else if ((rctx->documentsOnly || rctx->accumScoreMode) && (root->type == DocsSetIterators::Type::Disjunction || root->type == DocsSetIterators::Type::DisjunctionAllPLI)) {
                std::vector<Trinity::DocsSetIterators::Iterator *> its;

                switch (root->type) {
                        case DocsSetIterators::Type::Disjunction:
                                for (auto containerIt : static_cast<DocsSetIterators::Disjunction *>(root)->pq)
                                        its.emplace_back(containerIt);
                                break;

                        case DocsSetIterators::Type::DisjunctionAllPLI:
                                for (auto containerIt : static_cast<DocsSetIterators::DisjunctionAllPLI *>(root)->pq)
                                        its.emplace_back(containerIt);
                                break;

                        default:
                                std::abort();
                }

                if (rctx->accumScoreMode)
                        return std::make_unique<DocsSetSpanForDisjunctionsWithThreshold>(1, its, true);
                else
                        return std::make_unique<DocsSetSpanForDisjunctions>(its);
        } else if (root->type == DocsSetIterators::Type::Filter) {
                const auto f          = static_cast<DocsSetIterators::Filter *>(root);
                const auto filterCost = Trinity::DocsSetIterators::cost(f->filter);
                const auto reqCost    = Trinity::DocsSetIterators::cost(f->req);

                if (traceCompile || traceExec)
                        SLog("filterCost ", filterCost, " reqCost ", reqCost, "\n");

                if (filterCost <= reqCost) {
                        auto req = build_span(f->req, rctx);

                        return std::make_unique<FilteredDocsSetSpan>(req.release(), f->filter);
                } else
                        return std::make_unique<GenericDocsSetSpan>(root);
        } else {
                return std::make_unique<GenericDocsSetSpan>(root);
        }
}

#pragma mark Trinity Queries Execution Engine

void Trinity::exec_query(const query &in,
                         IndexSource *const __restrict__ idxsrc,
                         masked_documents_registry *const __restrict__ maskedDocumentsRegistry,
                         MatchedIndexDocumentsFilter *__restrict__ const matchesFilter,
                         IndexDocumentsFilter *__restrict__ const documentsFilter,
                         const uint32_t                      execFlags,
                         Similarity::IndexSourceTermsScorer *scorer) {
        struct query_term_instance final
            : public query_term_ctx::instance_struct {
                str8_t token;
        };

        if (!in) {
                if constexpr (traceCompile)
                        SLog("No root node\n");

                return;
        }

        // We need a copy of that query here
        // for we we will need to modify it
        const auto _start = Timings::Microseconds::Tick();
        query      q(in, true); // shallow copy, no need for a deep copy here

        // Normalize just in case
        if (!q.normalize()) {
                if constexpr (traceCompile)
                        SLog("No root node after normalization\n");

                return;
        }

        const bool documentsOnly  = execFlags & uint32_t(ExecFlags::DocumentsOnly);
        const bool accumScoreMode = execFlags & uint32_t(ExecFlags::AccumulatedScoreScheme);
        const bool defaultMode    = !documentsOnly && !accumScoreMode;

        // We need to collect all term instances in the query
        // so that we the score function will be able to take that into account (See matched_document::queryTermInstances)
        // We only need to do this for specific AST branches and node types(i.e we ignore all RHS expressions of logical NOT nodes)
        //
        // This must be performed before any query optimizations, for otherwise because the optimiser will most definitely rearrange the query, doing it after
        // the optimization passes will not capture the original, input query tokens instances information.
        //
        // This is required if the default execution mode is selected
        std::vector<query_term_instance> originalQueryTokenInstances;

        if (accumScoreMode) {
                // Just in case
                EXPECT(scorer);
        }

        if (defaultMode) {
                std::vector<ast_node *> stack{q.root}; // use a stack because we don't care about the evaluation order
                std::vector<phrase *>   collected;

                // collect phrases from the AST
                do {
                        auto n = stack.back();

                        stack.pop_back();
                        switch (n->type) {
                                case ast_node::Type::Token:
                                case ast_node::Type::Phrase:
                                        collected.emplace_back(n->p);
                                        break;

                                case ast_node::Type::MatchSome:
                                        stack.insert(stack.end(), n->match_some.nodes, n->match_some.nodes + n->match_some.size);
                                        break;

                                case ast_node::Type::UnaryOp:
                                        if (n->unaryop.op != Operator::NOT)
                                                stack.emplace_back(n->unaryop.expr);
                                        break;

                                case ast_node::Type::ConstTrueExpr:
                                        stack.emplace_back(n->expr);
                                        break;

                                case ast_node::Type::BinOp:
                                        if (n->binop.op == Operator::AND || n->binop.op == Operator::STRICT_AND || n->binop.op == Operator::OR) {
                                                stack.emplace_back(n->binop.lhs);
                                                stack.emplace_back(n->binop.rhs);
                                        } else if (n->binop.op == Operator::NOT)
                                                stack.emplace_back(n->binop.lhs);
                                        break;

                                default:
                                        break;
                        }
                } while (!stack.empty());

                // collected phrases
                for (const auto it : collected) {
                        const uint8_t rep = it->size == 1 ? it->rep : 1;
                        const auto    toNextSpan{it->toNextSpan};
                        const auto    flags{it->flags};
                        const auto    rewriteRange{it->rewrite_ctx.range};
                        const auto    translationCoefficient{it->rewrite_ctx.translationCoefficient};
                        const auto    srcSeqSize{it->rewrite_ctx.srcSeqSize};
                        const auto    app_phrase_id{it->app_phrase_id};

                        // for each phrase token
                        for (uint16_t pos{it->index}, i{0}; i != it->size; ++i, ++pos) {
                                if constexpr (traceCompile)
                                        SLog("Collected instance: [", it->terms[i].token, "] index:", pos, " rep:", rep, " toNextSpan:", i == (it->size - 1) ? toNextSpan : 1, "\n");

                                originalQueryTokenInstances.push_back({{pos, flags, rep, uint8_t(i == (it->size - 1) ? toNextSpan : 1), app_phrase_id, {rewriteRange, translationCoefficient, srcSeqSize}}, it->terms[i].token}); // need to be careful to get this right for phrases
                        }
                }
        }

        if constexpr (traceCompile)
                SLog("Compiling:", q, "\n");

        queryexec_ctx rctx(idxsrc, documentsOnly, accumScoreMode);

        struct comp_ctx final
            : public compilation_ctx {
                queryexec_ctx *const rctx;

                comp_ctx(queryexec_ctx *const r)
                    : rctx{r} {
                }

		// To get (index => query term), you can use
		// get all distinct terms (index=>str8_t) from the query
		// and use query_term_ctx::term_struct::id to index in that set
                inline uint16_t resolve_query_term(const str8_t term) final {
                        const auto res = rctx->resolve_term(term);

                        if constexpr (traceCompile)
                                SLog("Attempting to resolve [", term, "] ", res, "\n");

                        return res;
                }

        } compilationCtx(&rctx);

        const auto before       = Timings::Microseconds::Tick();
        auto       rootExecNode = compile_query(q.root, compilationCtx);

        if constexpr (traceCompile)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to compile, ", duration_repr(Timings::Microseconds::Since(_start)), " since start:", rootExecNode, "\n");

        if (unlikely(rootExecNode.fp == ENT::dummyop || rootExecNode.fp == ENT::constfalse)) {
                if constexpr (traceCompile)
                        SLog("Nothing to do\n");

                return;
        }

        // Prepare and further optimize tree for execution
        rootExecNode = prepare_tree(rootExecNode, rctx);

        // Now that we have compiled the AST into an execution nodes tree, we could
        // group nodes into matchallnodes and matchanynodes groups.
        // There is really no need to do it now, but for a Percolator like scheme, where
        // you want to attempt to matchd documents against queries, it would be very handy.
        //
        // TODO(markp): we need to move some state out of queryexec_ctx, to e.g a compilation_ctx
        // which can exist independently of a queryexec_ctx, so that we can use it for a percolator impl.
        // group_execnodes(rootExecNode, rctx.allocator);

        // see query_index_terms and MatchedIndexDocumentsFilter::prepare() comments
        query_index_terms **queryIndicesTerms;
        const auto          maxQueryTermIDPlus1 = rctx.termsDict.size() + 1;

        curRCTX         = &rctx;
        curRCTX->scorer = scorer;

        if (defaultMode) {
                std::vector<const query_term_instance *>           collected;
                std::vector<std::pair<uint16_t, query_index_term>> originalQueryTokensTracker; // query index => (termID, toNextSpan)
                std::vector<query_index_term>                      list;
                uint16_t                                           maxIndex{0};

                // Build rctx.originalQueryTermInstances
                // It is important to only do this after we have optimised the copied original query, just as it is important
                // to capture the original query instances before we optimise.
                //
                // We need access to that information for scoring documents -- see matches.h
                if (const auto required = sizeof(query_term_ctx *) * maxQueryTermIDPlus1; rctx.allocator.can_allocate(required))
                        rctx.originalQueryTermCtx = static_cast<query_term_ctx **>(rctx.allocator.Alloc(required));
                else {
                        rctx.originalQueryTermCtx = static_cast<query_term_ctx **>(malloc(required));
                        rctx.large_allocs.emplace_back(rctx.originalQueryTermCtx);
                }

                memset(rctx.originalQueryTermCtx, 0, sizeof(query_term_ctx *) * maxQueryTermIDPlus1);
                std::sort(originalQueryTokenInstances.begin(), originalQueryTokenInstances.end(), [](const auto &a, const auto &b) noexcept { return terms_cmp(a.token.data(), a.token.size(), b.token.data(), b.token.size()) < 0; });

                for (const auto *p = originalQueryTokenInstances.data(), *const e = p + originalQueryTokenInstances.size(); p != e;) {
                        const auto token = p->token;

                        if constexpr (traceCompile)
                                SLog("Collecting token [", token, "]\n");

			// only if this token has actually been used in the compiled query
                        if (const auto termID = rctx.termsDict[token]) {
                                collected.clear();
                                do {
                                        collected.emplace_back(p);
                                } while (++p != e && p->token == token);

                                if constexpr (traceCompile)
                                        SLog("Collected ", collected.size(), " for token [", token, "]\n");

                                const auto cnt = collected.size();

                                // XXX: maybe we should just support more instances?
                                DEXPECT(cnt <= sizeof(query_term_ctx::instancesCnt) << 8);

                                auto p = static_cast<query_term_ctx *>(rctx.allocator.Alloc(sizeof(query_term_ctx) + cnt * sizeof(query_term_ctx::instance_struct)));

                                p->instancesCnt = cnt;
                                p->term.id      = termID;
                                p->term.token   = token;

                                std::sort(collected.begin(), collected.end(), [](const auto &a, const auto &b) noexcept { return a->index < b->index; });
                                for (size_t i{0}; i != collected.size(); ++i) {
                                        auto it = collected[i];

                                        p->instances[i].index                              = it->index;
                                        p->instances[i].rep                                = it->rep;
                                        p->instances[i].flags                              = it->flags;
                                        p->instances[i].toNextSpan                         = it->toNextSpan;
                                        p->instances[i].app_phrase_id                      = it->app_phrase_id;
                                        p->instances[i].rewrite_ctx.range                  = it->rewrite_ctx.range;
                                        p->instances[i].rewrite_ctx.translationCoefficient = it->rewrite_ctx.translationCoefficient;
                                        p->instances[i].rewrite_ctx.srcSeqSize             = it->rewrite_ctx.srcSeqSize;

                                        if constexpr (traceCompile)
                                                SLog("<<<<<< token index ", it->index, "\n");

                                        maxIndex = std::max(maxIndex, it->index);
                                        originalQueryTokensTracker.push_back({it->index, {.termID = termID, .flags = it->flags, .toNextSpan = it->toNextSpan}});
                                }

                                rctx.originalQueryTermCtx[termID] = p;
                        } else {
                                // this original query token is not used in the optimised query
                                // rctx.originalQueryTermCtx[termID] will be nullptr
                                // see capture_matched_term() for why this is important.

                                if constexpr (traceCompile)
                                        SLog("Ignoring ", token, "\n");

                                do {
                                        ++p;
                                } while (p != e && p->token == token);
                        }
                }

                // See docwordspace.h comments
                // we are allocated (maxIndex + 8) and memset() that to 0 in order to make some optimizations possible in consider()
                if (const auto required = sizeof(query_index_terms *) * (maxIndex + 8); rctx.allocator.can_allocate(required))
                        queryIndicesTerms = static_cast<query_index_terms **>(rctx.allocator.Alloc(required));
                else {
                        queryIndicesTerms = static_cast<query_index_terms **>(malloc(required));
                        rctx.large_allocs.emplace_back(queryIndicesTerms);
                }

                memset(queryIndicesTerms, 0, sizeof(queryIndicesTerms[0]) * (maxIndex + 8));
                std::sort(originalQueryTokensTracker.begin(), originalQueryTokensTracker.end(), [](const auto &a, const auto &b) noexcept {
                        if (a.first < b.first)
                                return true;
                        else if (a.first == b.first) {
                                if (a.second.termID < b.second.termID)
                                        return true;
                                else if (a.second.termID == b.second.termID) {
                                        if (a.second.toNextSpan < b.second.toNextSpan)
                                                return true;
                                        else if (a.second.toNextSpan == b.second.toNextSpan)
                                                return a.second.flags < b.second.flags;
                                }
                        }

                        return false;
                });

                if (execFlags & uint32_t(ExecFlags::DisregardTokenFlagsForQueryIndicesTerms)) {
                        for (const auto *p = originalQueryTokensTracker.data(), *const e = p + originalQueryTokensTracker.size(); p != e;) {
                                const auto idx = p->first;

                                list.clear();
                                do {
                                        const auto info = p->second;

                                        list.push_back({.termID = info.termID, .flags = 0, .toNextSpan = info.toNextSpan});
                                        do {
                                                ++p;
                                        } while (p != e && p->first == idx && p->second.termID == info.termID && p->second.toNextSpan == info.toNextSpan);
                                } while (p != e && p->first == idx);

                                if constexpr (traceCompile) {
                                        SLog("For index ", idx, " ", list.size(), "\n");

                                        for (const auto &it : list)
                                                SLog("(", it.termID, ", ", it.toNextSpan, ")\n");
                                }

                                const uint16_t cnt = list.size();
                                auto           ptr = static_cast<query_index_terms *>(rctx.allocator.Alloc(sizeof(query_index_terms) + cnt * sizeof(query_index_term)));

                                ptr->cnt = cnt;
                                memcpy(ptr->uniques, list.data(), cnt * sizeof(query_index_term));
                                queryIndicesTerms[idx] = ptr;
                        }
                } else {
                        for (const auto *p = originalQueryTokensTracker.data(), *const e = p + originalQueryTokensTracker.size(); p != e;) {
                                const auto idx = p->first;

                                // unique query_index_term for idx
                                list.clear();
                                do {
                                        const auto info = p->second;

                                        list.emplace_back(info);
                                        do {
                                                ++p;
                                        } while (p != e && p->first == idx && p->second == info);

                                } while (p != e && p->first == idx);

                                if constexpr (traceCompile) {
                                        SLog("For index ", idx, " ", list.size(), "\n");

                                        for (const auto &it : list)
                                                SLog("(", it.termID, ", ", it.toNextSpan, ")\n");
                                }

                                const uint16_t     cnt = list.size();
                                query_index_terms *ptr;

                                if (const auto required = sizeof(query_index_terms) + cnt * sizeof(query_index_term); rctx.allocator.can_allocate(required))
                                        ptr = static_cast<query_index_terms *>(rctx.allocator.Alloc(required));
                                else {
                                        ptr = static_cast<query_index_terms *>(malloc(required));
                                        rctx.large_allocs.emplace_back(ptr);
                                }

                                ptr->cnt = cnt;
                                memcpy(ptr->uniques, list.data(), cnt * sizeof(query_index_term));
                                queryIndicesTerms[idx] = ptr;
                        }
                }
        }

        isrc_docid_t                matchedDocuments{0}; // isrc_docid_t so that we can support whatever number of distinct documents are allowed by sizeof(isrc_docid_t)
        [[maybe_unused]] const auto start                   = Timings::Microseconds::Tick();
        const auto                  requireDocIDTranslation = idxsrc->require_docid_translation();

        if (defaultMode) {
                // doesn't make sense in other exec.modes
                matchesFilter->prepare(const_cast<const query_index_terms **>(queryIndicesTerms), q.final_index());
        }

        if constexpr (traceCompile)
                SLog("RUNNING: ", duration_repr(Timings::Microseconds::Since(_start)), " since start, documentsOnly = ", documentsOnly, "\n");

                // this is probably a good idea (improved cache locality)
                // but not likely a great idea
#define DOCSONLY_BATCH_SIZE 0


#pragma mark Execution
        try {
                if (rootExecNode.fp == ENT::matchterm && !accumScoreMode) {
                        isrc_docid_t docID;

                        // SPECIALIZATION: single term
                        if constexpr (traceCompile)
                                SLog("SPECIALIZATION: single term\n");

                        if (documentsOnly) {
                                // SPECIALIZATION: 1 term, documents only
                                const auto  termID  = exec_term_id_t(rootExecNode.u16);
                                auto *const decoder = rctx.decode_ctx.decoders[termID];
                                auto *const it      = rctx.reg_pli(decoder->new_iterator());
#if DOCSONLY_BATCH_SIZE > 0
                                docid_t  queue[DOCSONLY_BATCH_SIZE];
                                uint32_t queue_size{0};
#endif

                                if constexpr (traceCompile)
                                        SLog("SPECIALIZATION: documentsOnly\n");

                                if (documentsFilter) {
                                        if constexpr (traceCompile)
                                                SLog("SPECIALIZATION: documentsFilter\n");

                                        while (likely((docID = it->next()) != DocIDsEND)) {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID)) {
#if DOCSONLY_BATCH_SIZE > 0
                                                        queue[queue_size++] = globalDocID;
                                                        if (queue_size == DOCSONLY_BATCH_SIZE) {
                                                                matchesFilter->consider(queue, DOCSONLY_BATCH_SIZE);
                                                                queue_size = 0;
                                                        }
#else
                                                        matchesFilter->consider(globalDocID);
#endif
                                                }
                                        }
                                } else if (nullptr == maskedDocumentsRegistry || maskedDocumentsRegistry->empty()) {
                                        if constexpr (traceCompile)
                                                SLog("SPECIALIZATION: fast\n");

                                        while (likely((docID = it->next()) != DocIDsEND)) {
#if DOCSONLY_BATCH_SIZE > 0
                                                const auto id = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                queue[queue_size++] = id;
                                                if (queue_size == DOCSONLY_BATCH_SIZE) {
                                                        matchesFilter->consider(queue, DOCSONLY_BATCH_SIZE);
                                                        queue_size = 0;
                                                }

#else
                                                matchesFilter->consider(requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID);
#endif
                                        }
                                } else {
                                        if constexpr (traceCompile)
                                                SLog("Specialization: masked\n");

                                        while (likely((docID = it->next()) != DocIDsEND)) {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                if (!maskedDocumentsRegistry->test(globalDocID)) {
#if DOCSONLY_BATCH_SIZE > 0
                                                        queue[queue_size++] = globalDocID;
                                                        if (queue_size == DOCSONLY_BATCH_SIZE) {
                                                                matchesFilter->consider(queue, DOCSONLY_BATCH_SIZE);
                                                                queue_size = 0;
                                                        }

#else
                                                        matchesFilter->consider(globalDocID);
#endif
                                                }
                                        }
                                }

#if DOCSONLY_BATCH_SIZE > 0
                                if (queue_size)
                                        matchesFilter->consider(queue, queue_size);
#endif

                        } else {
                                // SPECIALIZATION: 1 term, collect terms
                                const auto        termID = exec_term_id_t(rootExecNode.u16);
                                auto              cd_    = std::make_unique<candidate_document>(&rctx);
                                auto *const       cd     = cd_.get();
                                matched_document &matchedDocument{cd->matchedDocument};
                                auto *const       decoder    = rctx.decode_ctx.decoders[termID];
                                auto *const       it         = rctx.reg_pli(decoder->new_iterator());
                                auto *const       p          = &matchedDocument.matchedTerms[0];
                                auto *const       th         = &cd->termHits[termID];
                                auto *const __restrict__ dws = matchedDocument.dws = new DocWordsSpace(idxsrc->max_indexed_position());

                                require(th);
                                th->set_freq(1);
                                matchedDocument.matchedTermsCnt = 1;
                                p->queryCtx                     = rctx.originalQueryTermCtx[termID];
                                p->hits                         = th;

                                if (traceExec || traceCompile)
                                        SLog("SPECIALIZATION: collect terms\n");

                                if (documentsFilter) {
                                        if (maskedDocumentsRegistry && false == maskedDocumentsRegistry->empty()) {
                                                if constexpr (traceExec)
                                                        SLog("documentsFilter AND maskedDocumentsRegistry\n");

                                                while (likely((docID = it->next()) != DocIDsEND)) {
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                        if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID)) {
                                                                it->materialize_hits(dws, th->all);
                                                                matchedDocument.id = globalDocID;
                                                                matchesFilter->consider(matchedDocument);
                                                        }
                                                }
                                        } else {
                                                if constexpr (traceExec)
                                                        SLog("documentsFilter\n");

                                                while (likely((docID = it->next()) != DocIDsEND)) {
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                        if (!documentsFilter->filter(globalDocID)) {
                                                                it->materialize_hits(dws, th->all);
                                                                matchedDocument.id = globalDocID;
                                                                matchesFilter->consider(matchedDocument);
                                                        }
                                                }
                                        }
                                } else if (maskedDocumentsRegistry && false == maskedDocumentsRegistry->empty()) {
                                        if constexpr (traceExec)
                                                SLog("maskedDocumentsRegistry\n");

                                        while (likely((docID = it->next()) != DocIDsEND)) {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                if (!maskedDocumentsRegistry->test(globalDocID)) {
                                                        it->materialize_hits(dws, th->all);
                                                        matchedDocument.id = globalDocID;
                                                        matchesFilter->consider(matchedDocument);
                                                }
                                        }
                                } else {
                                        if constexpr (traceExec)
                                                SLog("No filtering\n");

                                        while (likely((docID = it->next()) != DocIDsEND)) {
                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(docID) : docID;

                                                it->materialize_hits(dws, th->all);
                                                matchedDocument.id = globalDocID;
                                                matchesFilter->consider(matchedDocument);
                                        }
                                }
                        }
                } else {
                        if constexpr (traceCompile)
                                SLog("BUILDING ITERATORS from ", rootExecNode, "\n");

                        auto *const sit = rctx.build_iterator(rootExecNode, execFlags);
                        // Over-estimate capacity, make sure we won't overrun any buffers
                        const std::size_t capacity = rctx.tctxMap.size() + rctx.allIterators.size() + rctx.docsetsIterators.size() + 64;
                        auto              span     = build_span(sit, &rctx);

                        rctx.collectedIts.init(capacity);
                        rctx.reusableCDS.capacity = std::max<uint16_t>(512, capacity);
                        rctx.reusableCDS.data     = static_cast<candidate_document **>(malloc(sizeof(candidate_document *) * rctx.reusableCDS.capacity));
                        rctx.rootIterator         = sit;

                        // We will create different Handlers depending on the mode and other execution options so
                        // because process() is a hot method and we 'd like to reduce checks in there if we can
                        if (documentsOnly) {
                                if (documentsFilter) {
                                        if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty()) {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        const bool           requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *__restrict__ const rdp) final {
                                                                const auto id          = rdp->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID)) {
                                                                        matchesFilter->consider(globalDocID);
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}, documentsFilter{df} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        } else {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        const bool           requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) final {
                                                                const auto id          = rdp->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID)) {
                                                                        matchesFilter->consider(globalDocID);
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, documentsFilter{df} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                } else if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty()) {
                                        struct Handler final
                                            : public MatchesProxy {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const   idxsrc;
                                                const bool           requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *const rdp) final {
                                                        const auto id          = rdp->document();
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        if (!maskedDocumentsRegistry->test(globalDocID)) {
                                                                matchesFilter->consider(globalDocID);
                                                                ++n;
                                                        }
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr} {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                } else {
                                        if (idxsrc->require_docid_translation()) {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) final {
                                                                const auto id = rdp->document();

                                                                matchesFilter->consider(idxsrc->translate_docid(id));
                                                                ++n;
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                            : idxsrc{src}, ctx{c}, matchesFilter{mf} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        } else {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *const rdp) final {
                                                                const auto id = rdp->document();

                                                                matchesFilter->consider(id);
                                                                ++n;
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                            : idxsrc{src}, ctx{c}, matchesFilter{mf} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                }
                        } else if (accumScoreMode) {
                                if (documentsFilter) {
                                        if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty()) {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        const bool           requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) final {
                                                                const auto id          = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID)) {
                                                                        matchesFilter->consider(globalDocID, relDoc->score());
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}, documentsFilter{df} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        } else {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        const bool           requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) final {
                                                                const auto id          = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID)) {
                                                                        matchesFilter->consider(globalDocID, relDoc->score());
                                                                        ++n;
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, documentsFilter{df} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                } else if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty()) {
                                        struct Handler final
                                            : public MatchesProxy {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const   idxsrc;
                                                const bool           requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) final {
                                                        const auto id          = relDoc->document();
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        if (!maskedDocumentsRegistry->test(globalDocID)) {
                                                                matchesFilter->consider(globalDocID, relDoc->score());
                                                                ++n;
                                                        }
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr} {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                } else {
                                        struct Handler final
                                            : public MatchesProxy {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const   idxsrc;
                                                const bool           requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) final {
                                                        const auto                  id          = relDoc->document();
                                                        [[maybe_unused]] const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        matchesFilter->consider(globalDocID, relDoc->score());
                                                        ++n;
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf} {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                        } else {

                                if constexpr (traceExec)
                                        SLog("Executing query\n");

                                if (documentsFilter) {
                                        if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty()) {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        const bool           requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) final {
                                                                const auto id          = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID) && !maskedDocumentsRegistry->test(globalDocID)) {
                                                                        auto doc = ctx->document_by_id(id);

                                                                        ctx->prepare_match(doc);

                                                                        auto &matchedDocument = doc->matchedDocument;

                                                                        matchedDocument.id = globalDocID;
                                                                        matchesFilter->consider(matchedDocument);
                                                                        ++n;

                                                                        ctx->cds_release(doc);
                                                                        ctx->gc_retained_docs(id);
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr}, documentsFilter{df} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        } else {
                                                struct Handler final
                                                    : public MatchesProxy {
                                                        queryexec_ctx *const ctx;
                                                        IndexSource *const   idxsrc;
                                                        const bool           requireDocIDTranslation;
                                                        MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                        IndexDocumentsFilter *__restrict__ const documentsFilter;
                                                        std::size_t n{0};

                                                        void process(relevant_document_provider *relDoc) final {
                                                                const auto id          = relDoc->document();
                                                                const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                                if (!documentsFilter->filter(globalDocID)) {
                                                                        auto doc = ctx->document_by_id(id);

                                                                        ctx->prepare_match(doc);

                                                                        auto &matchedDocument = doc->matchedDocument;

                                                                        matchedDocument.id = globalDocID;
                                                                        matchesFilter->consider(matchedDocument);
                                                                        ++n;

                                                                        ctx->cds_release(doc);
                                                                        ctx->gc_retained_docs(id);
                                                                }
                                                        }

                                                        Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, IndexDocumentsFilter *df)
                                                            : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, documentsFilter{df} {
                                                        }

                                                } handler(&rctx, idxsrc, matchesFilter, documentsFilter);

                                                span->process(&handler, 1, DocIDsEND);
                                                matchedDocuments = handler.n;
                                        }
                                } else if (maskedDocumentsRegistry && !maskedDocumentsRegistry->empty()) {
                                        struct Handler final
                                            : public MatchesProxy {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const   idxsrc;
                                                const bool           requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                masked_documents_registry *const __restrict__ maskedDocumentsRegistry;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) final {
                                                        const auto id          = relDoc->document();
                                                        const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;

                                                        if (!maskedDocumentsRegistry->test(globalDocID)) {
                                                                auto doc = ctx->document_by_id(id);

                                                                ctx->prepare_match(doc);

                                                                auto &matchedDocument = doc->matchedDocument;

                                                                matchedDocument.id = globalDocID;
                                                                matchesFilter->consider(matchedDocument);
                                                                ++n;

                                                                ctx->cds_release(doc);
                                                                ctx->gc_retained_docs(id);
                                                        }
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf, masked_documents_registry *mr)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf}, maskedDocumentsRegistry{mr} {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter, maskedDocumentsRegistry);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                } else {
                                        struct Handler final
                                            : public MatchesProxy {
                                                queryexec_ctx *const ctx;
                                                IndexSource *const   idxsrc;
                                                const bool           requireDocIDTranslation;
                                                MatchedIndexDocumentsFilter *__restrict__ const matchesFilter;
                                                std::size_t n{0};

                                                void process(relevant_document_provider *relDoc) final {
                                                        const auto                  id          = relDoc->document();
                                                        [[maybe_unused]] const auto globalDocID = requireDocIDTranslation ? idxsrc->translate_docid(id) : id;
                                                        auto                        doc         = ctx->document_by_id(id);

                                                        ctx->prepare_match(doc);

                                                        auto &matchedDocument = doc->matchedDocument;

                                                        matchedDocument.id = globalDocID;
                                                        matchesFilter->consider(matchedDocument);
                                                        ++n;

                                                        ctx->cds_release(doc);
                                                        ctx->gc_retained_docs(id);
                                                }

                                                Handler(queryexec_ctx *const c, IndexSource *const src, MatchedIndexDocumentsFilter *mf)
                                                    : idxsrc{src}, ctx{c}, requireDocIDTranslation{src->require_docid_translation()}, matchesFilter{mf} {
                                                }

                                        } handler(&rctx, idxsrc, matchesFilter);

                                        span->process(&handler, 1, DocIDsEND);
                                        matchedDocuments = handler.n;
                                }
                        }
                }
        } catch (const aborted_search_exception &e) {
                // search was aborted
        } catch (...) {
                // something else, throw it and let someone else handle it
                throw;
        }

        const auto duration    = Timings::Microseconds::Since(start);
        const auto durationAll = Timings::Microseconds::Since(_start);

        if (traceCompile || traceExec)
                SLog(ansifmt::bold, ansifmt::color_red, dotnotation_repr(matchedDocuments), " matched in ", duration_repr(duration), ansifmt::reset, " (", Timings::Microseconds::ToMillis(duration), " ms) ", duration_repr(durationAll), " all\n");
}
