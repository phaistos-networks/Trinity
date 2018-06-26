#include "docset_iterators.h"
#include "similarity.h"
#include "queryexec_ctx.h"

using namespace Trinity;
using namespace Trinity::DocsSetIterators;

static IteratorScorer *default_wrapper(queryexec_ctx *const rctx, DocsSetIterators::Iterator *const it) {
        switch (it->type) {
                case DocsSetIterators::Type::PostingsListIterator: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Similarity::IndexSourceTermsScorer *const scorer;
                                Similarity::ScorerWeight *                weight;

                                Wrapper(Iterator *const it, queryexec_ctx *const rctx)
                                    : IteratorScorer{it}, scorer{rctx->scorer} {
                                        const auto termID = static_cast<Codecs::PostingsListIterator *>(it)->decoder()->execCtxTermID;
                                        const auto term   = rctx->tctxMap[termID].second;

                                        weight = scorer->new_scorer_weight(&term, 1);
                                }

                                ~Wrapper() {
                                        delete weight;
                                }

                                inline double iterator_score() override final {
                                        const auto i = static_cast<const Codecs::PostingsListIterator *>(it);

                                        return scorer->score(i->current(), i->freq, weight);
                                }
                        };

                        return new Wrapper(it, rctx);
                } break;

                case DocsSetIterators::Type::DisjunctionSome: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Wrapper(Iterator *const it)
                                    : IteratorScorer{it} {
                                }

                                double iterator_score() override final {
                                        double sum{0};
                                        auto   i = static_cast<DisjunctionSome *>(it);

                                        i->update_matched_cnt();
                                        for (auto l{i->lead}; l; l = l->next)
                                                sum += static_cast<IteratorScorer *>(l->it->rdp)->iterator_score();
                                        return sum;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Filter: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Wrapper(Iterator *it)
                                    : IteratorScorer{it} {
                                }

                                // TODO: for when we support this
                                // inline auto iterator() override final { return static_cast<Filter *>(it)->req; }

                                double iterator_score() override final {
                                        return static_cast<IteratorScorer *>(static_cast<Filter *>(it)->req->rdp)->iterator_score();
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Optional: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Wrapper(Iterator *it)
                                    : IteratorScorer{it} {
                                }

                                // TODO: for when we support it
                                // inline auto iterator() override final { return statc_cast<Optional *>(it)->main; }

                                double iterator_score() override final {
                                        auto       it = static_cast<Optional *>(this->it);
                                        auto       main{it->main};
                                        auto       opt{it->opt};
                                        auto       score = static_cast<IteratorScorer *>(main->rdp)->iterator_score();
                                        const auto id    = main->current();
                                        auto       optId = opt->current();

                                        if (optId < id)
                                                optId = opt->advance(id);
                                        if (optId == id)
                                                score += static_cast<IteratorScorer *>(opt->rdp)->iterator_score();

                                        return score;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::DisjunctionAllPLI: {
                        struct Wrapper final
                            : public IteratorScorer {

                                Wrapper(Iterator *it)
                                    : IteratorScorer{it} {
                                }

                                double iterator_score() override final {
                                        double sum{0};

                                        static_cast<DisjunctionAllPLI *>(it)->pq.for_each_top([&sum](const auto it) { sum += static_cast<IteratorScorer *>(it->rdp)->iterator_score(); },
                                                                                              [](const auto a, const auto b) noexcept {
                                                                                                      return a->current() == b->current();
                                                                                              });
                                        return sum;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Disjunction: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Wrapper(Iterator *it)
                                    : IteratorScorer{it} {
                                }

                                double iterator_score() override final {
                                        double sum{0};

                                        static_cast<Disjunction *>(it)->pq.for_each_top([&sum](const auto it) { sum += static_cast<IteratorScorer *>(it->rdp)->iterator_score(); },
                                                                                        [](const auto a, const auto b) noexcept {
                                                                                                return a->current() == b->current();
                                                                                        });
                                        return sum;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::ConjuctionAllPLI: {
                        struct Wrapper final
                            : public IteratorScorer {

                                Wrapper(Iterator *it)
                                    : IteratorScorer{it} {
                                }

                                double iterator_score() override final {
                                        double     res{0};
                                        auto       it = static_cast<ConjuctionAllPLI *>(this->it);
                                        const auto size{it->size};
                                        const auto its{it->its};

                                        for (uint16_t i{0}; i != size; ++i)
                                                res += static_cast<IteratorScorer *>(its[i]->rdp)->iterator_score();
                                        return res;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Conjuction: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Wrapper(Iterator *it)
                                    : IteratorScorer{it} {
                                }

                                double iterator_score() override final {
                                        double     res{0};
                                        auto       it = static_cast<Conjuction *>(this->it);
                                        const auto size{it->size};
                                        const auto its{it->its};

                                        for (uint16_t i{0}; i != size; ++i)
                                                res += static_cast<IteratorScorer *>(its[i]->rdp)->iterator_score();
                                        return res;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Phrase: {
                        struct Wrapper final
                            : public IteratorScorer {
                                Similarity::IndexSourceTermsScorer *const scorer;
                                Similarity::ScorerWeight *                weight;

                                Wrapper(Iterator *it, queryexec_ctx *const rctx)
                                    : IteratorScorer{it}, scorer{rctx->scorer} {
                                        auto   p = static_cast<DocsSetIterators::Phrase *>(it);
                                        str8_t terms[p->size];

                                        for (uint32_t i{0}; i != p->size; ++i) {
                                                const auto termID = static_cast<const Codecs::PostingsListIterator *>(p->its[i])->decoder()->execCtxTermID;
                                                const auto term   = rctx->tctxMap[termID].second;

                                                terms[i] = term;
                                        }

                                        weight = scorer->new_scorer_weight(terms, p->size);
                                }

                                ~Wrapper() {
                                        delete weight;
                                }

                                inline double iterator_score() override final {
                                        const auto it = static_cast<const Phrase *>(this->it);

                                        return scorer->score(it->current(), it->matchCnt, weight);
                                }
                        };

                        return new Wrapper(it, rctx);
                }

                case DocsSetIterators::Type::VectorIDs:
                case DocsSetIterators::Type::Dummy:
                case DocsSetIterators::Type::AppIterator:
                        return nullptr;
        }

        return nullptr;
}

DocsSetIterators::Iterator *DocsSetIterators::wrap_iterator(queryexec_ctx *const rctx, DocsSetIterators::Iterator *it) {
        it->rdp = default_wrapper(rctx, it);
        return it;
}

