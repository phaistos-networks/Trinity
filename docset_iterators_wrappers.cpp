#include "docset_iterators.h"
#include "scores.h"
#include "runtime_ctx.h"

using namespace Trinity;
using namespace Trinity::DocsSetIterators;

static IteratorWrapper *default_wrapper(runtime_ctx *const rctx, DocsSetIterators::Iterator *const it)
{
        switch (it->type)
        {
                case DocsSetIterators::Type::PostingsListIterator:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Trinity::Similarity::Scorer *const scorer;

                                Wrapper(Iterator *const it, runtime_ctx *const rctx)
                                    : IteratorWrapper{it}, scorer{rctx->scorer}
                                {
                                }

                                inline double iterator_score() override final
                                {
                                        auto i = static_cast<Codecs::PostingsListIterator *>(it);

                                        return scorer->score(i->current(), i->freq);
                                }
                        };

                        return new Wrapper(it, rctx);
                }
                break;

                case DocsSetIterators::Type::DisjunctionSome:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Wrapper(Iterator *const it)
					: IteratorWrapper{it}
                                {
                                }

                                double iterator_score() override final
                                {
                                        double sum{0};
					auto i = static_cast<DisjunctionSome *>(it);

                                        i->update_matched_cnt();
                                        for (auto l{i->lead}; l; l = l->next)
                                                sum += static_cast<IteratorWrapper *>(l->it->rdp)->iterator_score();
                                        return sum;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Filter:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Wrapper(Iterator *it)
					: IteratorWrapper{it}
                                {
                                }

				// TODO: for when we support this
				// inline auto iterator() override final { return static_cast<Filter *>(it)->req; }

                                double iterator_score() override final
                                {
                                        return static_cast<IteratorWrapper *>(static_cast<Filter *>(it)->req->rdp)->iterator_score();
                                }

                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Optional:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Wrapper(Iterator *it)
					: IteratorWrapper{it}
                                {
                                }

				// TODO: for when we support it
				// inline auto iterator() override final { return statc_cast<Optional *>(it)->main; }

                                double iterator_score() override final
                                {
					auto it = static_cast<Optional *>(this->it);
					auto main{it->main};
					auto opt{it->opt};
                                        auto score = static_cast<IteratorWrapper *>(main->rdp)->iterator_score();
                                        const auto id = main->current();
                                        auto optId = opt->current();

                                        if (optId < id)
                                                optId = opt->advance(id);
                                        if (optId == id)
                                                score += static_cast<IteratorWrapper *>(opt->rdp)->iterator_score();

                                        return score;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::DisjunctionAllPLI:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
				
                                Wrapper(Iterator *it)
					: IteratorWrapper{it}
                                {
                                }

                                double iterator_score() override final
                                {
                                        double sum{0};

                                        static_cast<DisjunctionAllPLI*>(it)->pq.for_each_top([&sum](const auto it) { sum += static_cast<IteratorWrapper *>(it->rdp)->iterator_score(); },
                                                            [](const auto a, const auto b) noexcept {
                                                                    return a->current() == b->current();
                                                            });
                                        return sum;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Disjunction:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Wrapper(Iterator *it)
                                    : IteratorWrapper{it}
                                {
                                }

                                double iterator_score() override final
                                {
                                        double sum{0};

                                        static_cast<Disjunction *>(it)->pq.for_each_top([&sum](const auto it) { sum += static_cast<IteratorWrapper *>(it->rdp)->iterator_score(); },
                                                                                        [](const auto a, const auto b) noexcept {
                                                                                                return a->current() == b->current();
                                                                                        });
                                        return sum;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::ConjuctionAllPLI:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {

                                Wrapper(Iterator *it)
					: IteratorWrapper{it}
                                {
                                }

                                double iterator_score() override final
                                {
                                        double res{0};
					auto it = static_cast<ConjuctionAllPLI *>(this->it);
					const auto size{it->size};
					const auto its{it->its};

                                        for (uint16_t i{0}; i != size; ++i)
                                                res += static_cast<IteratorWrapper *>(its[i]->rdp)->iterator_score();
                                        return res;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Conjuction:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Wrapper(Iterator *it)
                                    : IteratorWrapper{it}
                                {
                                }

                                double iterator_score() override final
                                {
                                        double res{0};
                                        auto it = static_cast<Conjuction *>(this->it);
                                        const auto size{it->size};
                                        const auto its{it->its};

                                        for (uint16_t i{0}; i != size; ++i)
                                                res += static_cast<IteratorWrapper *>(its[i]->rdp)->iterator_score();
                                        return res;
                                }
                        };

                        return new Wrapper(it);
                }

                case DocsSetIterators::Type::Phrase:
                {
                        struct Wrapper final
                            : public IteratorWrapper
                        {
                                Similarity::Scorer *const scorer;

                                Wrapper(Iterator *it, runtime_ctx *const rctx)
                                    : IteratorWrapper{it}, scorer{rctx->scorer}
                                {
                                }

                                inline double iterator_score() override final
                                {
                                        auto it = static_cast<Phrase *>(this->it);

                                        return scorer->score(it->current(), it->matchCnt);
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

DocsSetIterators::Iterator *DocsSetIterators::wrap_iterator(runtime_ctx *const rctx, DocsSetIterators::Iterator *it)
{
        it->rdp = default_wrapper(rctx, it);
        return it;
}

