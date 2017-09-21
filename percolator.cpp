#include "percolator.h"

using namespace Trinity;

bool percolator_query::match()
{
	return exec(root);
}

bool percolator_query::exec(const exec_node n)
{
        switch (n.fp)
        {
		case ENT::matchterm:
			return match_term(n.u16);

                case ENT::constfalse:
                        return false;

                case ENT::consttrue:
                        return true;

                case ENT::matchallterms:
                {
                        const auto run = static_cast<const compilation_ctx::termsrun *>(n.ptr);

                        for (uint32_t i{0}; i != run->size; ++i)
                        {
                                if (!match_term(run->terms[i]))
                                        return false;
                        }

                        return true;
                }
                break;

                case ENT::matchanyterms:
                {
                        const auto run = static_cast<const compilation_ctx::termsrun *>(n.ptr);

                        for (uint32_t i{0}; i != run->size; ++i)
                        {
                                if (match_term(run->terms[i]))
                                        return true;
                        }

                        return false;
                }
                break;

                case ENT::unaryand:
                        return exec(static_cast<const compilation_ctx::unaryop_ctx *>(n.ptr)->expr);

                case ENT::unarynot:
                        return !exec(static_cast<const compilation_ctx::unaryop_ctx *>(n.ptr)->expr);

                case ENT::matchanyphrases:
                {
                        const auto run = static_cast<const compilation_ctx::phrasesrun *>(n.ptr);

                        for (uint32_t i{0}; i != run->size; ++i)
                        {
                                const auto p = run->phrases[i];

                                if (match_phrase(p->termIDs, p->size))
                                        return true;
                        }

                        return false;
                }

                case ENT::matchallphrases:
                {
                        const auto run = static_cast<const compilation_ctx::phrasesrun *>(n.ptr);

                        for (uint32_t i{0}; i != run->size; ++i)
                        {
                                const auto p = run->phrases[i];

                                if (!match_phrase(p->termIDs, p->size))
                                        return false;
                        }

                        return true;
                }

                case ENT::matchphrase:
                {
                        const auto p = static_cast<const compilation_ctx::phrase *>(n.ptr);

                        return match_phrase(p->termIDs, p->size);
                }

                case ENT::logicaland:
                {
                        const auto b = static_cast<const compilation_ctx::binop_ctx *>(n.ptr);

                        return exec(b->lhs) && exec(b->rhs);
                }

                case ENT::logicalnot:
                {
                        const auto b = static_cast<const compilation_ctx::binop_ctx *>(n.ptr);

                        return exec(b->lhs) && !exec(b->rhs);
                }

                case ENT::logicalor:
                {
                        const auto b = static_cast<const compilation_ctx::binop_ctx *>(n.ptr);

                        return exec(b->lhs) || exec(b->rhs);
                }

                case ENT::matchsome:
                {
                        const auto pm = static_cast<compilation_ctx::partial_match_ctx *>(n.ptr);
                        uint16_t matched{0};

                        for (uint32_t i{0}; i != pm->size; ++i)
                        {
                                if (exec(pm->nodes[i]) && ++matched == pm->min)
                                        return true;
                        }
                        return false;
                }

                case ENT::matchallnodes:
                {
                        const auto g = static_cast<compilation_ctx::nodes_group *>(n.ptr);

                        for (uint32_t i{0}; i != g->size; ++i)
                        {
                                if (!exec(g->nodes[i]))
                                        return false;
                        }
                        return true;
                }

                case ENT::matchanynodes:
                {
                        const auto g = static_cast<compilation_ctx::nodes_group *>(n.ptr);

                        for (uint32_t i{0}; i != g->size; ++i)
                        {
                                if (exec(g->nodes[i]))
                                        return true;
                        }
                        return false;
                }

		case ENT::consttrueexpr:
			return true;

		case ENT::dummyop:
                case ENT::SPECIALIMPL_COLLECTION_LOGICALOR:
                case ENT::SPECIALIMPL_COLLECTION_LOGICALAND:
                        std::abort();
        }
}
