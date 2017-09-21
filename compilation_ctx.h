#pragma once
#include "common.h"
#include "queries.h"
#include "runtime.h"
#include <switch_mallocators.h>

namespace Trinity
{
        enum class ENT : uint8_t
        {
                matchterm = 0,
                constfalse,
                consttrue,
                dummyop,
                matchallterms,
                matchanyterms,
                unaryand,
                unarynot,
                matchanyphrases,
                matchallphrases,
                matchphrase,
                consttrueexpr,
                logicaland,
                logicalnot,
                logicalor,
                matchsome,
                // matchallnodes and matchanynodes are handled by the compiler/optimizer, though
                // no exec. nodes of that type are generated during compilation.
                matchallnodes,
                matchanynodes,
                SPECIALIMPL_COLLECTION_LOGICALOR,
                SPECIALIMPL_COLLECTION_LOGICALAND,
        };

        struct exec_node final
        {
                ENT fp;

                union {
                        void *ptr;
                        uint32_t u32;
                        uint16_t u16;
                };

                // cost is computed by reorder_execnode()
                // and we keep track of this here so that we can make some higher level optimizations later
                uint64_t cost;
        };

        struct compilation_ctx
        {
                simple_allocator allocator{4096 * 6};
                simple_allocator runsAllocator{4096}, ctxAllocator{4096};

                struct partial_match_ctx final
                {
                        uint16_t size;
                        uint16_t min;
                        exec_node nodes[0];
                };

                struct nodes_group final
                {
                        uint16_t size;
                        exec_node nodes[0];
                };

                struct termsrun final
                {
                        uint16_t size;
                        exec_term_id_t terms[0];

                        static_assert(std::numeric_limits<decltype(size)>::max() >= Limits::MaxTermLength);

                        bool operator==(const termsrun &o) const noexcept;

                        bool is_set(const exec_term_id_t id) const noexcept;

                        bool erase(const exec_term_id_t id) noexcept;

                        bool erase(const termsrun &o);

                        auto empty() const noexcept
                        {
                                return !size;
                        }
                };

                struct cacheable_termsrun
                {
                        isrc_docid_t lastConsideredDID;
                        const termsrun *run;
                        bool res;
                };

                struct phrase final
                {
                        uint8_t size;
                        exec_term_id_t termIDs[0];

                        static_assert(std::numeric_limits<decltype(size)>::max() >= Trinity::Limits::MaxPhraseSize);

                        uint8_t intersection(const termsrun *const tr, exec_term_id_t *const out) const noexcept;

                        // returns terms found in run, but missing from this phrase
                        uint8_t disjoint_union(const termsrun *const tr, exec_term_id_t *const out) const noexcept;

                        bool intersected_by(const termsrun *const tr) const noexcept;

                        bool operator==(const phrase &o) const noexcept;

                        bool is_set(const exec_term_id_t id) const noexcept;

                        bool is_set(const exec_term_id_t *const l, const uint8_t n) const noexcept;
                };

                struct phrasesrun final
                {
                        uint16_t size;
                        phrase *phrases[0];
                };

                struct binop_ctx final
                {
                        exec_node lhs;
                        exec_node rhs;
                };

                struct unaryop_ctx final
                {
                        exec_node expr;
                };

                inline uint16_t register_token(const Trinity::phrase *p)
                {
                        return resolve_query_term(p->terms[0].token);
                }

                phrase *register_phrase(const Trinity::phrase *p);

                binop_ctx *register_binop(const exec_node lhs, const exec_node rhs)
                {
                        auto ptr = ctxAllocator.New<binop_ctx>();

                        ptr->lhs = lhs;
                        ptr->rhs = rhs;
                        return ptr;
                }

                unaryop_ctx *register_unaryop(const exec_node expr)
                {
                        auto ptr = ctxAllocator.New<unaryop_ctx>();

                        ptr->expr = expr;
                        return ptr;
                }

                virtual uint16_t resolve_query_term(const str8_t term) = 0;
        };

	exec_node compile_query(ast_node *root, compilation_ctx &cctx);

        void group_execnodes(exec_node &, simple_allocator &);
}
