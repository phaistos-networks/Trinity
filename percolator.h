// See https://www.youtube.com/watch?v=f4lqBb1d7no&list=PLcGKfGEEONaDzd0Hkn2f1talsTu1HLDYu&index=21
//  Describes the Predicate Index Twitter employs to reduce number of distinct rules to
// attempt to match against a new tweet.
#include "common.h"
#include "compilation_ctx.h"
#include "queries.h"

namespace Trinity {
        struct percolator_document_proxy {
                // Just override those two methods
                // You can access the actual term via term_by_index(idx)
                //
                // You can e.g reset state, and then match()
                virtual bool match_term(const uint16_t term) = 0;

                virtual bool match_phrase(const uint16_t *, const uint16_t cnt) = 0;
        };

        class percolator_query final {
              protected:
                struct CCTX final
                    : public compilation_ctx {
                        std::unordered_map<str8_t, uint16_t> localMap;
                        std::vector<str8_t>                  allTerms; // we need to keep track of those here

                        uint16_t resolve_query_term(const str8_t term) override final {
                                const auto res = localMap.insert({term, 0});

                                if (res.second) {
                                        res.first->second = localMap.size();
                                        const_cast<str8_t *>(&res.first->first)->Set(allocator.CopyOf(term.data(), term.size()), term.size());

                                        require(allTerms.size() == localMap.size() - 1);
                                        allTerms.push_back(res.first->first);
                                }

                                return res.first->second;
                        }

                } comp_ctx;

                exec_node root;

              protected:
                bool exec(const exec_node, percolator_document_proxy &) const;

              public:
                auto term_by_index(const uint16_t idx) const {
                        return comp_ctx.allTerms[idx - 1];
                }

                auto &distinct_terms() noexcept {
                        return comp_ctx.allTerms;
                }

                const auto &distinct_terms() const noexcept {
                        return comp_ctx.allTerms;
                }

              public:
                // After compilation, you can access all distinct terms, i.e all distinct terms you may be
                // interested in, in a document, via distinct_terms()
                percolator_query(const Trinity::query &q) {
                        if (!q) {
                                root.fp = ENT::constfalse;
                                return;
                        }

                        root = compile_query(q.root, comp_ctx);
                        if (root.fp == ENT::constfalse || root.fp == ENT::dummyop)
                                root.fp = ENT::constfalse;
                        else
                                group_execnodes(root, comp_ctx.allocator);
                }

                percolator_query() {
                        root.fp = ENT::constfalse;
                }

                operator bool() const noexcept {
                        return root.fp != ENT::constfalse && root.fp != ENT::dummyop;
                }

                bool match(percolator_document_proxy &) const;
        };
} // namespace Trinity
