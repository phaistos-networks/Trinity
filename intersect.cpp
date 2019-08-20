#include "intersect.h"

using namespace Trinity;

void Trinity::intersect_impl(const uint64_t                                 stopwordsMask,
                             const std::vector<std::unordered_set<str8_t>> &tokens,
                             IndexSource *__restrict__ const src,
                             masked_documents_registry *const __restrict__ maskedDocumentsRegistry,
                             std::vector<std::pair<uint64_t, uint32_t>> *const out) {

        static constexpr bool trace{false};
        struct tracked        final {
                Codecs::PostingsListIterator *it;
                uint8_t                       tokenIdx;
                Codecs::Decoder *             dec;
        } remaining[512];

        DEXPECT(tokens.size() <= sizeof(uint64_t) << 3);
        DEXPECT(out);

        uint64_t origMask{0}; // we don't want to match the original query
        size_t   rem{0};
        bool     anyUnknown{false};

        for (uint8_t i{0}; i != tokens.size(); ++i) {
                for (const auto &token : tokens[i]) {
                        if (const auto tctx = src->term_ctx(token); tctx.documents) {
                                auto dec = src->new_postings_decoder(token, tctx);
                                auto it  = dec->new_iterator();

                                it->next(); // seek to the first document
                                remaining[rem++] = {it, i, dec};
                                origMask |= uint64_t(1u) << i;

                                if (trace)
                                        SLog("For [", token, "] ", tctx.documents, "\n");

                        } else {
                                if (trace)
                                        SLog("Unknown for [", token, "]\n");

                                anyUnknown = true;
                        }
                }
        }

        if (!rem)
                return;

        if (anyUnknown)
                origMask = 0;

        struct ctx final {
                uint64_t mapPrev{0};
                uint8_t  indexPrev{0};

                struct match final {
                        uint64_t v;
                        uint32_t cnt;
                };

                std::vector<match> matches;

                void consider(const uint64_t map) {
                        if (map == mapPrev)
                                ++matches[indexPrev].cnt;
                        else {
                                auto matchesCnt = matches.size();
                                auto all        = matches.data();

                                mapPrev = map;
                                for (size_t i{0}; i < matchesCnt;) {
                                        if (const auto v = all[i].v; (v & map) == map) {
                                                // [wars jedi] [star wars jedi]
                                                if (map == v)
                                                        ++all[i].cnt;
                                                indexPrev = i;
                                                return;
                                        } else if ((map & v) == v) {
                                                // [star wars return jedi] [wars jedi]
                                                matches[i] = matches.back();
                                                matches.pop_back();
                                                --matchesCnt;
                                        } else
                                                ++i;
                                }

                                indexPrev = matchesCnt;
                                matches.push_back({map, 1});
                        }
                }

                void finalize() {
                        std::sort(matches.begin(), matches.end(), [](const auto &a, const auto &b) noexcept {
                                const auto r = int8_t(SwitchBitOps::PopCnt(b.v)) - int8_t(SwitchBitOps::PopCnt(a.v));

                                return r < 0 || (!r && b.cnt < a.cnt);
                        });
                }
        };

        uint16_t   selected[sizeof_array(remaining)];
        ctx        c;
        const auto before = Timings::Microseconds::Tick();

        // TODO: use Switch::priority_queue<>
        for (;;) {
                uint16_t     cnt{1};
                isrc_docid_t lowest;
                const auto & it   = remaining[0];
                uint64_t     mask = uint64_t(1u) << it.tokenIdx;
                uint8_t      first{0}, last{0};

                selected[0] = 0;
                lowest      = it.it->curDocument.id;
                for (size_t i{1}; i != rem; ++i) {
                        const auto &it = remaining[i];

                        if (const auto docID = it.it->curDocument.id; docID == lowest) {
                                const auto m = uint64_t(1u) << it.tokenIdx;

                                mask |= m;
                                selected[cnt++] = i;
                                last            = i;
                        } else if (docID < lowest) {
                                mask        = uint64_t(1u) << it.tokenIdx;
                                selected[0] = i;
                                cnt         = 1;
                                lowest      = docID;
                                first = last = i;
                        }

                l100:;
                }

                [[maybe_unused]] static constexpr bool trace{false};

                if (mask != origMask) {
                        if (0 == (stopwordsMask & ((uint64_t(1) << first) | (uint64_t(1) << last)))) {
                                if (!maskedDocumentsRegistry->test(lowest))
                                        c.consider(mask);
                        }
                }

                while (cnt) {
                        const auto idx = selected[--cnt];

                        if (unlikely(DocIDsEND == remaining[idx].it->next())) {
                                delete remaining[idx].dec;
                                delete remaining[idx].it;

                                if (--rem == 0)
                                        goto l10;
                                else
                                        remaining[idx] = remaining[rem];
                        }
                }
        }

l10:
        c.finalize();
        if (trace)
                SLog(duration_repr(Timings::Microseconds::Since(before)), " to intersect, c.matches.size = ", c.matches.size(), "\n");

        for (const auto &it : c.matches) {
                if (trace)
                        SLog("output:", it.v, ", cnt = ", it.cnt, ", popcnt = ", SwitchBitOps::PopCnt(uint64_t(it.v)), "\n");
                out->push_back({it.v, it.cnt});
        }
}

std::vector<std::pair<uint64_t, uint32_t>> Trinity::intersect(const uint64_t stopwordsMask, const std::vector<std::unordered_set<str8_t>> &tokens, IndexSourcesCollection *collection) {
        std::vector<std::pair<uint64_t, uint32_t>> out;
        const auto                                 n = collection->sources.size();

        // TODO: use std::async()?
        for (size_t i{0}; i != n; ++i) {
                auto source  = collection->sources[i];
                auto scanner = collection->scanner_registry_for(i);

                intersect_impl(stopwordsMask, tokens, source, scanner.get(), &out);
        }

        std::sort(out.begin(), out.end(), [](const auto &a, const auto &b) noexcept { return a.first < b.first; });

        auto o = out.data();
        for (const auto *p = out.data(), *const e = p + out.size(); p != e;) {
                const auto mask = p->first;
                auto       cnt  = p->second;

                for (++p; p != e && p->first == mask; ++p) {
                        cnt += p->second;
                        continue;
                }

                *o++ = {mask, cnt};
        }

        out.resize(o - out.data());
        return out;
}

uint8_t Trinity::intersection_indices(uint64_t mask, uint8_t *const out) {
        uint8_t shift{0}, collected{0};

        do {
                auto       idx        = SwitchBitOps::LeastSignificantBitSet(mask);
                const auto translated = idx + shift - 1;

                out[collected++] = translated;

                mask >>= idx;
                shift += idx;
        } while (mask);

        return collected;
}

std::vector<std::pair<range_base<str8_t *, uint8_t>, std::pair<uint8_t, std::size_t>>>
Trinity::intersection_alternatives(const query &           originalQuery,
                                   const query &           rewrittenQuery,
                                   IndexSourcesCollection &collection,
                                   simple_allocator *const a) {
        static constexpr bool                                                                  trace{false};
        std::vector<std::pair<range_base<str8_t *, uint8_t>, std::pair<uint8_t, std::size_t>>> resp;

        if (!originalQuery.can_intersect()) {
                return resp;
        }

        auto &q{rewrittenQuery};

        // This works because Trinity::rewrite_query() sets rewrite_ctx for each token/phrase
        std::vector<Trinity::phrase *> v;
        std::unordered_set<str8_t>     seen;

        for (const auto n : q.nodes()) {
                if (n->type == Trinity::ast_node::Type::Token)
                        v.emplace_back(n->p);
        }

        std::sort(v.begin(), v.end(), [](const auto a, const auto b) noexcept {
                return a->rewrite_ctx.range.offset < b->rewrite_ctx.range.offset;
        });

        // Collect offset => set(tokens)
        std::vector<std::unordered_set<str8_t>> V;

        for (const auto *p = v.data(), *const e = p + v.size(); p != e;) {
                const auto                 idx = (*p)->rewrite_ctx.range.offset;
                std::unordered_set<str8_t> S;

                // The first token for an index is the original token (and any other for that index
                // were expanded from that token)
                if (seen.insert((*p)->terms[0].token).second) {
                        do {
                                S.insert((*p)->terms[0].token);
                        } while (++p != e && (*p)->rewrite_ctx.range.offset == idx);
                } else {
                        for (++p; p != e && (*p)->rewrite_ctx.range.offset == idx; ++p)
                                continue;
                }

                V.emplace_back(std::move(S));
        }

        if (trace) {
                for (const auto &it : V) {
                        Print("NEXT\n");

                        for (const auto &t : it)
                                Print("\t", t, "\n");
                }
        }

        auto res = Trinity::intersect(0, V, &collection);

        // by order of tokens in the query ASC,  total matched tokens DESC, total products DESC
        std::sort(res.begin(), res.end(), [](const auto &a, const auto &b) noexcept {
                const auto p1 = SwitchBitOps::PopCnt(a.first), p2 = SwitchBitOps::PopCnt(b.first);

                return p2 < p1 || (p2 == p1 && b.second < a.second);
        });

        uint8_t                             indices[64];
        Buffer                              b, b2;
        std::unordered_map<uint8_t, str8_t> map;
        std::vector<str8_t>                 tokens;

        // So that we can map original query index => original query token at index
        for (const auto n : originalQuery.nodes()) {
                if (n->type == ast_node::Type::Token) {
                        map.insert({n->p->index, n->p->terms[0].token});
                }
        }

        for (size_t i{0}; i != res.size() && i != 5; ++i) {
                const auto m   = res[i].first;
                const auto cnt = res[i].second;
                const auto n   = Trinity::intersection_indices(m, indices);

                if (trace) {
                        b.clear();
                        b2.clear();
                        for (size_t i{0}; i != n; ++i) {
                                b.append(indices[i], ' ');
                                b2.append(map[indices[i]], ' ');
                        }

                        if (b.size()) {
                                b.shrink_by(1);
                                b2.shrink_by(1);
                        }

                        Print("FOR ", b.AsS32(), " [", b2, "] popcnt ", SwitchBitOps::PopCnt(m), " =>", cnt, "\n");
                } else {
                        tokens.clear();

                        for (size_t i{0}; i != n; ++i)
                                tokens.push_back(map[indices[i]]);

                        resp.push_back({{a->CopyOf(tokens.data(), tokens.size()), uint8_t(tokens.size())}, {(uint8_t)SwitchBitOps::LeastSignificantBitSet(m), cnt}});
                }
        }

        return resp;
}
