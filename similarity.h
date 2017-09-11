#pragma once
#include "common.h"
#include "index_source.h"

// Whatever's here is specific to the "accumuluated score scheme" execution mode, where
// we just aggregate a similarity score for each iterator on the "current" document, similar to what Lucene's doing.
namespace Trinity
{
        namespace Similarity
        {
                struct IndexSourcesCollectionScorer;

                // You may want to compute/track more than just a double-worth of state
                // for each term/phrase. Even though this is somewhat expensive, it's not that expensive
                // and allows for encapsulation/representation of complex types in it.
                struct ScorerWeight
                {
			virtual ~ScorerWeight() {};
                };

                // Responsible for providing scores for a single IndexSource
                // You may want to compute some IndexSource specific state in the constructor.
                // This is created by IndexSourcesCollectionScorer::new_source_scorer() for each IndexSource involed
                // in a query execution.
                struct IndexSourceScorer
                {
                        IndexSource *const src;
                        IndexSourcesCollectionScorer *const collectionScorer;

                        IndexSourceScorer(IndexSourcesCollectionScorer *const r, IndexSource *const s)
                            : collectionScorer{r}, src{s}
                        {
                        }

			virtual ~IndexSourceScorer()
			{

			}

                        // For each term and phrase, this will be invoked, passed a single term, or 1+ terms (for phrases)
                        virtual ScorerWeight *new_scorer_weight(const str8_t *const terms, const uint16_t cnt)
                        {
                                return nullptr;
                        }

                        // Scores a single document; freq is the number of matches in the current document of
			// either a single term or a phrase
                        virtual float score(const isrc_docid_t id, const uint16_t freq, const ScorerWeight *) = 0;
                };

                struct IndexSourcesCollectionScorer
                {
                        // Override this if you want to, for example, aggregate all field_statistics of all
                        // index sources involved in the execution session collection and store that for later access
                        virtual void reset(const IndexSourcesCollection *)
                        {
                        }

                        virtual IndexSourceScorer *new_source_scorer(IndexSource *) = 0;

                        virtual ~IndexSourcesCollectionScorer()
                        {
                        }
                };

                // A trivial scorer that simply scores based on the matches
                struct IndexSourcesCollectionTrivialScorer
                    : public IndexSourcesCollectionScorer
                {
                        struct Scorer final
                            : public IndexSourceScorer
                        {
                                Scorer(IndexSourcesCollectionScorer *r, IndexSource *src)
                                    : IndexSourceScorer(r, src)
                                {
                                }

                                float score(const isrc_docid_t, const uint16_t freq, const ScorerWeight *) override final
                                {
                                        return freq;
                                }
                        };

                        IndexSourceScorer *new_source_scorer(IndexSource *s) override final
                        {
                                return new Scorer(this, s);
                        }
                };

                // A TF-IDF scorer
                struct IndexSourcesCollectionTFIDFScorer
                    : public IndexSourcesCollectionScorer
                {
                        IndexSource::field_statistics dfsAccum;
                        const IndexSourcesCollection *collection;

                        struct Scorer final
                            : public IndexSourceScorer
                        {
                                const IndexSource::field_statistics fs;

                                // docFreq: count of documents that contain the term
                                // docsCnt: total documents in a collection
                                static inline double idf(const uint32_t docFreq, const uint64_t docsCnt)
                                {
                                        return std::log((docsCnt + 1) / (double)(docFreq + 1)) + 1.0;
                                }

                                // Computers a score factor, based on a term or phrase's frequency in a document.
                                // Terms and phrases repeated in a document indicate the topic of the document, so impls. of
                                // this method usually return large values when freq is large, and smaller values when freq is small
                                static inline float tf(const float freq)
                                {
                                        return sqrt(freq);
                                }

                                Scorer(IndexSourcesCollectionScorer *r, IndexSource *src)
                                    : IndexSourceScorer(r, src), fs(src->default_field_stats())
                                {
                                }

                                struct ScorerWeight
                                    : public Similarity::ScorerWeight
                                {
                                        const double v;

                                        ScorerWeight(const double value)
                                            : v{value}
                                        {
                                        }
                                };

                                Similarity::ScorerWeight *new_scorer_weight(const str8_t *const terms, const uint16_t cnt) override final
                                {
                                        const auto cs = static_cast<IndexSourcesCollectionTFIDFScorer *>(collectionScorer);
                                        const auto collection = cs->collection;
                                        // aggregate sums across all index sources, pre-computed in reset()
                                        const auto &stats = cs->dfsAccum;
                                        const auto documentsCnt{stats.docsCnt};
                                        double weight{0};

                                        // We need to aggregate document frequency for each term, across all sources
                                        // XXX: we should probably cache this/compute this once by delegating it to
                                        // collectionScorer which should do it for us
                                        for (uint32_t i{0}; i != cnt; ++i)
                                        {
                                                const auto term = terms[i];
                                                uint64_t df{0};

                                                for (const auto src : collection->sources)
                                                        df += src->resolve_term_ctx(term).documents;

                                                weight += idf(df, documentsCnt);
                                        }

                                        return new ScorerWeight(weight);
                                }

                                // documentMatches: freq, i.e how many matches of a term in a document
                                // weight: See IndexSourcesCollectionScorer::compute_iterator_wrapper_weight() decl. comments
                                inline float score(const isrc_docid_t id, const uint16_t freq, const Similarity::ScorerWeight *sw) override final
                                {
                                        const auto v = tf(freq) * static_cast<const ScorerWeight *>(sw)->v; // tf-idf

                                        // TODO: if we had normalizations, we 'd instead return v * decodeNormValue(id) or something
                                        return v;
                                }
                        };

                        // currently, no support for multiple fields
                        // so a single reset() will do
                        void reset(const IndexSourcesCollection *const c) override final
                        {
                                collection = c;
                                memset(&dfsAccum, 0, sizeof(dfsAccum));

                                for (auto it : c->sources)
                                {
                                        const auto s = it->default_field_stats();

                                        dfsAccum.sumTermHits += s.sumTermHits;
                                        dfsAccum.totalTerms += s.totalTerms;
                                        dfsAccum.sumTermsDocs += s.sumTermsDocs;
                                        dfsAccum.docsCnt += s.docsCnt;
                                }
                        }

                        IndexSourceScorer *new_source_scorer(IndexSource *s) override final
                        {
                                return new Scorer(this, s);
                        }
                };

                struct IndexSourcesCollectionBM25Scorer
                    : public IndexSourcesCollectionScorer
                {
                        IndexSource::field_statistics dfsAccum;
                        const IndexSourcesCollection *collection;
                        // controls non-linear term frequence normalization (saturation)
                        static constexpr float k1{1.2};
                        // controls degree document length normalizes tf valuies
                        static constexpr float b{0.75};

                        struct Scorer final
                            : public IndexSourceScorer
                        {
                                static float normalizationTable[256]; // will be initialized elsewhere
                                static bool initializer;

                                static inline double idf(const uint32_t docFreq, const uint64_t docsCnt)
                                {
                                        return std::log(1 + (docsCnt - docFreq + 0.5f) / (docFreq + 0.5f));
                                }

                                static inline float tf(const float freq)
                                {
                                        return sqrt(freq);
                                }

                                Scorer(IndexSourcesCollectionScorer *r, IndexSource *src)
                                    : IndexSourceScorer(r, src)
                                {
                                }

                                struct ScorerWeight final
                                    : public Similarity::ScorerWeight
                                {
                                        const double idf;
                                        const uint32_t avgDocTermFrq;
                                        float cache[256];

                                        ScorerWeight(const double i, const uint32_t a)
                                            : idf{i}, avgDocTermFrq{a}
                                        {
                                        }
                                };

                                ScorerWeight *new_scorer_weight(const str8_t *const terms, const uint16_t cnt) override final
                                {
                                        const auto cs = static_cast<IndexSourcesCollectionTFIDFScorer *>(collectionScorer);
                                        const auto collection = cs->collection;
                                        const auto &stats = cs->dfsAccum;
                                        const auto documentsCnt{stats.docsCnt};
                                        double idf_{0};

                                        for (uint32_t i{0}; i != cnt; ++i)
                                        {
                                                const auto term = terms[i];
                                                uint64_t df{0};

                                                for (const auto src : collection->sources)
                                                        df += src->resolve_term_ctx(term).documents;

                                                idf_ += idf(df, documentsCnt);
                                        }

                                        const auto avgDocTermFrq = stats.sumTermsDocs / stats.docsCnt;
                                        auto w = std::make_unique<ScorerWeight>(idf_, avgDocTermFrq);

                                        for (uint32_t i{0}; i != 256; ++i)
                                                w->cache[i] = k1 * ((1 - b) + b * double(normalizationTable[i] / avgDocTermFrq));

                                        return w.release();
                                }

                                inline float score(const isrc_docid_t id, const uint16_t freq, const Similarity::ScorerWeight *weight) override final
                                {
                                        // otherwise cache[norms.get(doc)]
                                        const auto norm{k1};
                                        const auto w = static_cast<const ScorerWeight *>(weight);
                                        const auto idf = w->idf;

                                        return idf * float(freq) / double(freq + norm);
                                }
                        };

                        void reset(const IndexSourcesCollection *const c) override final
                        {
                                collection = c;
                                memset(&dfsAccum, 0, sizeof(dfsAccum));

                                for (auto it : c->sources)
                                {
                                        const auto s = it->default_field_stats();

                                        dfsAccum.sumTermHits += s.sumTermHits;
                                        dfsAccum.totalTerms += s.totalTerms;
                                        dfsAccum.sumTermsDocs += s.sumTermsDocs;
                                        dfsAccum.docsCnt += s.docsCnt;
                                }
                        }

                        IndexSourceScorer *new_source_scorer(IndexSource *s) override final
                        {
                                return new Scorer(this, s);
                        }
                };
        }
}
