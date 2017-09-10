#pragma once
#include "common.h"
#include "index_source.h"

// Whatever's here is specific to the "accumuluated score scheme" execution mode, where
// we just aggregate a similarity score for each iterator on the "current" document, similar to what Lucene's doing.
//
// We really need 1 Similarity::Scorer/execution mode, and one
// Similarity::Weight for each distinct term
namespace Trinity
{
        union index_source_weight {
                double d;
                uint64_t u64;
                uint32_t u32;
        };

	// TODO: 
	// - provide Similarity subclass  to exec_query_par()
	// - a method will be invoked with the index sources collection so that it can do whatever (optionally)
	// 	- also a good idea, pointer to the collection
	// - will invoke compute_weight() for each term. field statistics may be retrieved from the index source(cache it if needed), or 
	// 	whatever else. Maybe just compute a weight based on term docs count for the involved terms
        struct Similarity
        {
                struct Scorer
                {
                        virtual float score(const isrc_docid_t id, const uint16_t cnt) = 0;

                        virtual index_source_weight compute_weight(const IndexSource::field_statistics &, const uint32_t *termsDocsCnt, const uint16_t cnt) = 0;

			virtual ~Scorer() { }
                };

		virtual ~Similarity()
		{

		}
        };


	struct TrivialScorer final
		: public Similarity::Scorer
        {
                float score(const isrc_docid_t id, const uint16_t cnt) override final
                {
                        return cnt;
                }

                index_source_weight compute_weight(const IndexSource::field_statistics &, const uint32_t *termsDocsCnt, const uint16_t cnt) override final
                {
                        return {};
                }
        };

#if 0
        class Similarity
        {
              public:
                class Scorer
                {
                      public:
                        // `cnt` is the number of matches of a term, or a phrase, in the document `id`
                        virtual float score(const isrc_docid_t id, const uint16_t cnt) = 0;

                        virtual ~Scorer()
                        {
                        }

                        virtual void init(const IndexSource::field_statistics &fs, const uint32_t *termsDocsCnt, const uint16_t cnt) = 0;
                };

                virtual Scorer *new_scorer(const IndexSource::field_statistics &fs, const uint32_t *termsDocsCnt, const uint16_t cnt) = 0;

                virtual ~Similarity()
                {
                }
        };

        class TrivialSimilarity final
            : public Similarity
        {
                class TrivialScorer final
                    : public Similarity::Scorer
                {
                      public:
                        void init(const IndexSource::field_statistics &fs, const uint32_t *termsDocsCnt, const uint16_t cnt) override final
                        {
                        }

                        float score(const isrc_docid_t id, const uint16_t cnt) override final
                        {
                                return cnt;
                        }
                };
        };

        class TFIDFSimilarity final
            : public Similarity
        {
              private:
                double weight{0};

              private:
                static inline idf(const uint32_t docFreq, const uint32_t docsCnt)
                {
                        return log(1 + (double(docsCount - docFreq) + 0.5) / (double(docFreq) + 0.5));
                }

                static inline float tf(const float freq)
                {
                        return sqrt(freq);
                }

              public:
                void init(const IndexSource::field_statistics &fs, const uint32_t *termsDocsCnt, const uint16_t cnt) override final
                {
                        const auto docsCnt{fs.docsCnt};

                        for (uint32_t i{0}; i != cnt; ++i)
                        {
                                const auto df{termsDocsCnt[i]};
                                const auto idf_ = idf(df, docsCnt);

                                weight += idf_;
                        }
                }

                inline float score(const isrc_docid_t id, const uint16_t cnt) override final
                {
                        const auto v = tf(cnt) * weight;

                        return v; //  return norms == null ? raw : raw * decodeNormValue(norms.get(doc)); //
                }
        };
#endif
}
