#pragma once
#include "docwordspace.h"
#include "runtime.h"

namespace Trinity
{
        // We assign an index (base 0) to each token in the query, which is monotonically increasing, except
        // when we are assigning to tokens in OR expressions, where we need to do more work and it gets more complicated (see assign_query_indices() for how that works).
        //
        // Long story short, we track all distinct (termIDs, toNextSpan) combinations for each query index, where
        // termID is the term ID (execution space) and toNextSpan is how many indices ahead to advance to get
        // to the net term (1 unless specific OR queries are processed.
        // Can also be 0 if there is no other token to the right)
        // Please see Trinity::phrase comments
        //
        // This is built by exec_query() and passed to MatchedIndexDocumentsFilter::prepare()
        // It is useful for proximity checks in conjuction with DocWordsSpace
        struct query_index_term final
        {
                exec_term_id_t termID;
                query_term_flags_t flags;
                uint8_t toNextSpan;

                inline bool operator==(const query_index_term &o) const noexcept
                {
                        return termID == o.termID && flags == o.flags && toNextSpan == o.toNextSpan;
                }
        };

        struct query_index_terms final
        {
                uint16_t cnt;
                // all distinct query_index_termS
                // uniques are sorted by (termID ASC, toNextSpan ASC, flags ASC)
                query_index_term uniques[0];
        };

        // Materialized hits for a term and the current document
        // This is used both for evaluation and for scoring documents
        struct term_hits final
        {
                // total hits for the term
                tokenpos_t freq;
                term_hit *all{0};

                // Facilitates execution -- ignored during scoring
                // This is internal and specific to the execution engine impl.
                uint16_t allCapacity{0};
                union {
                        uint16_t docSeq{0};
                        isrc_docid_t docID;
                };

                void set_freq(const tokenpos_t newFreq)
                {
                        if (unlikely(newFreq > allCapacity))
                        {
                                allCapacity = newFreq + 128;

                                if (all)
                                        std::free(all);

                                all = (term_hit *)std::malloc(sizeof(term_hit) * allCapacity);
                        }

                        freq = newFreq;
                }

                ~term_hits()
                {
                        if (all)
                                std::free(all);
                }
        };

        // We record an instance for each term instances in a original/input query
        // you can e.g use this information to determine if adjacent terms in the original query are both matched
        struct query_term_ctx final
        {
                // Information about the term itself
                // This is mostly for debugging during score consideration, but having access to
                // the distinct termID may be used to facilitate fancy tracking schemes in your MatchedIndexDocumentsFilter::consider()
                struct term_struct
                {
                        exec_term_id_t id;
                        str8_t token;
                } term;

                uint8_t instancesCnt; // i.e if your query is [world of warcraft mists of pandaria] then you will have 2 instances for token "of" in the query, with rep = 1
                struct instance_struct
                {
                        // see Trinity::phrase decl. comments
                        uint16_t index;
                        query_term_flags_t flags;
                        uint8_t rep;
                        uint8_t toNextSpan;
                        struct
                        {
                                range_base<uint16_t, uint8_t> range;
                                float translationCoefficient;
                                uint8_t srcSeqSize;
                        } rewrite_ctx;
                } instances[0];
        };

        struct matched_query_term final
        {
                const query_term_ctx *queryCtx;
                term_hits *hits;
        };

        // Score functions are provided with a matched_document
        // and are expected to return a score
        struct matched_document final
        {
                docid_t id; // document ID (GLOBAL)
                uint16_t matchedTermsCnt{0};
                matched_query_term *matchedTerms;
                // lazily initialized
                DocWordsSpace *dws{nullptr};

                matched_document()
                {
                }

                ~matched_document()
                {
                        delete dws;
                }
        };

        struct aborted_search_exception final
            : public std::exception
        {
                const char *what() const noexcept override
                {
                        return "Search Aborted";
                }
        };

        struct MatchedIndexDocumentsFilter
        {
                const query_index_terms **queryIndicesTerms;


		// There are 3 different consider() implementations, and which is invoked by the exec. enginedepends on the
		// ExecFlags passed to Trinity::exec_query().
		//
		// Your subclass should override whichever method(s) of those 3 are required based on which flags you use.
		//
		// When the default execution mode is seleted, this method will be invoked
                [[gnu::always_inline]] virtual void consider(const matched_document &match)
                {
                }

		// If the Documents Only mode is selected, this will be invoked, passed
		// the global document ID. Given that you only really want the 
		virtual void consider(const docid_t id)
		{
		}


		// If the Accumulated Score Scheme mode is selected instead, this
		// will be invoked; the global document ID and its scaore will passed to the call
		virtual void consider(const docid_t id, const double score)
		{

		}

                // Invoked before the query execution begins by the exec.engine
		// You may want to override this if you want to be notified and get a chance to do anything before
		// the engine executes the query in the index source
                virtual void prepare(const query_index_terms **queryIndicesTerms_)
                {
                        queryIndicesTerms = queryIndicesTerms_;
                }

                virtual ~MatchedIndexDocumentsFilter()
                {
                }
        };

        // You can provide an IndexDocumentsFilter derived class instance to exec_query() and friends, and if you do
        // it will invoke test(documentId) and if it returns true, the document will be ignored (in addition to
        // checking maskedDocumentsRegistry->test(docID), that is).
        //
        // That way, you will be able to ignore documents before the query is evaluated for them, like we do with
        // maskedDocumentsRegistry.  For example, say you only care for documents created in a specific time range, or have a specific state etc. Instead
        // of evaluating the query, and for matching documents, filtering them in consider() - thereby incurring the cost and overhead of evaluating the
        // query on a document you will eventually disregard anyway - you get to do that before the query is evaluated.
        //
        // In addition to that, you may have your own rules for ignoring documents and that can be implemented in your filter.
        struct IndexDocumentsFilter
        {
                // return true if you want to disregard/ignore the document
                virtual bool filter(const docid_t) = 0;
        };
}
