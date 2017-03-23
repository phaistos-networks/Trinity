#pragma once
#include <switch.h>
#include <switch_dictionary.h>
#include "runtime.h"
#include "docwordspace.h"

namespace Trinity
{
	// Used to track for each query index(i.e index in the query terms) the distinct termIDs that map to it
	// That will usually be one term, except when there are OR sequences
	// e.g  [apple laptop OR "macbook pro"]
	// both laptop and macbook will be map to the query index(1)
	//
	// This is built by exec_query() and passed to MatchedIndexDocumentsFilter::prepare()
	// It is useful for proximity checks in conjuction with DocWordsSpace
	struct query_index_terms final
	{
		uint16_t cnt;
		exec_term_id_t termIDs[0];
	};

        // Materialized hits for a term and the current document
        // this is used both for evaluation and for scoring documents
        struct term_hits final
        {
                term_hit *all{0};
                tokenpos_t freq; 		// total hits for the term


		// facilitates execution -- ignoredg during scoring
		// this is internal and specific to the execution engine impl.
                uint16_t allCapacity{0};
		uint16_t docSeq;

                void set_freq(const tokenpos_t newFreq)
                {
                        if (newFreq > allCapacity)
                        {
                                allCapacity = newFreq + 32;
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
	// e.g for query [apple iphone] (this is not a phrase), if we match both apple and iphone in a document, but
	// in one document they are next to each other while on another they are far apart, we could rank that first document higher
        struct query_term_instances final
        {
		// information about the term itself
		// this is mostly for debugging during score consideration, but having access to
		// the distinct termID may be used to facilitate fancy tracking schemes
		struct 
		{
			exec_term_id_t id;
			str8_t token;
		} term;

                uint8_t cnt; // i.e if your query is [world of warcraft mists of pandaria] then you will have 2 instances for token "of" in the query, with rep = 1
                struct instance_struct
                {
                        uint16_t index;
                        uint8_t rep;
			uint8_t flags;
                } instances[0];
        };

        struct matched_query_term final
        {
                // Every instance of the term in the query
                // the token may be used more than once
                // and each time a different rep/index may be set
                const query_term_instances *queryTermInstances;	// see runtime_ctx.originalQueryTermInstances
                term_hits *hits;     				// all hits in current document for this term. See runtime_ctx.termHits
        };


	// Score functions are provided with a matched_document
	// and are expected to return a score 
        struct matched_document final
        {
                docid_t id; // document ID
                uint16_t matchedTermsCnt;
                matched_query_term *matchedTerms;

		// matchedTerms[] are in not in a particular order when passed to consider()
		// and so you almost never get them in order they appear in the original query
		// This handy utility method will sort them so that they are in order they appear in the query
		// which makes trivial to perform proximity checks (e.g dws.test(nextWordInQuery, pos+1))
		// It is optimised to take as little time as possible, using sort networks, insertion sort and std::sort depending on the
		// value of matchedTermsCnt
		void sort_matched_terms_by_query_index();
        };

	struct MatchedIndexDocumentsFilter
	{
		DocWordsSpace *dws;
		const query_index_terms **queryIndicesTerms;

		enum class ConsiderResponse : uint8_t
		{
			Continue = 0,
			// If you return Abort, then the execution engine will stop immediately.
			// You should probably never to do that, but if you do, because for example you
			// are only interested in the first few documents matched regardless of their scores
			// then you can return Abort to return immediately from the execution to the callee
			// See RECIPES.md and CONCEPTS.md
			Abort,
		};

		// You can query_term_instances::term.id with dws
		// `match` is not const because you may want to match.sort_matched_terms_by_query_index()
		virtual ConsiderResponse consider(matched_document &match)
		{
			return ConsiderResponse::Continue;
		}

		// Invoked before the query execution begins
		virtual void prepare(DocWordsSpace *dws_, const query_index_terms **queryIndicesTerms_)
		{
			dws = dws_;
			queryIndicesTerms = queryIndicesTerms_;
		}

		virtual ~MatchedIndexDocumentsFilter()
		{
		}
	};
}
