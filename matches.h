#pragma once
#include <switch.h>
#include <switch_dictionary.h>
#include "runtime.h"
#include "docwordspace.h"

namespace Trinity
{
        // Materialized hits for a term and the current document
        // this is used both for evaluation and for scoring documents
        struct term_hits final
        {
                term_hit *all{0};
                uint16_t freq;
                uint16_t allCapacity{0};

		// facilitates execution -- ignoring during scoring
		uint16_t docSeq;

                void set_freq(const uint16_t newFreq)
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

                uint8_t cnt;
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
                uint32_t id; // document ID
                uint16_t matchedTermsCnt;
                matched_query_term *matchedTerms;

		// matchedTerms[] are in no particularl order when passed to consider()
		// and so you almost never get them in order they appear in the original query
		// this handy utility method will sort them so that they are in order they appear in the query
		// which makes trivial to perform proximity checks (e.g dws.test(nextWordInQuery, pos+1))
		// It is optimised to take as little time as possible, using sort networks, insertion sort and std::sort depending on the
		// value of matchedTermsCnt
		void sort_matched_terms_by_query_index();
        };

	// You may for example keep the top-K matches, just count the documents, whatever else
	// For example, you may want to track the top-K documents and then eventually merge them all together
	struct MatchedIndexDocumentsFilter
	{
		enum class ConsiderResponse : uint8_t
		{
			Continue = 0,
			// If you return Abort, then the execution engine will stop immediately.
			// You should probably never to do that, but if you do, because for example you
			// are only interested in the first few documents matched regardless of their scores
			// (see for example:https://blog.twitter.com/2010/twitters-new-search-architecture "efficient early query termination")
			// then you can return Abort to return immediately from the execution to the callee
			Abort,
		};


		// You may want to use dws to improve score based on how terms document matches proximity relative to their proximity in the query
		// You can query_term_instances::term.id with dws
		// `match` is not const because you may want to match.sort_matched_terms_by_query_index()
		virtual ConsiderResponse consider(matched_document &match, const DocWordsSpace &dws) 
		{
			return ConsiderResponse::Continue;
		}

		virtual ~MatchedIndexDocumentsFilter()
		{

		}
	};
}
