#pragma once
#include "codecs.h"
#include <switch.h>
#include <switch_dictionary.h>
#include <switch_refcnt.h>

namespace Trinity
{
        // An index source provides term_index_ctx and decoders to the query execution runtime
        // It can be a RO wrapper to an index segment, a wrapper to a simple hashtable/list, anything
        // Lucene implements near real-time search by providing a segment wrapper(i.e index source) which accesses the indexer state directly
        // With index sources, we could accomplish that as well
        //
        // One could build a fairly impressive scheme, where custom IndexSource and Trinity::Codec::Decoder sub-classes would allow for very interesting use cases
	// An Index Source doesn't need to be a segment, or use the terms infrastructure to store and access terms. It can be anything, as long as it implements the 3 methods and
	// a decoder doesn't need to be accessing a segment either. It can be accessing e.g dicts and lists or vectors or whatever.
        class IndexSource
            : public RefCounted<IndexSource>
        {
              protected:
#ifdef LEAN_SWITCH
		std::unordered_map<str8_t, term_index_ctx> cache;
#else
                Switch::unordered_map<str8_t, term_index_ctx> cache;
#endif
                uint64_t gen{0}; // See IndexSourcesCollection

              public:
                inline auto generation() const noexcept
                {
                        return gen;
                }

                // TODO: serialize access
                term_index_ctx term_ctx(const str8_t term)
                {

#ifndef LEAN_SWITCH
                        term_index_ctx *ptr;

                        if (cache.Add(term, {}, &ptr))
                                *ptr = resolve_term_ctx(term);
                        return *ptr;
#else
                        auto p = cache.insert({term, {}});
                        if (p.second)
                                p.first->second = resolve_term_ctx(term);

                        return p.first->second;
#endif
                }

#if 0 		// This would probably be a good idea, but we don't need this, and it would make some optimisations in updated_documents_scanner::test() possible because
		// of document IDs(global space) are always expected to be considered in ascending order would not work, and it would also require some effort to
		// get this right everywhere we deal with document IDs (e.g merging documents).
		//
		// It may be a good idea to support this in the future though, with the added benefit that returning 0 would
		// save the execution engine from eval()uating the query for the document, and that we could use e.g 32bit integers for index source-local IDs mapped to e.g 64bit integers
		// and that could have some impact in the index size (and it would allow for e.g LUCENE_ENCODE_FREQ1_DOCDELTA).
		// Consider it for later.
		//
		//
		// Theory: two docIDs spaces. One is global, and another local to each index source/segment and irrelevant outside that context.
		// By using 2 different document IDs spaces, we could use an e.g u32 integer for the document IDs of documents indexed in a segment, and translate them
		// to e.g u64 global IDs during execution, prior to evaluating the query on the document. (e.g while indexing store in a file the global ID for each segment-local logical id
		// and then mmap that file in your IndexSource and directly dereference it to get the global ID from the local ID).
		//
		// By default, it's an identity method -- no translation between index source and global space
		// You can override it to return 0, if you want to ommit the document ID completely for whatever reason
		// or otherwise translate it to global space here
		virtual docid_t translate_docid(const docid_t indexSourceSpaceDocumentID)
		{
			return indexSourceSpaceDocumentID;
		}
#endif



                // Subclasses only need to implement 3 methods
                virtual term_index_ctx resolve_term_ctx(const str8_t term) = 0;

                // factory method
		// see RECIPES.md for when you should perhaps make use of the passed `term`
                virtual Trinity::Codecs::Decoder *new_postings_decoder(const str8_t term, const term_index_ctx ctx) = 0;

                // Override if you have any masked documents
                virtual updated_documents masked_documents()
                {
                        return {};
                }

		// Returns the maximum position expected
		// you may want to override to provide a more accurate value
		// This is used by the execution engine when creating a new DocWordsSpace
		// It cannot be higher than Limits::MaxPosition or lower than 1
		// 
		// For example, if you are only indexing titles, you should probably override this method
		// in your IndexSource variant to return a low max indexed position (e.g 100) because you likely don't
		// have any title term indexed at a higher position.
		virtual uint32_t max_indexed_position() const
		{
			return 8192;
		}

                virtual ~IndexSource()
                {
                }
        };

	// A simple IndexSource that only masks documents
	// You may want to use it to quickly mask documents without having to index any documents
	// Create one with an updated_documents and insert it into an IndexSourcesCollection and you are done
	class TrivialMaskedDocumentsIndexSource
		: public IndexSource
        {
              private:
                const updated_documents maskedDocuments;

              public:
                TrivialMaskedDocumentsIndexSource(const updated_documents ud)
                    : maskedDocuments{ud}
                {
                        gen = Timings::Microseconds::SysTime();
                }

                term_index_ctx resolve_term_ctx(const str8_t term) override final
                {
                        // fails for every term because we are only blocking here
                        return {};
                }

                // Currently, term is not really useful, but see RECIPES.md for how it could really help with some specific designs
                Trinity::Codecs::Decoder *new_postings_decoder(const str8_t term, const term_index_ctx) override final
                {
                        return nullptr;
                }

                updated_documents masked_documents() override final
                {
                        return maskedDocuments;
                }
        };

        // A collection of IndexSource; an index of segments or other sources
        // Each index source is identified by a generation, and no two sources can share the same generation
        // The generation represents the order of the sources in relation to each other; a higher generation means that
        // a source with that generation has been created after another with a lower generation.
        // In practice those are likely Timings::Microseconds::SysTime() at the time of the source creation and
        // for segments thats when they were persisted to disk. (in fact for segments, their name is their
        // generation)
        //
        // Each source is also associated with an `updated_documents` instance which tracked all documents updated or deleted when the source was created.
        // This is not used directly by the source itself, but when executing a query on a source(see exec_query() API), we need to consider the source's generation
        // and, for any other sources that will be involved in the search session that have generation HIGHER than the source's generation, we need to check that
        // a document is not set in any of their `updated_documents` instances (because that would mean that there is more recent information about that document
        // in another source that will be considered in this search session).
        //
        // IndexSourcesCollection facilitates that arrangement.
        // It represents a `search session` collection of index sources, and for each such source, it creates a masked_documents_registry that contains scanners
        // for all more recent sources.
        //
        // It also retains all sources.
	// See Trinity::exec_query(const query&, IndexSourcesCollection *) for how to do this in sequence, but you can and should do
	// this in paralle using multple threads and collecting the top-k results from every exec() and then use merge/reduce to come up with the final set of top-k results
	//
	// A great use case would be to have one IndexSourcesCollection which retains many sources and whenever you want to reload segments/sources etc, create
	// a new IndexSourcesCollection for them and atomically exchange pointers (old, new IndexSourcesCollection)
	//
	// It is very important you don't forget to invoke commit() otherwise updated/masked documents state will not be built
        class IndexSourcesCollection final
        {
              private:
                std::vector<updated_documents> all;
                // for each source, we track how many of the first update_documents in all[]
                // we should consider for masking documents
                std::vector<std::pair<IndexSource *, uint16_t>> map;

              public:
                std::vector<IndexSource *> sources;

              public:
                void insert(IndexSource *is)
                {
                        is->Retain();
                        sources.push_back(is);
                }

                ~IndexSourcesCollection();

                void commit();

                std::unique_ptr<Trinity::masked_documents_registry> scanner_registry_for(const uint16_t idx) ;
        };
}
