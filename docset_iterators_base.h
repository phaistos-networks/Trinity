// Keep Trinity::DocsSetIterators::Iterator separate, so that we can
// include just this file, and not docset_iterators.h, which "pollutes" Trinity namespace with a forward decl of runtime_ctx
// in case some application needs it and would result in amiguous reference errors
#pragma once
#include "relevant_documents.h"
#include <switch.h>

namespace Trinity
{
        namespace DocsSetIterators
        {
                enum class Type : uint8_t
                {
                        PostingsListIterator = 0,
			DisjunctionSome,
                        Filter,
                        Optional,
                        Disjunction,
                        DisjunctionAllPLI,
                        Phrase,
                        Conjuction,
                        ConjuctionAllPLI,
			AppIterator,
			VectorIDs,
                        Dummy,
                };

		// An iterator provides access to a set of documents ordered by increasing ID
		// the two main methods, next() and advance(), are used to access the documents.
		// It subclasses relevant_document_provider, which means it may also provide a score
		// if any Iterator subclass implements score() -- which is useful/required for support
		// of "Accumulated Score Scheme" execution mode.
		// 
		// 
		// subclassing relevant_document_provider is somewhat expensive, but we need it to
		// support the semantics described in relevant_documents.h
                struct Iterator
			: public relevant_document_provider
                {
                      public:
                        // This is here so that we can directly access it without having to go through the vtable
                        // to invoke current() which would be overriden by subclasses -- i.e subclasses are expected
                        // to update curDocument{} so that current() won't be virtual
                        struct __anonymous final
                        {
                                isrc_docid_t id{0};
                        } curDocument;

                        // This is handy, and beats bloating the vtable with e.g a virtual reset_depth(), a virtual ~Iterator() etc, 
			// that are only used during engine bootstrap, not runtime execution
                        const Type type;

                      public:
                        Iterator(const Type t)
                            : type{t}
                        {
                        }

                        inline auto current() const noexcept
                        {
                                return curDocument.id;
                        }

			inline isrc_docid_t document() const noexcept override final 	// relevant_document_provider::document() override
			{
				return curDocument.id;
			}


                        // Advances to the first beyond the current whose document id that is >= target, and returns that document ID
                        // Example:
                        // isrc_docid_t advance(const isrc_docid_t target) { isrc_docid_t id; while ((id = next()) < target) {} return id; }
                        //
                        // XXX: some of the Iterators will check if current == target, i.e won't next() before they check
                        // for performance and simplicity reasons. It doesn't really affect our use so it is OK
                        // UPDATE: it actually does to some extent
                        virtual isrc_docid_t advance(const isrc_docid_t target) = 0;

                        // If at the end of the set, returns DocIDsEND otherwise advances to the next document and returns the current document
                        virtual isrc_docid_t next() = 0;

			// This is not virtual so that we won't bloat the vtable
			// instead, it just invokes DocsSetIterators::cost(Iterator *) passing this
			uint64_t cost();
                };


		class IndexSource;

		// If you are going to provide your own application iterator, you will need
		// to subclass AppIterator. It's main purpose is to provide a virtual destructor, which
		// is required for docsetsIterators destruction, and some other facilities specific to those iterators
		//
		// They are produced by factory functions and are trackedby the execution engine.
		// That is, ast_node nodes of Type::app_ids_set (or whaever) will embed a pointer to a factory class
		// which will be asked to provide an AppIterator instance (which would be passed the context embedded in
		// the ast_node).
		struct AppIterator
			: public Iterator
		{
			IndexSource *const isrc;

                        AppIterator(IndexSource *const src)
                            : Iterator(Type::AppIterator), isrc{src}
                        {
                        }

                        virtual ~AppIterator()
			{

			}
		};
        }
}
