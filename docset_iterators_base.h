// Keep Trinity::DocsSetIterators::Iterator separate, so that we can
// include just this file, and not docset_iterators.h, which "pollutes" Trinity namespace with a forward decl of runtime_ctx
// in case some application needs it and would result in amiguous reference errors
#pragma once
#include "common.h"
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
                        OptionalOptPLI,
                        OptionalAllPLI,
                        Disjunction,
                        DisjunctionAllPLI,
                        Phrase,
                        Conjuction,
                        ConjuctionAllPLI,
			AppIterator,
			VectorIDs,
                        Dummy,
                };

                struct Iterator
                {
                      public:
                        // This is here so that we can directly access it without having to go through the vtable
                        // to invoke current() which would be overriden by subclasses -- i.e subclasses are expected
                        // to update curDocument{} so that current() won't be virtual
                        struct __anonymous final
                        {
                                isrc_docid_t id{0};
                        } curDocument;

#if 0 // we no longer make use of this
                        // Distance from the root
                        // depths >= std::numeric_limits<uint16_t>::max() / 2 are treated specially; they are used
                        // by Filter and Optional iterators, and they in turn set their own iterators's depth to be
                        // >= that magic value.
                        // We may end up using depth for other optimizations later
			//
                        uint16_t depth;
#endif

                        // This is handy, and beats bloating the vtable with e.g a virtual reset_depth(), a virtual ~Iterator() etc, that are only used during engine bootstrap, not runtime execution
                        // All told, depth and type only come down to a 4bytes overhead to Iterator, so that's not that much anyway
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

                        // If we wanted to support [min, max] range semantics, i.e only accept a documents within tha range min inclusive, max inclusive
                        // it could work like so:
                        // for (;;
                        // {
                        // 	auto id = next();
                        //
                        //	if (id < min) id = advance(min);
                        // 	while (id < max) { consider(id); id = next(); }
                        // }
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
