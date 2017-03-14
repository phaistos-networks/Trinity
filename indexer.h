#pragma once
#include "codecs.h"
#include <buffer.h>
#include <compress.h>
#include <switch_dictionary.h>
#include <switch_mallocators.h>

namespace Trinity
{
	// persists an index sesion as a segment
	// the application is responsible for persisting the terms (see SegmentIndexSession::commit() for example)
	void persist_segment(Trinity::Codecs::IndexSession *const sess, std::vector<uint32_t> &updatedDocumentIDs);


	// A utility class suitable for indexing document terms and persisting the index and other codec specifc data into a directory
	// It offers a simple API for adding, updating and erasing documents
	// You should use SegmentIndexSource to load the segment(and use it for search)
        class SegmentIndexSession final
        {
              private:
                IOBuffer b;
                IOBuffer hitsBuf;
                std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> hits;
                std::vector<docid_t> updatedDocumentIDs;
                simple_allocator dictionaryAllocator;
                Switch::unordered_map<str8_t, uint32_t> dictionary;
		Switch::unordered_map<uint32_t, str8_t> invDict;

              public:
                struct document_proxy final
                {
                        SegmentIndexSession &sess;
                        const docid_t did;
                        std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> &hits;
                        IOBuffer &hitsBuf;

                        uint32_t term_id(const str8_t term)
                        {
                                return sess.term_id(term);
                        }

                        document_proxy(SegmentIndexSession &s, docid_t documentID, std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> &h, IOBuffer &hb)
                            : sess{s}, did{documentID}, hits{h}, hitsBuf{hb}
                        {
                        }

                        void insert(const uint32_t termID, const uint32_t position, range_base<const uint8_t *, const uint8_t> payload);

			// new `term` hit at `position` with `payload`(attrs.)
                        void insert(const str8_t term, const uint32_t position, range_base<const uint8_t *, const uint8_t> payload)
                        {
                                insert(term_id(term), position, payload);
                        }

                        void insert(const uint32_t termID, const uint32_t position)
                        {
                                insert(termID, position, {});
                        }

			// new `term` hit at `position`
                        void insert(const str8_t term, const uint32_t position)
                        {
                                insert(term_id(term), position, {});
                        }

                        template <typename T>
                        void insert(const uint32_t termID, const uint32_t position, const T &v)
                        {
                                insert(termID, position, {static_cast<const uint8_t *>(&v), uint32_t(sizeof(T))});
                        }

                        template <typename T>
                        void insert(const str8_t term, const uint32_t position, const T &v)
                        {
                                insert(term_id(term), position, {static_cast<const uint8_t *>(&v), uint32_t(sizeof(T))});
                        }
                };

              private:
                void commit_document_impl(const document_proxy &proxy, const bool isUpdate);

              public:
                uint32_t term_id(const str8_t term);

                void clear()
                {
                        b.clear();
                }

                void erase(const docid_t documentID);

		// after you have obtained a document_proxy, you can use its insert methods to register term hits
		// and when you are done indexing a document, use insert() or update() to insert as a NEW document or update an existing
		// document in case it is already indexed in another segment
                document_proxy begin(const docid_t documentID);

                // In the past we 'd store those in reverse, so that if we were to erase a document and then index it
                // we 'd read the document's terms first (because we 'd read in reverse from the buffer) and would ignore
                // the erase (document, 0) pair
                // it should be easy to implement that again later
                void insert(const document_proxy &proxy)
                {
                        commit_document_impl(proxy, false);
                }

                void update(const document_proxy &proxy)
                {
                        commit_document_impl(proxy, true);
                }

		// Persist index and masked products into the directory s->basePath
		// See also SegmentIndexSource::SegmentIndexSource()
                void commit(Trinity::Codecs::IndexSession *const s);
        };
}
