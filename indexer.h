#pragma once
#include "codecs.h"
#include <buffer.h>
#include <switch_dictionary.h>
#include <switch_mallocators.h>
#include <switch_bitops.h>

namespace Trinity
{
        // Persists an index sesion as a segment
        // The application is responsible for persisting the terms (see SegmentIndexSession::commit() for example, and IndexSession::persist_terms())
        void persist_segment(Trinity::Codecs::IndexSession *const sess, std::vector<uint32_t> &updatedDocumentIDs, int fd);

	// Wrapper for persist_segment(); opens the index file and passes it to persist_segment()
        void persist_segment(Trinity::Codecs::IndexSession *const sess, std::vector<uint32_t> &updatedDocumentIDs);

        // A utility class suitable for indexing document terms and persisting the index and other codec specifc data into a directory
        // It offers a simple API for adding, updating and erasing documents
        // You can use SegmentIndexSource to load the segment(and use it for search)
        class SegmentIndexSession final
        {
              private:
                IOBuffer b;
                IOBuffer hitsBuf;
		int backingFileFD{-1};
                std::vector<std::pair<uint32_t, std::pair<uint32_t, range_base<uint32_t, uint8_t>>>> hits;
                std::vector<docid_t> updatedDocumentIDs;
                simple_allocator dictionaryAllocator;
                Switch::unordered_map<str8_t, uint32_t> dictionary;
                Switch::unordered_map<uint32_t, str8_t> invDict;
		// See IndexSession::indexOutFlushed comments
		uint32_t flushFreq{0}, intermediateStateFlushFreq{0};

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

                        void insert(const uint32_t termID, const tokenpos_t position, range_base<const uint8_t *, const uint8_t> payload);

                        // new `term` hit at `position` with `payload`(attrs.)
                        void insert(const str8_t term, const tokenpos_t position, range_base<const uint8_t *, const uint8_t> payload)
                        {
				Dexpect(term.size() <= Limits::MaxTermLength);
                                insert(term_id(term), position, payload);
                        }

                        void insert(const uint32_t termID, const tokenpos_t position)
                        {
                                insert(termID, position, {});
                        }

                        // new `term` hit at `position`
                        void insert(const str8_t term, const tokenpos_t position)
                        {
				Dexpect(term.size() <= Limits::MaxTermLength);
                                insert(term_id(term), position, {});
                        }

                        template <typename T>
                        void insert(const uint32_t termID, const tokenpos_t position, const T &v)
                        {
                                insert(termID, position, {reinterpret_cast<const uint8_t *>(&v), uint32_t(sizeof(T))});
                        }

                        template <typename T>
                        void insert(const str8_t term, const tokenpos_t position, const T &v)
                        {
				Dexpect(term.size() <= Limits::MaxTermLength);
                                insert(term_id(term), position, {static_cast<const uint8_t *>(&v), uint32_t(sizeof(T))});
                        }

			void insert_var_payload(const uint32_t termID, const tokenpos_t pos, const uint64_t payload)
			{
				const uint8_t requiredBytes = ((64 - SwitchBitOps::LeadingZeros(payload)) + 7) >> 3;

				insert(termID, pos, {reinterpret_cast<const uint8_t *>(&payload), requiredBytes});
			}
                };

              private:
                void commit_document_impl(const document_proxy &proxy, const bool isUpdate);

              public:
                uint32_t term_id(const str8_t term);

		str8_t term(const uint32_t id);

                void clear()
                {
                        b.clear();
                }

		// When != 0, whenever the session's indexOut size exceeds that value, the index
		// will be flushed.
		void set_flush_freq(const size_t n)
		{
			flushFreq = n;
		}

		void set_intermediate_state_flush_freq(const size_t n)
		{
			intermediateStateFlushFreq = n;
		}

                void erase(const docid_t documentID);

                // After you have obtained a document_proxy, you can use its insert methods to register term hits
                // and when you are done indexing a document, use insert() or update() to insert as a NEW document or UPDATE an existing
                // document in case it is already indexed in another segments.
                document_proxy begin(const docid_t documentID);

                // In the past we 'd store those in reverse, so that if we were to erase a document and then index it
                // We 'd read the document's terms first (because we 'd read in reverse from the buffer) and would ignore
                // the erase (document, 0) pair
                // It should be easy to implement that again later
                void insert(const document_proxy &proxy)
                {
                        commit_document_impl(proxy, false);
                }

		// Use this method instead of insert() when you are updating a document.
		// If you are not, you should (but are not required to) use insert()
                void update(const document_proxy &proxy)
                {
                        commit_document_impl(proxy, true);
                }

                // Persist index and masked products into the directory s->basePath
                // See also SegmentIndexSource::SegmentIndexSource()
                void commit(Trinity::Codecs::IndexSession *const s);

		auto any_indexed() const noexcept
		{
			return backingFileFD != -1 || hitsBuf.size();
		}

		~SegmentIndexSession()
		{
			if (backingFileFD != -1)
				close(backingFileFD);
		}
        };
}
