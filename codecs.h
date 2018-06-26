#pragma once
#include "common.h"
#include "docidupdates.h"
#include "docset_iterators_base.h"
#include "docwordspace.h"
#include "runtime.h"

// Use of Codecs::Google results in a somewhat large index, while the access time is similar(maybe somewhat slower) to Lucene's codec
namespace Trinity {
        struct candidate_document;
        struct queryexec_ctx;

        // Information about a term's posting list and number of documents it matches.
        // We track the number of documents because it may be useful(and it is) to some codecs, and also
        // is extremely useful during execution where we re-order the query nodes based on evaluation cost which
        // is directly related to the a term's posting list size based on the number of documents it matches.
        struct term_index_ctx final {
                uint32_t documents;

                // For each each term, the inverted index contains a `posting list`, where
                // each posting contains the occurrences information (e.g frequences and positions)
                // for documents that contain the trerm. This is the chunk in the index that
                // holds the posting list.
                // This is codec specific though -- for some codecs, this could mean something else
                // e.g for a memory-resident index source/segment, you could use inexChunk to refer to some in-memory data
                range32_t indexChunk;

                term_index_ctx(const uint32_t d, const range32_t c)
                    : documents{d}, indexChunk{c} {
                }

                term_index_ctx(const term_index_ctx &o) {
                        documents  = o.documents;
                        indexChunk = o.indexChunk;
                }

                term_index_ctx(term_index_ctx &&o) {
                        documents  = o.documents;
                        indexChunk = o.indexChunk;
                }

                term_index_ctx &operator=(const term_index_ctx &o) {
                        documents  = o.documents;
                        indexChunk = o.indexChunk;
                        return *this;
                }

                term_index_ctx &operator=(const term_index_ctx &&o) {
                        documents  = o.documents;
                        indexChunk = o.indexChunk;
                        return *this;
                }

                term_index_ctx() = default;
        };

        namespace Codecs {
                struct Encoder;
                struct AccessProxy;

                // Represents a new indexer session
                // All indexer sessions have an `indexOut` that holds the inverted index(posting lists for each distinct term)
                // but other codecs may e.g open/track more files or buffers depending on their needs.
                //
                // The base path makes sense for disk based storage, but some codecs may only operate on in-memory
                // data so it may not be used in those designs.
                struct IndexSession {
                        enum class Capabilities : uint8_t {
                                // append_index_chunk() is implemented
                                AppendIndexChunk = 1,
                                Merge            = 1 << 1
                        };

                        const uint8_t caps;
                        IOBuffer      indexOut;
                        // Whenever you flush indexOut to disk, say, every few MBs or GBs,
                        // you need to adjust indexOutFlushed, because the various codecs implementations need to compute some offset in the index
                        //
                        // Obviously, you are not required to flush, but it's an option.
                        // Doing this would also likely improve performance, for instead of expanding indexOut's underlying allocated memory and thus
                        // likely incurring a memmcpy() cost, by keeping the buffer small and flushing it periodically to a backing file, this is avoided, memory
                        // allocation remainins low/constant and no need for memcpy() is required (if no reallocations are required)
                        //
                        // TODO: consider an alternative idea, where instead of flushing to disk, we 'll just steal indexOut memory (via IOBuffer::release() ) and
                        // length, and track that in a vector, and flush indexOut, and in the end, we 'll just compile a new indexOut from those chunks.
                        // This would provide those benefits:
                        // - no need to allocate large chunks of memory to hold the whold index; will allocate smaller chunks (and maybe even in the end
                        //	serialize all them to disk, free their memory, and allocate memory for the index and load it from disk)
                        // - no need to resize the IOBuffer, i.e no need for memcpy() the data to new buffers on reallocation
                        uint32_t indexOutFlushed;
                        char     basePath[PATH_MAX];

                        // The segment name should be the generation
                        // e.g for path Trinity/Indices/Wikipedia/Segments/100
                        // the generation is extracted as 100, but, again, this is codec specific
                        IndexSession(const char *bp, const uint8_t capabilities = 0)
                            : caps{capabilities}, indexOutFlushed{0} {
                                strcpy(basePath, bp);
                        }

                        virtual ~IndexSession() {
                        }

                        // Utility method
                        // Demonstrates how you should update indexOutFlushed
                        void flush_index(int fd);

                        // Handy utility function
                        // see SegmentIndexSession::commit()
                        void persist_terms(std::vector<std::pair<str8_t, term_index_ctx>> &);

                        // Subclasses should e.g open files, allocate memory etc
                        virtual void begin() = 0;

                        // Subclasses should undo begin() ops. e.g close files and release resources
                        virtual void end() = 0;

                        // This may be stored in a segment directory in order to determine
                        // which codec to use to access it
                        virtual strwlen8_t codec_identifier() = 0;

                        // Constructs a new encoder
                        // Handy utility function
                        virtual Encoder *new_encoder() = 0;

                        // This is used during merge
                        // By providing the access to an index and a term's tctx, we should append to the current indexOut
                        // for most codecs, that's just a matter of copying the region in tctx.indexChunk into indexOut, but
                        // some other codecs may need to access other files in the src
                        // (e.g lucene codec uses an extra files for positions/attributes)
                        //
                        // must hold that (src->codec_identifier() == this->codec_identifier())
                        //
                        // XXX: If you want to filter a chunk's documents though, don't use this method.
                        // See MergeCandidatesCollection::merge()
                        //
                        // If you have a fancy codec that you don't expect to use for merging other segments
                        // (e.g some fancy in-memory wrapper that is
                        // really only used for queries as an IndexSource), then you can just implement this and merge() as no-ops.
                        //
                        // see MergeCandidatesCollection::merge() impl.
                        //
                        // UPDATE: this is now optional. If your codec implements it, make sure you set (Capabilities::AppendIndexChunk)
                        // in capabilities flags passed to Codecs::IndexSession::IndexSession(). Both Google and Lucene's do so.
                        // The default impl. does nothing/aborts
                        virtual range32_t append_index_chunk(const AccessProxy *src, const term_index_ctx srcTCTX) {
                                std::abort();
                                return {};
                        }

                        // If we have multiple postng lists for the same term(i.e merging 2+ segments
                        // and same term exists in 2+ of them)
                        // where we can't just use append_index_chunk() but instead we need to merge them,
                        // and to do that, we we need to use this codec-specific merge function
                        // that will do this for us. Use append_index_chunk() otherwise.
                        //
                        // You are expected to have encoder->begin_term() before invoking this method,
                        // and to invoke encoder->end_term() after the method has returned
                        //
                        // The input pairs hold an AccessProxy for the data and a range in the the index of that data, and
                        // the encoder the target codec encoder
                        //
                        // See MergeCandidatesCollection::merge() impl.
                        struct merge_participant final {
                                AccessProxy *              ap;
                                term_index_ctx             tctx;
                                masked_documents_registry *maskedDocsReg;
                        };

                        // UPDATE: now optional; if you override and implement it, make sure you set Capabilities::Merge in the constructo
                        virtual void merge(merge_participant *participants, const uint16_t participantsCnt, Encoder *const encoder) {
                        }
                };

                // Encoder interface for encoding a single term's posting list
                struct Encoder {
                        IndexSession *const sess;

                        // You should have s->begin() before you use encode any postlists for that session
                        Encoder(IndexSession *s)
                            : sess{s} {
                        }

                        virtual ~Encoder() {
                        }

                        virtual void begin_term() = 0;

                        virtual void begin_document(const isrc_docid_t documentID) = 0;

                        // If you want to register a hit for a special token (e.g site:foo.com) where position makes no sense,
                        // you should use position 0(or a very high position, but 0 is preferrable)
                        // You will likely only need to set payload for special terms (e.g site:foo.com).
                        // Payload can be upto 8 byte sin size(sizeof(uint64_t)). You should try to keep that as low as possible.
                        virtual void new_hit(const uint32_t position, const range_base<const uint8_t *, const uint8_t> payload) = 0;

                        virtual void end_document() = 0;

                        virtual void end_term(term_index_ctx *) = 0;
                };

                // This is how postings lists are accessed and hits are materialized.
                // It is created by a Codecs::Decoder, and it should in practice hold a reference
                // to the decoder and delegate work to it, but that should depend on your codec's impl.
                //
                // In the past, Decoder's implemented next() and advance()/seek(), but it turned to be a bad idea
                // because it was somewhat challenging to support multiple terms in the query, where you want to access
                // the same underlying decoder state, but traverse them independently.
                struct Decoder;

                struct PostingsListIterator
                    : public Trinity::DocsSetIterators::Iterator {
                        // decoder that created this iterator
                        Decoder *const dec;

                        // For current document
                        tokenpos_t freq;

                        PostingsListIterator(Decoder *const d)
                            : Iterator{Trinity::DocsSetIterators::Type::PostingsListIterator}, dec{d} {
                        }

                        virtual ~PostingsListIterator() {
                        }

                        // Materializes hits for the _current_ document in the postings list
                        // You must also dwspace->set(termID, pos) for positions != 0
                        //
                        // XXX: if you materialize, it's likely that curDocument.freq will be reset to 0
                        // so you should not materialize if have already done so.
                        virtual void materialize_hits(DocWordsSpace *dwspace, term_hit *out) = 0;

                        inline auto decoder() noexcept {
                                return dec;
                        }

                        inline const Decoder *decoder() const noexcept {
                                return dec;
                        }

#ifdef RDP_NEED_TOTAL_MATCHES
                        inline uint32_t total_matches() override final {
                                return freq;
                        }
#endif
                };

                // Decoder interface for decoding a single term's posting list.
                // If a decoder makes use of skiplist data, they are expected to be serialized in the index
                // and the decoder is responsible for deserializing and using them from there, although this is specific to the codec
                //
                // Decoder is responsible for maintaining term-specific segment state, and for creating new Posting Lists iterators, which in turn
                // should just contain iterator-specific state and logic working in conjuction with the Decoder that created/provided it.
                struct Decoder {
			// Term specific data for the term of this decode
			// see e.g DocsSetIterators::cost()
                        term_index_ctx indexTermCtx;

                        // use set_exec() to set those
                        exec_term_id_t execCtxTermID{0};
                        queryexec_ctx *rctx{nullptr};

                        constexpr auto exec_ctx_termid() const noexcept {
                                return execCtxTermID;
                        }

                        // Initialise the decoding state for accessing the postings list
                        //
                        // Not going to rely on a constructor for initialization because
                        // we want to force subclasses to define a method with this signature
                        virtual void init(const term_index_ctx &, AccessProxy *) = 0;

                        // This is how you are going to access the postings list
                        virtual PostingsListIterator *new_iterator() = 0;

                        Decoder() {
                        }

                        virtual ~Decoder() {
                        }

                        void set_exec(exec_term_id_t tid, queryexec_ctx *const r) {
                                execCtxTermID = tid;
                                rctx          = r;
                        }
                };

                // Responsible for initializing segment/codec specific state and for generating(factory) decoders for terms
                // All AccessProxy instances have a pointer to the actual index(posts lists) in common
                struct AccessProxy {
                        const char *basePath;
                        // Index of the posting lists for all distinct terms
                        // This is typically a pointer to memory mapped index file, or a memory-resident anything, or
                        // something other specific to the codec
                        const uint8_t *const indexPtr;

                        // Utility function: returns an initialised new decoder for a term's posting list
                        // Some codecs(e.g lucene's) may need to access the filesystem and/or other codec specific state
                        // AccessProxy faciliates that (this is effectively a pointer to self)
                        //
                        // XXX: Make sure you Decoder::init() before you return from the method
                        // see e.g Google::AccessProxy::new_decoder()
                        //
                        // See Codecs::Decoder for execCtxTermID
                        virtual Decoder *new_decoder(const term_index_ctx &tctx) = 0;

                        AccessProxy(const char *bp, const uint8_t *index_ptr)
                            : basePath{bp}, indexPtr{index_ptr} {
                                // Subclasses should open files, etc
                        }

                        // See IndexSession::codec_identifier()
                        virtual strwlen8_t codec_identifier() = 0;

                        virtual ~AccessProxy() {
                        }
                };
        } // namespace Codecs
} // namespace Trinity
