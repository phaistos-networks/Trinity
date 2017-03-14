#pragma once
#include "docwordspace.h"
#include "docidupdates.h"
#include "runtime.h"

namespace Trinity
{
	// We are going to map term=>term_index_ctx representation in a segment's termlist file
        struct term_index_ctx final
        { 
		// total documents matching term in the index(i.e segment, or whatever else)
		// this is really important
		//
		// However, it's also important to remember that we can't compute TFIDF like Lucene claims to do if multiple segments
		// are involved in an execution plan and at least one of them has updated documents that override 1+ other segments, because
		// we can't really add the documents across all segments for the same term in order to determine how many document. 
		// Maybe we can track that counts during evaluation though
                uint32_t documents;  

		// chunk that holds all postings for a given term
		// We need to keep track of that size because it allows for all kind of optimizations and efficient skiplist access
		// This is codec specific
		range32_t indexChunk;

		term_index_ctx(const uint32_t d, const range32_t c)
			: documents{d}, indexChunk{c}
		{

		}

		term_index_ctx(const term_index_ctx &o)
		{
			documents = o.documents;
			indexChunk = o.indexChunk;
		}

		term_index_ctx(term_index_ctx &&o)
		{
			documents = o.documents;
			indexChunk = o.indexChunk;
		}

		term_index_ctx &operator=(const term_index_ctx &o)
		{
			documents = o.documents;
			indexChunk = o.indexChunk;
			return *this;
		}

		term_index_ctx &operator=(const term_index_ctx &&o)
		{
			documents = o.documents;
			indexChunk = o.indexChunk;
			return *this;
		}

		term_index_ctx()= default;
        };

        namespace Codecs
        {
		struct Encoder;
		struct AccessProxy;

		// represents a new indexer session
		// all indexer sessions have an indexOut that holds the index(posts)
		// but other codecs may e.g open/track more files or buffers
		//
		// The base path makes sense for disk based storage, but some codecs may only operate on in-memory
		// data so it may not be used in those designs.
		struct IndexSession
                {
                        IOBuffer indexOut;
			const char *basePath;

			// The segment name should be the generation
                        IndexSession(const char *bp)
				: basePath{bp}
			{

			}

			virtual ~IndexSession()
			{

			}

			// handy utility function
			// see SegmentIndexSession::commit()
			void persist_terms(std::vector<std::pair<str8_t, term_index_ctx>> &);


			// Subclasses should e.g open files, allocate memory etc
                        virtual void begin() = 0;

			// Subclasses should undo begin() ops. e.g close files and release resources
                        virtual void end() = 0;


			// This may be stored in a segment directory in order to determine
			// which codec to use to access it
			virtual strwlen8_t codec_identifier() = 0;

			
			// constructs a new encoder 
			// handy utility function
			virtual Encoder *new_encoder() = 0;



			// This is used during merge
			// By providing the access to an index and a term's tctx, we should append to the current indexOut
			// for most codecs, that's just a matter of copying the region in tctx.indexChunk into indexOut, but
			// some other codecs may need to access other files in the src (e.g lucene codec uses two extra files for positions/attributes)
			//
			// must hold that (src->codec_identifier() == this->codec_identifier())
			//
			// XXX: If you want to filter a chunk's documents though, don't use this method. see MergeCandidatesCollection::merge()
			//
			// If you have a fancy codec that you don't expect to use for merging other segments (e.g some fancy in-memory wrapper that is
			// really only used for queries as an IndexSource), then you can just implement this and merge() as no-ops.
			virtual range32_t append_index_chunk(const AccessProxy *src, const term_index_ctx srcTCTX) = 0;


			// If we have multiple postlists for the same term(i.e merging 2+ segments and same term exists in 2+ of them) then
			// where we can't just use append_index_chunk() but instead we need to merge them, we need to use this codec-specific merge function
			// that will do this for us. Use append_index_chunk() otherwise.
			// 
			// You are expected to have encoder->begin_term() before invoking this method, and to invoke encoder->end_term() after the method has returned
			// The input pairs hold an AccessProxy for the data and a range in the the index of that data, and the encoder the target codec encoer
			struct merge_participant
			{
				AccessProxy *ap;
				term_index_ctx tctx;
				masked_documents_registry *maskedDocsReg;
			};
				
                        virtual void merge(merge_participant *participants, const uint16_t participantsCnt, Encoder *const encoder) = 0;
                };

                // Encoder interface for encoding a single term's postlists
                struct Encoder
                {
			IndexSession *const sess;

			// You should have s->begin() before you use encode any postlists for that session
			Encoder(IndexSession *s)
				: sess{s}
			{

			}

			virtual ~Encoder()
			{

			}

                        virtual void begin_term() = 0;

                        virtual void begin_document(const docid_t documentID, const uint16_t totalHits) = 0;

			// If you want to register a hit for a special token (e.g site:foo.com) where position makes no sense, you should
			// use position 0(or a very high position, but 0 is preferrable)
			// You will likely only need to set payload for special terms (e.g site:foo.com). Payload can be upto 8 byte sin size(sizeof(uint64_t)). You
			// should try to keep that as low as possible.
                        virtual void new_hit(const uint32_t position, const range_base<const uint8_t *, const uint8_t> payload) = 0;

                        virtual void end_document() = 0;

                        virtual void end_term(term_index_ctx *) = 0;
                };



		// Decoder interface for decoding a single term's postlists
		// If a decoder makes use of skiplist data, they are expected to be serialized in the index
		// and the decoder is responsible for deserializing and using them from there
                struct Decoder
                {
			// We now use a curDocument structure that holds the current document and its frequency(number of hits)
			// whereas in the past we only had access to those via cur_doc_id() and cur_doc_freq()
			// That results in potentially millions of virtual calls to those methods, wheras now can access them without
			// having to call anything.
			//
			// The only minor downside is that Decoder subclases *MUST* update curDocument in their begin(), next() and seek() methods
			// Recall that document ID MaxDocIDValue can be used for 'no more documents'. 
			// 
			// Using `dec->curDocument` instead of dec->cur_doc_id() and dec->cur_doc_freq()
			// results in a drop from 0.238s to 0.222s for a very fancy query
			//
			// UPDATE: now no longer expose cur_doc_id() and cur_doc_freq() which saves us 2*sizeof(void*) bytes from the vtable
			// which is potentially great
			struct
			{
				docid_t id;
				uint16_t freq;
			} curDocument;



			// Before iterating via next(), you need to begin()
			//
			// If you do not intend to iterate the documents list, and only wish to seek documents
			// you can/should skip begin() and just use seek()
			//
			// Returns MaxDocIDValue if there are no documents
			// (Remeber to update curDocument)
                        virtual docid_t begin() = 0;

			// Returns false if no documents list has been exchausted
			// or advances to the next document and returns true
			//
			// - if at the end, return false
			// - otherwise advance by one and return the current document
			//
			// Make sure that you do not advance to `nowhere` if you are already at the end, because e.g for a query that contains the same 
			// term twice e.g [trinity search engine called trinity] (trinity is set twice in the query)
			// the execution engine may invoke seek() to the shared decoder for the second trinity 
			// instance and getting to the end, and then attempt to advance the leader decoder for first trinity
			// by invoking next(), so the next() should be able to handle being at the end already
			// (Remeber to update curDocument)
                        virtual bool next() = 0;

			// Seeks to a target document
			// 
			// - If you are at a document > target, return false
			// - If you are at the end of the documents/hits, return false
			// - If you matched the document, return true
			//
			// XXX: it is important that if you fail to seek to target you stop to ONE document after target, or if you have exhausted the documents list then 
			// just return false and set curDocument.id  = MaxDocIDValue
			// i.e if you are at curDocument.id = 50 and the ids in the list are (20, 50, 55, 70, 80, 100, 150, 200) and you
			// seek to 110, then you must stop at 150 (or earlier but not earlier than 50)
			// See provided codecs implementations.
			//
			// This is important so that subsequent accesses to curDocument, and call to next() and seek() will work as expected
			// (Remeber to update curDocument)
                        virtual bool seek(const docid_t target) = 0;

			// Materializes hits for the _current_ document
			// You must also dwspace->set(termID, pos) for positions != 0
			//
			// XXX: if you materialize, it's likely that curDocument.freq will be reset to 0
			// so you should not materialize if have already done so.
                        virtual void materialize_hits(const exec_term_id_t termID, DocWordsSpace *dwspace, term_hit *out) = 0;

			// not going to rely on a constructor for initialization because
			// we want to force subclasses to define a method with this signature
			// TODO: skiplist data should be specific to the AccessProxy
			virtual void init(const term_index_ctx &, AccessProxy *) = 0;

			virtual ~Decoder()
			{

			}
                };


		// Responsible for initializing segment/codec specific state and for generating(factory) decoders for terms
		// All AccessProxy instances have a pointer to the actual index(posts lists) in common
		struct AccessProxy
                {
			const char *basePath;	 // not really necessary, but keep it around just in case
			const uint8_t *const indexPtr;

			// utility function: returns a new decoder for posts list
			// Some codecs(e.g lucene's) may need to access the filesystem and/or other codec specific state
			// AccessProxy faciliates that (this is effectively a pointer to self)
			//
			// XXX: Make sure you Decoder::init() before you return it
			// see e.g Google::AccessProxy::new_decoder()
                        virtual Decoder *new_decoder(const term_index_ctx &tctx) = 0;

			AccessProxy(const char *bp, const uint8_t *index_ptr)
				: basePath{bp}, indexPtr{index_ptr}
			{
				// Subclasses should open files, etc
			}

			// See IndexSession::codec_identifier()
			virtual strwlen8_t codec_identifier() = 0;

                        virtual ~AccessProxy()
			{

			}
                };
        }
}
