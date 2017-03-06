#pragma once
#include "docwordspace.h"
#include "docidupdates.h"

namespace Trinity
{
	// We are going to map term=>term_segment_ctx representation in a segment's termlist file
        struct term_segment_ctx final
        { 
		// total documents matching term
		// this is really important
		// However, it's also important to remember that we can't compute TFIDF like Lucene claims to do if multiple segments
		// are involved in an execution plan and at least one of them has updated documents that override 1+ other segments, because
		// we can't really add the documents across all segments for the same term in order to determine how many document. 
		// Maybe we can track that counts during evaluation though
                uint32_t documents;  

		// size of the chunk that holds all postings for a given term
		// We need to keep track of that size because it allows for all kind of optimizations and efficient skiplist access
		uint32_t chunkSize;  

		// codec specific
		// offsets[0] is usually the offset in the index
                uint32_t offsets[3]; 

		term_segment_ctx(const term_segment_ctx &o)
		{
			documents = o.documents;
			chunkSize = o.chunkSize;
			offsets[0]= o.offsets[0];
			offsets[1]= o.offsets[1];
			offsets[2]= o.offsets[2];
		}

		term_segment_ctx(term_segment_ctx &&o)
		{
			documents = o.documents;
			chunkSize = o.chunkSize;
			offsets[0]= o.offsets[0];
			offsets[1]= o.offsets[1];
			offsets[2]= o.offsets[2];
		}

		term_segment_ctx &operator=(const term_segment_ctx &o)
		{
			documents = o.documents;
			chunkSize = o.chunkSize;
			offsets[0]= o.offsets[0];
			offsets[1]= o.offsets[1];
			offsets[2]= o.offsets[2];
			return *this;
		}

		term_segment_ctx &operator=(const term_segment_ctx &&o)
		{
			documents = o.documents;
			chunkSize = o.chunkSize;
			offsets[0]= o.offsets[0];
			offsets[1]= o.offsets[1];
			offsets[2]= o.offsets[2];
			return *this;
		}

		term_segment_ctx()= default;
        };
 
	// a materialized document term hit
	struct term_hit final
        {
                uint64_t payload;
                uint16_t pos;
                uint8_t payloadLen;
        };

        namespace Codecs
        {
		struct Encoder;

		// represents a new indexer session
		// all indexer sessions have an indexOut that holds the index(posts)
		// but other codecs may e.g open/track more files or buffers
		struct IndexSession
                {
                        IOBuffer indexOut;
			const char *basePath;

                        IndexSession(const char *bp)
				: basePath{bp}
			{

			}

			virtual ~IndexSession()
			{

			}


                        virtual void begin() = 0;

                        virtual void end() = 0;

			virtual uint8_t dictionary_offsets_count() const 
			{
				return 1;
			}

			// If we have multiple postlists for the same term(i.e merging 2+ segments and same term exists in 2+ of them) then
			// where we can't just serialize (chunk, chunkSize) but instead we need to merge them, we need to use this codec-specific merge function
			// that will do this for us
			// 
			// You are expected to have encoder->begin_term() before invoking this method, and to invoke encoder->end_term() after the method has returned
                        virtual void merge(range_base<const uint8_t *, uint32_t> *in, const uint32_t chunksCnt, Encoder *const encoder, dids_scanner_registry *maskedDocuments) = 0 ;

			
			// constructs a new encoder 
			// handy utility function
			virtual Encoder *new_encoder(IndexSession *) = 0;
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

                        virtual void begin_document(const uint32_t documentID, const uint16_t totalHits) = 0;

                        virtual void new_hit(const uint32_t position, const range_base<const uint8_t *, const uint8_t> payload) = 0;

                        virtual void end_document() = 0;

                        virtual void end_term(term_segment_ctx *) = 0;
                };


		class AccessProxy;

		// Decoder interface for decoding a single term's postlists
                struct Decoder
                {
                        virtual uint32_t begin() = 0;

			// returns false if no documents list has been exchausted
                        virtual bool next() = 0;

                        virtual bool seek(const uint32_t target) = 0;

			// returns UINT32_MAX if no more documents left
			virtual uint32_t cur_doc_id() = 0;

			virtual uint16_t cur_doc_freq() = 0;

			// XXX: if you materialize, it's likely that cur_doc_freq() will return, at best, 0
			// so you should not materialize if have already done so -- if you know the codec you use
			// will reset freqs tracker internally so that cur_doc_freq() == 0 you can check for that
                        virtual void materialize_hits(const exec_term_id_t termID, DocWordsSpace *dwspace, term_hit *out) = 0;

			// not going to rely on a constructor for initialization because
			// we want to force subclasses to define a method with this signature
			// TODO: skiplist data should be specific to the AccessProxy
			virtual void init(const term_segment_ctx &, AccessProxy *, const uint8_t *skiplistData) = 0;

			virtual ~Decoder()
			{

			}
                };


		// Responsible for initializing segment/codec specific state and for generating(factory) decoders for terms
		// All AccessProxy instances have a pointer to the actual index(posts lists) in common
		struct AccessProxy
                {
			const char *basePath;
			const uint8_t *const indexPtr;

			// utility function: returns a new decoder for posts list
                        virtual Decoder *decoder(const term_segment_ctx &tctx, AccessProxy *access, const uint8_t *skiplistData) = 0;

			AccessProxy(const char *bp, const uint8_t *index_ptr)
				: basePath{bp}, indexPtr{index_ptr}
			{
				// Subclasses should open files, etc
			}

                        virtual ~AccessProxy()
			{

			}
                };
        }
}
