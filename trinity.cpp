#include <switch.h>
#include <compress.h>


// Instead of relying on continuations and gotos for reentrancy
// which would require a function call, we should probably unpack one block/time
// so that we we 'll be able to operate on lists of documents and positions directly
// This also means that caches will remain hot for as long as we unpack a block
struct Indexer
{
        IOBuffer documentIdsBuf, hitsBuf;
        Switch::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t>>> skiplist;
        uint32_t totalDocuments{0};
        uint32_t documentsChunkSize{0}, hitsChunkSize{0};
        uint32_t skiplistThreshold{80 * 1024};
        static constexpr uint8_t blockCapacity{32};
        uint8_t blockSize{blockCapacity};
        IOBuffer blockDocumentIdsBuf, blockHitsBuf;
        uint32_t prevBlockLastDocumentId{0};
        uint32_t blockHitsBase;
	uint32_t lastDocumentId{0};

        struct
        {
                uint32_t hitsCnt;
                uint32_t hitsBase;
		uint16_t lastPos;
        } curDocument;

        void begin_document(const uint32_t id)
        {
                if (blockSize == blockCapacity)
                {
			if (prevBlockLastDocumentId)
                        {
                                const auto delta = lastDocumentId - prevBlockLastDocumentId;
                                const auto blockHitsChunkLen = hitsBuf.size() - blockHitsBase;

                                documentIdsBuf.SerializeVarUint32(lastBlockIdDelta);
                                documentIdsBuf.SerializeVarUint32(blockHitsChunkLen);

                                documentsChunkSize += blockLen;
                                hitsChunkSize += blockHitsChunkLen;
                        }

                        blockSize = 0;
                        blockHitsBase = hitsBuf.size();
                        blockHitsBuf.clear();

                        if (documentsChunkSize > skiplistThreshold)
                        {
                                skiplist.push_back({id, {documentsChunkSize, hitsChunkSize}});
                                skiplistThreshold = documentsChunkSize + 80 * 1024;
                        }
                }


		// We could use Rice-k or Gamma encoding ( see http://static.googleusercontent.com/media/research.google.com/en//people/jeff/WSDM09-keynote.pdf )
		// or we could use PFor (see Lucene, and Lemire's repos)

                curDocument.hitsCnt = 0;
                curDocument.hitsBase = hitsBuf.size();
		curDocument.lastPos = 0;
		curBlockDocumentsCnt = 0;
		
		const auto delta = id - lastDocumentId;


		documentIdsBuf.SerializeVarUint32(delta);
		lastDocumentId = id;
        }

        void register_hit(const uint32_t position, const uint32_t attributes)
        {
                // need 2 bits, do we have a position, do we have attributes?
                // encode:
                // 	0: no position, no attributes
                //
                // UPDATE: for now do something simple, figure out something that yields better compression once we get the baseline to work
                // It is important to consider situations where position is, well, 0
                // what happens then? we use delta encoding so we need to do something about it
                // UPDATE: when we decode words, if word is the same as the last one, that's not a word -- so ignore it
                const auto delta = position - curDocument.lastPos;

                curDocument.lastPos = position;
                if (!attributes)
                {
                        // Just the position
                        hitsBuf.SerializeVarUint32(delta << 1);
                }
                else
                {
                        hitsBuf.SerializeVarUint32((delta << 1) | 1);
                        hitsBuf.SerializeVarUint32(attributes);
                }

                ++curDocument.hitsCnt;
        }

        void end_document() noexcept
        {
        }
}

int
main()
{


	return 0;
}
