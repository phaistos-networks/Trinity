#include "exec.h"
#include "google_codec.h"
#include "indexer.h"
#include "segments.h"

using namespace Trinity;

#if 0
int main(int argc, char *argv[])
{
        // Inex
        SegmentIndexSession indexerSess;

        {
                auto proxy = indexerSess.begin(1);

                proxy.insert("apple"_s8, 1);
                proxy.insert("macbook"_s8, 2);
                proxy.insert("pro"_s8, 3);

                indexerSess.insert(proxy);
        }

        {
                auto proxy = indexerSess.begin(2);

                proxy.insert("apple"_s8, 1);
                proxy.insert("iphone"_s8, 2);

                indexerSess.insert(proxy);
        }

        Trinity::Codecs::Google::IndexSession codecIndexSess("/tmp/segment_1/");

        indexerSess.commit(&codecIndexSess);

        // Search
        int fd = open("/tmp/segment_1/index", O_RDONLY | O_LARGEFILE);

        require(fd != -1);
        const auto fileSize = lseek64(fd, 0, SEEK_END);
        auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

        close(fd);
        require(fileData != MAP_FAILED);

        auto ap = new Trinity::Codecs::Google::AccessProxy("/tmp/segment_1/", (uint8_t *)fileData);
        auto seg = Switch::make_sharedref<Trinity::segment>(ap);
        auto maskedDocsReg = dids_scanner_registry::make(nullptr, 0);

        exec_query(Trinity::query("apple"_s32), *seg.get(), maskedDocsReg);

        return 0;
}
#endif

#if 1
int main(int argc, char *argv[])
{
	Trinity::Codecs::Google::IndexSession sess("/tmp/");
	Trinity::Codecs::Google::Encoder encoder(&sess);
	term_segment_ctx appleTCTX, iphoneTCTX, crapTCTX;


	sess.begin();

	encoder.begin_term();

	encoder.begin_document(10, 2);
	encoder.new_position(1);
	encoder.new_position(2);
	encoder.end_document();

	encoder.begin_document(11, 5);
	encoder.new_position(15);
	encoder.new_position(20);
	encoder.new_position(21);
	encoder.new_position(50);
	encoder.new_position(55);
	encoder.end_document();

	encoder.begin_document(15, 1);
	encoder.new_position(20);
	encoder.end_document();


	encoder.begin_document(25, 1);
	encoder.new_position(18);
	encoder.end_document();


	encoder.begin_document(50,1);
	encoder.new_position(20);
	encoder.end_document();

	encoder.end_term(&appleTCTX);



	// iphone
	encoder.begin_term();
	
	encoder.begin_document(11, 1);
	encoder.new_position(51);
	encoder.end_document();

	encoder.begin_document(50, 1);
	encoder.new_position(25);
	encoder.end_document();

	encoder.end_term(&iphoneTCTX);


	// crap
	encoder.begin_term();
	
	encoder.begin_document(25, 1);
	encoder.new_position(1);
	encoder.end_document();
	encoder.end_term(&crapTCTX);

	sess.end();



	Print(" ============================================== DECODING\n");

#if 0

        {
                range_base<const uint8_t *, uint32_t> range{(uint8_t *)sess.indexOut.data(), appleTCTX.chunkSize};
		term_segment_ctx tctx;
                Codecs::Google::IndexSession mergeSess("/tmp/foo");
                Codecs::Google::Encoder enc(&mergeSess);
		auto maskedDocuments = dids_scanner_registry::make(nullptr, 0);

                enc.begin_term();
                mergeSess.merge(&range, 1, &enc, maskedDocuments);
                enc.end_term(&tctx);

                SLog(sess.indexOut.size(), " ", mergeSess.indexOut.size(), "\n");
                return 0;
        }
#endif

#if 0
	Trinity::Codecs::Google::Decoder decoder;

	decoder.init(tctx, (uint8_t *)indexData.data(), nullptr); //(uint8_t *)encoder.skipListData.data());

	SLog("chunk size= ", tctx.chunkSize, "\n");

	decoder.begin();
#if 0
	decoder.seek(2);
	decoder.seek(28);
	decoder.seek(50);
	decoder.seek(501);
#endif

	while (decoder.cur_document() != UINT32_MAX)
	{
		Print(ansifmt::bold, "document ", decoder.cur_document(), ansifmt::reset,"\n");
		decoder.next();
	}

	return 0;
#endif


	std::unique_ptr<Trinity::Codecs::Google::AccessProxy> ap(new Trinity::Codecs::Google::AccessProxy("/tmp/", (uint8_t *)sess.indexOut.data()));
	auto seg = Switch::make_sharedref<segment>(ap.release());

	seg->tctxMap.insert({"apple"_s8, appleTCTX});
	seg->tctxMap.insert({"iphone"_s8, iphoneTCTX});
	seg->tctxMap.insert({"crap"_s8, crapTCTX});


	//query q("apple OR iphone NOT crap"_s32);
	query q("\"apple iphone\""_s32);
	auto maskedDocumentsRegistry = dids_scanner_registry::make(nullptr, 0);

	exec_query(q, *seg.get(), maskedDocumentsRegistry);


        return 0;
}
#endif
