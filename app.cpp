#include "exec.h"
#include "google_codec.h"
#include "indexer.h"
#include "playground_index_source.h"
#include "terms.h"
#include "segment_index_source.h"
#include <set>


using namespace Trinity;

int main(int argc, char *argv[])
{
	{
                SegmentIndexSession sess;

                {
                        auto doc = sess.begin(1);

                        doc.insert("apple"_s8, 1);
                        doc.insert("iphone"_s8, 2);

                        sess.update(doc);
                }

                auto is = new Trinity::Codecs::Google::IndexSession("/tmp/TSEGMENTS/1/");

                sess.commit(is);

                delete is;
        }


#if 0
	{
		auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/1/");
		auto maskedDocuments = dids_scanner_registry::make(nullptr, 0);
		query q("apple");

		exec_query(q, ss, maskedDocuments);

		free(maskedDocuments);
		ss->Release();
	}
#else
	{
		query q("apple");
		IndexSourcesCollection bpIndex;
		auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/1/");

		bpIndex.insert(ss);
		ss->Release();
	
		bpIndex.commit();

		for (uint32_t i{0}; i != bpIndex.sources.size(); ++i)
		{
			auto source = bpIndex.sources[i];
			auto reg = bpIndex.scanner_registry_for(i);

			exec_query(q, source, reg);
			free(reg);
		}
	}
#endif

        return 0;
}

#if 0
int main(int argc, char *argv[])
{
	std::vector<std::pair<strwlen8_t, term_index_ctx>> terms;
	const strwlen32_t allTerms(_S("world of warcraft amiga 1200 apple iphone ipad macbook pro imac ipod edge zelda gamecube playstation psp nes snes gameboy sega nintendo atari commodore ibm"));
	IOBuffer index, data;
	Switch::vector<terms_skiplist_entry> skipList;
	simple_allocator allocator;

	for (const auto it : allTerms.Split(' '))
	{
		const term_index_ctx tctx{1, {uint32_t(it.data() - allTerms.data()), it.size()}};
		
		terms.push_back({{it.data(), it.size()}, tctx});
	}

	pack_terms(terms, &data, &index);

	unpack_terms_skiplist({(uint8_t *)index.data(), index.size()}, &skipList, allocator);

	for (uint32_t i{1}; i != argc; ++i)
        {
                const strwlen8_t q(argv[i]);
                const auto res = lookup_term({(uint8_t *)data.data(), data.size()}, q, skipList);

                Print(q , " => ", res.indexChunk, "\n");
        }
        return 0;
}
#endif


#if 0
int main(int argc, char *argv[])
{
	int fd = open("/home/system/Data/BestPrice/SERVICE/clusters.data", O_RDONLY|O_LARGEFILE);

	require(fd != -1);

	const auto fileSize = lseek64(fd, 0, SEEK_END);
	auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	simple_allocator a;
	std::set<strwlen8_t> uniq;
	std::vector<std::pair<strwlen8_t, term_index_ctx>> terms;
	size_t termsLenSum{0};

	close(fd);
	require(fileData != MAP_FAILED);

	for (const auto *p = static_cast<const uint8_t *>(fileData), *const e = p + fileSize; p != e; )
	{
		p+=sizeof(uint16_t);
		const auto chunkSize = *(uint32_t *)p;

		p+=sizeof(uint32_t);

		for (const auto chunkEnd = p + chunkSize; p != chunkEnd; )
		{
			p+=sizeof(uint32_t);
			strwlen8_t title((char *)p + 1, *p); p+=title.size() + sizeof(uint8_t);
			++p;
			const auto n = *(uint16_t *)p; p+=sizeof(uint16_t);

			p+=n * sizeof(uint32_t);

			title.p = a.CopyOf(title.data(), title.size());


			for (const auto *p = title.p, *const e = p+ title.size(); p != e; )
                        {
                                if (const auto len = Text::TermLengthWithEnd(p, e))
                                {
					auto mtp = (char *)p;

					for (uint32_t i{0}; i != len; ++i)
						mtp[i] = Buffer::UppercaseISO88597(p[i]);
					
					const strwlen8_t term(p, len);

					if (uniq.insert(term).second)
					{
						terms.push_back({term, { 1, {0, 0} }});
						termsLenSum+=term.size();
					}
	

					p+=len;
                                }
                                else
                                        ++p;
                        }
                }
	}
	munmap(fileData, fileSize);
	// 574,584 distinct terms across all cluster titles



	SLog(terms.size(), "\n"); 	



	IOBuffer index, data;
	Switch::vector<terms_skiplist_entry> skipList;
	simple_allocator allocator;
	uint64_t before;


	before = Timings::Microseconds::Tick();
	pack_terms(terms, &data, &index);	 // Took 2.856s to pack 106.15kb 9mb 4.05mb (we 'd have need about 14MBs without prefix compression, so we save 35%)
	SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " to pack ", size_repr(index.size()), " ", size_repr(data.size()), " ", size_repr(termsLenSum), "\n");

	before = Timings::Microseconds::Tick();
	unpack_terms_skiplist({(uint8_t *)index.data(), index.size()}, &skipList, allocator); 	// Took 0.002s to unpack 4489
	SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " to unpack ", skipList.size(), "\n");

	for (uint32_t i{1}; i != argc; ++i)
        {
                const strwlen8_t q(argv[i]);
		const auto before = Timings::Microseconds::Tick();
                const auto res = lookup_term({(uint8_t *)data.data(), data.size()}, q, skipList);
		const auto t = Timings::Microseconds::Since(before); // 3 to 26us 

                Print(q , " => (", res.documents, ", ",  res.indexChunk, ") in ", duration_repr(t), "\n"); 
        }
        return 0;
}
#endif


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

#if 0
int main(int argc, char *argv[])
{
	Trinity::Codecs::Google::IndexSession sess("/tmp/");
	Trinity::Codecs::Google::Encoder encoder(&sess);
	term_index_ctx appleTCTX, iphoneTCTX, crapTCTX;


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
		term_index_ctx tctx;
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
	auto idxSrc = Switch::make_sharedref<PlaygroundIndexSource>(ap.release());

	idxSrc->tctxMap.insert({"apple"_s8, appleTCTX});
	idxSrc->tctxMap.insert({"iphone"_s8, iphoneTCTX});
	idxSrc->tctxMap.insert({"crap"_s8, crapTCTX});


	//query q("apple OR iphone NOT crap"_s32);
	query q("\"apple iphone\""_s32);
	auto maskedDocumentsRegistry = dids_scanner_registry::make(nullptr, 0);

	exec_query(q, idxSrc.get(), maskedDocumentsRegistry);


        return 0;
}
#endif
