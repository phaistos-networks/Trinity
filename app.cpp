#include "exec.h"
#include "google_codec.h"
#include "indexer.h"
#include "playground_index_source.h"
#include "terms.h"
#include "segment_index_source.h"
#include <set>
#include <text.h>
#include "merge.h"
#include "lucene_codec.h"
#include <crypto.h>


using namespace Trinity;

#if 0
int main(int argc, char *argv[])
{
	MergeCandidatesCollection collection;
	int fd = open("/tmp/TSEGMENTS/1500/terms.data", O_RDONLY);

	require(fd != -1);
	auto fileSize = lseek64(fd, 0, SEEK_END);
	auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	close(fd);
	require(fileData != MAP_FAILED);
	
	IndexSourcePrefixCompressedTermsView itv({(const uint8_t *)fileData, (uint32_t)fileSize});
	fd = open("/tmp/TSEGMENTS/1500/index", O_RDONLY);
	require(fd != -1);
	fileSize = lseek64(fd, 0, SEEK_END);
	fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	close(fd);
	require(fileData != MAP_FAILED);

	Trinity::Codecs::Lucene::AccessProxy ap("/tmp/TSEGMENTS/1500/", (uint8_t *)fileData);

	SegmentTerms st("/tmp/TSEGMENTS/1500/");

	collection.insert({1500, &itv, &ap, {}});


	std::vector<std::pair<str8_t, Trinity::term_index_ctx>> terms;
	simple_allocator allocator;
	auto outSession = new Trinity::Codecs::Lucene::IndexSession("/tmp/TSEGMENTS/1800/");


	collection.commit();
	outSession->begin();
	collection.merge(outSession, &allocator, &terms);
	outSession->end();

	SLog("positionsOut.size = ", outSession->positionsOut.size(), "\n");

	require(outSession->indexOut.size() == fileSize);
	SLog("indexOut.size = ", outSession->indexOut.size(), "\n");
	delete outSession;



	return 0;
}
#endif

#if 0
int main()
{
	matched_document document;
	auto qt = [](const uint16_t pos)
	{
		auto mqt = new matched_query_term();
		auto ptr = (query_term_instances *)malloc(sizeof(query_term_instances) + sizeof(query_term_instances::instance_struct));

		ptr->instances[0].index = pos;
		mqt->queryTermInstances = ptr;
		return *mqt;
	};
	matched_query_term terms[] =
	{
		qt(10),
		qt(5),
		qt(1),
		qt(2),
		qt(50),
		qt(8),
		qt(100),
		qt(102),
		qt(80),
		qt(81),
		qt(85),
		qt(70),
		qt(150),
		qt(250),
		qt(15),
		qt(1000),
		qt(28),
	};

	document.matchedTermsCnt = sizeof_array(terms);
	document.matchedTerms = terms;

	document.sort_by_query_index();

	for (uint32_t i{0}; i != sizeof_array(terms); ++i)
	{
		Print(terms[i].queryTermInstances->instances[0].index, "\n");
	}

	return 0;
}
#endif

#if 0
int main(int argc, char *argv[])
{
	simple_allocator allocator;
	const strwlen32_t in(argv[1]);
	Trinity::query q(in);

	q.process_runs(true, true, [&q](auto &v)
	{
		for (uint32_t i{0}; i != v.size(); ++i)
		{
			const auto it = v[i];

			if (it->p->size == 1 && it->p->terms[0].token.Eq(_S("jump")))
			{
				*it = *ast_parser("(jump OR hop OR leap)"_s32, q.allocator).parse();
			}
                        else if (i +3 <= v.size() && it->p->size == 1 && it->p->terms[0].token.Eq(_S("world")) 
				&& v[i+1]->p->size == 1 && v[i+1]->p->terms[0].token.Eq(_S("of"))
				&& v[i+2]->p->size == 1 && v[i+2]->p->terms[0].token.Eq(_S("warcraft")))
			{
				query::replace_run(v.data() + i, 3, ast_parser("(wow OR (world of warcraft))"_s32, q.allocator).parse());
			}
			else if (it->p->size == 1 && it->p->terms[0].token.Eq(_S("puppy")))
			{
				*it = *ast_parser("puppy OR (kitten OR kittens OR cat OR cats OR puppies OR dogs OR pets)"_s32, q.allocator).parse();
			}
		}
	});

	q.normalize();
	Print(q, "\n");

	return 0;
}
#endif


#if 0
int main(int argc, char *argv[])
{
	auto sess = std::make_unique<Trinity::Codecs::Lucene::IndexSession>("/tmp/LUCENE/1/");
	//auto sess = std::make_unique<Trinity::Codecs::Google::IndexSession>("/tmp/LUCENE/1/");
	auto enc = sess->new_encoder();
	term_index_ctx tctx;
	term_hit hits[8192];
	uint32_t pos{1};

        enc->begin_term();

#if 1
	const size_t n{15};

        enc->begin_document(1, n);
        for (uint32_t i{10}; i != 10 + n; ++i)
        {
		enc->new_hit(pos++, {(uint8_t *)&i, sizeof(uint32_t)});
	}
	enc->end_document();

	enc->begin_document(10, 2);
	enc->new_hit(15, {});
	enc->new_hit(16, {});
	enc->end_document();

	enc->begin_document(100, 2);
	enc->new_hit(25, {});
	enc->new_hit(50, {});
	enc->end_document();

#else
        for (uint32_t i{1}; i < 250; ++i)
        {
                if (i > 100 && i < 125)
                {
                        enc->begin_document(i, 2);
                        enc->new_hit(pos++, {(uint8_t *)&i, sizeof(uint32_t)});
                        enc->new_hit(pos++, {});
                        enc->end_document();
                }
                else
                {
                        enc->begin_document(i, 1);
                        enc->new_hit(pos++, {(uint8_t *)&i, sizeof(uint32_t)});
                        enc->end_document();
                }

                if (i < 15)
                        ++i;
        }
#endif
        enc->end_term(&tctx);

	SLog(sess->indexOut.size(), "\n");
        delete enc;



	Print("\n\n\n\n\n");

        auto ap = new Trinity::Codecs::Lucene::AccessProxy("/tmp/LUCENE/1/", (uint8_t *)sess->indexOut.data(), (uint8_t *)sess->positionsOut.data());
	auto dec = ap->new_decoder(tctx);
	DocWordsSpace dws;

	dec->begin();
	dec->seek(1);
	//if (!dec->seek(80)) { SLog("Failed to seek\n"); } dec->seek(51000);

	if (dec->curDocument.id == MaxDocIDValue)
	{
		SLog("Done\n");
		dec->seek(5550000);

		return 0;
	}

	for (;;)
	{
		const auto freq = dec->curDocument.freq;

		Print(ansifmt::color_magenta, "DOCUMENT:", dec->curDocument.id, " ", freq, ansifmt::reset, "\n");
		dec->materialize_hits(0, &dws, hits);

		for (uint32_t i{0}; i != freq; ++i)
		{
			Print("HIT:     ", hits[i].pos,  " PAYLOAD:", *(uint32_t *)&hits[i].payload, "\n");
		}


		if (dec->next() == false)
			break;
	}

	delete dec;
	delete ap;
        return 0;
}
#endif


#if 1

int main(int argc, char *argv[])
{
	if (argc == 1)
        {
#if 0
                const auto index_document = [](auto &documentSess, const strwlen32_t input) {
                        uint32_t pos{1};

                        for (const auto *p = input.data(), *const e = p + input.size(); p != e;)
                        {
                                if (const auto len = Text::TermLengthWithEnd(p, e))
                                {
                                        documentSess.insert(strwlen8_t(p, len), pos++);
                                        p += len;
                                }
                                else
                                {
                                        if (!isblank(*p))
                                                ++pos;
                                        ++p;
                                }
                        }
                };

                SegmentIndexSession sess;
		auto doc = sess.begin(1);
                auto is = new Trinity::Codecs::Lucene::IndexSession("/tmp/TSEGMENTS/1500");

                index_document(doc, "WORLD OF WARCRAFT GAME MISTS OF PANDARIA GAME AND EVEN MORE MIST OF PANDARIAS HELLOW OF MISTS OF MOUNTAINS IPOD MIST OF HILLTOPS"_s32);
															     // 17
		sess.insert(doc);
		sess.commit(is);
		delete is;

#else
                int fd = open("/home/system/Data/BestPrice/SERVICE/clusters.data", O_RDONLY | O_LARGEFILE);

                require(fd != -1);

                const auto fileSize = lseek64(fd, 0, SEEK_END);
                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
                simple_allocator a;
                SegmentIndexSession indexSess;

                close(fd);
                require(fileData != MAP_FAILED);

                Print("INDEXING\n");
                for (const auto *p = static_cast<const uint8_t *>(fileData), *const e = p + fileSize; p != e;)
                {
                        p += sizeof(uint16_t);
                        const auto chunkSize = *(uint32_t *)p;

                        p += sizeof(uint32_t);

                        for (const auto chunkEnd = p + chunkSize; p != chunkEnd;)
                        {
                                //const auto id = *(uint32_t *)p;
                                const auto id = (*(uint32_t *)p) | (1 << 31);
                                p += sizeof(uint32_t);
                                strwlen8_t title((char *)p + 1, *p);
                                p += title.size() + sizeof(uint8_t);
                                ++p;
                                const auto n = *(uint16_t *)p;
                                p += sizeof(uint16_t);

                                p += n * sizeof(uint32_t);

                                //if(id != 2152925656 && id != 2154740801 && id != 2153883399 && id != 2151148486) continue;
                                //if (id != 2152925656) continue;

                                title.p = a.CopyOf(title.data(), title.size());

                                auto d = indexSess.begin(id);
                                uint16_t pos{1};

                                for (const auto *p = title.p, *const e = p + title.size(); p != e;)
                                {
                                        if (const auto len = Text::TermLengthWithEnd(p, e))
                                        {
                                                auto mtp = (char *)p;

                                                for (uint32_t i{0}; i != len; ++i)
                                                        mtp[i] = Buffer::UppercaseISO88597(p[i]);

                                                const strwlen8_t term(p, len);

                                                //if (term.Eq(_S("BREATH")))
                                                {
                                                        uint32_t hit{25121561};

                                                        d.insert(term, pos, {(uint8_t *)&hit, sizeof(uint32_t)});
                                                        //d.insert(term, 0, {});
                                                }

                                                ++pos;

                                                p += len;
                                        }
                                        else
                                        {
                                                if (!isspace(*p))
                                                        pos += 2;
                                                ++p;
                                        }
                                }

                                indexSess.update(d);
                        }
                }
                munmap(fileData, fileSize);

#if 0
                auto is = new Trinity::Codecs::Google::IndexSession("/tmp/TSEGMENTS/100");

                indexSess.commit(is);
#else
                auto is = new Trinity::Codecs::Lucene::IndexSession("/tmp/TSEGMENTS/1500");

                indexSess.commit(is);

#endif
                delete is;
#endif
        }
	else
	{
		std::vector<uint32_t> maskedProducts{6908848};
		IOBuffer maskedProductsBuf;

		pack_updates(maskedProducts, &maskedProductsBuf);
		auto updates = unpack_updates({(uint8_t *)maskedProductsBuf.data(), maskedProductsBuf.size()});
		auto maskedDocsSrc = new TrivialMaskedDocumentsIndexSource(updates);
		IndexSourcesCollection sources;
		Buffer asuc;

#if 0
		{
                        auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/8192");

                        sources.insert(ss);
                        ss->Release();
		}
#else

                if (false)
                {
                        auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/100");

                        sources.insert(ss);
                        ss->Release();
                }

                if (true)
                {
                        auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/1500");
                        sources.insert(ss);
                        ss->Release();
                }

#if 0
		{
			MergeCandidatesCollection collection;
			simple_allocator a;
			std::vector<std::pair<str8_t, term_index_ctx>> outTerms;
			auto outSess = new Trinity::Codecs::Lucene::IndexSession("/tmp/TSEGMENTS/8192/");

			for (const auto it : sources.sources)
			{
				auto ss = (SegmentIndexSource *)it;
				auto tv = ss->segment_terms()->new_terms_view();

                                collection.insert({ss->generation(), tv, ss->access_proxy(), ss->masked_documents()});
                        }

			collection.commit();

			Print("MERGING\n");
			collection.merge(outSess, &a, &outTerms);
			outSess->persist_terms(outTerms);

			std::vector<uint32_t> dids;
			persist_segment(outSess, dids);
			return 0;
		}
#endif
#endif


                //sources.insert(maskedDocsSrc);
		maskedDocsSrc->Release();


		sources.commit();

#if 0
		{
			const auto ud = sources.sources.back()->masked_documents();
			auto reg = masked_documents_registry::make(&ud, 1);

			Print(reg->test(7512491), "\n");
			return 0;
		}
#endif



		auto filter = std::make_unique<MatchedIndexDocumentsFilter>();



#if 1
                struct BPFilter final
                    : public MatchedIndexDocumentsFilter
                {

			// A unique term's hits
                        struct query_term_hits
                        {
                                const term_hit *it;
                                const term_hit *end;
				bool considered; 	// XXX: maybe there is a way to avoid the extra ivar
                        };

			// distinct (query index, toNextSpan) among all matched terms
                        struct tracked
                        {
                                query_term_hits *th;
                                uint16_t index;
				uint8_t toNextSpan;
                                exec_term_id_t termID;
                        };

			// A sequence of span  > 1 that we either accept or reject
                        struct tracked_span
                        {
				uint16_t queryTokenIndex;
                                exec_term_id_t termID;
                                tokenpos_t pos;
                                uint16_t span;
                        };

                        query_term_hits qthStorage[Trinity::Limits::MaxQueryTokens];
                        tracked allTracked[Trinity::Limits::MaxQueryTokens];
                        query_term_hits *allQTH[Trinity::Limits::MaxQueryTokens];
                        std::vector<tracked_span> trackedSpans;


                        void consider_sequence(const tokenpos_t pos, const uint16_t queryIndex, uint16_t *__restrict__ const span)
                        {
                                static constexpr bool trace{true};

                                if (auto adjacent = queryIndicesTerms[queryIndex])
                                {
                                        const auto nextPos = pos + 1;
                                        const auto cnt = adjacent->cnt;

                                        if (trace)
                                                SLog(adjacent->cnt, " adjacent at query index ", queryIndex, "\n");

                                        for (uint32_t i{0}; i != cnt; ++i)
                                        {
						const auto &pair = adjacent->uniques[i];
                                                const auto termID = pair.first;

                                                if (trace)
                                                        SLog("CHECK term ", termID, " AT pos = ", pos, ":", dws->test(termID, pos), " (span = ", *span, ")\n");

                                                if (dws->test(termID, pos))
                                                {
                                                        (*span)++;
                                                        consider_sequence(nextPos, queryIndex + pair.second, span);
                                                }
                                        }
                                }
                                else
                                {
                                        if (trace)
                                                SLog("No queryIndicesTerms[", queryIndex, "]\n");
                                }
                        }

			// TODO: use insertion sort or sorting networks to sort instead of std::sort<>()
			// TODO: demonostrate how instance_struct::rep can be used
			// TODO: demonostrate how position can be used e.g if its a title hit/sequence
                        ConsiderResponse consider(const matched_document &match) override final
                        {
                                //static constexpr bool trace{false};
				const bool trace=match.id == 2152925656;
                                const auto totalMatchedTerms{match.matchedTermsCnt};
                                uint16_t rem{0}, remQTH{0};
				uint32_t score{0};

				// We need to consider all possible combinations of (starting query index, toNextSpan)
				// consider the query: [world of warcraft game is warcraft of blizzard].
				// We have two ocurrences of [of] in the query, and we need to consider
				// [of warcraft...] first, and then [warcraft of..] and then [of blizzard..]
				//
				// If we were to process of occurrences first, then we 'd process
				// [of warcraft ..] and then [of blizzard..] but because we dws->unset() accepted sequences,  if we were
				// to process [of blizzard..] before [warcraft is blizzard] we 'd miss that 3 token sequence.
				//
				// This is also about query tokens where the adjacent token is not found at query token index + 1. e.g
				// [world of (warcraft OR (war craft)) mists  warcraft is a blizzard game]
				// for [warcraft] at index 1, its next logical token is [mists] and its not found at [warcraft] index + 1, whereas
				// for the next [warcraft] token in the query, its next logical token is right next to it ([is]).
				// Trinity::phrase decl.comments
                                for (uint32_t i{0}; i != totalMatchedTerms; ++i)
                                {
                                        const auto mt = match.matchedTerms + i;
                                        const auto qi = mt->queryCtx;
                                        auto p = qthStorage + i;
					const auto instancesCnt = qi->instancesCnt;

                                        p->it = mt->hits->all;
                                        p->end = p->it + mt->hits->freq;
					p->considered = false;
					
					if (trace)
                                        {
                                                SLog("FOR [", qi->term.token, "] ", qi->term.id, "\n");
                                                for (uint32_t i{0}; i != mt->hits->freq; ++i)
                                                {
                                                        Print("POSITION:", mt->hits->all[i].pos, "\n");
                                                }
                                        }

                                        allQTH[remQTH++] = p;
                                        for (uint32_t i{0}; i != instancesCnt; ++i)
                                                allTracked[rem++] = {p, qi->instances[i].index, qi->instances[i].toNextSpan, qi->term.id};
                                }

                                std::sort(allTracked, allTracked + rem, [](const auto &a, const auto &b) {
                                        return a.index < b.index;
                                });

                                if (trace)
                                {
                                        for (uint32_t i{0}; i != rem; ++i)
                                        {
                                                const auto &it = allTracked[i];

                                                SLog("Tracking index = ", it.index, ", toNextSpan = ", it.toNextSpan, ", termID = ", it.termID, "\n");
                                        }
                                }

                                for (;;)
                                {
                                        trackedSpans.clear();

					// It is important that we consider all query tokens
					// and then advance hits iterator for all matched terms afterwards,
					// because e.g for query [world OF warcraft mists OF pandaria]
					// [of] is found twice in the query and we want to check at position X
					// for both [OF pandaria] and [OF warcraft mists..], but we can't do that one term at a time because
					// of reasons described earlier
                                        for (uint32_t i{0}; i < rem;)
                                        {
                                                const auto it = allTracked + i;
						auto th = it->th;

                                                if (unlikely(th->it == th->end))
                                                {
                                                        if (--rem == 0)
                                                                goto l10;
                                                        else
                                                                memmove(it, it + 1, (rem - i) * sizeof(allTracked[0]));
                                                }
                                                else
                                                {
                                                        if (const auto pos = th->it->pos)
                                                        {
                                                                uint16_t span{1};

                                                                if (trace)
								{
                                                                        SLog(ansifmt::bold, ansifmt::color_blue, "START OFF AT index(", it->index, ") pos(", pos, ") term = ", it->termID, ansifmt::reset, "\n");
								}

                                                                consider_sequence(pos + 1, it->index + it->toNextSpan, &span);
                                                                if (span != 1)
                                                                        trackedSpans.push_back({it->index, it->termID, pos, span});
								else
								{
									// XXX:
									// for e.g query [barrack obama]
									// check if obama is found _two_ positions ahead of barrack
									// so that we can e.g boost barrack HUSSEIN obama matches
								}
                                                        }

							if (!th->considered)
							{
								// first termID to consider for this hit (if we have the same term in multiple query indices
								// we need to guard against that)
								th->considered = true;
								//  check payload etc here

								++score;
							}

                                                        ++i;
                                                }
                                        }

                                        std::sort(trackedSpans.begin(), trackedSpans.end(), [](const auto &a, const auto &b) {
						return a.pos < b.pos  || (a.pos == b.pos && b.span < a.span);
                                        });

                                        if (trace)
                                        {
                                                Print("===============================================termID:\n");
                                                for (const auto &it : trackedSpans)
                                                        Print("termID:", it.termID, " ", " pos:", it.pos, " span:", it.span, ", query index:", it.queryTokenIndex, "\n");
                                        }
				
                                        for (const auto *p = trackedSpans.data(), *const e = p + trackedSpans.size(); p != e;)
                                        {
                                                const auto pos = p->pos;
                                                const auto span = p->span;
                                                const auto l = pos + span;

						if (trace)
						{
                                                	Print("ACCEPTING SPAN ", span, " at position ", pos, "\n");
						}

						if (span > 4)
							score+=25;
						else
						{
							static constexpr uint8_t v[] = {0, 0, 5, 8, 14, 20 };

							score+=v[span];
						}




						// this is important
                                                for (uint32_t i{0}; i != span; ++i)
                                                        dws->unset(pos + i);

                                                // Ignore overlaps
                                                for (++p; p != e && p->pos < l; ++p)
                                                        continue;
                                        }





                                        for (uint32_t i{0}; i < remQTH;)
                                        {
                                                if (++allQTH[i]->it == allQTH[i]->end)
                                                {
                                                        if (--remQTH == 0)
                                                                goto l10;
                                                        else
								allQTH[i] = allQTH[remQTH];
                                                }
                                                else
						{
							allQTH[i]->considered = false;
                                                        ++i;
						}
                                        }
                                }


                        l10:;


				if (trace)
					SLog("score ", score, "\n");

				static uint32_t bestDocument{0};
				static uint32_t bestScore{0};

                                if (score > bestScore)
                                {
                                        bestScore = score;
                                        bestDocument = match.id;

                                        SLog("FOR ", bestDocument, " ", bestScore, "\n");
                                }

                                return ConsiderResponse::Continue;
                        }
                };
#endif






                asuc.append(argv[1]);

		for (uint32_t i{0}; i != asuc.size(); ++i)
			asuc.data()[i] = Buffer::UppercaseISO88597(asuc.data()[i]);

		query q(asuc.AsS32());

#if 0
		auto node = ast_node::make(q.allocator, ast_node::Type::BinOp);
		auto specialTokensNode = ast_node::make(q.allocator, ast_node::Type::ConstTrueExpr);

		specialTokensNode->expr = ast_parser("(LEGEND OR ZELDA)"_s32, q.allocator).parse();
		specialTokensNode->set_alltokens_flags(1);

		node->binop.rhs = q.root;
		node->binop.lhs = specialTokensNode;
		node->binop.op = Operator::AND;

		q.root = node;
#endif



		


		
		//exec_query<MatchedIndexDocumentsFilter>(q, &sources);
		exec_query<BPFilter>(q, &sources);
	}

        return 0;
}
#endif


#if 0
int main(int argc, char *argv[])
{
	{
                SegmentIndexSession sess;

                const auto index_document = [](auto &documentSess, const strwlen32_t input) {
			uint32_t pos{1};

                        for (const auto *p = input.data(), *const e = p + input.size(); p != e;)
                        {
                                if (const auto len = Text::TermLengthWithEnd(p, e))
                                {
					documentSess.insert(strwlen8_t(p, len), pos++);
					p+=len;
                                }
                                else
				{
					if (!isblank(*p))
						++pos;
                                        ++p;
				}
                        }
                };

                {
                        auto doc = sess.begin(1);

			index_document(doc, "world of warcraft mists of pandaria is the most successful MMORPG ever created"_s32);
			//index_document(doc, "the"_s32);
                        sess.update(doc);
                }

                {
                        auto doc = sess.begin(2);

			index_document(doc, "lord of the rings the return of the king. an incredible film about hobits, rings and powerful wizards in the mythical middle earth"_s32);
			//index_document(doc, "the"_s32);
                        sess.update(doc);
                }

                auto is = new Trinity::Codecs::Google::IndexSession("/tmp/TSEGMENTS/1/");

                sess.commit(is);

                delete is;
        }


#if 0
	{
		auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/1/");
		auto maskedDocuments = masked_documents_registry::make(nullptr, 0);
		query q("apple");

		exec_query(q, ss, maskedDocuments);

		free(maskedDocuments);
		ss->Release();
	}
#else
	{
		query q(argv[1]);
		IndexSourcesCollection bpIndex;
		auto ss = new SegmentIndexSource("/tmp/TSEGMENTS/1/");

		bpIndex.insert(ss);
		ss->Release();
	
		bpIndex.commit();

		exec_query<MatchedIndexDocumentsFilter>(q, &bpIndex);
	}
#endif

        return 0;
}
#endif


#if 0
int main(int argc, char *argv[])
{
        const auto index_document = [](auto &documentSess, const strwlen32_t input) {
                uint32_t pos{1};

                for (const auto *p = input.data(), *const e = p + input.size(); p != e;)
                {
                        if (const auto len = Text::TermLengthWithEnd(p, e))
                        {
                                documentSess.insert(strwlen8_t(p, len), pos++);
                                p += len;
                        }
                        else
                        {
                                if (!isblank(*p))
                                        ++pos;
                                ++p;
                        }
                }
        };


        {
                SegmentIndexSession sess;

                {
                        auto doc = sess.begin(1);

                        index_document(doc, "world of warcraft mists of pandaria is the most successful MMORPG ever created"_s32);
                        sess.update(doc);
                }

                {
                        auto doc = sess.begin(2);

                        index_document(doc, "lord of the rings the return of the king. an incredible film about hobits, rings and powerful wizards in the mythical middle earth"_s32);
                        sess.update(doc);
                }

                auto is = new Trinity::Codecs::Google::IndexSession("/tmp/TSEGMENTS/1/");

                sess.commit(is);
                delete is;
        }


        {
                SegmentIndexSession sess;

                {
                        auto doc = sess.begin(1);

                        index_document(doc, "world of warcraft mists of pandaria is the most successful MMORPG ever created"_s32);
                        sess.update(doc);
                }


                auto is = new Trinity::Codecs::Google::IndexSession("/tmp/TSEGMENTS/2/");

                sess.commit(is);
                delete is;
        }


	int fd = open("/tmp/TSEGMENTS/1/terms.data", O_RDONLY|O_LARGEFILE);
	require(fd != -1);
	auto fileSize = lseek64(fd, 0, SEEK_END);
	auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	const range_base<const uint8_t *, uint32_t> termsData1((uint8_t *)fileData, uint32_t(fileSize));
	IndexSourcePrefixCompressedTermsView tv1(termsData1);

	fd = open("/tmp/TSEGMENTS/1/index", O_RDONLY|O_LARGEFILE);
	require(fd != -1);
	fileSize = lseek64(fd, 0, SEEK_END);
	fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	auto ap1 = new Trinity::Codecs::Google::AccessProxy("/tmp/TSEGMENTS/1/", (uint8_t *)fileData);

	fd = open("/tmp/TSEGMENTS/2/terms.data", O_RDONLY|O_LARGEFILE);
	require(fd != -1);
	fileSize = lseek64(fd, 0, SEEK_END);
	fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	const range_base<const uint8_t *, uint32_t> termsData2((uint8_t *)fileData, uint32_t(fileSize));
	IndexSourcePrefixCompressedTermsView tv2(termsData2);

	fd = open("/tmp/TSEGMENTS/2/index", O_RDONLY|O_LARGEFILE);
	require(fd != -1);
	fileSize = lseek64(fd, 0, SEEK_END);
	fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	auto ap2 = new Trinity::Codecs::Google::AccessProxy("/tmp/TSEGMENTS/2/", (uint8_t *)fileData);

	
	MergeCandidatesCollection collection;

	collection.insert({1, &tv1, ap1, {}});
	collection.insert({2, &tv2, ap2, {}});
	collection.commit();

	Print("====================================================================\n");

#if 0
	{
		while (!tv1.done())
		{
			Print(tv1.cur().first, "\n");
			tv1.next();
		}

		Print("-- --- -- -- - - - ----\n");
		while (!tv2.done())
		{
			Print(tv2.cur().first, "\n");
			tv2.next();
		}
		return 0;
	}
#endif

	auto is = new Trinity::Codecs::Google::IndexSession("/tmp/TSEGMENTS/3");
	simple_allocator allocator;
	std::vector<std::pair<strwlen8_t, term_index_ctx>> terms;

	collection.merge(is, &allocator, &terms);

	SLog(size_repr(is->indexOut.size()), "\n");



        return 0;
}
#endif


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

	for (const auto &&it : terms_data_view({(uint8_t *)data.data(), data.size()}))
	{
		Print("[", it.first, "] => [", it.second.documents, "]\n");
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
	pack_terms(terms, &data, &index);	  // Took 2.923s to pack 79.01kb 5.74mb 4.06mb (We 'd need about 14MB without prefix compression and varint encoding of term_index_ctx)
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
        auto maskedDocsReg = masked_documents_registry::make(nullptr, 0);

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
		auto maskedDocuments = masked_documents_registry::make(nullptr, 0);

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
	auto maskedDocumentsRegistry = masked_documents_registry::make(nullptr, 0);

	exec_query(q, idxSrc.get(), maskedDocumentsRegistry);


        return 0;
}
#endif
