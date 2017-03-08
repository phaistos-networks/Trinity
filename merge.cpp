#include "merge.h"
#include <text.h>
#include "docwordspace.h"

void Trinity::MergeCandidatesCollection::commit()
{
	std::sort(candidates.begin(), candidates.end(), [](const auto &a, const auto &b)
	{
		return b.gen < a.gen;
	});

	map.clear();
	all.clear();

	for (const auto &c : candidates)
	{
		const auto &ud = c.maskedDocuments;

		map.push_back({c, all.size()});
		if (ud)
			all.push_back(ud);
	}
}

Trinity::dids_scanner_registry *Trinity::MergeCandidatesCollection::scanner_registry_for(const uint16_t idx)
{
	const auto n = map[idx].second;
	auto res = dids_scanner_registry::make(all.data(), n);

	return res;
}

// Make sure you have commited first
void Trinity::MergeCandidatesCollection::merge(Trinity::Codecs::IndexSession *is, simple_allocator *allocator, std::vector<std::pair<strwlen8_t, Trinity::term_index_ctx>> *const terms)
{
	struct tracked_candidate
	{
		uint16_t idx;
		merge_candidate candidate;
	};

        std::vector<tracked_candidate> all_;
        uint16_t rem{uint16_t(candidates.size())};


        for (uint16_t i{0}; i != rem; ++i)
        {
                if (false == candidates[i].terms->done())
                {
                        all_.push_back({i, candidates[i]});
                }
        }

        if (all_.empty())
                return;

        uint16_t toAdvance[128];
        auto all = all_.data();
        const auto isCODEC = is->codec_identifier();
	DocWordsSpace dws{65536}; // dummy, for materialize_hits()
	size_t termHitsCapacity{0};
	term_hit *termHitsStorage{nullptr};
	std::vector<std::pair<Trinity::Codecs::AccessProxy *, range32_t>> mergePairs;
	std::vector<Trinity::Codecs::Decoder *> decodersV;

        Defer(
            {
                    if (termHitsStorage)
                            std::free(termHitsStorage);
            });

        for (;;)
        {
                uint8_t toAdvanceCnt{1};
                const auto pair = all[0].candidate.terms->cur();
                auto selected{pair};
		auto codec = all[0].candidate.ap->codec_identifier();
		bool sameCODEC{true};


                toAdvance[0] = 0;
                for (uint16_t i{1}; i != rem; ++i)
                {
                        const auto pair = all[i].candidate.terms->cur();
                        const auto r = Text::StrnncasecmpISO88597(pair.first.data(), pair.first.size(), selected.first.data(), selected.first.size());

                        if (r < 0)
                        {
                                toAdvanceCnt = 1;
                                toAdvance[0] = i;
                                selected = pair;
				sameCODEC = true;
				codec = all[i].candidate.ap->codec_identifier();
                        }
                        else if (r == 0)
			{
				if (sameCODEC)
                                {
                                        auto c = all[i].candidate.ap->codec_identifier();

                                        if (c != codec)
                                                sameCODEC = false;
                                }

                                toAdvance[toAdvanceCnt++] = i;
			}
                }

		const strwlen8_t outTerm(allocator->CopyOf(selected.first.data(), selected.first.size()), selected.first.size());
		[[maybe_unused]] const bool fastPath = sameCODEC && codec == isCODEC;

		if (toAdvanceCnt == 1)
		{
                        auto c = all[toAdvance[0]].candidate;

                        if (fastPath)
			{
				const auto chunk = is->append_index_chunk(c.ap, selected.second);
				
				terms->push_back({outTerm, {selected.second.documents, chunk}});
			}
			else
			{
				term_index_ctx tctx;
				std::unique_ptr<Trinity::Codecs::Encoder> enc(is->new_encoder(is));
				std::unique_ptr<Trinity::Codecs::Decoder> dec(c.ap->new_decoder(selected.second, c.ap));

				dec->begin();
				enc->begin_term();

				do
				{
					const auto freq = dec->cur_doc_freq();
					const auto docID = dec->cur_doc_id();

					// TODO: check if docID is masked

					if (freq > termHitsCapacity)
					{
						if (termHitsStorage)
							std::free(termHitsStorage);
						termHitsCapacity = freq + 128;
						termHitsStorage = (term_hit *)malloc(sizeof(term_hit) * termHitsCapacity);
					}


					enc->begin_document(docID, freq);
					dec->materialize_hits(1 /* dummy */, &dws /* dummy */, termHitsStorage);

					for (uint32_t i{0}; i != freq; ++i)
					{
						const auto &th = termHitsStorage[i];
						const auto bytes = (uint8_t *)&th.payload;

						enc->new_hit(th.pos, {bytes, th.payloadLen});
					}

					enc->end_document();

				} while (dec->next());

				enc->end_term(&tctx);
				terms->push_back({outTerm, tctx});
			}
		}
		else
		{
			if (sameCODEC)
			{
				std::unique_ptr<Trinity::Codecs::Encoder> enc(is->new_encoder(is));
				term_index_ctx tctx;

				mergePairs.clear();
				for (uint16_t i{0}; i != toAdvanceCnt; ++i)
				{
					const auto idx = toAdvance[i];

                                        mergePairs.push_back({all[idx].candidate.ap, all[idx].candidate.terms->cur().second.indexChunk});
                                }

				enc->begin_term();
				is->merge(mergePairs.data(), mergePairs.size(), enc.get(), nullptr);
				enc->end_term(&tctx);
				terms->push_back({outTerm, tctx});
			}
			else
			{
				// this is tricky now
				// we got to merge-sort across different codecs and output to an encoder of a differen codec
				term_index_ctx tctx;
				std::unique_ptr<Trinity::Codecs::Encoder> enc(is->new_encoder(is));

				for (uint16_t i{0}; i != toAdvanceCnt; ++i)
				{
					const auto idx = toAdvance[i];
					auto ap = all[idx].candidate.ap;
					auto dec = ap->new_decoder(all[idx].candidate.terms->cur().second, ap);
					
					dec->begin();
					decodersV.push_back(dec);
				}

				uint16_t rem{toAdvanceCnt};
				auto decoders = decodersV.data();
				uint16_t toAdvance[128]; 

				require(sizeof_array(toAdvance) <= decodersV.size());

				enc->begin_term();

				for (;;)
				{
					uint16_t toAdvanceCnt{1};
					uint32_t lowestDID = decoders[0]->cur_doc_id();

					for (uint16_t i{1}; i != rem; ++i)
					{
						const auto id = decoders[i]->cur_doc_id();

						if (id < lowestDID)
						{
							lowestDID = id;
							toAdvanceCnt = 1;
							toAdvance[0] = i;
						}
						else if (id == lowestDID)
							toAdvance[toAdvanceCnt++] = i;
					}


					// always choose the first because they are always sorted by gen DESC
					auto dec = decoders[toAdvance[0]];
					const auto freq = dec->cur_doc_freq();

					if (freq > termHitsCapacity)
					{
						if (termHitsStorage)
							std::free(termHitsStorage);
						termHitsCapacity = freq + 128;
						termHitsStorage = (term_hit *)malloc(sizeof(term_hit) * termHitsCapacity);
					}

					enc->begin_document(lowestDID, freq);
					dec->materialize_hits(1 /* dummy */, &dws /* dummy */, termHitsStorage);

					for (uint32_t i{0}; i != freq; ++i)
					{
						const auto &th = termHitsStorage[i];
						const auto bytes = (uint8_t *)&th.payload;

						enc->new_hit(th.pos, {bytes, th.payloadLen});
					}
					enc->end_document();



					do
					{
						const auto idx = toAdvance[--toAdvanceCnt];
						auto dec = decoders[idx];

						if (!dec->next())
						{
							delete dec;
							if (!--rem)
								goto l10;

							memmove(decoders + idx, decoders + idx + 1, (rem - idx) * sizeof(decoders[0]));
						}
					} while (toAdvanceCnt);
				}

l10:
				decodersV.clear();
				enc->end_term(&tctx);
				terms->push_back({outTerm, tctx});
			}
		}



                do
                {
                        const auto idx = toAdvance[--toAdvanceCnt];
                        auto terms = all[idx].candidate.terms;

                        terms->next();
                        if (terms->done())
                        {
                                if (!--rem)
                                        goto l1;

                                memmove(all + idx, all + idx + 1, (rem - idx) * sizeof(all[0]));
                        }
                } while (toAdvanceCnt);
        }
l1:;
}
