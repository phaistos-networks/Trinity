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

std::unique_ptr<Trinity::masked_documents_registry> Trinity::MergeCandidatesCollection::scanner_registry_for(const uint16_t idx)
{
	const auto n = map[idx].second;

	return masked_documents_registry::make(all.data(), n);
}

// Make sure you have commited first
// Unlike with e.g SegmentIndexSession where the order of postlists in the index is based on our translation(term=>integer id) and the ascending order of that id
// here the order will match the order the terms are found in `tersm`, because we perform a merge-sort and so we process terms in lexicograpphic order
void Trinity::MergeCandidatesCollection::merge(Trinity::Codecs::IndexSession *is, simple_allocator *allocator, std::vector<std::pair<str8_t, Trinity::term_index_ctx>> *const terms)
{
	static constexpr bool trace{false};

	struct tracked_candidate
	{
		uint16_t idx;
		merge_candidate candidate;
	};

        std::vector<tracked_candidate> all_;
        uint16_t rem{uint16_t(candidates.size())};

	if (trace)
		SLog("Merging ", candidates.size(), " candidates\n");

        for (uint16_t i{0}; i != rem; ++i)
        {
		if (trace)
			SLog("Candidate ", i, " ", candidates[i].gen, "\n");

                if (false == candidates[i].terms->done())
                        all_.push_back({i, candidates[i]});
        }

        if (all_.empty())
                return;

        uint16_t toAdvance[all_.size()];
        auto all = all_.data();
        const auto isCODEC = is->codec_identifier();
	DocWordsSpace dws{Limits::MaxPosition}; // dummy, for materialize_hits()
	size_t termHitsCapacity{0};
	term_hit *termHitsStorage{nullptr};
	std::vector<Trinity::Codecs::IndexSession::merge_participant> mergeParticipants;
	std::vector<std::pair<Trinity::Codecs::Decoder *, masked_documents_registry *>> decodersV;

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
                        const auto r = terms_cmp(pair.first.data(), pair.first.size(), selected.first.data(), selected.first.size());

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

		if (trace)
			SLog("TERM [", selected.first, "], toAdvanceCnt = ", toAdvanceCnt, ", sameCODEC = ", sameCODEC, ", first = ", toAdvance[0], "\n");

		const str8_t outTerm(allocator->CopyOf(selected.first.data(), selected.first.size()), selected.first.size());
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
				std::unique_ptr<Trinity::Codecs::Encoder> enc(is->new_encoder());
				std::unique_ptr<Trinity::Codecs::Decoder> dec(c.ap->new_decoder(selected.second));
				auto maskedDocsReg = scanner_registry_for(all[toAdvance[0]].idx);

				dec->begin();
				enc->begin_term();

				do
				{
					const auto docID = dec->curDocument.id;
					const auto freq = dec->curDocument.freq;

					if (!maskedDocsReg->test(docID))
                                        {
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
                                        }

                                } while (dec->next());

				enc->end_term(&tctx);
				terms->push_back({outTerm, tctx});
			}
		}
		else
		{
			if (sameCODEC)
			{
				std::unique_ptr<Trinity::Codecs::Encoder> enc(is->new_encoder());
				term_index_ctx tctx;

				mergeParticipants.clear();
				for (uint16_t i{0}; i != toAdvanceCnt; ++i)
				{
					const auto idx = toAdvance[i];

					mergeParticipants.push_back(
					{
						all[idx].candidate.ap,
						all[idx].candidate.terms->cur().second,
						scanner_registry_for(all[idx].idx).release()
					});
                                }

				enc->begin_term();
				is->merge(mergeParticipants.data(), mergeParticipants.size(), enc.get());
				enc->end_term(&tctx);
				terms->push_back({outTerm, tctx});

				for (uint16_t i{0}; i != toAdvanceCnt; ++i)
					delete mergeParticipants[i].maskedDocsReg;
			}
			else
			{
				// we got to merge-sort across different codecs and output to an encoder of a different, potentially, codec
				term_index_ctx tctx;
				std::unique_ptr<Trinity::Codecs::Encoder> enc(is->new_encoder());

				for (uint16_t i{0}; i != toAdvanceCnt; ++i)
				{
					const auto idx = toAdvance[i];
					auto ap = all[idx].candidate.ap;
					auto dec = ap->new_decoder(all[idx].candidate.terms->cur().second);
					auto reg = scanner_registry_for(all[idx].idx).release();
					
					require(reg);
					dec->begin();
					decodersV.push_back({dec, reg});
				}

				uint16_t rem{toAdvanceCnt};
				auto decoders = decodersV.data();
				uint16_t toAdvance[128]; 

				require(sizeof_array(toAdvance) >= decodersV.size());

				enc->begin_term();

				for (;;)
				{
					uint16_t toAdvanceCnt{1};
					auto lowestDID = decoders[0].first->curDocument.id;

					toAdvance[0] = 0;
					for (uint16_t i{1}; i != rem; ++i)
					{
						const auto id = decoders[i].first->curDocument.id;

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
					if (!decoders[toAdvance[0]].second->test(lowestDID))
                                        {
                                                auto dec = decoders[toAdvance[0]].first;
                                                const auto freq = dec->curDocument.freq;

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
                                        }

                                        do
					{
						const auto idx = toAdvance[--toAdvanceCnt];
						auto dec = decoders[idx].first;

						if (!dec->next())
						{
							delete dec;
							delete decoders[idx].second;

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
