#include "segments.h"
#include "google_codec.h"

using namespace Trinity;

Trinity::Codecs::Decoder *Trinity::segment::new_postings_decoder(const term_segment_ctx ctx)
{
	SLog("Creating new decoder for ", ctx.documents, " ", ctx.chunkSize, " ", ctx.offsets[0], "\n");

	return accessProxy->decoder(ctx, accessProxy.get(), nullptr);
}
