# Specific fields(terms) updates and parallel indices
Supporting parallel indices/sources in order to, for example, build a gmail like search scheme, where operators such as in:inbox, is:starred may require 
a design that doesn¢t require re-indexing the whole message and those special attributes(terms), is not possible now, but it shouldn¢t be hard to accomplish it.
Specifically, `IndexSource::new_postings_decoder()` is provided both the term  (e.g `is:starred`) and the `term_ctx`, and then if the term is a special term, 
use a different segment or storage path to return a decoder for that to the execution engine.  You should also consider the term in `IndexSource::resolve_term_ctx()`.

So your IndexSource could be just a source that bundles other sources(one for all terms and another for special terms) and checks the term and uses whichever.
This would mean you could index messages and special terms indecently. That¢s trivial. The only problem is with that `IndexSource::masked_documents()` doesn¢t accept a 
term for now, but that¢s easy to change, and that there is a single `masked_documents_registry` per index source. Those should be easy to implement and support though if required.

# Indexing documents without indexing any hits
The included codecs (google and lucene) both ignore hits if (position == 0 and payloadLength == 0) so just invoke new_hit() with position 0 and payload length = 0
and that's all you need
