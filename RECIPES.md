# Specific fields(terms) updates and parallel indices
Supporting parallel indices/sources in order to, for example, build a gmail like search scheme, where operators such as in:inbox, is:starred may require 
a design that doesn¢t require re-indexing the whole message and those special attributes(terms), is not possible now, but it shouldn¢t be hard to accomplish it.
Specifically, `IndexSource::new_postings_decoder()` is provided both the term  (e.g `is:starred`) and the `term_ctx`, and then if the term is a special term, 
use a different segment or storage path to return a decoder for that to the execution engine.  You should also consider the term in `IndexSource::resolve_term_ctx()`.

So your IndexSource could be just a source that bundles other sources(one for all terms and another for special terms) and checks the term and uses whichever.
This would mean you could index messages and special terms indecently. That's trivial. The only problem is with that `IndexSource::masked_documents()` doesn't accept a 
term for now, but that's easy to account for that, and that there is a single `masked_documents_registry` per index source. Those should be easy to implement and support though if required.


# Indexing documents without indexing any hits
The included codecs (google and lucene) both ignore hits if (position == 0 and payloadLength == 0) so just invoke `new_hit()` with position 0 and payload length = 0
and that's all you need. (Or e.g call `Trinity::SegmentIndexSession::document_proxy::inseert()` with position 0 and empty payload, if you use it). That way you can just match documents without indexing term hits which means the index will be smallert and search will be faster.


# Document IDs in indices in descending order and early abort
Document IDs in posting lists are sorted in ascending order, which is the case for most if not all IR systems. However, you may have specific needs that require ordering the document IDs in descending order like [Twitter](https://blog.twitter.com/2010/twitters-new-search-architecture) does. They cosndider matched documents and as soon as they consider the top(latest) K tweets, the search is aborted. 
You could modify `Trinity::MergeCandidatesCollection::merge()`, the varioud data structures in docidupdates and SegmentIndexSession::commit() ( if you are using the utility SegmentIndexSession ) and the various codecs, if you wish to encode document IDs in that order in the index. You probably don't want to do that though, because there is far simpler way to accomplish it. You use a very simple translation scheme when indexing documents and in you your `consider()` implemntation. 
Specifically, when you want to index a document with id X, you should use
```cpp
const auto translatedDocumentID = std::numeric_limits<Trinity::docid_t>::max() - actualDocumentID;
```
and in your consider() implementation:
```cpp
const auto actualDocumentID = std::numeric_limits<Trinity::docid_t>::max() -  match.id;
```
The highest the document ID, the lowest document ID it will be used for indexing it so it won't require any changes whatsoever. Having implemented this simple translation scheme, your consider() implementation can simply collect documents and stop as soon as it has collected the first K ones by returning `ConsiderResponse::Abort`
Other fancier ordering schemes (e.g based on some static score / document) are also possible.

# Rewriting queries
A query is essentially an AST tree, which means you can easily modify it. If you want to delete a node, you can use `set_dummy()` and next time you normalize the query, it will be GCed. If you want to replace a node, you can replace its value. If you want to replace a run of terms, you can use the handy `query::replace_run()` function to do it.


# Optional stopwords
You can use `ast_node::Type::ConstTrueExpr` nodes. Simply rewrite e.g [legend of zelda breath of the wild] to [legend <of> zelda breath <of the> wild], assuming "of" and "the" are stopwords and you are done.
Note that using those nodes you can construct far more elaborate expressions.

# Attempt to match special tokens only iff the original query matches a document
If we assume you for some reason want all documents that match the original query [apple iphone] but you also want to attempt to match the query [silver OR black OR "jet black"] but only of [apple iphone] matches.
You can rewrite the original query like so `[<silver OR black OR "jet black"> apple iphone]`. For every document that matches [apple iphone] it will try to match [silver OR black OR "jet black"] which may or may not succeed.
This is similar to how you would would do it for making stopwords optional.

# Match multi-token terms only 
Suppose you have a category of documents called "video games", and you want to index the term "video games" to all products found in that category. That is to say, if someone is searching for [video games] or [video games skyrim] (assuming there is a document with the term Skyrim in video games), you want to match the documents in the video games category, but NOT if someone is searching for [video] or [video skyrim]. You want to match a multi-token term. There are two ways to accomplish that, one of which involves special 'compound' terms and filtering of their hits in a match method, which will not be described here(that's the way it was done with previous Trinity generations, but this other alternative described here is more elegant and doesn't bloat the index with compound terms.
You designate a special separator character (e.g '|') and then use it to construct the composite term by joining its tokens with that character. For example, for video games you get video|games. Then, when you are pre-processing a query [video games skyrim] you identify all possible composite terms in `term runs` and collect them. For this example, those could be:
- video|games
- video|games|skyrim
- games|skyrim
and then you rewrite the `term run` as an expression like so(assuming your composite terms can be upto 3 tokens in size):

video AND (video|games OR (video games)

video
video|games
video|games|skyrim
games|skyrim



video|games OR (video games) skyrim
video games|video OR (games video) 

video|games|skyrim OR (video games skyrim)






