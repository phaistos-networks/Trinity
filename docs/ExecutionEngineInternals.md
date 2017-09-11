This is an overview of some of the main concepts particular to Trinity¢s query execution engine internals and design. Not all concepts are described, but if you need information, I advise you to study the codebase. Most data structures and algorithms are documented.


## Relevant document providers
`relevant_document_provider` is a very simple structure, with only a single abstract/virtual method, `document()`. It is an interface to ¡something¢ that provides a matched document, and possibly a score.


## Iterators
Iterators are driving the execution engine. Each iterator has two simple methods. `next()` and `advance()`. There are quite a few Iterator subclasses, including `Codecs::PostingsListIterator` which is used to iterate postings lists, `Conjuction` and `Disunction` for iterating sets of iterators, etc.   

If you are going to create new Iterator subclasses, please make sure that such subclasses cannot advance themselves or any of their sub-iterators in e.g the Iterator sub-class constructors. This is because Docs Sets expect all iterators they manage to be reset, i.e not having advanced to a document when passed to their constructor.

Iterators subclass `relevant_document_provider`

## Docset span
Instead of accessing an index source¢s documents directly using Iterators, using a DocsSetSpan makes more sense, because they provide additional functionality and using them often results in much faster access to the matched documents, than using the Iterators directly.

There is only one virtual method subclasses implement, `process()`, and it accepts a range of IDs - which in turns allows for some very interesting designs. 
It also requires a `MatchesProxy` pointer, which is a simple structure with a single virtual method `process()`. Every DocsSetSpan process() implementation will invoke that for each matched document passing it a `relevant_document_provider *`.

Please refer to the comments and code in `docset_spans.h` for information.

The execution engine (except for when the query is a single-term query) builds iterators, and then builds docset spans from them, and operates on them.

If you create a DocsSetSpan subclass, you may want to advance the iterators provided in the constructor to keep the impl. simpler, if it makes sense. Some of the DocsSetSpan subclasses do so. 

## Similarity
Trinity supports 3 different query execution modes. The default is more expensive than the other two, but also far more powerful and by providing rich match information to the callback, facilitates computing very high quality relevancy data models, which is not possible with other two executions modes. 

One of the other two execution modes is selected by passing `ExecFlags::DisregardTokenFlagsForQueryIndicesTerms` to `Trinity::exec_query_par()`. Please see comments of ExecFlags and `exec_query_par()` for more.
This mode matches Lucene¢s default and only scoring semantics, where it simply aggregates a score for all matched iterators. When you invoke `exec_query_par()`, you can provide a `IndexSourcesCollectionScorer()` instance, which is going to be used to drive the scoring scheme. Effectively, it will create a new `IndexSourceScorer` for each index source involved in the query execution, and that in turn will eventually provide a score for all terms and phrases of the query. This score is used by `IteratorScorer` subclasses -- please see `docset_iterators_scorers.cpp`. 
All told, it is somewhat complex, but it¢s designed for flexility and expressiveness. With those APIs you can build your own scoring models (`Trinity::default_wrapper()` provides a default IteratorScorer for each Iterator, depending on its type, but in the future you will be able to override that and provide your own), and by providing a suitable IndexSourcesCollectionScorer, you will control the score of terms in a document (TF-IDF and BM25 are already implemented so you may just want to use those).

Please note that while Lucene is Scorer centric -- a lucene Scorer either creates and owns an Iterator, or wraps one, Trinity is Iterator centric; the Iterator is the main construct, and it may and is used on its own. It may include a scorer which is set by the execution engine. This distinction is important; Trinity¢s default query execution mode is not based on scores aggregation, unlike with Lucene where it is the only execution mode, so this decision makes sense here.
