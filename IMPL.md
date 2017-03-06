Each segment is a self-contained index. It has its own terms dictionary(i.e there is no real integer termID, certainly not one shared among different segments), and through the use of Codecs different implementations can be used that make different tradeoffs and are more suitable than other for specific use cases.

There are however logical, transient termIDs in use during indexing (see indexer_session) and during executin (see segments and exec). They facilitate and simplify indexing and execution, but the IDs are transient, session specific and are meaningless otherwise, and not persisted anywhere.


## Term ID spaces

### Indexer Session
Transient Term to ID association that faciliates indexing. They are not persisted at all and are meaningless outside that process

### Segment for execution
For each distinct term resolved(in 1+ queries used in 1+ executions), we assign it a unique integer ID that's meaningless outside the segment space and are used to facilitate execution.
A `segment` instance can be reused to execute multiple queries, so caching the results of term resolution is beneficial. 


### Runtime context for execution
Each exec() calls configures and uses its own `runtime_ctx`. For each such `runtime_ctx`, we assign a distinct indexer ID of type `exec_term_id_t` which maps from runtime_ctx space to segment space.
We could have used the segment space, but we really need to make sure the maximum distinct term ID in the context of the execution is low.


### Score context
When scoring docuemnts, runtime context is provided and list of all matched terms. Those terms are referenced by runtime-local `exec_term_id_t` identifiers. You can use the varous runtime_ctx methods to
access e.g all positions and hits for that term, etc

## Fields
This implementation does not support different fields/segment, like Lucene does, because in practice we have no need for such feature, and implementing it would only complicate things.
The one possible future use-case would be to support e.g GMail like indexing where wethere a message is starred or the folder it is in likely is likely indexed and updated indepentently from the message content index where
they e.g either use special index/segment/decoder for e.g "is:starred" tokens or something like that we can't currently support, but we never needed that anyway and we better not implement support for it until and if we do
Lucene simply inclueds the field num/id in the terms dictionary along with the actual term, so that term lookups take into account the field as well. There is no special index / field or anything like that.
It should be quite easy to support this as well in the future. See [https://lucene.apache.org/core/2_9_4/fileformats.html#Term Dictionary]()

