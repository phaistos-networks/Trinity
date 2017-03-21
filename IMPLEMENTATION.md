# Query normalization and optimization
We need to implement some query optimiations (either directly on the AST or in the resulted exec nodes tree) multiple times, some in queries.cpp and others in exec.cpp, which is innevitable because
the execution engine has information not available during parsing. There are many opportunities for optimizations and it is important that we get this right and that we continously look for
more ideas, because query processing is almost all about the cost of evaluating the execution nodes tree.
See:
- `normalize_root()`
- `reorder_root()`
- `reorder_execnode()`
- `compile_node()`
- `expand_node()`
`normalize_root()` will not optimize the query per-se nor will it reorder it; it will identify logical issues and the final query will 'make sense'. This is important because a query
can be used for other purproses other than for execution by the execution engine (e.g you could use it for something else in your application).
When you are passing a query to the executione engine, it creates a copy of it, and that may be updated by `reorder_root()` before it is compiled and the resulting exec nodes tree may be restructured and optimised.

It's easy to extend and enhance the optimiser and parser though, so whenever we identify opportunities to do so, it should be trivial to implement them.

