# Test data

`nation.impala.parquet` is taken from the [parquet-compatibility](https://github.com/Parquet/parquet-compatibility)
repository (`parquet-testdata/impala/1.1.1-NONE/`), which is licensed under the
Apache License 2.0.

It is written by Impala 1.1.1 and predates the UTF8 logical type annotation, so its
`n_name` and `n_comment` columns are plain byte arrays rather than annotated strings.
That makes it a useful fixture: it exercises the byte-array-to-`String` conversion in
`ParquetFiles._materialize`, and it is a file produced by a foreign writer rather than
one round-tripped through the Julia parquet stack.
