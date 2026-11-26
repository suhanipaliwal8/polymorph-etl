# Loading into MongoDB

## Layout (flat copy)
Each source table becomes one collection with the same name; each row becomes
one document. Folding related tables into nested documents is Phase 8.
Related tables can be folded into one document with `collections`; the
embedded rows are translated and transformed like any other row. See
[`folding.md`](folding.md).

## The `_id` field
- Single-column primary key: `_id` is that value. The key column is not
  repeated as a normal field.
- Composite primary key: `_id` is a subdocument `{col_a: ..., col_b: ...}`
  with fields **always in primary-key order**, because MongoDB compares
  subdocument ids field by field in order.
- A non-key source column named `_id` is rejected (it would collide).

## Idempotent writes
Documents are written with `ReplaceOne({"_id": id}, document, upsert=True)`
in unordered bulk operations.

- **Replace, not `$set`**: a re-run makes the document identical to the source
  again; stale fields from earlier runs cannot survive.
- **Unordered**: every document is independent, so one failure does not block
  the others and the server may parallelise.
- Running a pipeline twice produces the same collections as running it once.

## Idempotency is not checkpointing
Idempotent writes guarantee that repeating work is **harmless**. Checkpoints
(Phase 9) make repeating work **unnecessary**. After a crash today the run
starts from the beginning, but no duplicate documents appear.

## Pre-flight checks
Before the first write the pipeline verifies that: the mode is supported,
every table exists and has a primary key, and MongoDB is reachable and the
target URL names a database. A failure here changes nothing in the target.

## Types
Values are converted by the schema-driven translator described in
[`type-translation.md`](type-translation.md). The loader receives values that
are already BSON-safe and only builds documents.

## Limits
- A MongoDB document cannot exceed 16 MB.
- Rows are copied as they are: poison records included. Transformations
  (Phase 7) and the dead-letter queue (Phase 10) deal with them.