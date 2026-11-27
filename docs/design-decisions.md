# Design decisions

A running log of the architectural choices behind PolyMorph ETL.

## 1. Python instead of Java
The value of this project is in ETL concepts (keyset pagination,
checkpointing, idempotency, join folding), which are language-independent.
The workload is I/O-bound, so threads are sufficient for concurrency.

## 2. Database-agnostic SQL layer via SQLAlchemy Core
SQL sources and targets are accessed through SQLAlchemy Core, not raw
drivers and not the ORM.

- **Why Core:** connection URLs, schema reflection (tables, columns, foreign
  keys of unknown databases), a common type system and dialect-specific
  upsert constructs.
- **Why not the ORM:** table structures are unknown at coding time.
- **Why not raw drivers:** every new database would need its own code path.
- **Limits:** a new database still needs a small dialect adapter (upsert
  syntax and type mapping). Officially tested: PostgreSQL, MySQL, SQLite.

## 3. Deterministic demo data
The seed generator uses a fixed random seed, so the same data is produced on
every run. This makes tests and checksum-based verification reproducible.
It contains deliberate edge cases: missing profiles, customers without
orders, a value that overflows float64, and "poison" records that the
pipeline must route to a dead-letter queue.

## 4. Non-default host ports for demo databases
PostgreSQL 5433, MySQL 3307 and MongoDB 27018 avoid clashing with databases
already installed on a developer machine.

## 5. YAML manifest with environment-variable secrets
Pipelines are declarative YAML files that can be reviewed in git and re-run
identically. Connection URLs use `${ENV_VAR}` placeholders so credentials
never appear in files, reports or error messages. Validation errors are
formatted manually because Pydantic's default errors echo the offending input,
which could be a secret.

## 6. `validate` never connects to a database
Manifest validation is a pure, fast check that works offline and in CI.
Connectivity checks will be a separate, explicit option.

## 7. Strict manifests
Unknown keys are rejected (`extra="forbid"`) so typos fail loudly instead of
being silently ignored. The manifest `name` must be a slug because it will
name checkpoint files and run folders.


## 8. Schema discovery through SQLAlchemy reflection
`polymorph inspect` uses SQLAlchemy's `Inspector`, so the same code reads
tables, columns, primary keys and foreign keys from any supported database.
Types are shown as the source database spells them (for example `UUID` on
PostgreSQL and `CHAR(32)` on MySQL), which makes cross-database differences
visible before a migration.

## 9. Drivers are optional extras
SQLAlchemy is a core dependency; database drivers are installed per need
(`polymorph-etl[postgres]`, `polymorph-etl[mysql]`). Missing drivers produce
an actionable error message instead of a stack trace.

## 10. Source URLs come from the environment
`inspect` reads `SOURCE_URL` by default. Passwords typed on the command line
end up in shell history, so the environment variable is the recommended path.

## 11. Foreign-key ordering with a topological sort
Tables are listed parents-first using `graphlib.TopologicalSorter`. Self
references are ignored; real cycles are reported as warnings because no safe
load order exists for them.

## 12. Safe-by-default file and database behaviour
Row counts are exact by default (`--no-counts` for huge tables). Table names
are quoted by SQLAlchemy. A mistyped SQLite path is an error instead of a
silently created empty database. Draft manifests never overwrite an existing
file without `--force`, and always use `${ENV_VAR}` placeholders for
credentials.


## 13. Keyset pagination instead of OFFSET
Each batch is `WHERE key > cursor ORDER BY key LIMIT n`. Cost per batch stays
constant, rows are not skipped or duplicated when the source changes during a
run, and the cursor is exactly what a checkpoint needs to store. A benchmark
(`polymorph.demo.pagination_benchmark`) makes the difference measurable.

## 14. One short transaction per batch, no server-side cursor
A streaming cursor would hold one transaction open for the whole migration
and could not be resumed after a crash. Per-batch queries release their
connection before the batch is handed to the consumer.

## 15. Composite and non-integer keys
Composite keys use `a > x OR (a = x AND b > y)` because row-value comparison
is not supported everywhere. Ordering and comparison stay in the database, so
text keys use the database's own collation consistently.

## 16. Fail fast, then stream
`extract_batches` validates (table exists, has a primary key, cursor matches)
and only then returns a generator, so errors surface at the call site rather
than at the first iteration. Tables without a primary key are rejected
explicitly instead of being read unsafely.


## 17. One collection per table, primary key becomes `_id`
Phase 5 copies tables flat. The primary key becomes `_id` and is not repeated
as a field; composite keys become a subdocument whose fields always follow
primary-key order (MongoDB compares subdocument ids field by field). Phase 8
replaces the flat layout with folded documents.

## 18. Idempotent bulk upserts with ReplaceOne
Documents are written with unordered `ReplaceOne(..., upsert=True)`. Replace
(not `$set`) means a re-run always restores the exact source state, with no
stale fields. Unordered because each document is independent.

## 19. Idempotency and checkpointing are different guarantees
Idempotent writes make repeated work harmless (correctness). Checkpoints
(Phase 9) make repeated work unnecessary (speed). Phase 5 delivers the first.

## 20. Pre-flight checks before the first write
Mode, tables, primary keys and the MongoDB connection are all verified before
anything is written, so a configuration mistake never leaves a half-migrated
target. Source and target connections are always released in a `finally` block.

## 21. A deliberately temporary type bridge
PyMongo cannot store `Decimal` or `UUID`, so a minimal bridge converts them
(Decimal128, string). Known gaps (MySQL booleans as 0/1, millisecond
datetimes, silent Decimal128 rounding beyond 34 digits) are documented in
`docs/loading.md` and addressed by the Phase 6 translator.

## 22. Test strategy for MongoDB
Loader logic is unit-tested with a fake collection. End-to-end tests run
against a real MongoDB in throwaway databases and skip automatically when
none is reachable; CI will provide one through Testcontainers (Phase 15).

## 23. Continuous integration from the start of the engine
GitHub Actions runs the full suite on every pull request across Python
3.11-3.13 with a MongoDB service container. In CI a missing MongoDB fails the
build (`POLYMORPH_REQUIRE_MONGO=1`) instead of silently skipping integration
tests, so a green check always means those tests really ran. Jobs get
read-only repository permissions and superseded runs are cancelled. Real
PostgreSQL/MySQL service containers are added with the Phase 15 integration
tests.

## 24. Types are translated from the reflected column type
A translator is planned once per table from the reflected SQL types and then
applied to every row. This replaces the Phase 5 bridge, which guessed from
Python values. The mapping is unit-tested for PostgreSQL and MySQL types
without any database by compiling against SQLAlchemy dialect objects.

## 25. Exactness over convenience
Decimals become Decimal128 and are rejected beyond 34 digits; floats are never
accepted as decimals; integers outside 64 bits are rejected. A wrong or
oversized value stops the run with a clear message rather than being altered.

## 26. The tool never guesses a time zone
A timestamp without a zone is read in `settings.naive_timestamp_timezone`
(default `UTC`) and every such column is announced in the run output.
Sub-millisecond digits are truncated explicitly, because BSON dates hold
milliseconds.

## 27. Overrides for what a type cannot express
`type_overrides` (`table.column: uuid | boolean | string`) tells the tool how
to read a column whose meaning is not visible in its type, such as a
`CHAR(32)` holding UUIDs. Unknown tables or columns are rejected in
pre-flight.

## 28. Type errors and business-rule errors are different
A text value in an integer column is a translation error and stops the run;
the message names the table, column and primary key but never the value, which
may be personal data. Values that are valid for their type but bad for the
business (an invalid email) are handled by transforms and the dead-letter
queue in later phases.

## 29. Transforms run after type translation
Rules receive clean, BSON-safe values. Expressions see `Decimal128` as an
exact `Decimal`, so money arithmetic stays exact, and results are converted
back to BSON before loading.

## 30. Rules are validated three times
The manifest checks the shape of each rule and the syntax of its expressions;
a pre-flight pass checks field names against the real table while tracking
how earlier rules change the field set (a renamed field has only its new
name); the runtime applies the rule to each row. Typos are caught before any
write, with a "did you mean" hint.

## 31. A safe expression language instead of `eval`
Expressions are parsed with `ast`, checked against a whitelist of node types
and functions, and run by a small interpreter. Attribute access, lambdas,
comprehensions, `**`, text repetition and `%` formatting are refused.
Runtime failures report only the exception class, never the data.

## 32. Filtered, rejected and `on_row_error`
A filter drops rows on purpose; a rejection means the row is invalid.
`fail` (default) stops at the first rejection; `skip` counts and continues and
the run exits with code 3 so scripts notice. A batch is written only after
all its rows were processed. Type errors always stop the run; the dead-letter
queue (Phase 10) will record rejected rows.

## 33. Primary keys are not transformable
`_id` and resume cursors depend on them, so rename, drop and in-place
masking, hashing or normalising are refused for primary key columns. A hash
may still be written to a new field. A non-key column named `_id` must be
renamed.

## 34. Privacy defaults
Salted hashes use HMAC-SHA256 with the salt taken from the environment;
unsalted hashes work but are announced. Partial masks never reveal more than
half of a value. Rejection messages contain the table, field, reason and
primary key, never the value.

## 35. Rules are a discriminated union
Each rule is a Pydantic model selected by its `type` key. The manifest, the
pre-flight checks and the pipeline need no change to support a new rule: add a
class with `check()` and `apply()` and list it in the `Rule` union.

## 36. `preview --manifest` as the feedback loop
Showing the exact documents a pipeline would produce makes rules easy to
write and review, and gives the CLI an end-to-end test that needs no MongoDB.

## 37. URL helpers live in their own module
`polymorph.urls` removes a circular import between the manifest (which now
imports the transform rules) and the inspector.

## 38. Folding is declarative and reads the schema
`collections` in the manifest describe the shape (root table, embeds,
`one`/`many`); the join columns come from the foreign keys in the database.
`join: {child: parent}` covers undeclared or ambiguous keys. The key is
called `join` because an unquoted `on` is parsed by YAML as a boolean.

## 39. Children are fetched in batches, not per parent
For each batch of root rows one query per embedded table fetches all children
(`WHERE fk IN (...)`, 500 keys per chunk), which are grouped in memory. A
single SQL JOIN would duplicate parent columns and break keyset `LIMIT`;
per-parent queries are the N+1 problem. Memory per batch is roughly
`batch_size` times the average family size.

## 40. Cardinality is explicit and the shape is predictable
`one` gives an object or `null`, `many` gives an array that is always present
(empty when there are no rows). A `one` relation that the database does not
enforce is announced, and a parent with two matching rows is rejected.

## 41. Documents are all-or-nothing
A rejected embedded row rejects its whole document instead of silently
shipping an incomplete one. The reason names the failing row and the
document; rerunning after fixing the source repairs just that document.
Oversized documents (16 MB) and 1:1 violations are rejected the same way.

## 42. Stable ordering keeps reruns idempotent
Embedded rows are ordered by primary key, so a rerun produces byte-identical
documents and MongoDB reports them unchanged.

## 43. Foreign key columns disappear from embedded rows
The nesting expresses the relationship, so the child's foreign key columns
are removed (`keep_foreign_key` keeps them). Embedded rows keep their primary
key as an ordinary field; only the root's key becomes `_id`.

## 44. A table's rules run after its children are embedded
Rules can therefore read embedded fields (`len(items)`, `profile is not
None`). Filtering an embedded table removes its rows from the array, which
also resolves the Phase 7 "orphaned rows" limitation. The `_id` collision
check applies to root tables only.

## 45. Safety nets against silent data loss
If the database matches a child to a parent but Python's key comparison does
not (case-insensitive collation, padded text), the run stops instead of
dropping the row. Counters are kept per embedded table, so source rows can be
reconciled against documents.

## 46. Write first, checkpoint second
Each batch is written to MongoDB and only then checkpointed. A crash between
the two repeats one batch on resume; idempotent upserts make that harmless
(at-least-once delivery plus idempotency gives an effectively-once result).
The reverse order could silently lose a batch.

## 47. The checkpoint is a SQLite file with atomic transactions
The cursor and the cumulative counters are saved in one transaction, so they
can never disagree and a crash leaves the old or the new state, never a mix.
Standard-library `sqlite3`, one file per pipeline, no new dependency.

## 48. Only interrupted runs leave a checkpoint
A completed run clears its state, so the next run is a full idempotent pass,
which keeps the earlier repair-by-rerun behaviour. Resuming skips collections
that already finished and continues the unfinished one.

## 49. Fingerprints guard against stale progress
Each collection's checkpoint carries a hash of everything that shapes its
documents: structure, transforms (salts only as digests), type overrides,
time zone, the source schema, and the host and database (never credentials)
of source and target. `batch_size` and `on_row_error` are excluded. A mismatch
restarts that collection, which is always safe.

## 50. Counters are checkpointed with the cursor
Without them a resumed run would lose the rejections that happened before the
crash and report the wrong exit code. Counters are cumulative, so the
accounting identity still holds.

## 51. Typed JSON cursors, never pickle
Cursor values are stored with a type tag so UUIDs, decimals and datetimes
come back exactly. Unpickling a file can execute code. Key types that cannot
be saved are detected at planning time; that collection runs without resume.

## 52. An OS file lock instead of a PID file
The lock is released by the operating system when the process dies, even after
SIGKILL, so a crash never leaves a stale lock.

## 53. Fault injection is part of the tool
A hidden `--crash-at STAGE:N` ends the process with `os._exit(137)` without
any cleanup. It makes crash recovery testable and demonstrable (a real
subprocess test, and the Phase 14 demo).


## 54. Rejected documents go to a dead-letter file, not just a counter
Each entry names the rejected document (root table and key) and the cause (the
failing row, which can be an embedded one, its stage and reason). Entries
never contain row data unless `dlq_include_rows` is set, because raw rows hold
unmasked personal data; keys are enough to find the row in the source. Entries
are pure ASCII so poison data can never make writing the file fail.

## 55. Type errors are poison records too
A value that does not fit its column type (a number beyond 64 bits, more than
34 decimal digits) is a data problem, not a schema problem. Under
`on_row_error: skip` it is rejected and dead-lettered instead of stopping a
large migration; under `fail` the run still stops, now after recording the row.

## 56. Exactly-once dead letters through a write-ahead offset
The size of the dead-letter file is saved in the checkpoint. After a crash the
file is truncated to that size and the unfinished batch runs again, so its
entries are written once. Order per batch: documents, dead letters with
`fsync`, checkpoint. This keeps the file as a live, append-only artifact
instead of an export of a database table, and works with and without
checkpoints.

## 57. Offsets form a chain across collections
All collections share one file, so once a collection runs or restarts, the
saved progress of the collections after it no longer fits the file and they
restart too. That is always safe because writes are idempotent, and it avoids
interleaving entries of different attempts.

## 58. The file describes the latest run
A new run starts with an empty file, so fixing the source and re-running
leaves no stale entries. A missing or shortened file during a resume is
refused (`--fresh`) rather than silently producing an incomplete record.

## 59. Checkpoint format v2 with a repeatable migration
`dlq_offset` was added with a migration that checks whether the column exists
before adding it, because DDL is not transactional here and a crash half-way
must leave a file the next start can finish.