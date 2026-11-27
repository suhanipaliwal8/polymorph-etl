# PolyMorph ETL

[![CI](https://github.com/suhanipaliwal8/polymorph-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/suhanipaliwal8/polymorph-etl/actions/workflows/ci.yml)

Schema-aware CLI ETL and migration engine that extracts relational data,
reshapes it, and loads it into denormalized document stores with
crash-safe precision.

> **Status:** 🚧 Under active development. Built phase by phase.

## Goals

- Declarative YAML pipelines (`sql-to-nosql`, `sql-to-sql`)
- In-flight transformations (rename, mask, hash, filter, computed fields)
- Join folding: customers + orders + order_items into nested documents
- Lossless type translation (e.g. `DECIMAL` to `Decimal128`)
- Memory-safe keyset streaming with concurrent workers
- Crash-resilient checkpointing with idempotent upserts
- Dead-letter queue and SHA-256 reconciliation audit
- SQL-to-SQL migration across database engines (PostgreSQL, MySQL, SQLite;
  extensible via dialect adapters)

## Roadmap

- [x] Phase 0: Project skeleton
- [x] Phase 1: Demo databases (Docker Compose + seed data)
- [x] Phase 2: CLI and pipeline manifest (`validate`)
- [x] Phase 3: Schema discovery (`inspect`) and draft manifests
- [x] Phase 4: Keyset streaming extractor
- [x] Phase 5: MongoDB bulk loader and first end-to-end run
- [x] Phase 6: Type and precision translator
- [x] Phase 7: Transform engine
- [x] Phase 8: Join folding
- [x] Phase 9: Checkpointing and crash resume
- [x] Phase 10: Dead-letter queue
- [ ] Phase 11: Concurrency
- [ ] Phase 12: Verification audit, run reports and dry run
- [ ] Phase 13: SQL-to-SQL for any supported database
- [ ] Phase 14: One-command demo
- [ ] Phase 15: Integration tests against real PostgreSQL and MySQL in CI, final polish

## Demo databases

Requires Docker Desktop and Python 3.11+.

```bash
# 1. Install the project with dev and demo dependencies
pip install -e ".[dev,demo]"

# 2. Start PostgreSQL (5433), MySQL (3307) and MongoDB (27018)
docker compose up -d
docker compose ps          # wait until all are "healthy"

# 3. Seed the source database with deterministic demo data
python -m polymorph.demo.seed --db postgres

# Stop everything (add -v to also delete the data)
docker compose down
```

The seed data includes a 1:1 relation (`customers` to `customer_profiles`),
1:N relations (`customers` to `orders` to `order_items`), a high-precision
`DECIMAL(19,4)` value, and deliberate poison records used to exercise the
dead-letter queue.

## Usage

Pipelines are described in a YAML manifest. Connection URLs are read from
environment variables so credentials never live in the file.

```bash
export SOURCE_URL="postgresql+psycopg://polymorph:polymorph@localhost:5433/shop"
export TARGET_URL="mongodb://localhost:27018/shop"

polymorph --help
polymorph validate examples/pipelines/postgres-to-mongo.yaml
```

### Inspect a source database

```bash
export SOURCE_URL="postgresql+psycopg://polymorph:polymorph@localhost:5433/shop"

# Engine, tables (in foreign-key order), columns, keys, row counts, warnings
polymorph inspect

# Generate a draft pipeline manifest to review and edit
polymorph inspect --generate-manifest pipeline.yaml --mode sql-to-nosql
polymorph validate pipeline.yaml
```

`inspect` is read-only. Use `--no-counts` to skip row counts on very large
tables and `--schema` to inspect a non-default schema.

Database drivers are optional extras: `pip install "polymorph-etl[postgres]"`
or `"polymorph-etl[mysql]"`. See
[`docs/manifest-reference.md`](docs/manifest-reference.md) for the manifest
format and connection URL examples.

Example manifests live in [`examples/pipelines/`](examples/pipelines/).
See [`docs/workflow-and-outputs.md`](docs/workflow-and-outputs.md) for the
planned end-to-end workflow, run reports and exit codes.

Special characters in passwords must be URL-encoded inside connection URLs
(for example `@` becomes `%40`).

### Preview source data

```bash
export SOURCE_URL="postgresql+psycopg://polymorph:polymorph@localhost:5433/shop"

# First rows of a table as JSON lines (progress goes to stderr)
polymorph preview customers --limit 5

# See the batching: 25 rows read in batches of 10
polymorph preview customers --limit 25 --batch-size 10
```

Tables are read with **keyset pagination** (`WHERE id > last_seen ORDER BY id
LIMIT n`) instead of `OFFSET`, so every batch costs the same and reading can
resume from the last key. Compare the two strategies yourself:

```bash
python -m polymorph.demo.pagination_benchmark --rows 500000
```

See [`docs/extraction.md`](docs/extraction.md) for the design and limitations.

### Run a migration

```bash
export SOURCE_URL="postgresql+psycopg://polymorph:polymorph@localhost:5433/shop"
export TARGET_URL="mongodb://localhost:27018/shop"

polymorph run examples/pipelines/postgres-to-mongo.yaml
```

```
TABLE                          READ  CREATED  UPDATED  UNCHANGED     TIME
customers                       200      200        0          0    0.10s
...
```

Each table becomes a collection and its primary key becomes `_id`. Writes are
**idempotent** (bulk `ReplaceOne` upserts): running the same pipeline twice
creates no duplicates, and a re-run repairs deleted or stale documents.
All checks (tables exist, primary keys, MongoDB reachable) happen before the
first write.

Current limits: only `sql-to-nosql`, flat collections, no transformations,
and a minimal type bridge. See [`docs/loading.md`](docs/loading.md).

Tests that need MongoDB are marked `integration` and are skipped when it is
not running: `pytest -m "not integration"` runs only the fast ones.

Current limits: only `sql-to-nosql`, flat collections and no transformations.

### Type translation

Column types are mapped by the **reflected SQL type**, not guessed:
`DECIMAL` becomes `Decimal128` (exact, never a float), timestamps become UTC
dates, UUIDs become canonical strings, `TINYINT(1)` becomes a boolean, and
values that do not fit their column type stop the run with the table, column
and primary key of the row.

Timestamps without a time zone (for example MySQL `DATETIME`) are read in
`settings.naive_timestamp_timezone` (default `UTC`), and a hint such as
`type_overrides: {customers.external_id: uuid}` teaches the tool what a type
alone cannot say. The same data migrated from PostgreSQL and from MySQL
produces identical MongoDB documents. See
[`docs/type-translation.md`](docs/type-translation.md).

### Transform rows

Declarative rules run between type translation and loading: `rename`,
`drop`, `normalize`, `mask`, `hash`, `compute`, `filter` and `require`.

```yaml
settings:
  on_row_error: skip        # fail (default) | skip

transforms:
  customers:
    - type: require
      expression: email is not None
      message: customer email is required
    - type: hash
      field: email
      into: email_hash
      salt: ${HASH_SALT}
    - type: mask
      field: email
      style: email          # asha@example.com -> a***@***.com
```

```bash
export HASH_SALT="a-long-secret"

# See the documents MongoDB would receive, without writing anything
polymorph preview customers --manifest examples/pipelines/postgres-to-mongo-masked.yaml

polymorph run examples/pipelines/postgres-to-mongo-masked.yaml
```

Rows a `filter` drops are **filtered**; invalid rows are **rejected**. With
`on_row_error: skip` rejected rows are counted and the run exits with code
**3**; by default the run stops at the first one. Every row is accounted for:
`read = filtered + rejected + created + updated + unchanged`.

Expressions use a small, safe language (no `eval`). See
[`docs/transforms.md`](docs/transforms.md) for every rule, the expression
reference and the limits.

### Fold related tables into documents

Declare a collection and the tool reads the foreign keys from the database to
build **one document per root row**, with related rows embedded:

```yaml
collections:
  customers:
    from: customers
    embed:
      - table: customer_profiles
        as: profile
        relation: one         # object, or null
      - table: orders
        as: orders
        relation: many        # array (empty when there are none)
        embed:
          - table: order_items
            as: items
            relation: many
```

```bash
polymorph preview customers --manifest examples/pipelines/postgres-to-mongo-folded.yaml
polymorph run examples/pipelines/postgres-to-mongo-folded.yaml
```

```
COLLECTION                     READ  FILTERED  REJECTED  CREATED  UPDATED  UNCHANGED     TIME
customers                       200         0         1      199        0          0    0.40s
  + customer_profiles           180         0         0
  + orders                      410         0         0
    + order_items             1,259         0         1
```

Children are fetched per batch with `WHERE fk IN (...)` (no N+1 queries) and
ordered by primary key, so reruns are idempotent. Documents are
all-or-nothing: a rejected embedded row rejects its document. Rules see
embedded fields (`len(orders)`). See [`docs/folding.md`](docs/folding.md).

### Crash safety: checkpoints and resume

Progress is saved after every batch (a SQLite file in `.polymorph/`). If a run
is killed or fails, run the same command again: finished collections are
skipped and the unfinished one continues after its last checkpoint. Nothing is
duplicated, and the counts and the exit code cover the whole run.

```bash
polymorph run pipeline.yaml            # interrupted (kill -9, power loss, network drop)
polymorph status pipeline.yaml         # what was saved
polymorph run pipeline.yaml            # resumes where it stopped

polymorph run pipeline.yaml --fresh          # discard saved progress, start over
polymorph run pipeline.yaml --no-checkpoint  # neither save nor use checkpoints
```

The batch is written to MongoDB first and checkpointed second, so a crash can
at worst repeat one batch, which idempotent upserts make harmless. A
fingerprint of the configuration and the source schema protects against
resuming with stale progress, and an OS file lock keeps two runs of one
pipeline apart. See [`docs/checkpointing.md`](docs/checkpointing.md), including
how to inject a crash for testing.

### Dead-letter queue

Rows that cannot be migrated (an invalid email, a negative quantity, a value
that does not fit its type) never stop a run under `on_row_error: skip`. Every
rejected document is written to a JSON-lines file with the exact reason, and
the run exits with code 3:

```bash
polymorph run examples/pipelines/postgres-to-mongo-folded.yaml
polymorph dlq examples/pipelines/postgres-to-mongo-folded.yaml
```

```
COLLECTION            STAGE      TABLE                COUNT  REASON
customers             transform  order_items              1  quantity must be positive

First 1:
  customers id=17 <- order_items id=5 [transform]: quantity must be positive
```

Entries hold table names, primary keys and reasons, never row data (opt in
with `dlq_include_rows`), so the file is safe to share and the source row is
one `SELECT ... WHERE id = ...` away. Fix the source, re-run, and the file is
empty again. The file is crash-safe: its size is saved with every checkpoint
and cut back on resume, so entries are never duplicated or lost. See
[`docs/dead-letter-queue.md`](docs/dead-letter-queue.md).

## Continuous integration

Every pull request and every push to `main` runs the test suite on Python
3.11, 3.12 and 3.13 with a real MongoDB service container
(`.github/workflows/ci.yml`). The MongoDB integration tests must run in CI:
if MongoDB is unreachable there, the build fails instead of skipping them.

Reproduce CI locally:

```bash
docker compose up -d mongo
POLYMORPH_REQUIRE_MONGO=1 pytest -ra --strict-markers
```

## Tech stack

Python 3.12, Typer, Pydantic, SQLAlchemy, PostgreSQL, MySQL, MongoDB, SQLite, Docker Compose, pytest.