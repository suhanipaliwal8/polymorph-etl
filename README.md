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
- [ ] Phase 8: Join folding
- [ ] Phase 9: Checkpointing and resume
- [ ] Phase 10: Dead-letter queue
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