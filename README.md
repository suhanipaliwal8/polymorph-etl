# PolyMorph ETL

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
- [ ] Phase 5: MongoDB bulk loader
- [ ] Phase 6: Type and precision translator
- [ ] Phase 7: Transform engine
- [ ] Phase 8: Join folding
- [ ] Phase 9: Checkpointing and resume
- [ ] Phase 10: Dead-letter queue
- [ ] Phase 11: Concurrency
- [ ] Phase 12: Verification audit, run reports and dry run
- [ ] Phase 13: SQL-to-SQL for any supported database
- [ ] Phase 14: One-command demo
- [ ] Phase 15: Integration tests and CI

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

## Tech stack

Python 3.12, Typer, Pydantic, SQLAlchemy, PostgreSQL, MySQL, MongoDB, SQLite, Docker Compose, pytest.