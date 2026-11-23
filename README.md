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
- [x] Phase 2: CLI and pipeline manifest
- [ ] Phase 3: Keyset streaming extractor
- [ ] Phase 4: MongoDB bulk loader
- [ ] Phase 5: Type translator
- [ ] Phase 6: Transform engine
- [ ] Phase 7: Join folding
- [ ] Phase 8: Checkpointing and resume
- [ ] Phase 9: Dead-letter queue
- [ ] Phase 10: Concurrency
- [ ] Phase 11: Verification audit
- [ ] Phase 12: SQL-to-SQL mode
- [ ] Phase 13: One-command demo
- [ ] Phase 14: Integration tests and CI

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

Example manifests live in [`examples/pipelines/`](examples/pipelines/).
See [`docs/workflow-and-outputs.md`](docs/workflow-and-outputs.md) for the
planned end-to-end workflow, run reports and exit codes.

Special characters in passwords must be URL-encoded inside connection URLs
(for example `@` becomes `%40`).

## Tech stack

Python 3.12, Typer, Pydantic, SQLAlchemy, PostgreSQL, MySQL, MongoDB, SQLite, Docker Compose, pytest.