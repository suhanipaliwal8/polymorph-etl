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