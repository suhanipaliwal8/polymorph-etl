"""Compare OFFSET pagination with keyset pagination.

Usage:
    python -m polymorph.demo.pagination_benchmark
    python -m polymorph.demo.pagination_benchmark --rows 500000 --batch-size 1000
"""

from __future__ import annotations

import argparse
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert, select
from sqlalchemy.engine import Engine

metadata = MetaData()

numbers = Table(
    "numbers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("payload", String(40), nullable=False),
)


@dataclass(frozen=True)
class BenchmarkResult:
    rows: int
    batch_size: int
    offset_rows: int
    offset_seconds: float
    keyset_rows: int
    keyset_seconds: float


def _populate(engine: Engine, rows: int) -> None:
    metadata.create_all(engine)
    chunk = 50_000
    with engine.begin() as conn:
        for start in range(1, rows + 1, chunk):
            stop = min(start + chunk, rows + 1)
            conn.execute(
                insert(numbers),
                [{"id": i, "payload": f"row-{i}"} for i in range(start, stop)],
            )


def _read_with_offset(engine: Engine, batch_size: int) -> int:
    total = 0
    offset = 0
    with engine.connect() as conn:
        while True:
            query = select(numbers).order_by(numbers.c.id).limit(batch_size).offset(offset)
            batch = conn.execute(query).all()
            if not batch:
                return total
            total += len(batch)
            offset += batch_size


def _read_with_keyset(engine: Engine, batch_size: int) -> int:
    total = 0
    last_seen = None
    with engine.connect() as conn:
        while True:
            query = select(numbers).order_by(numbers.c.id).limit(batch_size)
            if last_seen is not None:
                query = query.where(numbers.c.id > last_seen)
            batch = conn.execute(query).all()
            if not batch:
                return total
            total += len(batch)
            last_seen = batch[-1].id


def _timed(read, engine: Engine, batch_size: int) -> tuple[int, float]:
    started = time.perf_counter()
    count = read(engine, batch_size)
    return count, time.perf_counter() - started


def run(rows: int, batch_size: int) -> BenchmarkResult:
    with tempfile.TemporaryDirectory() as folder:
        engine = create_engine(f"sqlite:///{(Path(folder) / 'bench.db').as_posix()}")
        try:
            _populate(engine, rows)
            offset_rows, offset_seconds = _timed(_read_with_offset, engine, batch_size)
            keyset_rows, keyset_seconds = _timed(_read_with_keyset, engine, batch_size)
        finally:
            engine.dispose()  # Windows cannot delete a file that is still open
    return BenchmarkResult(
        rows=rows,
        batch_size=batch_size,
        offset_rows=offset_rows,
        offset_seconds=offset_seconds,
        keyset_rows=keyset_rows,
        keyset_seconds=keyset_seconds,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="OFFSET vs keyset pagination benchmark.")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    print(f"Building a table with {args.rows:,} rows...")
    result = run(args.rows, args.batch_size)

    batches = -(-result.rows // result.batch_size)  # ceiling division
    speedup = result.offset_seconds / max(result.keyset_seconds, 1e-9)
    print(f"Reading it in {batches:,} batches of {result.batch_size:,}:")
    print(f"  OFFSET pagination: {result.offset_seconds:7.2f} s ({result.offset_rows:,} rows)")
    print(f"  Keyset pagination: {result.keyset_seconds:7.2f} s ({result.keyset_rows:,} rows)")
    print(f"  Keyset is {speedup:.1f}x faster")


if __name__ == "__main__":
    main()