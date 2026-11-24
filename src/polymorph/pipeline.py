"""End-to-end pipeline execution: extract from the source, load into the target."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from pymongo.errors import InvalidName

from polymorph.extractor import ExtractError, extract_batches, reflect_table
from polymorph.inspector import create_source_engine, inspect_database
from polymorph.manifest import Manifest, Mode
from polymorph.mongo_loader import LoadStats, build_document, connect, write_documents


class PipelineError(Exception):
    """The pipeline could not run. The message is user-facing."""


@dataclass(frozen=True)
class TableResult:
    table: str
    rows_read: int
    created: int
    updated: int
    unchanged: int
    seconds: float


@dataclass(frozen=True)
class RunSummary:
    pipeline: str
    source_engine: str
    target_database: str
    tables: tuple[TableResult, ...]
    seconds: float

    @property
    def rows_read(self) -> int:
        return sum(table.rows_read for table in self.tables)


TABLE_HEADER = (
    f"{'TABLE':<26}{'READ':>9}{'CREATED':>9}{'UPDATED':>9}{'UNCHANGED':>11}{'TIME':>9}"
)


def format_table_row(result: TableResult) -> str:
    return (
        f"{result.table:<26}{result.rows_read:>9,}{result.created:>9,}"
        f"{result.updated:>9,}{result.unchanged:>11,}{result.seconds:>8.2f}s"
    )


def format_footer(summary: RunSummary) -> str:
    return (
        f"Done: {len(summary.tables)} table(s), {summary.rows_read:,} rows in "
        f"{summary.seconds:.2f}s -> MongoDB database '{summary.target_database}'"
    )


def run_pipeline(
    manifest: Manifest,
    on_table_done: Callable[[TableResult], None] | None = None,
) -> RunSummary:
    """Run a pipeline. All checks happen before the first write."""
    if manifest.mode is not Mode.SQL_TO_NOSQL:
        raise PipelineError(
            "Mode 'sql-to-sql' is not available yet (planned for Phase 13); "
            "use 'sql-to-nosql' for now."
        )

    started = time.perf_counter()
    if manifest.tables is not None:
        names = list(manifest.tables)
    else:
        names = _discover_tables(manifest.source.url)

    source = create_source_engine(manifest.source.url)
    client = None
    try:
        plans = _plan_tables(source, names)  # pre-flight: tables exist, have a primary key
        client, database = connect(manifest.target.url)  # pre-flight: MongoDB is reachable

        results = []
        for name, pk_names in plans:
            try:
                collection = database[name]
            except InvalidName as exc:
                raise PipelineError(f"'{name}' is not a valid MongoDB collection name") from exc

            result = _copy_table(
                source, collection, name, pk_names, manifest.settings.batch_size
            )
            results.append(result)
            if on_table_done is not None:
                on_table_done(result)

        return RunSummary(
            pipeline=manifest.name,
            source_engine=source.dialect.name,
            target_database=database.name,
            tables=tuple(results),
            seconds=time.perf_counter() - started,
        )
    finally:
        source.dispose()
        if client is not None:
            client.close()


def _discover_tables(url: str) -> list[str]:
    schema = inspect_database(url, count_rows=False)
    if not schema.tables:
        raise PipelineError("No tables found in the source database.")
    return [table.name for table in schema.tables]


def _plan_tables(source, names: list[str]) -> list[tuple[str, list[str]]]:
    plans = []
    for name in names:
        table = reflect_table(source, name)
        pk_names = [column.name for column in table.primary_key.columns]
        if not pk_names:
            raise ExtractError(
                f"Table '{name}' has no primary key; keyset pagination "
                "needs a unique, ordered key"
            )
        plans.append((name, pk_names))
    return plans


def _copy_table(source, collection, table: str, pk_names: list[str], batch_size: int) -> TableResult:
    started = time.perf_counter()
    rows_read = 0
    stats = LoadStats(0, 0, 0)

    for batch in extract_batches(source, table, batch_size=batch_size):
        documents = [build_document(row, pk_names) for row in batch.rows]
        stats += write_documents(collection, documents)
        rows_read += len(batch.rows)

    return TableResult(
        table=table,
        rows_read=rows_read,
        created=stats.created,
        updated=stats.updated,
        unchanged=stats.unchanged,
        seconds=time.perf_counter() - started,
    )