"""End-to-end pipeline execution: extract, translate types, load."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pymongo.errors import InvalidName

from polymorph.extractor import ExtractError, extract_batches, reflect_table
from polymorph.inspector import create_source_engine, inspect_database
from polymorph.manifest import Manifest, Mode
from polymorph.mongo_loader import LoadStats, build_document, connect, write_documents
from polymorph.translate import TranslationError, Translator, build_translator


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


@dataclass(frozen=True)
class TablePlan:
    name: str
    pk_names: list[str]
    translator: Translator


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
    on_plan: Callable[[Sequence[str]], None] | None = None,
    on_table_done: Callable[[TableResult], None] | None = None,
) -> RunSummary:
    """Run a pipeline. Every check happens before the first write."""
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
        plans = _plan_tables(source, names, manifest)  # pre-flight: source side
        client, database = connect(manifest.target.url)  # pre-flight: MongoDB reachable

        collections = {}
        for plan in plans:  # pre-flight: every collection name is valid
            try:
                collections[plan.name] = database[plan.name]
            except InvalidName as exc:
                raise PipelineError(
                    f"'{plan.name}' is not a valid MongoDB collection name"
                ) from exc

        if on_plan is not None:
            on_plan([note for plan in plans for note in plan.translator.notes])

        results = []
        for plan in plans:
            result = _copy_table(
                source, collections[plan.name], plan, manifest.settings.batch_size
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


def _plan_tables(source, names: list[str], manifest: Manifest) -> list[TablePlan]:
    overrides_by_table: dict[str, dict[str, str]] = {}
    for key, kind in manifest.type_overrides.items():
        table_name, column_name = key.split(".", 1)
        overrides_by_table.setdefault(table_name, {})[column_name] = kind.value

    not_migrated = sorted(set(overrides_by_table) - set(names))
    if not_migrated:
        raise PipelineError(
            "type_overrides refers to table(s) that are not being migrated: "
            + ", ".join(not_migrated)
        )

    naive_tz = manifest.settings.naive_tz
    plans = []
    for name in names:
        table = reflect_table(source, name)
        pk_names = [column.name for column in table.primary_key.columns]
        if not pk_names:
            raise ExtractError(
                f"Table '{name}' has no primary key; keyset pagination "
                "needs a unique, ordered key"
            )

        column_names = {column.name for column in table.columns}
        if "_id" in column_names and "_id" not in pk_names:
            raise PipelineError(
                f"Table '{name}' has a column named '_id' that is not its primary key; "
                "it would collide with MongoDB's _id field"
            )

        overrides = overrides_by_table.get(name, {})
        unknown = sorted(set(overrides) - column_names)
        if unknown:
            raise PipelineError(
                f"type_overrides refers to unknown column(s) of '{name}': {', '.join(unknown)}"
            )

        translator = build_translator(table, source.dialect, naive_tz, overrides)
        plans.append(TablePlan(name=name, pk_names=pk_names, translator=translator))
    return plans


def _copy_table(source, collection, plan: TablePlan, batch_size: int) -> TableResult:
    started = time.perf_counter()
    rows_read = 0
    stats = LoadStats(0, 0, 0)

    for batch in extract_batches(source, plan.name, batch_size=batch_size):
        documents = []
        for row in batch.rows:
            try:
                translated = plan.translator.translate_row(row)
            except TranslationError as exc:
                key = ", ".join(f"{name}={row[name]!r}" for name in plan.pk_names)
                raise PipelineError(f"{exc} (row {key})") from exc
            documents.append(build_document(translated, plan.pk_names))

        stats += write_documents(collection, documents)
        rows_read += len(batch.rows)

    return TableResult(
        table=plan.name,
        rows_read=rows_read,
        created=stats.created,
        updated=stats.updated,
        unchanged=stats.unchanged,
        seconds=time.perf_counter() - started,
    )