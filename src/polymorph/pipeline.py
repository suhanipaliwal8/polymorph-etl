"""End-to-end pipeline execution: extract, translate types, transform, load."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pymongo.errors import InvalidName

from polymorph.extractor import ExtractError, extract_batches, reflect_table
from polymorph.inspector import create_source_engine, inspect_database
from polymorph.manifest import Manifest, Mode, RowErrorPolicy
from polymorph.mongo_loader import LoadStats, build_document, connect, write_documents
from polymorph.transform import (
    RowRejected,
    TransformConfigError,
    TransformPlan,
    build_transform_plan,
)
from polymorph.translate import TranslationError, Translator, build_translator

MAX_REJECTION_SAMPLES = 10


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
    filtered: int = 0
    rejected: int = 0
    rejection_samples: tuple[str, ...] = ()


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

    @property
    def filtered(self) -> int:
        return sum(table.filtered for table in self.tables)

    @property
    def rejected(self) -> int:
        return sum(table.rejected for table in self.tables)


@dataclass(frozen=True)
class TablePlan:
    name: str
    pk_names: list[str]
    translator: Translator
    transform: TransformPlan


@dataclass(frozen=True)
class RowOutcome:
    """What happened to one row.

    document set      -> the row is ready to load
    rejection set     -> the row is invalid (the message explains why)
    both None         -> a filter rule dropped the row
    """

    document: dict[str, Any] | None = None
    rejection: str | None = None


TABLE_HEADER = (
    f"{'TABLE':<26}{'READ':>9}{'FILTERED':>10}{'REJECTED':>10}"
    f"{'CREATED':>9}{'UPDATED':>9}{'UNCHANGED':>11}{'TIME':>9}"
)


def format_table_row(result: TableResult) -> str:
    return (
        f"{result.table:<26}{result.rows_read:>9,}{result.filtered:>10,}{result.rejected:>10,}"
        f"{result.created:>9,}{result.updated:>9,}{result.unchanged:>11,}{result.seconds:>8.2f}s"
    )


def format_footer(summary: RunSummary) -> str:
    parts = [f"Done: {len(summary.tables)} table(s), {summary.rows_read:,} rows read"]
    if summary.filtered:
        parts.append(f"{summary.filtered:,} filtered")
    if summary.rejected:
        parts.append(f"{summary.rejected:,} rejected")
    return (
        f"{', '.join(parts)} in {summary.seconds:.2f}s "
        f"-> MongoDB database '{summary.target_database}'"
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
            on_plan(
                [
                    note
                    for plan in plans
                    for note in (*plan.translator.notes, *plan.transform.notes)
                ]
            )

        results = []
        for plan in plans:
            result = _copy_table(
                source,
                collections[plan.name],
                plan,
                manifest.settings.batch_size,
                manifest.settings.on_row_error,
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
    migrated = set(names)
    references = {
        "type_overrides": {key.split(".", 1)[0] for key in manifest.type_overrides},
        "transforms": set(manifest.transforms),
    }
    for section, tables in references.items():
        missing = sorted(tables - migrated)
        if missing:
            raise PipelineError(
                f"{section} refers to table(s) that are not being migrated: {', '.join(missing)}"
            )
    return [plan_table(source, name, manifest) for name in names]


def plan_table(source, name: str, manifest: Manifest) -> TablePlan:
    """Reflect one table and validate everything that will be applied to it."""
    table = reflect_table(source, name)
    pk_names = [column.name for column in table.primary_key.columns]
    if not pk_names:
        raise ExtractError(
            f"Table '{name}' has no primary key; keyset pagination "
            "needs a unique, ordered key"
        )
    column_names = [column.name for column in table.columns]

    overrides: dict[str, str] = {}
    for key, kind in manifest.type_overrides.items():
        table_name, column_name = key.split(".", 1)
        if table_name == name:
            overrides[column_name] = kind.value
    unknown = sorted(set(overrides) - set(column_names))
    if unknown:
        raise PipelineError(
            f"type_overrides refers to unknown column(s) of '{name}': {', '.join(unknown)}"
        )

    translator = build_translator(table, source.dialect, manifest.settings.naive_tz, overrides)
    try:
        transform = build_transform_plan(
            name, column_names, pk_names, manifest.transforms.get(name, [])
        )
    except TransformConfigError as exc:
        raise PipelineError(str(exc)) from exc

    return TablePlan(name=name, pk_names=pk_names, translator=translator, transform=transform)


def _row_key(plan: TablePlan, row: Mapping[str, Any]) -> str:
    return ", ".join(f"{name}={row[name]!r}" for name in plan.pk_names)


def process_row(plan: TablePlan, row: Mapping[str, Any]) -> RowOutcome:
    """Translate types, apply the transform rules and build the document for one row."""
    try:
        translated = plan.translator.translate_row(row)
    except TranslationError as exc:
        # A type error means the data does not match its schema: always fatal.
        raise PipelineError(f"{exc} (row {_row_key(plan, row)})") from exc

    try:
        kept = plan.transform.apply(translated)
    except RowRejected as exc:
        return RowOutcome(rejection=f"{plan.name}: {exc} (row {_row_key(plan, row)})")

    if kept is None:
        return RowOutcome()
    return RowOutcome(document=build_document(kept, plan.pk_names))


def _copy_table(
    source, collection, plan: TablePlan, batch_size: int, on_row_error: RowErrorPolicy
) -> TableResult:
    started = time.perf_counter()
    rows_read = filtered = rejected = 0
    samples: list[str] = []
    stats = LoadStats(0, 0, 0)

    for batch in extract_batches(source, plan.name, batch_size=batch_size):
        documents = []
        for row in batch.rows:
            outcome = process_row(plan, row)
            if outcome.rejection is not None:
                if on_row_error is RowErrorPolicy.FAIL:
                    raise PipelineError(
                        f"{outcome.rejection}. Set settings.on_row_error: skip "
                        "to continue past rejected rows."
                    )
                rejected += 1
                if len(samples) < MAX_REJECTION_SAMPLES:
                    samples.append(outcome.rejection)
            elif outcome.document is None:
                filtered += 1
            else:
                documents.append(outcome.document)

        # A batch is written only after every row in it was processed, so a
        # failing batch is never half-loaded.
        stats += write_documents(collection, documents)
        rows_read += len(batch.rows)

    return TableResult(
        table=plan.name,
        rows_read=rows_read,
        created=stats.created,
        updated=stats.updated,
        unchanged=stats.unchanged,
        seconds=time.perf_counter() - started,
        filtered=filtered,
        rejected=rejected,
        rejection_samples=tuple(samples),
    )