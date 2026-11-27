"""End-to-end pipeline execution: extract, fold related tables, translate types,
transform, load, dead-letter rejected documents, and resume after a crash."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import bson
from bson.errors import BSONError
from pymongo.errors import InvalidName
from sqlalchemy import Table

from polymorph.checkpoint import (
    CheckpointStore,
    Cursor,
    Progress,
    RunLock,
    checkpoint_path,
    keys_can_be_checkpointed,
    lock_path,
)
from polymorph.dlq import DeadLetterWriter, Rejection, format_entry
from polymorph.extractor import ExtractError, extract_batches, fetch_related, reflect_table
from polymorph.inspector import create_source_engine, inspect_database
from polymorph.manifest import EmbedSpec, Manifest, Mode, Relation, RowErrorPolicy
from polymorph.mongo_loader import LoadStats, build_document, connect, write_documents
from polymorph.transform import (
    RowRejected,
    TransformConfigError,
    TransformPlan,
    build_transform_plan,
)
from polymorph.translate import TranslationError, Translator, build_translator

MAX_REJECTION_SAMPLES = 10
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024  # MongoDB's limit for a single document
FINGERPRINT_VERSION = 1

# label of an embedded table -> {join key -> that key's child rows, in primary-key order}
Groups = dict[str, dict[tuple[Any, ...], list[dict[str, Any]]]]


class PipelineError(Exception):
    """The pipeline could not run. The message is user-facing."""


# --- results --------------------------------------------------------------------------


@dataclass(frozen=True)
class EmbeddedResult:
    """Source rows consumed by one embedded table of a collection."""

    label: str
    table: str
    depth: int
    rows_read: int
    filtered: int
    rejected: int


@dataclass(frozen=True)
class TableResult:
    """Result for one collection. Counts are DOCUMENTS (root rows), cumulative across
    interrupted attempts."""

    table: str
    rows_read: int
    created: int
    updated: int
    unchanged: int
    seconds: float
    filtered: int = 0
    rejected: int = 0
    rejection_samples: tuple[str, ...] = ()
    embedded: tuple[EmbeddedResult, ...] = ()
    resumed: bool = False  # continued from a checkpoint
    skipped: bool = False  # finished by an earlier attempt; not run again


@dataclass(frozen=True)
class RunSummary:
    pipeline: str
    source_engine: str
    target_database: str
    tables: tuple[TableResult, ...]
    seconds: float

    @property
    def rows_read(self) -> int:
        """Source rows read: root rows plus every embedded row."""
        return sum(
            table.rows_read + sum(e.rows_read for e in table.embedded) for table in self.tables
        )

    @property
    def filtered(self) -> int:
        return sum(
            table.filtered + sum(e.filtered for e in table.embedded) for table in self.tables
        )

    @property
    def rejected(self) -> int:
        """Rejected DOCUMENTS (a rejected embedded row rejects its document)."""
        return sum(table.rejected for table in self.tables)


# --- plans ------------------------------------------------------------------------------


@dataclass(frozen=True)
class TablePlan:
    name: str
    pk_names: list[str]
    translator: Translator
    transform: TransformPlan


@dataclass(frozen=True)
class EmbedPlan:
    label: str  # e.g. "customers.orders.items"
    alias: str
    depth: int
    relation: Relation
    child_columns: tuple[str, ...]
    parent_columns: tuple[str, ...]
    removed_columns: tuple[str, ...]
    sa_table: Table
    table: TablePlan
    children: tuple[EmbedPlan, ...]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CollectionPlan:
    name: str
    root: TablePlan
    embeds: tuple[EmbedPlan, ...] = ()
    resume_problem: str | None = None  # set when the root key cannot be checkpointed

    def embed_nodes(self) -> list[EmbedPlan]:
        """Every embedded table, parents before children."""
        nodes: list[EmbedPlan] = []

        def walk(embeds: Sequence[EmbedPlan]) -> None:
            for embed in embeds:
                nodes.append(embed)
                walk(embed.children)

        walk(self.embeds)
        return nodes

    @property
    def notes(self) -> list[str]:
        notes = [*self.root.translator.notes, *self.root.transform.notes]
        for node in self.embed_nodes():
            notes.extend((*node.notes, *node.table.translator.notes, *node.table.transform.notes))
        if self.resume_problem is not None:
            notes.append(
                f"{self.name}: {self.resume_problem}; this collection cannot resume "
                "after an interruption"
            )
        return notes


@dataclass(frozen=True)
class RowOutcome:
    """What happened to one row.

    document set      -> the row is ready (a nested document for embedded tables)
    rejection set     -> the row is invalid; `detail` says why in a structured way, and
                         `propagated` is True when the cause is an embedded row further down
    both None         -> a filter rule dropped the row
    """

    document: dict[str, Any] | None = None
    rejection: str | None = None
    propagated: bool = False
    detail: Rejection | None = None


@dataclass
class _Counter:
    read: int = 0
    filtered: int = 0
    rejected: int = 0


# --- formatting -------------------------------------------------------------------------

TABLE_HEADER = (
    f"{'COLLECTION':<26}{'READ':>9}{'FILTERED':>10}{'REJECTED':>10}"
    f"{'CREATED':>9}{'UPDATED':>9}{'UNCHANGED':>11}{'TIME':>9}"
)


def format_table_row(result: TableResult) -> str:
    return (
        f"{result.table:<26}{result.rows_read:>9,}{result.filtered:>10,}{result.rejected:>10,}"
        f"{result.created:>9,}{result.updated:>9,}{result.unchanged:>11,}{result.seconds:>8.2f}s"
    )


def format_embedded_row(result: EmbeddedResult) -> str:
    name = f"{'  ' * result.depth}+ {result.table}"
    return f"{name:<26}{result.rows_read:>9,}{result.filtered:>10,}{result.rejected:>10,}"


def format_footer(summary: RunSummary) -> str:
    parts = [
        f"Done: {len(summary.tables)} collection(s), {summary.rows_read:,} source rows read"
    ]
    if summary.filtered:
        parts.append(f"{summary.filtered:,} filtered")
    if summary.rejected:
        parts.append(f"{summary.rejected:,} rejected")
    return (
        f"{', '.join(parts)} in {summary.seconds:.2f}s "
        f"-> MongoDB database '{summary.target_database}'"
    )


# --- running ----------------------------------------------------------------------------


def _notice(callback: Callable[[str], None] | None, message: str) -> None:
    if callback is not None:
        callback(message)


def run_pipeline(
    manifest: Manifest,
    on_plan: Callable[[Sequence[str]], None] | None = None,
    on_table_done: Callable[[TableResult], None] | None = None,
    state_dir: Path | None = None,
    fresh: bool = False,
    on_notice: Callable[[str], None] | None = None,
    on_batch: Callable[[str, str, int], None] | None = None,
    dlq_path: Path | None = None,
) -> RunSummary:
    """Run a pipeline. Every check happens before the first write.

    With `state_dir`, progress is checkpointed after every batch and an
    interrupted run resumes from its checkpoint. With `dlq_path`, every rejected
    document is recorded in a dead-letter file.

    `on_batch(stage, collection, batch_number)` is called with stage "written"
    (documents are in MongoDB and the dead letters are in the file, but the
    checkpoint is not saved yet) and "checkpointed". Tests and the demo use it to
    inject a crash.
    """
    if manifest.mode is not Mode.SQL_TO_NOSQL:
        raise PipelineError(
            "Mode 'sql-to-sql' is not available yet (planned for Phase 13); "
            "use 'sql-to-nosql' for now."
        )

    started = time.perf_counter()
    lock = store = client = source = None
    try:
        if state_dir is not None:
            lock = RunLock(lock_path(state_dir, manifest.name))
            lock.acquire()  # one run per pipeline at a time

        if manifest.collections or manifest.tables is not None:
            flat_tables = list(manifest.tables or [])
        else:
            flat_tables = _discover_tables(manifest.source.url)

        source = create_source_engine(manifest.source.url)
        plans = _plan_collections(source, flat_tables, manifest)  # pre-flight: source side
        client, database = connect(manifest.target.url)  # pre-flight: MongoDB reachable

        mongo_collections = {}
        for plan in plans:  # pre-flight: every collection name is valid
            try:
                mongo_collections[plan.name] = database[plan.name]
            except InvalidName as exc:
                raise PipelineError(
                    f"'{plan.name}' is not a valid MongoDB collection name"
                ) from exc

        # Pre-flight: the dead-letter file can be opened (it is not truncated here).
        dlq = None
        if dlq_path is not None:
            dlq = DeadLetterWriter(dlq_path, manifest.name, manifest.settings.dlq_include_rows)

        if on_plan is not None:
            on_plan([note for plan in plans for note in plan.notes])

        states = {}
        if state_dir is not None:
            store = CheckpointStore(checkpoint_path(state_dir, manifest.name), fresh=fresh)
            states = store.load()
            for name in sorted(set(states) - {plan.name for plan in plans}):
                store.discard(name)  # a collection that is no longer in the manifest

        results = []
        # All collections share one dead-letter file, so their saved offsets form a
        # chain. `chain_ok` turns False as soon as a collection runs or starts over:
        # saved progress of the collections after it no longer fits the file.
        chain_ok = True
        base = 0  # size of the dead-letter file that belongs to the collections before this one
        for plan in plans:
            checkpointing = store is not None and plan.resume_problem is None
            fingerprint = collection_fingerprint(manifest, plan) if checkpointing else ""
            state = states.get(plan.name) if checkpointing else None

            kind = "fresh"
            if state is not None:
                if not chain_ok:
                    store.discard(plan.name)
                    _notice(
                        on_notice,
                        f"{plan.name}: saved progress ignored because an earlier collection "
                        "starts over; this collection restarts from the beginning",
                    )
                elif state.fingerprint != fingerprint:
                    store.discard(plan.name)
                    _notice(
                        on_notice,
                        f"{plan.name}: the configuration or the source schema changed since the "
                        "interrupted run; this collection restarts from the beginning",
                    )
                elif state.status == "done":
                    kind = "skip"
                else:
                    kind = "resume"

            if kind == "skip":
                if dlq is not None:
                    dlq.require(state.dlq_offset)
                base = max(base, state.dlq_offset)
                _notice(
                    on_notice, f"{plan.name}: already completed by the interrupted run; skipped"
                )
                result = _result_from_progress(plan, state.progress)
            else:
                chain_ok = False
                after: Cursor | None = None
                progress = Progress()
                resumed = False
                if kind == "resume":
                    after, progress, resumed = state.cursor, state.progress, True
                    if dlq is not None:
                        dlq.truncate(state.dlq_offset)  # drop the entries of the unfinished batch
                        if state.dlq_offset == 0 and progress.rejected > 0:
                            _notice(
                                on_notice,
                                f"{plan.name}: {progress.rejected:,} rejection(s) happened before "
                                "the dead-letter file existed and cannot be recovered; "
                                "use --fresh for a complete file",
                            )
                    _notice(
                        on_notice,
                        f"{plan.name}: resuming; {progress.rows_read:,} root rows were already "
                        "processed",
                    )
                elif dlq is not None:
                    dlq.truncate(base)  # a fresh start: nothing of this collection is in the file

                result = _copy_collection(
                    source,
                    mongo_collections[plan.name],
                    plan,
                    manifest.settings.batch_size,
                    manifest.settings.on_row_error,
                    store=store if checkpointing else None,
                    fingerprint=fingerprint,
                    after=after,
                    progress=progress,
                    resumed=resumed,
                    on_batch=on_batch,
                    dlq=dlq,
                    pipeline=manifest.name,
                )
                if dlq is not None:
                    base = dlq.size()

            results.append(result)
            if on_table_done is not None:
                on_table_done(result)

        if store is not None:
            store.clear()  # a finished run leaves nothing to resume

        return RunSummary(
            pipeline=manifest.name,
            source_engine=source.dialect.name,
            target_database=database.name,
            tables=tuple(results),
            seconds=time.perf_counter() - started,
        )
    finally:
        if source is not None:
            source.dispose()
        if client is not None:
            client.close()
        if store is not None:
            store.close()
        if lock is not None:
            lock.release()


def _discover_tables(url: str) -> list[str]:
    schema = inspect_database(url, count_rows=False)
    if not schema.tables:
        raise PipelineError("No tables found in the source database.")
    return [table.name for table in schema.tables]


def _plan_collections(source, flat_tables: list[str], manifest: Manifest) -> list[CollectionPlan]:
    migrated = set(flat_tables)
    for spec in manifest.collections.values():
        migrated.add(spec.from_)
        migrated |= spec.embedded_tables()

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

    names = [*manifest.collections, *flat_tables]
    return [plan_collection(source, name, manifest) for name in names]


def plan_table(
    source,
    name: str,
    manifest: Manifest,
    extra_fields: Sequence[str] = (),
    removed_columns: Sequence[str] = (),
    root: bool = True,
) -> TablePlan:
    """Reflect one table and validate everything that will be applied to it."""
    return _plan_reflected(
        source, reflect_table(source, name), manifest, extra_fields, removed_columns, root
    )


def _plan_reflected(
    source,
    table: Table,
    manifest: Manifest,
    extra_fields: Sequence[str],
    removed_columns: Sequence[str],
    root: bool,
) -> TablePlan:
    name = table.name
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

    kept = [column for column in column_names if column not in set(removed_columns)]
    clash = sorted(set(extra_fields) & set(kept))
    if clash:
        raise PipelineError(
            f"embedded field name(s) {', '.join(clash)} collide with column(s) of table "
            f"'{name}'; choose another 'as' name"
        )

    try:
        transform = build_transform_plan(
            name,
            [*kept, *extra_fields],
            pk_names if root else [],
            manifest.transforms.get(name, []),
            root=root,
        )
    except TransformConfigError as exc:
        raise PipelineError(str(exc)) from exc

    return TablePlan(name=name, pk_names=pk_names, translator=translator, transform=transform)


def plan_collection(source, name: str, manifest: Manifest) -> CollectionPlan:
    """Plan a folded collection, or a flat copy when `name` is not a declared collection."""
    spec = manifest.collections.get(name)
    if spec is None:
        table = reflect_table(source, name)
        root = _plan_reflected(source, table, manifest, (), (), True)
        return CollectionPlan(
            name=name,
            root=root,
            resume_problem=keys_can_be_checkpointed(table.primary_key.columns),
        )

    root_table = reflect_table(source, spec.from_)
    root = _plan_reflected(
        source,
        root_table,
        manifest,
        extra_fields=[embed.as_ for embed in spec.embed],
        removed_columns=(),
        root=True,
    )
    embeds = tuple(_plan_embed(source, root_table, embed, name, 1, manifest) for embed in spec.embed)
    return CollectionPlan(
        name=name,
        root=root,
        embeds=embeds,
        resume_problem=keys_can_be_checkpointed(root_table.primary_key.columns),
    )


def _plan_embed(
    source, parent: Table, spec: EmbedSpec, path: str, depth: int, manifest: Manifest
) -> EmbedPlan:
    child = reflect_table(source, spec.table)
    child_columns, parent_columns = _resolve_join(parent, child, spec)
    removed = () if spec.keep_foreign_key else child_columns
    label = f"{path}.{spec.as_}"

    table_plan = _plan_reflected(
        source,
        child,
        manifest,
        extra_fields=[embed.as_ for embed in spec.embed],
        removed_columns=removed,
        root=False,
    )
    children = tuple(
        _plan_embed(source, child, embed, label, depth + 1, manifest) for embed in spec.embed
    )

    notes = []
    if spec.relation is Relation.ONE and set(child_columns) != set(table_plan.pk_names):
        notes.append(
            f"{label}: relation 'one' is not enforced by the database "
            f"({spec.table}.{', '.join(child_columns)} is not its primary key); "
            "a parent with more than one matching row is rejected"
        )

    return EmbedPlan(
        label=label,
        alias=spec.as_,
        depth=depth,
        relation=spec.relation,
        child_columns=child_columns,
        parent_columns=parent_columns,
        removed_columns=tuple(removed),
        sa_table=child,
        table=table_plan,
        children=children,
        notes=tuple(notes),
    )


def _resolve_join(
    parent: Table, child: Table, spec: EmbedSpec
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return (child columns, parent columns) that tie `child` rows to a `parent` row."""
    if spec.join is not None:
        child_columns = tuple(spec.join)
        parent_columns = tuple(spec.join.values())
        missing_child = sorted(set(child_columns) - {c.name for c in child.columns})
        missing_parent = sorted(set(parent_columns) - {c.name for c in parent.columns})
        if missing_child:
            raise PipelineError(
                f"'join' refers to unknown column(s) of '{child.name}': {', '.join(missing_child)}"
            )
        if missing_parent:
            raise PipelineError(
                f"'join' refers to unknown column(s) of '{parent.name}': {', '.join(missing_parent)}"
            )
        return child_columns, parent_columns

    matches = sorted(
        (fk for fk in child.foreign_key_constraints if fk.referred_table.name == parent.name),
        key=lambda fk: [column.name for column in fk.columns],
    )
    if not matches:
        raise PipelineError(
            f"no foreign key from '{child.name}' to '{parent.name}' was found; declare the join "
            "with 'join: {child_column: parent_column}'"
        )
    if len(matches) > 1:
        options = "; ".join(
            "(" + ", ".join(column.name for column in fk.columns) + ")" for fk in matches
        )
        raise PipelineError(
            f"'{child.name}' has more than one foreign key to '{parent.name}': {options}. "
            "Choose one with 'join: {child_column: parent_column}'"
        )
    fk = matches[0]
    return (
        tuple(column.name for column in fk.columns),
        tuple(element.column.name for element in fk.elements),
    )


# --- the fingerprint of a collection ----------------------------------------------------------
# A saved cursor only makes sense for the configuration that produced it. The
# fingerprint is a hash of everything that shapes the documents, never of
# credentials (only scheme, host and database of each URL are used).


def _identity(url: str) -> str:
    parts = urlsplit(url)
    host = parts.netloc.rpartition("@")[2].lower()  # drop "user:password@"
    return f"{parts.scheme.lower()}://{host}{parts.path}"


def _redact_salts(value: Any) -> Any:
    """Replace secret salts by their own hash: a changed salt still changes the fingerprint."""
    if isinstance(value, dict):
        return {
            key: (
                hashlib.sha256(item.encode("utf-8")).hexdigest()
                if key == "salt" and isinstance(item, str)
                else _redact_salts(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_salts(item) for item in value]
    return value


def _embed_signature(embed: EmbedPlan) -> dict[str, Any]:
    return {
        "alias": embed.alias,
        "table": embed.table.name,
        "relation": embed.relation.value,
        "child_columns": list(embed.child_columns),
        "parent_columns": list(embed.parent_columns),
        "removed_columns": list(embed.removed_columns),
        "children": [_embed_signature(child) for child in embed.children],
    }


def _table_signature(plan: TablePlan) -> dict[str, Any]:
    return {
        "primary_key": plan.pk_names,
        "columns": [[mapping.name, mapping.sql_type] for mapping in plan.translator.mappings],
    }


def collection_fingerprint(manifest: Manifest, plan: CollectionPlan) -> str:
    """SHA-256 of everything that determines the documents of one collection."""
    table_plans = {plan.root.name: plan.root}
    for node in plan.embed_nodes():
        table_plans[node.table.name] = node.table
    tables = sorted(table_plans)

    payload = {
        "version": FINGERPRINT_VERSION,
        "mode": manifest.mode.value,
        "source": _identity(manifest.source.url),
        "target": _identity(manifest.target.url),
        "collection": plan.name,
        "root": plan.root.name,
        "embeds": [_embed_signature(embed) for embed in plan.embeds],
        "schema": {name: _table_signature(table_plans[name]) for name in tables},
        "transforms": {
            name: _redact_salts([rule.model_dump(mode="json") for rule in manifest.transforms[name]])
            for name in tables
            if name in manifest.transforms
        },
        "type_overrides": {
            key: kind.value
            for key, kind in sorted(manifest.type_overrides.items())
            if key.split(".", 1)[0] in tables
        },
        "naive_timestamp_timezone": manifest.settings.naive_timestamp_timezone,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --- fetching children ---------------------------------------------------------------------


def _join_key(row: Mapping[str, Any], columns: Sequence[str]) -> tuple[Any, ...] | None:
    key = tuple(row[column] for column in columns)
    return None if any(value is None for value in key) else key


def prefetch_children(
    source,
    embeds: Sequence[EmbedPlan],
    parent_rows: Sequence[Mapping[str, Any]],
    groups: Groups,
) -> None:
    """Fetch the children of a whole batch of parent rows, one query per chunk of keys.

    Fills `groups[label]` = {join key: child rows}; nested embeds use the child
    rows just fetched as their parents.
    """
    for embed in embeds:
        keys: list[tuple[Any, ...]] = []
        seen: set[tuple[Any, ...]] = set()
        for row in parent_rows:
            key = _join_key(row, embed.parent_columns)
            if key is not None and key not in seen:
                seen.add(key)
                keys.append(key)

        rows = fetch_related(source, embed.sa_table, embed.child_columns, keys)

        grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(tuple(row[c] for c in embed.child_columns), []).append(row)

        if any(key not in seen for key in grouped):
            raise PipelineError(
                f"Rows of '{embed.table.name}' matched their parent rows in the database but "
                "not in Python; the key comparison differs (case-insensitive collation or "
                f"padded text keys?). Join columns: {', '.join(embed.child_columns)}"
            )

        groups[embed.label] = grouped
        prefetch_children(source, embed.children, rows, groups)


def _children_of(raw: Mapping[str, Any], embed: EmbedPlan, groups: Groups) -> list[dict[str, Any]]:
    key = _join_key(raw, embed.parent_columns)
    return [] if key is None else groups[embed.label].get(key, [])


def make_counters(plan: CollectionPlan) -> dict[str, _Counter]:
    return {node.label: _Counter() for node in plan.embed_nodes()}


# --- processing rows ---------------------------------------------------------------------------


def _row_key(plan: TablePlan, row: Mapping[str, Any]) -> str:
    return ", ".join(f"{name}={row[name]!r}" for name in plan.pk_names)


def _key_dict(plan: TablePlan, row: Mapping[str, Any]) -> dict[str, Any]:
    return {name: row[name] for name in plan.pk_names}


def _rejected(
    plan: TablePlan,
    raw: Mapping[str, Any],
    stage: str,
    reason: str,
    message: str | None = None,
) -> RowOutcome:
    """A rejected row: the user-facing message plus the structured detail for the DLQ."""
    text = message or f"{plan.name}: {reason} (row {_row_key(plan, raw)})"
    detail = Rejection(
        stage=stage,
        table=plan.name,
        key=_key_dict(plan, raw),
        reason=reason,
        message=text,
        row=dict(raw),
    )
    return RowOutcome(rejection=text, detail=detail)


def _translation_rejected(plan: TablePlan, raw: Mapping[str, Any], exc: TranslationError) -> RowOutcome:
    # A value that does not fit its column type is a poison record: rejected like any
    # other bad row (the run stops only under on_row_error: fail).
    return _rejected(plan, raw, "translate", str(exc), message=f"{exc} (row {_row_key(plan, raw)})")


def _apply_rules(plan: TablePlan, translated: dict[str, Any], raw: Mapping[str, Any]) -> RowOutcome:
    try:
        kept = plan.transform.apply(translated)
    except RowRejected as exc:
        return _rejected(plan, raw, "transform", str(exc))
    return RowOutcome(document=kept)  # None means a filter dropped the row


def process_row(plan: TablePlan, row: Mapping[str, Any]) -> RowOutcome:
    """Translate types, apply the transform rules and build the document for one flat row."""
    try:
        translated = plan.translator.translate_row(row)
    except TranslationError as exc:
        return _translation_rejected(plan, row, exc)

    outcome = _apply_rules(plan, translated, row)
    if outcome.document is None:
        return outcome
    return RowOutcome(document=build_document(outcome.document, plan.pk_names))


def _process_node(
    plan: TablePlan,
    embeds: Sequence[EmbedPlan],
    removed: Sequence[str],
    raw: Mapping[str, Any],
    groups: Groups,
    counters: dict[str, _Counter],
    label: str | None,
) -> RowOutcome:
    """One row of a root or embedded table: translate, embed its children, apply its rules."""
    counter = counters[label] if label is not None else None
    if counter is not None:
        counter.read += 1

    translated: dict[str, Any] | None = None
    own_rejection: RowOutcome | None = None
    try:
        translated = plan.translator.translate_row(raw)
    except TranslationError as exc:
        own_rejection = _translation_rejected(plan, raw, exc)
    else:
        for name in removed:
            translated.pop(name, None)

    relation_rejection: RowOutcome | None = None
    descendant: RowOutcome | None = None
    for embed in embeds:
        children = _children_of(raw, embed, groups)
        built: list[dict[str, Any]] = []
        for child_raw in children:  # every child is processed, so the counters stay exact
            outcome = _process_node(
                embed.table, embed.children, embed.removed_columns, child_raw, groups, counters,
                embed.label,
            )
            if outcome.rejection is not None:
                descendant = descendant or outcome
            elif outcome.document is not None:
                built.append(outcome.document)

        value: Any
        if embed.relation is Relation.ONE:
            if len(children) > 1 and relation_rejection is None:
                reason = (
                    f"expected at most one {embed.table.name} row for "
                    f"'{embed.alias}' but found {len(children)}"
                )
                relation_rejection = _rejected(plan, raw, "relation", reason)
            value = built[0] if built else None
        else:
            value = built
        if translated is not None:
            translated[embed.alias] = value

    own = own_rejection or relation_rejection
    if own is not None:
        if counter is not None:
            counter.rejected += 1
        return own
    if descendant is not None:
        # The cause was already counted where it happened.
        return RowOutcome(rejection=descendant.rejection, propagated=True, detail=descendant.detail)

    outcome = _apply_rules(plan, translated, raw)
    if counter is not None:
        if outcome.rejection is not None:
            counter.rejected += 1
        elif outcome.document is None:
            counter.filtered += 1
    return outcome


def _document_problem(document: dict[str, Any]) -> str | None:
    try:
        size = len(bson.encode(document))
    except (BSONError, OverflowError):
        return "document cannot be encoded as BSON"
    if size > MAX_DOCUMENT_BYTES:
        return (
            f"document is {size / 1_048_576:.1f} MB, over MongoDB's 16 MB limit "
            "(too many embedded rows for one document)"
        )
    return None


def process_document(
    plan: CollectionPlan,
    raw: Mapping[str, Any],
    groups: Groups,
    counters: dict[str, _Counter],
) -> RowOutcome:
    """Build the MongoDB document of one root row, with its embedded rows."""
    if not plan.embeds:
        return process_row(plan.root, raw)

    outcome = _process_node(plan.root, plan.embeds, (), raw, groups, counters, None)
    if outcome.rejection is not None:
        if outcome.propagated:
            suffix = f"; document rejected: {plan.root.name} {_row_key(plan.root, raw)}"
            message = outcome.rejection + suffix
            detail = (
                replace(outcome.detail, message=message)
                if outcome.detail is not None
                else None
            )
            return RowOutcome(rejection=message, detail=detail)
        return outcome
    if outcome.document is None:
        return outcome  # a filter dropped the root row

    document = build_document(outcome.document, plan.root.pk_names)
    problem = _document_problem(document)
    if problem is not None:
        return _rejected(plan.root, raw, "document", problem)
    return RowOutcome(document=document)


# --- copying one collection, with checkpoints and dead letters ----------------------------------


def _embedded_results(
    plan: CollectionPlan, values: Mapping[str, tuple[int, int, int]]
) -> tuple[EmbeddedResult, ...]:
    results = []
    for node in plan.embed_nodes():
        read, filtered, rejected = values.get(node.label, (0, 0, 0))
        results.append(
            EmbeddedResult(
                label=node.label,
                table=node.table.name,
                depth=node.depth,
                rows_read=read,
                filtered=filtered,
                rejected=rejected,
            )
        )
    return tuple(results)


def _result_from_progress(plan: CollectionPlan, progress: Progress) -> TableResult:
    """The result of a collection that an earlier attempt already finished."""
    return TableResult(
        table=plan.name,
        rows_read=progress.rows_read,
        created=progress.created,
        updated=progress.updated,
        unchanged=progress.unchanged,
        seconds=0.0,
        filtered=progress.filtered,
        rejected=progress.rejected,
        embedded=_embedded_results(plan, progress.embedded),
        skipped=True,
    )


def _snapshot(
    rows_read: int,
    filtered: int,
    rejected: int,
    stats: LoadStats,
    counters: dict[str, _Counter],
) -> Progress:
    return Progress(
        rows_read=rows_read,
        filtered=filtered,
        rejected=rejected,
        created=stats.created,
        updated=stats.updated,
        unchanged=stats.unchanged,
        embedded={
            label: (counter.read, counter.filtered, counter.rejected)
            for label, counter in counters.items()
        },
    )


def _dead_letter(
    dlq: DeadLetterWriter,
    pipeline: str,
    plan: CollectionPlan,
    row: Mapping[str, Any],
    outcome: RowOutcome,
) -> str | None:
    if outcome.detail is None:
        return None
    return format_entry(
        pipeline,
        plan.name,
        plan.root.name,
        _key_dict(plan.root, row),
        outcome.detail,
        include_row=dlq.include_rows,
    )


def _copy_collection(
    source,
    collection,
    plan: CollectionPlan,
    batch_size: int,
    on_row_error: RowErrorPolicy,
    *,
    store: CheckpointStore | None = None,
    fingerprint: str = "",
    after: Cursor | None = None,
    progress: Progress | None = None,
    resumed: bool = False,
    on_batch: Callable[[str, str, int], None] | None = None,
    dlq: DeadLetterWriter | None = None,
    pipeline: str = "",
) -> TableResult:
    started = time.perf_counter()
    progress = progress or Progress()

    # Start from the counters of the earlier attempt (all zero for a fresh start).
    rows_read, filtered, rejected = progress.rows_read, progress.filtered, progress.rejected
    stats = LoadStats(progress.created, progress.updated, progress.unchanged)
    counters = make_counters(plan)
    for label, (read, flt, rej) in progress.embedded.items():
        if label in counters:
            counters[label].read, counters[label].filtered, counters[label].rejected = read, flt, rej
    samples: list[str] = []

    batch_number = 0
    for batch in extract_batches(source, plan.root.name, batch_size=batch_size, after=after):
        batch_number += 1
        groups: Groups = {}
        prefetch_children(source, plan.embeds, batch.rows, groups)

        documents = []
        dead_letters: list[str] = []
        for row in batch.rows:
            outcome = process_document(plan, row, groups, counters)
            if outcome.rejection is not None:
                entry = _dead_letter(dlq, pipeline, plan, row, outcome) if dlq is not None else None
                if on_row_error is RowErrorPolicy.FAIL:
                    if dlq is not None and entry is not None:
                        dlq.append([entry])  # the row that stops the run is recorded too
                    raise PipelineError(
                        f"{outcome.rejection}. Set settings.on_row_error: skip "
                        "to continue past rejected rows."
                    )
                rejected += 1
                if len(samples) < MAX_REJECTION_SAMPLES:
                    samples.append(outcome.rejection)
                if entry is not None:
                    dead_letters.append(entry)
            elif outcome.document is None:
                filtered += 1
            else:
                documents.append(outcome.document)

        # A batch is written only after every row in it was processed, so a
        # failing batch is never half-loaded.
        stats += write_documents(collection, documents)
        rows_read += len(batch.rows)

        # Order: documents, then dead letters (fsync), then the checkpoint. A crash at any
        # point repeats at most this batch; resume cuts the dead-letter file back to the
        # offset saved in the checkpoint, so its entries are written exactly once.
        if dlq is not None:
            dlq.append(dead_letters)

        if on_batch is not None:
            on_batch("written", plan.name, batch_number)

        if store is not None:
            store.save_progress(
                plan.name, fingerprint, batch.cursor,
                _snapshot(rows_read, filtered, rejected, stats, counters),
                dlq_offset=dlq.size() if dlq is not None else 0,
            )

        if on_batch is not None:
            on_batch("checkpointed", plan.name, batch_number)

    final = _snapshot(rows_read, filtered, rejected, stats, counters)
    if store is not None:
        store.mark_done(
            plan.name, fingerprint, final, dlq_offset=dlq.size() if dlq is not None else 0
        )

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
        embedded=_embedded_results(plan, final.embedded),
        resumed=resumed,
    )