"""Dead-letter queue: every rejected document, with exact diagnostics, in a JSON-lines file.

The file is append-only while a run is going. Its size is saved with every
checkpoint, and after a crash the file is cut back to that size, so the batch
that is repeated writes its entries exactly once.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


class DeadLetterError(Exception):
    """The dead-letter file could not be written, or no longer matches the checkpoint."""


def dead_letter_path(state_dir: Path, pipeline: str) -> Path:
    return state_dir / f"{pipeline}.dlq_errors.jsonl"


@dataclass(frozen=True)
class Rejection:
    """Why one row could not be processed: the CAUSE of a rejected document."""

    stage: str  # translate | transform | relation | document
    table: str  # source table of the failing row (root or embedded)
    key: dict[str, Any]  # primary key of the failing row
    reason: str  # what went wrong, never containing row data
    message: str  # the user-facing text, e.g. "order_items: ... (row id=5)"
    row: dict[str, Any] | None = None  # raw source row, written only on request


def _json_default(value: Any) -> Any:
    """Make values that JSON cannot hold readable instead of failing."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def format_entry(
    pipeline: str,
    collection: str,
    root_table: str,
    root_key: dict[str, Any],
    cause: Rejection,
    include_row: bool = False,
    now: dt.datetime | None = None,
) -> str:
    """One dead-letter entry as a single JSON line (pure ASCII, so it can always be written)."""
    moment = now or dt.datetime.now(dt.timezone.utc)
    entry: dict[str, Any] = {
        "timestamp": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pipeline": pipeline,
        "collection": collection,
        "document": {"table": root_table, "key": root_key},
        "cause": {
            "stage": cause.stage,
            "table": cause.table,
            "key": cause.key,
            "reason": cause.reason,
        },
        "message": cause.message,
    }
    if include_row and cause.row is not None:
        entry["row"] = cause.row
    return json.dumps(entry, default=_json_default, ensure_ascii=True)


class DeadLetterWriter:
    """Appends entries to the dead-letter file and keeps its size in step with checkpoints."""

    def __init__(self, path: Path, pipeline: str, include_rows: bool = False) -> None:
        self.path = path
        self.pipeline = pipeline
        self.include_rows = include_rows
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "ab"):  # creates the file; never truncates an existing one
                pass
        except OSError as exc:
            raise DeadLetterError(
                f"Cannot open the dead-letter file {path}: {exc.strerror or exc}"
            ) from exc

    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError as exc:
            raise DeadLetterError(
                f"Cannot read the dead-letter file {self.path}: {exc.strerror or exc}"
            ) from exc

    def require(self, offset: int) -> None:
        """Make sure the file still holds everything the checkpoint counted on."""
        if self.size() < offset:
            raise DeadLetterError(
                f"The dead-letter file {self.path} is shorter than the saved progress expects "
                "(it was modified or replaced). Re-run with --fresh to start over."
            )

    def truncate(self, offset: int) -> None:
        """Cut the file back to `offset` bytes (drops the entries of an unfinished batch)."""
        self.require(offset)
        try:
            with open(self.path, "r+b") as handle:
                handle.truncate(offset)
        except OSError as exc:
            raise DeadLetterError(
                f"Cannot truncate the dead-letter file {self.path}: {exc.strerror or exc}"
            ) from exc

    def append(self, lines: Sequence[str]) -> None:
        """Append entries and force them to disk before the checkpoint is saved."""
        if not lines:
            return
        data = "".join(line + "\n" for line in lines).encode("utf-8")
        try:
            with open(self.path, "ab") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            raise DeadLetterError(
                f"Cannot write the dead-letter file {self.path}: {exc.strerror or exc}"
            ) from exc


# --- reading --------------------------------------------------------------------------------


def read_entries(path: Path) -> Iterator[dict[str, Any] | None]:
    """Yield every entry; None stands for a line that is not valid (for example a torn write)."""
    with open(path, "rb") as handle:
        for raw in handle:
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                entry = json.loads(text)
            except ValueError:
                yield None
                continue
            yield entry if isinstance(entry, dict) else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _key_text(key: Any) -> str:
    if isinstance(key, dict):
        return ", ".join(f"{name}={value!r}" for name, value in key.items())
    return str(key)


def describe_entry(entry: dict[str, Any]) -> str:
    """A one-line description: which document, which row caused it, and why."""
    document = _dict(entry.get("document"))
    cause = _dict(entry.get("cause"))
    return (
        f"{document.get('table', '?')} {_key_text(document.get('key'))} <- "
        f"{cause.get('table', '?')} {_key_text(cause.get('key'))} "
        f"[{cause.get('stage', '?')}]: {cause.get('reason', '?')}"
    )


@dataclass(frozen=True)
class DeadLetterSummary:
    entries: int
    unreadable: int
    # ((collection, stage, table, reason), count), most frequent first
    groups: tuple[tuple[tuple[str, str, str, str], int], ...]
    first: tuple[dict[str, Any], ...]


def summarize(path: Path, limit: int = 5) -> DeadLetterSummary:
    entries = unreadable = 0
    counts: Counter[tuple[str, str, str, str]] = Counter()
    first: list[dict[str, Any]] = []
    try:
        for entry in read_entries(path):
            if entry is None:
                unreadable += 1
                continue
            entries += 1
            cause = _dict(entry.get("cause"))
            counts[
                (
                    str(entry.get("collection", "?")),
                    str(cause.get("stage", "?")),
                    str(cause.get("table", "?")),
                    str(cause.get("reason", "?")),
                )
            ] += 1
            if len(first) < limit:
                first.append(entry)
    except OSError as exc:
        raise DeadLetterError(f"Cannot read {path}: {exc.strerror or exc}") from exc

    groups = tuple(sorted(counts.items(), key=lambda item: (-item[1], item[0])))
    return DeadLetterSummary(entries=entries, unreadable=unreadable, groups=groups, first=tuple(first))