"""Crash-safe progress tracking: a small SQLite store, a cursor codec and a run lock."""

from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2  # 1: Phase 9. 2: adds dlq_offset (size of the dead-letter file)

# The primary-key values of the last row seen, e.g. (500,) or (3, 12).
Cursor = tuple[Any, ...]

KEY_PYTHON_TYPES = (
    bool, int, float, str, Decimal, dt.datetime, dt.date, dt.time, uuid.UUID, bytes,
)


class CheckpointError(Exception):
    """Checkpoint state could not be read, written or locked. The message is user-facing."""


def checkpoint_path(state_dir: Path, pipeline: str) -> Path:
    return state_dir / f"{pipeline}.checkpoint.sqlite"


def lock_path(state_dir: Path, pipeline: str) -> Path:
    return state_dir / f"{pipeline}.lock"


# --- progress counters ---------------------------------------------------------------


@dataclass(frozen=True)
class Progress:
    """Cumulative counters of one collection, summed over interrupted attempts."""

    rows_read: int = 0
    filtered: int = 0
    rejected: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    # label of an embedded table -> (read, filtered, rejected)
    embedded: dict[str, tuple[int, int, int]] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "rows_read": self.rows_read,
                "filtered": self.filtered,
                "rejected": self.rejected,
                "created": self.created,
                "updated": self.updated,
                "unchanged": self.unchanged,
                "embedded": {label: list(values) for label, values in self.embedded.items()},
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str) -> Progress:
        try:
            data = json.loads(text)
            return cls(
                rows_read=int(data["rows_read"]),
                filtered=int(data["filtered"]),
                rejected=int(data["rejected"]),
                created=int(data["created"]),
                updated=int(data["updated"]),
                unchanged=int(data["unchanged"]),
                embedded={
                    label: (int(v[0]), int(v[1]), int(v[2]))
                    for label, v in data["embedded"].items()
                },
            )
        except (ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
            raise CheckpointError("The saved checkpoint progress is unreadable.") from exc


# --- cursor codec ---------------------------------------------------------------------
# Plain JSON cannot bring a UUID, a Decimal or a datetime back exactly, so every
# value is stored with a type tag: ["uuid", "1234..."]. (pickle would be shorter,
# but loading it from a file can run arbitrary code.)


def _encode_value(value: Any) -> list[Any]:
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]  # as text: JSON numbers lose big integers
    if isinstance(value, float):
        return ["float", repr(value)]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    if isinstance(value, dt.datetime):  # before date: datetime is a subclass of date
        return ["datetime", value.isoformat()]
    if isinstance(value, dt.date):
        return ["date", value.isoformat()]
    if isinstance(value, dt.time):
        return ["time", value.isoformat()]
    if isinstance(value, uuid.UUID):
        return ["uuid", str(value)]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ["bytes", bytes(value).hex()]
    raise CheckpointError(
        f"A primary key value of type {type(value).__name__} cannot be saved in a checkpoint."
    )


_DECODERS = {
    "bool": bool,
    "int": int,
    "float": float,
    "str": str,
    "decimal": Decimal,
    "datetime": dt.datetime.fromisoformat,
    "date": dt.date.fromisoformat,
    "time": dt.time.fromisoformat,
    "uuid": uuid.UUID,
    "bytes": bytes.fromhex,
}


def encode_cursor(cursor: Cursor) -> str:
    return json.dumps([_encode_value(value) for value in cursor])


def decode_cursor(text: str) -> Cursor:
    try:
        return tuple(_DECODERS[tag](raw) for tag, raw in json.loads(text))
    except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
        raise CheckpointError("The saved checkpoint cursor is unreadable.") from exc


def keys_can_be_checkpointed(columns: Any) -> str | None:
    """Return a problem description, or None when every key column's type can be saved."""
    for column in columns:
        problem = (
            f"primary key column '{column.name}' has a type that cannot be saved "
            "in a checkpoint"
        )
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            return problem
        if not (isinstance(python_type, type) and issubclass(python_type, KEY_PYTHON_TYPES)):
            return problem
    return None


# --- the store ----------------------------------------------------------------------------

_CREATE = """
CREATE TABLE IF NOT EXISTS collections (
    name        TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('in_progress', 'done')),
    cursor      TEXT,
    progress    TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    dlq_offset  INTEGER NOT NULL DEFAULT 0
)
"""

_UPSERT = """
INSERT INTO collections (name, fingerprint, status, cursor, progress, updated_at, dlq_offset)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    fingerprint = excluded.fingerprint,
    status      = excluded.status,
    cursor      = excluded.cursor,
    progress    = excluded.progress,
    updated_at  = excluded.updated_at,
    dlq_offset  = excluded.dlq_offset
"""


@dataclass(frozen=True)
class CollectionState:
    name: str
    fingerprint: str
    status: str  # "in_progress" or "done"
    cursor: Cursor | None
    progress: Progress
    updated_at: str
    dlq_offset: int = 0  # size of the dead-letter file when this state was saved


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class CheckpointStore:
    """One SQLite file with the progress of one pipeline.

    Every change is one transaction. SQLite commits are atomic and durable, so
    after a crash the file holds either the previous state or the new one,
    never something in between.
    """

    def __init__(self, path: Path, fresh: bool = False) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        if fresh:
            for suffix in ("", "-journal", "-wal", "-shm"):
                Path(str(path) + suffix).unlink(missing_ok=True)

        self._conn = sqlite3.connect(path, timeout=10)
        try:
            self._prepare()
        except CheckpointError:
            self._conn.close()
            raise
        except sqlite3.Error as exc:
            self._conn.close()
            raise CheckpointError(
                f"Checkpoint file {path} is unreadable ({exc}). Delete it, or run with --fresh."
            ) from exc

    def _has_column(self, name: str) -> bool:
        columns = self._conn.execute("PRAGMA table_info(collections)").fetchall()
        return any(column[1] == name for column in columns)

    def _prepare(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise CheckpointError(
                f"{self.path} was written by a newer version of PolyMorph "
                f"(checkpoint format {version}). Upgrade, or delete the file."
            )
        if version == 0:
            with self._conn:
                self._conn.execute(_CREATE)
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif version < SCHEMA_VERSION:
            # Migration 1 -> 2. DDL is not transactional here, so every step is repeatable:
            # a crash half-way leaves a file that the next start can finish.
            with self._conn:
                if not self._has_column("dlq_offset"):
                    self._conn.execute(
                        "ALTER TABLE collections ADD COLUMN dlq_offset INTEGER NOT NULL DEFAULT 0"
                    )
                self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        try:
            with self._conn:  # one transaction: commit on success, roll back on error
                self._conn.execute(sql, params)
        except sqlite3.Error as exc:
            raise CheckpointError(f"Could not write the checkpoint: {exc}") from exc

    def load(self) -> dict[str, CollectionState]:
        try:
            rows = self._conn.execute(
                "SELECT name, fingerprint, status, cursor, progress, updated_at, dlq_offset "
                "FROM collections"
            ).fetchall()
        except sqlite3.Error as exc:
            raise CheckpointError(f"Could not read the checkpoint: {exc}") from exc
        return {
            name: CollectionState(
                name=name,
                fingerprint=fingerprint,
                status=status,
                cursor=decode_cursor(cursor) if cursor is not None else None,
                progress=Progress.from_json(progress),
                updated_at=updated_at,
                dlq_offset=int(dlq_offset),
            )
            for name, fingerprint, status, cursor, progress, updated_at, dlq_offset in rows
        }

    def save_progress(
        self,
        name: str,
        fingerprint: str,
        cursor: Cursor,
        progress: Progress,
        dlq_offset: int = 0,
    ) -> None:
        """Record that everything up to `cursor` is safely written."""
        self._execute(
            _UPSERT,
            (
                name, fingerprint, "in_progress", encode_cursor(cursor),
                progress.to_json(), _now(), dlq_offset,
            ),
        )

    def mark_done(
        self, name: str, fingerprint: str, progress: Progress, dlq_offset: int = 0
    ) -> None:
        self._execute(
            _UPSERT, (name, fingerprint, "done", None, progress.to_json(), _now(), dlq_offset)
        )

    def discard(self, name: str) -> None:
        self._execute("DELETE FROM collections WHERE name = ?", (name,))

    def clear(self) -> None:
        self._execute("DELETE FROM collections")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> CheckpointStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


# --- one run at a time -----------------------------------------------------------------------


def _lock(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_UN)


class RunLock:
    """An operating-system file lock that keeps two runs of one pipeline apart.

    The OS releases it when the process ends, even after SIGKILL, so a crash
    never leaves a stale lock behind (a PID file would).
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            _lock(fd)
        except OSError as exc:
            os.close(fd)
            raise CheckpointError(
                "Another run of this pipeline is already in progress "
                f"(lock file: {self.path}). Wait for it to finish; the lock is "
                "released automatically if that process dies."
            ) from exc
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            _unlock(self._fd)
        except OSError:
            pass  # closing the file releases the lock anyway
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> RunLock:
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()