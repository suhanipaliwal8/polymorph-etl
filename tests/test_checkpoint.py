import datetime as dt
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import JSON, Column, Integer, Uuid
from sqlalchemy.types import NullType

from polymorph.checkpoint import (
    CheckpointError,
    CheckpointStore,
    Progress,
    RunLock,
    checkpoint_path,
    decode_cursor,
    encode_cursor,
    keys_can_be_checkpointed,
    lock_path,
)

# --- cursor codec ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "cursor",
    [
        (5,),
        (2**70,),
        ("abc",),
        ("é",),
        (Decimal("12345678901234.5678"),),
        (1.5,),
        (True,),
        (dt.datetime(2024, 1, 1, 12, 30, 15, 123456),),
        (dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),),
        (dt.date(2024, 5, 1),),
        (dt.time(9, 15),),
        (uuid.UUID("12345678-1234-5678-1234-567812345678"),),
        (b"\x00\xff",),
        (1, "x", Decimal("2.50")),
    ],
)
def test_cursors_come_back_exactly_with_their_types(cursor):
    decoded = decode_cursor(encode_cursor(cursor))
    assert decoded == cursor
    assert [type(value) for value in decoded] == [type(value) for value in cursor]


@pytest.mark.parametrize("value", [object(), (1, 2), {"a": 1}, None])
def test_values_that_cannot_be_saved_are_refused(value):
    with pytest.raises(CheckpointError, match="cannot be saved"):
        encode_cursor((value,))


@pytest.mark.parametrize(
    "text",
    ["not json", '[["int", "x"]]', '[["nope", "1"]]', '[["decimal", "abc"]]', "[1]", '{"a": 1}'],
)
def test_unreadable_cursors_are_reported(text):
    with pytest.raises(CheckpointError, match="unreadable"):
        decode_cursor(text)


def test_key_types_are_checked_from_the_column_types():
    assert keys_can_be_checkpointed([Column("id", Integer), Column("u", Uuid)]) is None
    assert "cannot be saved" in keys_can_be_checkpointed([Column("j", JSON)])
    assert "cannot be saved" in keys_can_be_checkpointed([Column("x", NullType())])


# --- progress -----------------------------------------------------------------------------


def test_progress_round_trips_through_json():
    progress = Progress(
        rows_read=14, filtered=1, rejected=2, created=10, updated=1, unchanged=0,
        embedded={"customers.orders": (30, 2, 1)},
    )
    assert Progress.from_json(progress.to_json()) == progress


def test_unreadable_progress_is_reported():
    with pytest.raises(CheckpointError, match="unreadable"):
        Progress.from_json('{"rows_read": "many"}')


# --- the store ------------------------------------------------------------------------------


def test_paths_are_derived_from_the_pipeline_name():
    assert checkpoint_path(Path("s"), "p") == Path("s") / "p.checkpoint.sqlite"
    assert lock_path(Path("s"), "p") == Path("s") / "p.lock"


def test_progress_survives_closing_and_reopening(tmp_path):
    path = tmp_path / "state" / "p.checkpoint.sqlite"
    progress = Progress(rows_read=14, created=14, embedded={"c.orders": (3, 1, 0)})

    with CheckpointStore(path) as store:
        store.save_progress("customers", "fp1", (14,), progress)
    with CheckpointStore(path) as store:
        state = store.load()["customers"]

    assert state.status == "in_progress"
    assert state.cursor == (14,)
    assert state.fingerprint == "fp1"
    assert state.progress == progress
    assert len(state.updated_at) == 19


def test_saving_again_replaces_the_previous_progress(tmp_path):
    with CheckpointStore(tmp_path / "p.sqlite") as store:
        store.save_progress("customers", "fp", (7,), Progress(rows_read=7))
        store.save_progress("customers", "fp", (14,), Progress(rows_read=14))
        states = store.load()

    assert list(states) == ["customers"]
    assert states["customers"].cursor == (14,)
    assert states["customers"].progress.rows_read == 14


def test_a_finished_collection_is_marked_done_without_a_cursor(tmp_path):
    with CheckpointStore(tmp_path / "p.sqlite") as store:
        store.save_progress("customers", "fp", (7,), Progress(rows_read=7))
        store.mark_done("customers", "fp", Progress(rows_read=50))
        state = store.load()["customers"]

    assert state.status == "done"
    assert state.cursor is None
    assert state.progress.rows_read == 50


def test_collections_can_be_discarded_or_cleared(tmp_path):
    with CheckpointStore(tmp_path / "p.sqlite") as store:
        store.save_progress("a", "fp", (1,), Progress())
        store.save_progress("b", "fp", (1,), Progress())
        store.discard("a")
        assert list(store.load()) == ["b"]
        store.clear()
        assert store.load() == {}


def test_fresh_starts_from_an_empty_file(tmp_path):
    path = tmp_path / "p.sqlite"
    with CheckpointStore(path) as store:
        store.save_progress("a", "fp", (1,), Progress())

    with CheckpointStore(path, fresh=True) as store:
        assert store.load() == {}


def test_a_file_from_a_newer_version_is_refused(tmp_path):
    path = tmp_path / "p.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 99")
    conn.close()

    with pytest.raises(CheckpointError, match="newer version"):
        CheckpointStore(path)


def test_a_corrupt_file_is_reported_and_fresh_replaces_it(tmp_path):
    path = tmp_path / "p.sqlite"
    path.write_bytes(b"this is definitely not a database " * 20)

    with pytest.raises(CheckpointError, match="unreadable"):
        CheckpointStore(path)

    with CheckpointStore(path, fresh=True) as store:
        assert store.load() == {}


# --- the run lock -------------------------------------------------------------------------------


def test_a_held_lock_refuses_a_second_holder_and_is_free_after_release(tmp_path):
    path = tmp_path / "state" / "p.lock"
    first = RunLock(path)
    first.acquire()
    try:
        with pytest.raises(CheckpointError, match="already in progress"):
            RunLock(path).acquire()
    finally:
        first.release()

    with RunLock(path):  # free again
        pass

def test_the_dead_letter_offset_is_saved_with_the_progress(tmp_path):
    path = tmp_path / "p.sqlite"
    with CheckpointStore(path) as store:
        store.save_progress("a", "fp", (1,), Progress(), dlq_offset=120)
        store.mark_done("b", "fp", Progress(), dlq_offset=300)
        store.save_progress("c", "fp", (1,), Progress())
    with CheckpointStore(path) as store:
        states = store.load()

    assert states["a"].dlq_offset == 120
    assert states["b"].dlq_offset == 300
    assert states["c"].dlq_offset == 0


def test_a_version_1_file_is_migrated_and_keeps_its_progress(tmp_path):
    path = tmp_path / "old.sqlite"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE collections (name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, "
        "status TEXT NOT NULL CHECK (status IN ('in_progress', 'done')), cursor TEXT, "
        "progress TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO collections VALUES ('customers', 'fp', 'in_progress', ?, ?, '2026-01-01 00:00:00')",
        ('[["int", "14"]]', Progress(rows_read=14).to_json()),
    )
    conn.execute("PRAGMA user_version = 1")
    conn.commit()
    conn.close()

    with CheckpointStore(path) as store:
        state = store.load()["customers"]
        assert state.cursor == (14,)
        assert state.progress.rows_read == 14
        assert state.dlq_offset == 0
        store.save_progress("customers", "fp", (21,), Progress(rows_read=21), dlq_offset=55)
        assert store.load()["customers"].dlq_offset == 55

    with CheckpointStore(path) as store:  # opening a migrated file again is fine
        assert store.load()["customers"].cursor == (21,)


def test_an_interrupted_migration_can_be_finished(tmp_path):
    path = tmp_path / "half.sqlite"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE collections (name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, "
        "status TEXT NOT NULL CHECK (status IN ('in_progress', 'done')), cursor TEXT, "
        "progress TEXT NOT NULL, updated_at TEXT NOT NULL, dlq_offset INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute("PRAGMA user_version = 1")  # column added, version not yet bumped
    conn.commit()
    conn.close()

    with CheckpointStore(path) as store:
        assert store.load() == {}