import datetime as dt
import json
import uuid
from decimal import Decimal

import pytest

from polymorph.dlq import (
    DeadLetterError,
    DeadLetterWriter,
    Rejection,
    dead_letter_path,
    describe_entry,
    format_entry,
    read_entries,
    summarize,
)

NOW = dt.datetime(2026, 11, 26, 10, 15, tzinfo=dt.timezone.utc)


def rejection(**overrides):
    values = {
        "stage": "transform",
        "table": "order_items",
        "key": {"id": 5},
        "reason": "quantity must be positive",
        "message": "order_items: quantity must be positive (row id=5)",
        "row": {"id": 5, "quantity": -3},
    }
    values.update(overrides)
    return Rejection(**values)


def entry(cause=None, **kwargs):
    line = format_entry("shop", "customers", "customers", {"id": 17}, cause or rejection(), now=NOW, **kwargs)
    return json.loads(line)


# --- the entry format ----------------------------------------------------------------------


def test_an_entry_names_the_document_and_the_cause():
    result = entry()

    assert result["timestamp"] == "2026-11-26T10:15:00Z"
    assert result["pipeline"] == "shop"
    assert result["collection"] == "customers"
    assert result["document"] == {"table": "customers", "key": {"id": 17}}
    assert result["cause"] == {
        "stage": "transform",
        "table": "order_items",
        "key": {"id": 5},
        "reason": "quantity must be positive",
    }
    assert result["message"].startswith("order_items:")


def test_row_data_is_left_out_unless_requested():
    assert "row" not in entry()
    assert entry(include_row=True)["row"] == {"id": 5, "quantity": -3}


def test_values_that_json_cannot_hold_are_converted():
    cause = rejection(
        key={
            "u": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "d": Decimal("1.50"),
            "t": dt.datetime(2024, 1, 1, 12, 0),
            "b": b"\x00\xff",
        }
    )

    key = entry(cause)["cause"]["key"]

    assert key == {
        "u": "12345678-1234-5678-1234-567812345678",
        "d": "1.50",
        "t": "2024-01-01T12:00:00",
        "b": "00ff",
    }


def test_entries_stay_pure_ascii_even_for_hostile_text():
    cause = rejection(reason="bad \ud800 text, हिन्दी", row={"name": "\ud800 नाम"})

    line = format_entry("shop", "customers", "customers", {"id": 1}, cause, include_row=True, now=NOW)

    assert line.isascii()
    assert "\n" not in line
    assert json.loads(line)["cause"]["reason"].endswith("हिन्दी")


def test_describe_entry_gives_a_one_line_summary():
    text = describe_entry(entry())
    assert text == "customers id=17 <- order_items id=5 [transform]: quantity must be positive"


def test_describe_entry_survives_malformed_entries():
    assert "?" in describe_entry({"document": "nonsense"})


# --- the writer -------------------------------------------------------------------------------


def test_lines_are_appended_and_the_size_matches_the_bytes(tmp_path):
    writer = DeadLetterWriter(tmp_path / "state" / "p.jsonl", "p")
    assert writer.size() == 0

    writer.append(['{"a": 1}', '{"b": 2}'])
    writer.append([])

    assert writer.size() == len(b'{"a": 1}\n{"b": 2}\n')
    assert writer.path.read_bytes() == b'{"a": 1}\n{"b": 2}\n'


def test_opening_an_existing_file_keeps_its_content(tmp_path):
    path = tmp_path / "p.jsonl"
    path.write_bytes(b'{"keep": 1}\n')

    DeadLetterWriter(path, "p")

    assert path.read_bytes() == b'{"keep": 1}\n'


def test_truncate_discards_everything_after_an_offset(tmp_path):
    writer = DeadLetterWriter(tmp_path / "p.jsonl", "p")
    writer.append(['{"a": 1}'])
    offset = writer.size()
    writer.append(['{"b": 2}', '{"c": 3}'])

    writer.truncate(offset)

    assert writer.path.read_bytes() == b'{"a": 1}\n'
    writer.truncate(0)
    assert writer.size() == 0


def test_a_file_shorter_than_the_checkpoint_expects_is_refused(tmp_path):
    writer = DeadLetterWriter(tmp_path / "p.jsonl", "p")
    writer.append(['{"a": 1}'])

    with pytest.raises(DeadLetterError, match="--fresh"):
        writer.require(writer.size() + 1)
    with pytest.raises(DeadLetterError, match="--fresh"):
        writer.truncate(writer.size() + 1)


def test_an_unwritable_path_is_reported(tmp_path):
    with pytest.raises(DeadLetterError, match="Cannot open"):
        DeadLetterWriter(tmp_path, "p")  # a directory, not a file


def test_the_default_path_belongs_to_the_pipeline(tmp_path):
    assert dead_letter_path(tmp_path, "shop") == tmp_path / "shop.dlq_errors.jsonl"


# --- reading and summarising ----------------------------------------------------------------------


def test_reading_tolerates_a_torn_last_line_and_blank_lines(tmp_path):
    path = tmp_path / "p.jsonl"
    path.write_bytes(b'{"collection": "c"}\n\n[1, 2]\n{"collection": "d"')

    assert list(read_entries(path)) == [{"collection": "c"}, None, None]


def test_the_summary_groups_by_collection_stage_table_and_reason(tmp_path):
    path = tmp_path / "p.jsonl"
    other = rejection(stage="translate", table="items", key={"id": 2}, reason="items.qty: expected an integer")
    lines = [
        format_entry("shop", "customers", "customers", {"id": 17}, rejection(), now=NOW),
        format_entry("shop", "customers", "customers", {"id": 18}, rejection(), now=NOW),
        format_entry("shop", "items", "items", {"id": 2}, other, now=NOW),
    ]
    path.write_text("\n".join(lines) + "\n{torn", encoding="utf-8")

    summary = summarize(path, limit=2)

    assert summary.entries == 3
    assert summary.unreadable == 1
    assert summary.groups[0] == (("customers", "transform", "order_items", "quantity must be positive"), 2)
    assert len(summary.groups) == 2
    assert len(summary.first) == 2


def test_summarising_an_empty_file(tmp_path):
    path = tmp_path / "p.jsonl"
    path.write_bytes(b"")
    summary = summarize(path)
    assert (summary.entries, summary.unreadable, summary.groups) == (0, 0, ())