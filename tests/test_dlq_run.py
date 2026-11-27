from typer.testing import CliRunner

from polymorph.cli import app

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from polymorph.checkpoint import CheckpointStore, checkpoint_path
from polymorph.dlq import DeadLetterError, read_entries
from polymorph.pipeline import PipelineError, run_pipeline

pytestmark = pytest.mark.integration  # needs MongoDB; skipped automatically if it is down

ITEM_5_OWNER = (
    "SELECT o.customer_id FROM order_items i JOIN orders o ON o.id = i.order_id WHERE i.id = 5"
)
QUANTITY_RULE = {
    "order_items": [
        {"type": "require", "expression": "quantity > 0", "message": "quantity must be positive"}
    ]
}
SKIP = {"on_row_error": "skip"}


class SimulatedCrash(Exception):
    pass


def crash_at(stage, collection, number):
    def hook(event_stage, event_collection, event_number):
        if (event_stage, event_collection, event_number) == (stage, collection, number):
            raise SimulatedCrash(f"{stage} {collection} batch {number}")

    return hook


def entries(path):
    return list(read_entries(path))


def scalar(url, sql):
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar_one()
    finally:
        engine.dispose()


def run_sql(url, sql):
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text(sql))
    engine.dispose()


def make_sqlite(tmp_path, name, *statements):
    path = tmp_path / name
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    engine.dispose()
    return f"sqlite:///{path.as_posix()}"


# --- what is recorded ---------------------------------------------------------------------------


def test_a_rejected_document_is_recorded_with_its_cause(
    shop_url, mongo_target, make_manifest, folding_spec, tmp_path
):
    target_url, _ = mongo_target
    dlq = tmp_path / "dlq.jsonl"
    manifest = make_manifest(
        shop_url, target_url, collections=folding_spec, transforms=QUANTITY_RULE, settings=SKIP
    )

    result = run_pipeline(manifest, dlq_path=dlq).tables[0]
    owner = scalar(shop_url, ITEM_5_OWNER)

    (entry,) = entries(dlq)
    assert result.rejected == 1
    assert entry["collection"] == "customers"
    assert entry["document"] == {"table": "customers", "key": {"id": owner}}
    assert entry["cause"] == {
        "stage": "transform",
        "table": "order_items",
        "key": {"id": 5},
        "reason": "quantity must be positive",
    }
    assert "document rejected" in entry["message"]
    assert "row" not in entry


def test_every_poison_row_of_a_flat_run_is_recorded(
    shop_url, mongo_target, make_manifest, masking_rules, tmp_path
):
    target_url, _ = mongo_target
    dlq = tmp_path / "dlq.jsonl"
    manifest = make_manifest(shop_url, target_url, transforms=masking_rules, settings=SKIP)

    summary = run_pipeline(manifest, dlq_path=dlq)

    recorded = {
        (e["cause"]["table"], e["cause"]["key"]["id"], e["cause"]["reason"]) for e in entries(dlq)
    }
    assert recorded == {
        ("customers", 13, "email: not a valid email address"),
        ("customers", 27, "customer email is required"),
        ("customers", 41, "customer name must not be blank"),
        ("order_items", 5, "quantity must be positive"),
    }
    assert len(entries(dlq)) == summary.rejected == 4


def test_row_data_is_only_written_when_requested(
    shop_url, mongo_target, make_manifest, masking_rules, tmp_path
):
    target_url, _ = mongo_target
    dlq = tmp_path / "dlq.jsonl"

    plain = make_manifest(shop_url, target_url, transforms=masking_rules, settings=SKIP)
    run_pipeline(plain, dlq_path=dlq)
    assert "not-an-email" not in dlq.read_text(encoding="utf-8")

    with_rows = make_manifest(
        shop_url,
        target_url,
        transforms=masking_rules,
        settings={**SKIP, "dlq_include_rows": True},
    )
    run_pipeline(with_rows, dlq_path=dlq)
    row = next(e["row"] for e in entries(dlq) if e["cause"]["key"] == {"id": 13})
    assert row["email"] == "not-an-email"


def test_type_errors_are_dead_lettered_under_skip(tmp_path, mongo_target, make_manifest):
    target_url, db = mongo_target
    url = make_sqlite(
        tmp_path,
        "typeless.db",
        "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)",
        "INSERT INTO items VALUES (1, 5), (2, 'abc')",
    )
    dlq = tmp_path / "dlq.jsonl"

    summary = run_pipeline(make_manifest(url, target_url, settings=SKIP), dlq_path=dlq)

    (entry,) = entries(dlq)
    assert summary.rejected == 1
    assert entry["cause"]["stage"] == "translate"
    assert entry["cause"]["table"] == "items"
    assert entry["cause"]["key"] == {"id": 2}
    assert "items.qty" in entry["cause"]["reason"]
    assert "abc" not in dlq.read_text(encoding="utf-8")
    assert db.items.count_documents({}) == 1


def test_the_row_that_stops_a_run_is_recorded_too(
    shop_url, mongo_target, make_manifest, masking_rules, tmp_path
):
    target_url, _ = mongo_target
    dlq = tmp_path / "dlq.jsonl"
    manifest = make_manifest(shop_url, target_url, transforms=masking_rules)  # on_row_error: fail

    with pytest.raises(PipelineError, match="on_row_error"):
        run_pipeline(manifest, dlq_path=dlq)

    (entry,) = entries(dlq)
    assert entry["cause"]["key"] == {"id": 13}


# --- lifecycle -----------------------------------------------------------------------------------


def test_a_rerun_rewrites_the_file_instead_of_adding_to_it(
    shop_url, mongo_target, make_manifest, masking_rules, tmp_path
):
    target_url, _ = mongo_target
    dlq = tmp_path / "dlq.jsonl"
    manifest = make_manifest(shop_url, target_url, transforms=masking_rules, settings=SKIP)

    run_pipeline(manifest, dlq_path=dlq)
    run_pipeline(manifest, dlq_path=dlq)

    assert len(entries(dlq)) == 4


def test_fixing_the_source_and_rerunning_leaves_an_empty_file(
    shop_url, mongo_target, make_manifest, folding_spec, tmp_path
):
    target_url, db = mongo_target
    dlq = tmp_path / "dlq.jsonl"
    manifest = make_manifest(
        shop_url, target_url, collections=folding_spec, transforms=QUANTITY_RULE, settings=SKIP
    )
    run_pipeline(manifest, dlq_path=dlq)
    assert len(entries(dlq)) == 1

    run_sql(shop_url, "UPDATE order_items SET quantity = 3 WHERE id = 5")
    result = run_pipeline(manifest, dlq_path=dlq).tables[0]

    assert dlq.stat().st_size == 0
    assert result.created == 1
    assert db.customers.count_documents({}) == 50


# --- crash safety ----------------------------------------------------------------------------------


def folded_manifest(shop_url, target_url, make_manifest, folding_spec):
    return make_manifest(
        shop_url, target_url, collections=folding_spec, transforms=QUANTITY_RULE, settings=SKIP
    )


def test_a_crash_between_the_write_and_the_checkpoint_does_not_duplicate_entries(
    shop_url, mongo_target, make_manifest, folding_spec, tmp_path
):
    target_url, _ = mongo_target
    state, dlq = tmp_path / "state", tmp_path / "dlq.jsonl"
    manifest = folded_manifest(shop_url, target_url, make_manifest, folding_spec)
    owners_batch = (scalar(shop_url, ITEM_5_OWNER) - 1) // 7 + 1  # batch_size is 7

    with pytest.raises(SimulatedCrash):
        run_pipeline(
            manifest, state_dir=state, dlq_path=dlq,
            on_batch=crash_at("written", "customers", owners_batch),
        )
    assert len(entries(dlq)) == 1  # the entry is on disk, the checkpoint is not

    summary = run_pipeline(manifest, state_dir=state, dlq_path=dlq)

    assert len(entries(dlq)) == 1  # the repeated batch did not add a second copy
    assert summary.rejected == 1


def test_a_crash_after_the_checkpoint_keeps_the_entry_and_the_count(
    shop_url, mongo_target, make_manifest, folding_spec, tmp_path
):
    target_url, _ = mongo_target
    state, dlq = tmp_path / "state", tmp_path / "dlq.jsonl"
    manifest = folded_manifest(shop_url, target_url, make_manifest, folding_spec)
    owners_batch = (scalar(shop_url, ITEM_5_OWNER) - 1) // 7 + 1

    with pytest.raises(SimulatedCrash):
        run_pipeline(
            manifest, state_dir=state, dlq_path=dlq,
            on_batch=crash_at("checkpointed", "customers", owners_batch),
        )
    with CheckpointStore(checkpoint_path(state, "test-pipeline")) as store:
        assert store.load()["customers"].dlq_offset == dlq.stat().st_size > 0

    summary = run_pipeline(manifest, state_dir=state, dlq_path=dlq)

    assert len(entries(dlq)) == 1
    assert summary.rejected == 1


def test_a_dead_letter_file_that_lost_data_is_refused_on_resume(
    shop_url, mongo_target, make_manifest, folding_spec, tmp_path
):
    target_url, _ = mongo_target
    state, dlq = tmp_path / "state", tmp_path / "dlq.jsonl"
    manifest = folded_manifest(shop_url, target_url, make_manifest, folding_spec)
    owners_batch = (scalar(shop_url, ITEM_5_OWNER) - 1) // 7 + 1
    with pytest.raises(SimulatedCrash):
        run_pipeline(
            manifest, state_dir=state, dlq_path=dlq,
            on_batch=crash_at("checkpointed", "customers", owners_batch),
        )

    dlq.write_bytes(b"")  # somebody emptied the file

    with pytest.raises(DeadLetterError, match="--fresh"):
        run_pipeline(manifest, state_dir=state, dlq_path=dlq)


def test_dead_letters_of_finished_collections_survive_a_resume(
    shop_url, mongo_target, make_manifest, masking_rules, tmp_path
):
    target_url, _ = mongo_target
    state, dlq = tmp_path / "state", tmp_path / "dlq.jsonl"
    manifest = make_manifest(shop_url, target_url, transforms=masking_rules, settings=SKIP)

    # customers (3 rejects) is done; order_items (1 reject, item 5) is checkpointed after batch 1.
    with pytest.raises(SimulatedCrash):
        run_pipeline(
            manifest, state_dir=state, dlq_path=dlq,
            on_batch=crash_at("checkpointed", "order_items", 1),
        )

    summary = run_pipeline(manifest, state_dir=state, dlq_path=dlq)

    assert len(entries(dlq)) == 4
    assert summary.rejected == 4


def test_a_restarted_collection_does_not_leave_stale_entries(
    shop_url, mongo_target, make_manifest, masking_rules, tmp_path
):
    target_url, _ = mongo_target
    state, dlq = tmp_path / "state", tmp_path / "dlq.jsonl"
    manifest = make_manifest(shop_url, target_url, transforms=masking_rules, settings=SKIP)
    with pytest.raises(SimulatedCrash):  # customer 13 is rejected in batch 2 and checkpointed
        run_pipeline(
            manifest, state_dir=state, dlq_path=dlq,
            on_batch=crash_at("checkpointed", "customers", 3),
        )
    assert len(entries(dlq)) == 1

    changed_rules = {
        **masking_rules,
        "customers": [
            *masking_rules["customers"],
            {"type": "compute", "into": "marker", "expression": "1"},
        ],
    }
    changed = make_manifest(shop_url, target_url, transforms=changed_rules, settings=SKIP)
    notices = []
    run_pipeline(changed, state_dir=state, dlq_path=dlq, on_notice=notices.append)

    assert any("restarts from the beginning" in notice for notice in notices)
    assert len(entries(dlq)) == 4  # the three customers and item 5, each exactly once

FOLDED_EXAMPLE = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "pipelines"
    / "postgres-to-mongo-folded.yaml"
)


def test_cli_run_reports_the_dead_letter_file_and_dlq_summarises_it(
    shop_url, mongo_target, tmp_path
):
    target_url, _ = mongo_target
    env = {"SOURCE_URL": shop_url, "TARGET_URL": target_url}
    state = str(tmp_path / "state")

    run_result = CliRunner().invoke(
        app, ["run", str(FOLDED_EXAMPLE), "--state-dir", state], env=env
    )

    assert run_result.exit_code == 3
    assert "dlq_errors.jsonl" in run_result.output
    assert "1 entry" in run_result.output

    dlq_result = CliRunner().invoke(
        app, ["dlq", str(FOLDED_EXAMPLE), "--state-dir", state], env=env
    )

    assert dlq_result.exit_code == 0
    assert "order_items" in dlq_result.output
    assert "quantity must be positive" in dlq_result.output