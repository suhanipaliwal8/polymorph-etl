import os
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, text

from polymorph.checkpoint import CheckpointStore, checkpoint_path
from polymorph.inspector import inspect_database
from polymorph.pipeline import run_pipeline

pytestmark = pytest.mark.integration  # needs MongoDB; skipped automatically if it is down


class SimulatedCrash(Exception):
    pass


def crash_at(stage, collection, number):
    """A hook for run_pipeline(on_batch=...) that 'crashes' at one exact point."""

    def hook(event_stage, event_collection, event_number):
        if (event_stage, event_collection, event_number) == (stage, collection, number):
            raise SimulatedCrash(f"{stage} {collection} batch {number}")

    return hook


def saved_state(state_dir, pipeline="test-pipeline"):
    with CheckpointStore(checkpoint_path(state_dir, pipeline)) as store:
        return store.load()


def source_scalar(url, sql):
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar_one()
    finally:
        engine.dispose()


# batch_size is 7 in these tests: 50 customers make 8 batches (ids 1-7, 8-14, ...).


def test_a_crash_between_the_write_and_the_checkpoint_loses_nothing_and_duplicates_nothing(
    shop_url, mongo_target, make_manifest, tmp_path
):
    target_url, db = mongo_target
    state = tmp_path / "state"
    manifest = make_manifest(shop_url, target_url, tables=["customers"])

    with pytest.raises(SimulatedCrash):
        run_pipeline(manifest, state_dir=state, on_batch=crash_at("written", "customers", 3))

    # Batches 1-3 are in MongoDB, but only batches 1-2 are checkpointed.
    assert db.customers.count_documents({}) == 21
    saved = saved_state(state)["customers"]
    assert saved.status == "in_progress"
    assert saved.cursor == (14,)

    notices = []
    result = run_pipeline(manifest, state_dir=state, on_notice=notices.append).tables[0]

    assert result.resumed is True
    assert any("resuming" in notice for notice in notices)
    assert db.customers.count_documents({}) == 50
    # Batch 3 was written twice; the repeat is reported as unchanged, never as a duplicate.
    assert (result.rows_read, result.created, result.updated, result.unchanged) == (50, 43, 0, 7)


def test_a_crash_right_after_a_checkpoint_continues_with_the_next_batch(
    shop_url, mongo_target, make_manifest, tmp_path
):
    target_url, db = mongo_target
    state = tmp_path / "state"
    manifest = make_manifest(shop_url, target_url, tables=["customers"])

    with pytest.raises(SimulatedCrash):
        run_pipeline(manifest, state_dir=state, on_batch=crash_at("checkpointed", "customers", 3))
    assert saved_state(state)["customers"].cursor == (21,)

    result = run_pipeline(manifest, state_dir=state).tables[0]

    assert db.customers.count_documents({}) == 50
    assert (result.rows_read, result.created, result.updated, result.unchanged) == (50, 50, 0, 0)


def test_a_completed_run_leaves_no_checkpoint_and_the_next_run_is_a_full_pass(
    shop_url, mongo_target, make_manifest, tmp_path
):
    target_url, _ = mongo_target
    state = tmp_path / "state"
    manifest = make_manifest(shop_url, target_url, tables=["customers"])

    run_pipeline(manifest, state_dir=state)
    assert saved_state(state) == {}

    second = run_pipeline(manifest, state_dir=state).tables[0]
    assert second.resumed is False
    assert (second.created, second.unchanged) == (0, 50)


def test_a_changed_configuration_restarts_the_collection(
    shop_url, mongo_target, make_manifest, tmp_path
):
    target_url, db = mongo_target
    state = tmp_path / "state"
    manifest = make_manifest(shop_url, target_url, tables=["customers"])
    with pytest.raises(SimulatedCrash):
        run_pipeline(manifest, state_dir=state, on_batch=crash_at("checkpointed", "customers", 3))

    changed = make_manifest(
        shop_url,
        target_url,
        tables=["customers"],
        transforms={"customers": [{"type": "compute", "into": "marker", "expression": "1"}]},
    )
    notices = []
    result = run_pipeline(changed, state_dir=state, on_notice=notices.append).tables[0]

    assert any("restarts from the beginning" in notice for notice in notices)
    assert result.resumed is False
    # The 21 documents written before the crash are rewritten with the new field.
    assert (result.rows_read, result.created, result.updated) == (50, 29, 21)
    assert db.customers.find_one({"_id": 1})["marker"] == 1


def test_fresh_ignores_the_saved_progress(shop_url, mongo_target, make_manifest, tmp_path):
    target_url, _ = mongo_target
    state = tmp_path / "state"
    manifest = make_manifest(shop_url, target_url, tables=["customers"])
    with pytest.raises(SimulatedCrash):
        run_pipeline(manifest, state_dir=state, on_batch=crash_at("checkpointed", "customers", 3))

    result = run_pipeline(manifest, state_dir=state, fresh=True).tables[0]

    assert result.resumed is False
    assert (result.created, result.unchanged) == (29, 21)


def test_collections_finished_before_the_crash_are_skipped(
    shop_url, mongo_target, make_manifest, tmp_path
):
    target_url, db = mongo_target
    state = tmp_path / "state"
    manifest = make_manifest(shop_url, target_url, tables=["customers", "customer_profiles"])
    with pytest.raises(SimulatedCrash):
        run_pipeline(
            manifest, state_dir=state, on_batch=crash_at("written", "customer_profiles", 2)
        )

    customers, profiles = run_pipeline(manifest, state_dir=state).tables

    assert customers.skipped is True
    assert (customers.rows_read, customers.created) == (50, 50)
    assert profiles.resumed is True
    assert profiles.rows_read == 45
    assert db.customer_profiles.count_documents({}) == 45


def test_rejections_before_a_crash_still_count_after_the_resume(
    shop_url, mongo_target, make_manifest, folding_spec, tmp_path
):
    target_url, db = mongo_target
    state = tmp_path / "state"
    owner = source_scalar(
        shop_url,
        "SELECT o.customer_id FROM order_items i JOIN orders o ON o.id = i.order_id WHERE i.id = 5",
    )
    rule = {
        "order_items": [
            {"type": "require", "expression": "quantity > 0", "message": "quantity must be positive"}
        ]
    }
    manifest = make_manifest(
        shop_url,
        target_url,
        collections=folding_spec,
        transforms=rule,
        settings={"on_row_error": "skip"},
    )
    owners_batch = (owner - 1) // 7 + 1
    with pytest.raises(SimulatedCrash):
        run_pipeline(
            manifest, state_dir=state, on_batch=crash_at("checkpointed", "customers", owners_batch)
        )

    summary = run_pipeline(manifest, state_dir=state)

    # The rejection happened before the crash, yet the final run still knows about it.
    assert summary.rejected == 1
    assert summary.tables[0].embedded[-1].rejected == 1
    assert db.customers.count_documents({}) == 49


def test_a_killed_process_resumes_from_its_checkpoint(shop_url, mongo_target, tmp_path):
    """A real crash: the process ends with no cleanup at all (like kill -9)."""
    target_url, db = mongo_target
    state = tmp_path / "state"
    manifest_file = tmp_path / "pipeline.yaml"
    manifest_file.write_text(
        "version: 1\n"
        "name: crash-demo\n"
        "mode: sql-to-nosql\n"
        "source:\n  url: ${SOURCE_URL}\n"
        "target:\n  url: ${TARGET_URL}\n"
        "settings:\n  batch_size: 7\n"
        "tables:\n  - customers\n  - orders\n",
        encoding="utf-8",
    )
    env = {**os.environ, "SOURCE_URL": shop_url, "TARGET_URL": target_url}
    command = [
        sys.executable, "-m", "polymorph", "run", str(manifest_file), "--state-dir", str(state)
    ]

    crashed = subprocess.run(
        [*command, "--crash-at", "written:3"], env=env, cwd=tmp_path, capture_output=True, text=True
    )
    assert crashed.returncode == 137
    assert "simulated crash" in crashed.stderr
    assert saved_state(state, "crash-demo")["customers"].cursor == (14,)

    resumed = subprocess.run(command, env=env, cwd=tmp_path, capture_output=True, text=True)

    assert resumed.returncode == 0, resumed.stderr
    assert "resuming" in resumed.stdout
    expected = {table.name: table.row_count for table in inspect_database(shop_url).tables}
    assert db.customers.count_documents({}) == expected["customers"]
    assert db.orders.count_documents({}) == expected["orders"]
    assert saved_state(state, "crash-demo") == {}