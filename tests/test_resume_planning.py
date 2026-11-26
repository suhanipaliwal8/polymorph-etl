import pytest
from sqlalchemy import create_engine, text

from polymorph.checkpoint import CheckpointError, RunLock, lock_path
from polymorph.inspector import create_source_engine
from polymorph.pipeline import collection_fingerprint, plan_collection, run_pipeline


def plan_for(url, manifest, name="customers"):
    engine = create_source_engine(url)
    try:
        return plan_collection(engine, name, manifest)
    finally:
        engine.dispose()


def fingerprint(url, manifest, name="customers"):
    engine = create_source_engine(url)
    try:
        return collection_fingerprint(manifest, plan_collection(engine, name, manifest))
    finally:
        engine.dispose()


def test_the_fingerprint_is_stable(shop_url, make_manifest):
    manifest = make_manifest(shop_url)
    first = fingerprint(shop_url, manifest)
    assert first == fingerprint(shop_url, manifest)
    assert len(first) == 64


def test_tuning_settings_do_not_change_the_fingerprint(shop_url, make_manifest):
    base = fingerprint(shop_url, make_manifest(shop_url))
    tuned = make_manifest(shop_url, settings={"batch_size": 50, "on_row_error": "skip"})
    assert fingerprint(shop_url, tuned) == base


def test_credentials_are_not_part_of_the_fingerprint_but_the_target_database_is(
    shop_url, make_manifest
):
    one = make_manifest(shop_url, target_url="mongodb://user:one@localhost:27018/shop")
    two = make_manifest(shop_url, target_url="mongodb://user:two@localhost:27018/shop")
    other = make_manifest(shop_url, target_url="mongodb://user:one@localhost:27018/other")

    assert fingerprint(shop_url, one) == fingerprint(shop_url, two)
    assert fingerprint(shop_url, one) != fingerprint(shop_url, other)


def test_transform_rules_of_the_collection_change_the_fingerprint(shop_url, make_manifest):
    base = fingerprint(shop_url, make_manifest(shop_url))
    changed = make_manifest(
        shop_url, transforms={"customers": [{"type": "drop", "fields": ["email"]}]}
    )
    assert fingerprint(shop_url, changed) != base


def test_rules_of_unrelated_tables_do_not_change_the_fingerprint(shop_url, make_manifest):
    base = fingerprint(shop_url, make_manifest(shop_url))
    other = make_manifest(shop_url, transforms={"orders": [{"type": "drop", "fields": ["status"]}]})
    assert fingerprint(shop_url, other) == base


def test_a_changed_salt_changes_the_fingerprint(shop_url, make_manifest):
    def with_salt(salt):
        rules = [{"type": "hash", "field": "email", "into": "email_hash", "salt": salt}]
        return make_manifest(shop_url, transforms={"customers": rules})

    assert fingerprint(shop_url, with_salt("salt-number-one")) != fingerprint(
        shop_url, with_salt("salt-number-two")
    )


def test_type_overrides_and_the_time_zone_change_the_fingerprint(shop_url, make_manifest):
    base = fingerprint(shop_url, make_manifest(shop_url))
    override = make_manifest(shop_url, type_overrides={"customers.external_id": "uuid"})
    zone = make_manifest(shop_url, settings={"naive_timestamp_timezone": "Asia/Kolkata"})

    assert fingerprint(shop_url, override) != base
    assert fingerprint(shop_url, zone) != base


def test_the_folding_structure_changes_the_fingerprint(shop_url, make_manifest, folding_spec):
    flat = fingerprint(shop_url, make_manifest(shop_url))
    folded = fingerprint(shop_url, make_manifest(shop_url, collections=folding_spec))
    assert folded != flat

    folding_spec["customers"]["embed"][1]["keep_foreign_key"] = True
    kept = fingerprint(shop_url, make_manifest(shop_url, collections=folding_spec))
    assert kept != folded


def test_a_changed_source_schema_changes_the_fingerprint(shop_url, make_manifest):
    manifest = make_manifest(shop_url)
    before = fingerprint(shop_url, manifest)

    engine = create_engine(shop_url)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE customers ADD COLUMN nickname TEXT"))
    engine.dispose()

    assert fingerprint(shop_url, manifest) != before


def test_an_unsupported_key_type_disables_resume_with_a_note(tmp_path, make_manifest):
    path = tmp_path / "odd.db"
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE odd (k JSON PRIMARY KEY, v TEXT)"))
    engine.dispose()
    url = f"sqlite:///{path.as_posix()}"

    plan = plan_for(url, make_manifest(url), name="odd")

    assert plan.resume_problem is not None
    assert any("cannot resume" in note for note in plan.notes)


def test_a_normal_key_type_can_resume(shop_url, make_manifest):
    plan = plan_for(shop_url, make_manifest(shop_url))
    assert plan.resume_problem is None


def test_a_second_run_of_the_same_pipeline_is_refused(shop_url, make_manifest, tmp_path):
    state = tmp_path / "state"
    manifest = make_manifest(shop_url)

    with RunLock(lock_path(state, manifest.name)):
        with pytest.raises(CheckpointError, match="already in progress"):
            run_pipeline(manifest, state_dir=state)