import pytest
from sqlalchemy import create_engine, text

from polymorph.extractor import ExtractError, extract_batches
from polymorph.inspector import create_source_engine
from polymorph.pipeline import (
    PipelineError,
    TableResult,
    format_table_row,
    plan_table,
    process_row,
    run_pipeline,
)


def make_sqlite(tmp_path, name, *statements):
    path = tmp_path / name
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    engine.dispose()
    return f"sqlite:///{path.as_posix()}"


def test_sql_to_sql_is_not_available_yet(shop_url, make_manifest):
    manifest = make_manifest(
        shop_url, target_url="mysql+pymysql://u:p@localhost/db", mode="sql-to-sql"
    )
    with pytest.raises(PipelineError, match="not available yet"):
        run_pipeline(manifest)


def test_unknown_table_fails_before_anything_is_written(shop_url, make_manifest):
    manifest = make_manifest(shop_url, tables=["nope"])
    with pytest.raises(ExtractError, match="not found"):
        run_pipeline(manifest)


def test_table_without_primary_key_fails_before_anything_is_written(tmp_path, make_manifest):
    url = make_sqlite(tmp_path, "nopk.db", "CREATE TABLE audit_log (message TEXT)")
    with pytest.raises(ExtractError, match="no primary key"):
        run_pipeline(make_manifest(url))


def test_database_without_tables_is_reported(tmp_path, make_manifest):
    path = tmp_path / "empty.db"
    path.touch()
    with pytest.raises(PipelineError, match="No tables found"):
        run_pipeline(make_manifest(f"sqlite:///{path.as_posix()}"))


def test_table_row_formatting():
    row = format_table_row(
        TableResult("customers", rows_read=1234, created=1000, updated=200, unchanged=34, seconds=1.5)
    )
    assert "customers" in row
    assert "1,234" in row
    assert row.endswith("1.50s")

def test_override_for_an_unknown_column_fails_before_anything_is_written(
    shop_url, make_manifest
):
    manifest = make_manifest(shop_url, type_overrides={"customers.nope": "uuid"})
    with pytest.raises(PipelineError, match="unknown column"):
        run_pipeline(manifest)


def test_override_for_a_table_that_is_not_migrated_is_rejected(shop_url, make_manifest):
    manifest = make_manifest(shop_url, type_overrides={"ghost.id": "uuid"})
    with pytest.raises(PipelineError, match="not being migrated"):
        run_pipeline(manifest)

def load_rows(url, table):
    engine = create_source_engine(url)
    try:
        return {
            row[next(iter(row))]: row
            for batch in extract_batches(engine, table, batch_size=100)
            for row in batch.rows
        }
    finally:
        engine.dispose()


def test_a_transform_with_an_unknown_field_fails_before_anything_is_written(
    shop_url, make_manifest
):
    rules = {"customers": [{"type": "mask", "field": "emial", "style": "email"}]}
    with pytest.raises(PipelineError, match="did you mean 'email'"):
        run_pipeline(make_manifest(shop_url, transforms=rules))


def test_transforms_for_a_table_that_is_not_migrated_are_rejected(shop_url, make_manifest):
    rules = {"ghost": [{"type": "drop", "fields": ["x"]}]}
    with pytest.raises(PipelineError, match="not being migrated"):
        run_pipeline(make_manifest(shop_url, transforms=rules))


def test_a_non_key_id_column_must_be_renamed(tmp_path, make_manifest):
    url = make_sqlite(tmp_path, "legacy.db", "CREATE TABLE t (pk INTEGER PRIMARY KEY, _id TEXT)")
    with pytest.raises(PipelineError, match="_id"):
        run_pipeline(make_manifest(url))


@pytest.fixture()
def customer_plan(shop_url, make_manifest, masking_rules):
    manifest = make_manifest(shop_url, transforms=masking_rules)
    engine = create_source_engine(shop_url)
    try:
        plan = plan_table(engine, "customers", manifest)
    finally:
        engine.dispose()
    return plan, load_rows(shop_url, "customers")


def test_a_valid_customer_is_masked_hashed_and_renamed(customer_plan):
    plan, rows = customer_plan

    document = process_row(plan, rows[1]).document

    assert document["_id"] == 1
    assert "id" not in document
    assert "is_active" not in document
    assert isinstance(document["active"], bool)
    assert "***" in document["email"]
    assert len(document["email_hash"]) == 64


@pytest.mark.parametrize(
    ("customer_id", "reason"),
    [
        (13, "not a valid email"),
        (27, "customer email is required"),
        (41, "customer name must not be blank"),
    ],
)
def test_poison_customers_are_rejected_with_a_reason(customer_plan, customer_id, reason):
    plan, rows = customer_plan

    outcome = process_row(plan, rows[customer_id])

    assert outcome.document is None
    assert reason in outcome.rejection
    assert f"id={customer_id}" in outcome.rejection


def test_rejection_messages_never_contain_the_data(customer_plan):
    plan, rows = customer_plan
    assert "not-an-email" not in process_row(plan, rows[13]).rejection


def test_filtered_rows_are_dropped_without_being_rejected(shop_url, make_manifest, masking_rules):
    manifest = make_manifest(
        shop_url, transforms={"customer_profiles": masking_rules["customer_profiles"]}
    )
    engine = create_source_engine(shop_url)
    try:
        plan = plan_table(engine, "customer_profiles", manifest)
    finally:
        engine.dispose()
    rows = load_rows(shop_url, "customer_profiles")

    outcomes = [process_row(plan, row) for row in rows.values()]

    dropped = [outcome for outcome in outcomes if outcome.document is None]
    assert dropped
    assert all(outcome.rejection is None for outcome in dropped)
    assert len(dropped) == sum(1 for row in rows.values() if row["loyalty_tier"] == "bronze")