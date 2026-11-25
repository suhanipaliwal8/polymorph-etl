from datetime import datetime, timedelta
from pathlib import Path
import uuid

from sqlalchemy import create_engine, text

from polymorph.pipeline import PipelineError
import pytest
from bson import Decimal128
from typer.testing import CliRunner

from polymorph.cli import app
from polymorph.inspector import inspect_database
from polymorph.pipeline import run_pipeline

pytestmark = pytest.mark.integration  # needs MongoDB; skipped automatically if it is down

EXAMPLE = (
    Path(__file__).resolve().parent.parent / "examples" / "pipelines" / "postgres-to-mongo.yaml"
)


def source_counts(url):
    return {table.name: table.row_count for table in inspect_database(url).tables}


def test_every_table_is_copied(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target

    summary = run_pipeline(make_manifest(shop_url, target_url))

    expected = source_counts(shop_url)
    for name, count in expected.items():
        assert db[name].count_documents({}) == count
    assert summary.rows_read == sum(expected.values())
    assert all(table.created == table.rows_read for table in summary.tables)


def test_document_shape(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    run_pipeline(make_manifest(shop_url, target_url))

    customer = db.customers.find_one({"_id": 1})
    assert "id" not in customer  # the primary key lives in _id only
    assert isinstance(customer["external_id"], str)
    assert isinstance(customer["created_at"], datetime)
    assert isinstance(customer["is_active"], bool)

    order = db.orders.find_one()
    assert isinstance(order["total_amount"], Decimal128)


def test_poison_records_are_copied_as_they_are(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    run_pipeline(make_manifest(shop_url, target_url))

    assert db.customers.find_one({"_id": 13})["email"] == "not-an-email"
    assert db.customers.find_one({"_id": 27})["email"] is None


def test_running_twice_creates_no_duplicates(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    manifest = make_manifest(shop_url, target_url)

    run_pipeline(manifest)
    second = run_pipeline(manifest)

    for name, count in source_counts(shop_url).items():
        assert db[name].count_documents({}) == count
    assert all(table.created == 0 for table in second.tables)
    assert all(table.updated + table.unchanged == table.rows_read for table in second.tables)


def test_rerun_removes_stale_fields(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    manifest = make_manifest(shop_url, target_url)
    run_pipeline(manifest)
    db.customers.update_one({"_id": 1}, {"$set": {"stale": True}})

    run_pipeline(manifest)

    assert "stale" not in db.customers.find_one({"_id": 1})


def test_rerun_restores_deleted_documents(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    manifest = make_manifest(shop_url, target_url)
    run_pipeline(manifest)
    db.customers.delete_many({"_id": {"$lte": 10}})

    summary = run_pipeline(manifest)

    customers = next(table for table in summary.tables if table.table == "customers")
    assert customers.created == 10
    assert db.customers.count_documents({}) == 50


def test_only_listed_tables_are_copied(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target

    run_pipeline(make_manifest(shop_url, target_url, tables=["customers"]))

    assert db.list_collection_names() == ["customers"]


def test_cli_run_prints_a_summary(shop_url, mongo_target):
    target_url, _ = mongo_target

    result = CliRunner().invoke(
        app,
        ["run", str(EXAMPLE)],
        env={"SOURCE_URL": shop_url, "TARGET_URL": target_url},
    )

    assert result.exit_code == 0
    assert "customers" in result.output
    assert "Done: 4 table(s)" in result.output

def test_type_overrides_normalise_uuid_columns(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    manifest = make_manifest(
        shop_url, target_url, type_overrides={"customers.external_id": "uuid"}
    )

    run_pipeline(manifest)

    value = db.customers.find_one({"_id": 1})["external_id"]
    assert str(uuid.UUID(value)) == value  # canonical hyphenated form


def test_naive_timestamps_use_the_configured_time_zone(shop_url, mongo_target, make_manifest):
    target_url, db = mongo_target
    run_pipeline(make_manifest(shop_url, target_url, tables=["customers"]))
    as_utc = db.customers.find_one({"_id": 1})["created_at"]

    run_pipeline(
        make_manifest(
            shop_url,
            target_url,
            tables=["customers"],
            settings={"naive_timestamp_timezone": "Asia/Kolkata"},
        )
    )
    as_ist = db.customers.find_one({"_id": 1})["created_at"]

    # The same wall-clock time, read in UTC+05:30, is 5h30m earlier in UTC.
    assert as_utc - as_ist == timedelta(hours=5, minutes=30)


def test_translation_errors_name_the_table_column_and_row(
    tmp_path, mongo_target, make_manifest
):
    target_url, db = mongo_target
    path = tmp_path / "typeless.db"
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    with engine.begin() as conn:  # SQLite does not enforce column types
        conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)"))
        conn.execute(text("INSERT INTO items VALUES (1, 5), (2, 'abc')"))
    engine.dispose()

    with pytest.raises(PipelineError, match=r"items\.qty.*id=2"):
        run_pipeline(make_manifest(f"sqlite:///{path.as_posix()}", target_url))

    assert db.list_collection_names() == []  # the failing batch was never written