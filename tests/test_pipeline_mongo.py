from datetime import datetime
from pathlib import Path

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