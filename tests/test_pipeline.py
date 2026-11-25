import pytest
from sqlalchemy import create_engine, text

from polymorph.extractor import ExtractError
from polymorph.pipeline import PipelineError, TableResult, format_table_row, run_pipeline


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