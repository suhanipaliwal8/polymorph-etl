from polymorph.draft import render_manifest_draft
from polymorph.inspector import (
    DatabaseSchema,
    TableInfo,
    inspect_database,
)
from polymorph.manifest import Mode, load_manifest

MYSQL = "mysql+pymysql://u:p@localhost:3307/shop"
MONGO = "mongodb://localhost:27018/shop"


def fake_schema(database: str) -> DatabaseSchema:
    table = TableInfo(
        name="items", columns=(), primary_key=("id",), foreign_keys=(), row_count=None
    )
    return DatabaseSchema(
        engine="postgresql",
        server_version="16.4",
        database=database,
        schema=None,
        tables=(table,),
        warnings=(),
    )


def test_draft_round_trips_through_load_manifest_for_sql_to_sql(shop_url, tmp_path):
    schema = inspect_database(shop_url)
    path = tmp_path / "draft.yaml"
    path.write_text(render_manifest_draft(schema, Mode.SQL_TO_SQL), encoding="utf-8")

    manifest = load_manifest(path, env={"SOURCE_URL": shop_url, "TARGET_URL": MYSQL})

    assert manifest.mode is Mode.SQL_TO_SQL
    assert manifest.tables == [t.name for t in schema.tables]


def test_draft_round_trips_through_load_manifest_for_sql_to_nosql(shop_url, tmp_path):
    schema = inspect_database(shop_url)
    path = tmp_path / "draft.yaml"
    path.write_text(render_manifest_draft(schema, Mode.SQL_TO_NOSQL), encoding="utf-8")

    manifest = load_manifest(path, env={"SOURCE_URL": shop_url, "TARGET_URL": MONGO})

    assert manifest.mode is Mode.SQL_TO_NOSQL


def test_draft_uses_placeholders_and_never_the_real_url(shop_url, tmp_path):
    text = render_manifest_draft(inspect_database(shop_url), Mode.SQL_TO_SQL)
    assert "${SOURCE_URL}" in text
    assert "${TARGET_URL}" in text
    assert str(tmp_path) not in text


def test_draft_name_is_always_a_valid_slug(tmp_path):
    path = tmp_path / "draft.yaml"
    path.write_text(
        render_manifest_draft(fake_schema("My Shop DB!"), Mode.SQL_TO_SQL),
        encoding="utf-8",
    )

    manifest = load_manifest(path, env={"SOURCE_URL": "sqlite:///x.db", "TARGET_URL": MYSQL})

    assert manifest.name == "my-shop-db-migration"