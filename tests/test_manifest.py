from datetime import timezone
import yaml
from pathlib import Path

import pytest

from polymorph.manifest import (
    ManifestError,
    Mode,
    Relation,
    RowErrorPolicy,
    TypeOverride,
    load_manifest,
    url_scheme,
)

POSTGRES = "postgresql+psycopg://user:secret@localhost:5433/shop"
MYSQL = "mysql+pymysql://user:secret@localhost:3307/shop"
MONGO = "mongodb://localhost:27018/shop"

ENV = {"SOURCE_URL": POSTGRES, "TARGET_URL": MONGO}

VALID = """\
version: 1
name: shop-to-mongo
mode: sql-to-nosql
source:
  url: ${SOURCE_URL}
target:
  url: ${TARGET_URL}
"""

EXAMPLES = Path(__file__).resolve().parent.parent / "examples" / "pipelines"


def write_manifest(tmp_path: Path, content: str = VALID) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def test_valid_manifest_loads_with_defaults(tmp_path):
    manifest = load_manifest(write_manifest(tmp_path), env=ENV)
    assert manifest.name == "shop-to-mongo"
    assert manifest.mode is Mode.SQL_TO_NOSQL
    assert manifest.source.url == POSTGRES
    assert manifest.settings.batch_size == 1000


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql+psycopg://u:p@h/db", "postgresql"),
        ("mysql+pymysql://u:p@h/db", "mysql"),
        ("mongodb+srv://u:p@h/db", "mongodb+srv"),
        ("sqlite:///demo.db", "sqlite"),
    ],
)
def test_url_scheme(url, expected):
    assert url_scheme(url) == expected


def test_missing_env_var_is_reported_by_name(tmp_path):
    with pytest.raises(ManifestError, match="TARGET_URL"):
        load_manifest(write_manifest(tmp_path), env={"SOURCE_URL": POSTGRES})


def test_unknown_key_is_rejected(tmp_path):
    content = VALID + "setings:\n  batch_size: 10\n"
    with pytest.raises(ManifestError, match="setings"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_sql_to_sql_accepts_sql_target(tmp_path):
    content = VALID.replace("sql-to-nosql", "sql-to-sql")
    manifest = load_manifest(
        write_manifest(tmp_path, content), env={**ENV, "TARGET_URL": MYSQL}
    )
    assert manifest.mode is Mode.SQL_TO_SQL


def test_target_scheme_must_match_mode(tmp_path):
    # sql-to-nosql needs a MongoDB target, not MySQL
    with pytest.raises(ManifestError, match="target scheme 'mysql'"):
        load_manifest(write_manifest(tmp_path), env={**ENV, "TARGET_URL": MYSQL})


def test_name_must_be_a_slug(tmp_path):
    content = VALID.replace("shop-to-mongo", "Shop Pipeline")
    with pytest.raises(ManifestError, match="name"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_batch_size_must_be_positive(tmp_path):
    content = VALID + "settings:\n  batch_size: 0\n"
    with pytest.raises(ManifestError, match="settings.batch_size"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_error_messages_never_contain_credentials(tmp_path):
    content = VALID.replace("version: 1", "version: 2")
    with pytest.raises(ManifestError) as excinfo:
        load_manifest(write_manifest(tmp_path, content), env=ENV)
    assert "secret" not in str(excinfo.value)


def test_missing_file_is_a_manifest_error(tmp_path):
    with pytest.raises(ManifestError, match="Cannot read"):
        load_manifest(tmp_path / "nope.yaml", env=ENV)


def test_top_level_must_be_a_mapping(tmp_path):
    with pytest.raises(ManifestError, match="mapping"):
        load_manifest(write_manifest(tmp_path, "- a\n- b\n"), env=ENV)


@pytest.mark.parametrize(
    ("filename", "target_url"),
    [
        ("postgres-to-mongo.yaml", MONGO),
        ("postgres-to-mysql.yaml", MYSQL),
        ("mysql-to-mongo.yaml", MONGO),
        ("postgres-to-mongo-folded.yaml", MONGO),
    ],
)
def test_example_pipelines_are_valid(filename, target_url):
    env = {"SOURCE_URL": POSTGRES, "TARGET_URL": target_url}
    manifest = load_manifest(EXAMPLES / filename, env=env)
    assert manifest.version == 1

def test_tables_list_is_optional(tmp_path):
    manifest = load_manifest(write_manifest(tmp_path), env=ENV)
    assert manifest.tables is None


def test_tables_list_is_loaded(tmp_path):
    content = VALID + "tables:\n  - customers\n  - orders\n"
    manifest = load_manifest(write_manifest(tmp_path, content), env=ENV)
    assert manifest.tables == ["customers", "orders"]


def test_empty_tables_list_is_rejected(tmp_path):
    content = VALID + "tables: []\n"
    with pytest.raises(ManifestError, match="at least one table"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_duplicate_tables_are_rejected(tmp_path):
    content = VALID + "tables:\n  - orders\n  - orders\n"
    with pytest.raises(ManifestError, match="duplicate"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)

def test_naive_timestamp_timezone_defaults_to_utc(tmp_path):
    manifest = load_manifest(write_manifest(tmp_path), env=ENV)
    assert manifest.settings.naive_timestamp_timezone == "UTC"
    assert manifest.settings.naive_tz is timezone.utc


def test_named_time_zone_is_accepted(tmp_path):
    content = VALID + "settings:\n  naive_timestamp_timezone: Asia/Kolkata\n"
    manifest = load_manifest(write_manifest(tmp_path, content), env=ENV)
    assert str(manifest.settings.naive_tz) == "Asia/Kolkata"


def test_unknown_time_zone_is_rejected(tmp_path):
    content = VALID + "settings:\n  naive_timestamp_timezone: Mars/Olympus\n"
    with pytest.raises(ManifestError, match="unknown time zone"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_type_overrides_are_loaded(tmp_path):
    content = VALID + "type_overrides:\n  customers.external_id: uuid\n"
    manifest = load_manifest(write_manifest(tmp_path, content), env=ENV)
    assert manifest.type_overrides == {"customers.external_id": TypeOverride.UUID}


def test_type_override_key_must_be_table_dot_column(tmp_path):
    content = VALID + "type_overrides:\n  external_id: uuid\n"
    with pytest.raises(ManifestError, match="table.column"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_type_override_value_must_be_known(tmp_path):
    content = VALID + "type_overrides:\n  customers.external_id: json\n"
    with pytest.raises(ManifestError, match="type_overrides"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_type_override_must_target_a_listed_table(tmp_path):
    content = (
        VALID
        + "tables:\n  - orders\n"
        + "type_overrides:\n  customers.external_id: uuid\n"
    )
    with pytest.raises(ManifestError, match="not listed"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


FOLDED = """\
collections:
  customers:
    from: customers
    embed:
      - table: orders
        as: orders
        relation: many
        embed:
          - table: order_items
            as: items
            relation: many
"""


def test_collections_are_loaded(tmp_path):
    manifest = load_manifest(write_manifest(tmp_path, VALID + FOLDED), env=ENV)

    spec = manifest.collections["customers"]
    orders = spec.embed[0]
    assert spec.from_ == "customers"
    assert (orders.table, orders.as_, orders.relation) == ("orders", "orders", Relation.MANY)
    assert orders.embed[0].as_ == "items"
    assert orders.keep_foreign_key is False
    assert manifest.used_tables() == {"customers", "orders", "order_items"}


def test_all_tables_are_used_when_neither_tables_nor_collections_are_given(tmp_path):
    assert load_manifest(write_manifest(tmp_path), env=ENV).used_tables() is None


@pytest.mark.parametrize(
    ("embed", "message"),
    [
        ("      - table: orders\n        as: o\n", "relation"),
        ("      - table: orders\n        as: o\n        relation: some\n", "relation"),
        ("      - table: orders\n        as: _id\n        relation: many\n", "reserved"),
        ("      - table: orders\n        as: a.b\n        relation: many\n", "valid MongoDB field name"),
        (
            "      - table: orders\n        as: o\n        relation: many\n"
            "      - table: order_items\n        as: o\n        relation: many\n",
            "duplicate",
        ),
        ("      - table: orders\n        as: o\n        relation: many\n        join: {}\n", "at least one"),
        ("      - table: orders\n        as: o\n        relation: many\n        foreign_key: x\n", "foreign_key"),
    ],
)
def test_invalid_embeds_are_rejected(tmp_path, embed, message):
    content = VALID + "collections:\n  customers:\n    from: customers\n    embed:\n" + embed
    with pytest.raises(ManifestError, match=message):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_embedding_deeper_than_five_levels_is_rejected(tmp_path):
    spec = {"table": "t6", "as": "l6", "relation": "many"}
    for level in range(5, 0, -1):
        spec = {"table": f"t{level}", "as": f"l{level}", "relation": "many", "embed": [spec]}
    data = {
        "version": 1,
        "name": "deep",
        "mode": "sql-to-nosql",
        "source": {"url": POSTGRES},
        "target": {"url": MONGO},
        "collections": {"root": {"from": "r", "embed": [spec]}},
    }
    path = tmp_path / "deep.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ManifestError, match="levels deep"):
        load_manifest(path, env={})


def test_collections_need_a_document_target(tmp_path):
    content = VALID.replace("sql-to-nosql", "sql-to-sql") + FOLDED
    with pytest.raises(ManifestError, match="need mode"):
        load_manifest(write_manifest(tmp_path, content), env={**ENV, "TARGET_URL": MYSQL})


def test_a_collection_name_cannot_also_be_a_flat_table(tmp_path):
    content = VALID + FOLDED + "tables:\n  - customers\n"
    with pytest.raises(ManifestError, match="also listed in tables"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)


def test_transforms_may_target_embedded_tables(tmp_path):
    content = VALID + FOLDED + "transforms:\n  order_items:\n    - type: drop\n      fields: [sku]\n"
    manifest = load_manifest(write_manifest(tmp_path, content), env=ENV)
    assert "order_items" in manifest.transforms


def test_transforms_must_target_tables_used_by_collections(tmp_path):
    content = VALID + FOLDED + "transforms:\n  ghost:\n    - type: drop\n      fields: [x]\n"
    with pytest.raises(ManifestError, match="not listed"):
        load_manifest(write_manifest(tmp_path, content), env=ENV)

def test_dlq_include_rows_defaults_to_off_and_can_be_enabled(tmp_path):
    default = load_manifest(write_manifest(tmp_path), env=ENV)
    assert default.settings.dlq_include_rows is False

    content = VALID + "settings:\n  dlq_include_rows: true\n"
    enabled = load_manifest(write_manifest(tmp_path, content), env=ENV)
    assert enabled.settings.dlq_include_rows is True