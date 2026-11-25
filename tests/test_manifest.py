from datetime import timezone
from pathlib import Path

import pytest

from polymorph.manifest import (
    ManifestError,
    Mode,
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