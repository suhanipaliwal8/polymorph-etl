import json
from pathlib import Path

from typer.testing import CliRunner

from polymorph import __version__
from polymorph.cli import app
from polymorph.manifest import load_manifest

runner = CliRunner()

EXAMPLE = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "pipelines"
    / "postgres-to-mongo.yaml"
)
MASKED = EXAMPLE.with_name("postgres-to-mongo-masked.yaml")
FOLDED = EXAMPLE.with_name("postgres-to-mongo-folded.yaml")


def preview_env(shop_url):
    return {
        "SOURCE_URL": shop_url,
        "TARGET_URL": "mongodb://localhost:27018/shop",
        "HASH_SALT": "unit-test-salt",
    }


def test_version_flag():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_validate_prints_summary_without_secrets(monkeypatch):
    monkeypatch.setenv("SOURCE_URL", "postgresql+psycopg://user:secret@localhost:5433/shop")
    monkeypatch.setenv("TARGET_URL", "mongodb://localhost:27018/shop")

    result = runner.invoke(app, ["validate", str(EXAMPLE)])

    assert result.exit_code == 0
    assert "shop-to-mongo" in result.output
    assert "postgresql" in result.output
    assert "secret" not in result.output


def test_validate_exits_with_code_1_when_env_is_missing(monkeypatch):
    monkeypatch.delenv("SOURCE_URL", raising=False)
    monkeypatch.delenv("TARGET_URL", raising=False)

    result = runner.invoke(app, ["validate", str(EXAMPLE)])

    assert result.exit_code == 1
    assert "SOURCE_URL" in result.output

def test_validate_reports_table_count(monkeypatch):
    monkeypatch.setenv("SOURCE_URL", "postgresql+psycopg://user:secret@localhost:5433/shop")
    monkeypatch.setenv("TARGET_URL", "mongodb://localhost:27018/shop")

    result = runner.invoke(app, ["validate", str(EXAMPLE)])

    assert result.exit_code == 0
    assert "tables: 4" in result.output


def test_inspect_prints_the_schema(shop_url):
    result = runner.invoke(app, ["inspect", shop_url, "--no-counts"])
    assert result.exit_code == 0
    assert "customers" in result.output
    assert "order_items" in result.output


def test_inspect_reads_the_url_from_the_environment(shop_url):
    result = runner.invoke(app, ["inspect", "--no-counts"], env={"SOURCE_URL": shop_url})
    assert result.exit_code == 0
    assert "Engine:   sqlite" in result.output


def test_inspect_generates_a_valid_manifest(shop_url, tmp_path):
    out = tmp_path / "draft.yaml"

    result = runner.invoke(
        app,
        ["inspect", shop_url, "--generate-manifest", str(out), "--mode", "sql-to-sql"],
    )

    assert result.exit_code == 0
    env = {"SOURCE_URL": shop_url, "TARGET_URL": "mysql+pymysql://u:p@h/db"}
    assert load_manifest(out, env=env).name == "shop-db-migration"


def test_generate_manifest_requires_a_mode(shop_url, tmp_path):
    out = tmp_path / "draft.yaml"
    result = runner.invoke(app, ["inspect", shop_url, "--generate-manifest", str(out)])
    assert result.exit_code == 2
    assert not out.exists()


def test_generate_manifest_refuses_to_overwrite(shop_url, tmp_path):
    out = tmp_path / "draft.yaml"
    out.write_text("keep me", encoding="utf-8")

    result = runner.invoke(
        app,
        ["inspect", shop_url, "--generate-manifest", str(out), "--mode", "sql-to-sql"],
    )

    assert result.exit_code == 1
    assert out.read_text(encoding="utf-8") == "keep me"


def test_force_overwrites_an_existing_manifest(shop_url, tmp_path):
    out = tmp_path / "draft.yaml"
    out.write_text("old", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "inspect", shop_url,
            "--generate-manifest", str(out),
            "--mode", "sql-to-sql",
            "--force",
        ],
    )

    assert result.exit_code == 0
    assert "version: 1" in out.read_text(encoding="utf-8")


def test_inspect_fails_with_exit_code_1_for_a_non_sql_url():
    result = runner.invoke(app, ["inspect", "mongodb://localhost:27018/shop"])
    assert result.exit_code == 1
    assert "SQL" in result.output

def test_preview_prints_rows_as_json_lines(shop_url):
    result = runner.invoke(
        app, ["preview", "customers", "--limit", "3"], env={"SOURCE_URL": shop_url}
    )
    assert result.exit_code == 0
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert [json.loads(line)["id"] for line in lines] == [1, 2, 3]


def test_preview_stops_at_the_limit_across_batches(shop_url):
    result = runner.invoke(
        app,
        ["preview", "customers", "--limit", "5", "--batch-size", "2"],
        env={"SOURCE_URL": shop_url},
    )
    assert result.exit_code == 0
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert [json.loads(line)["id"] for line in lines] == [1, 2, 3, 4, 5]


def test_preview_unknown_table_exits_with_code_1(shop_url):
    result = runner.invoke(app, ["preview", "nope"], env={"SOURCE_URL": shop_url})
    assert result.exit_code == 1
    assert "not found" in result.output


def test_preview_requires_a_source_url(monkeypatch):
    monkeypatch.delenv("SOURCE_URL", raising=False)
    result = runner.invoke(app, ["preview", "customers"])
    assert result.exit_code == 2

def test_run_requires_environment_variables(monkeypatch):
    monkeypatch.delenv("SOURCE_URL", raising=False)
    monkeypatch.delenv("TARGET_URL", raising=False)

    result = runner.invoke(app, ["run", str(EXAMPLE)])

    assert result.exit_code == 1
    assert "SOURCE_URL" in result.output


def test_run_rejects_sql_to_sql_until_it_is_implemented(monkeypatch):
    monkeypatch.setenv("SOURCE_URL", "postgresql+psycopg://user:secret@localhost:5433/shop")
    monkeypatch.setenv("TARGET_URL", "mysql+pymysql://user:secret@localhost:3307/shop")

    result = runner.invoke(app, ["run", str(EXAMPLE.with_name("postgres-to-mysql.yaml"))])

    assert result.exit_code == 1
    assert "not available yet" in result.output
    assert "secret" not in result.output

def test_validate_counts_transform_rules(monkeypatch):
    for name, value in preview_env("postgresql+psycopg://user:secret@localhost:5433/shop").items():
        monkeypatch.setenv(name, value)

    result = runner.invoke(app, ["validate", str(MASKED)])

    assert result.exit_code == 0
    assert "rules:  10" in result.output


def test_preview_with_a_manifest_shows_the_documents_mongodb_would_receive(shop_url):
    result = runner.invoke(
        app,
        ["preview", "orders", "--manifest", str(MASKED), "--limit", "2"],
        env=preview_env(shop_url),
    )

    assert result.exit_code == 0
    docs = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert len(docs) == 2
    assert all("$numberDecimal" in doc["total_with_tax"] for doc in docs)
    assert all("id" not in doc for doc in docs)


def test_preview_with_a_manifest_skips_rejected_rows_and_says_why(shop_url):
    result = runner.invoke(
        app,
        ["preview", "customers", "--manifest", str(MASKED), "--limit", "100"],
        env=preview_env(shop_url),
    )

    assert result.exit_code == 0
    docs = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert len(docs) == 47
    assert "***" in docs[0]["email"]
    assert "customer email is required" in result.output

def test_validate_reports_collections(monkeypatch):
    env = preview_env("postgresql+psycopg://user:secret@localhost:5433/shop")
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    result = runner.invoke(app, ["validate", str(FOLDED)])

    assert result.exit_code == 0
    assert "collections: 1" in result.output
    assert "secret" not in result.output


def test_preview_shows_a_folded_document(shop_url):
    result = runner.invoke(
        app,
        ["preview", "customers", "--manifest", str(FOLDED), "--limit", "50"],
        env=preview_env(shop_url),
    )

    assert result.exit_code == 0
    docs = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    with_orders = [doc for doc in docs if doc["orders"]]
    assert with_orders
    order = with_orders[0]["orders"][0]
    assert "customer_id" not in order
    assert "$numberDecimal" in order["items"][0]["unit_price"]
    assert order["item_count"] == len(order["items"])
    # Item 5 has a negative quantity: exactly one document is rejected.
    assert len(docs) == 49
    assert "quantity must be positive" in result.output