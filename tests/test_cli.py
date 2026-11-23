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