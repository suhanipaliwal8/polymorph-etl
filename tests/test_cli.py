from pathlib import Path

from typer.testing import CliRunner

from polymorph import __version__
from polymorph.cli import app

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