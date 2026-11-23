"""Pipeline manifest: loading, ${ENV_VAR} interpolation and validation."""

from __future__ import annotations

import os
import re
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Mapping, Self

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

SQL_SCHEMES = {"postgresql", "mysql", "mariadb", "sqlite", "mssql", "oracle"}
NOSQL_SCHEMES = {"mongodb", "mongodb+srv"}

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class ManifestError(Exception):
    """A manifest could not be loaded or is invalid. The message is user-facing."""


class Mode(StrEnum):
    SQL_TO_SQL = "sql-to-sql"
    SQL_TO_NOSQL = "sql-to-nosql"


def url_scheme(url: str) -> str:
    """Return the database engine part of a URL.

    'postgresql+psycopg://...' -> 'postgresql'
    'mongodb+srv://...'        -> 'mongodb+srv'
    """
    scheme = url.split("://", 1)[0].lower()
    if scheme in NOSQL_SCHEMES:
        return scheme
    return scheme.split("+", 1)[0]


class Endpoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str

    @field_validator("url")
    @classmethod
    def _url_must_have_a_scheme(cls, value: str) -> str:
        if "://" not in value:
            raise ValueError("must be a connection URL like scheme://host/database")
        return value


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_size: int = Field(default=1000, ge=1, le=100_000)


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    mode: Mode
    source: Endpoint
    target: Endpoint
    settings: Settings = Field(default_factory=Settings)

    @model_validator(mode="after")
    def _schemes_must_match_mode(self) -> Self:
        source = url_scheme(self.source.url)
        target = url_scheme(self.target.url)

        if source not in SQL_SCHEMES:
            raise ValueError(
                f"source scheme '{source}' is not a supported SQL database "
                f"(supported: {', '.join(sorted(SQL_SCHEMES))})"
            )

        allowed = SQL_SCHEMES if self.mode is Mode.SQL_TO_SQL else NOSQL_SCHEMES
        if target not in allowed:
            raise ValueError(
                f"target scheme '{target}' is not valid for mode '{self.mode.value}' "
                f"(expected: {', '.join(sorted(allowed))})"
            )
        return self


def _interpolate(value: Any, env: Mapping[str, str], missing: list[str]) -> Any:
    """Replace ${NAME} in every string of a parsed YAML structure."""
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in env:
                missing.append(name)
                return match.group(0)
            return env[name]

        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {key: _interpolate(item, env, missing) for key, item in value.items()}
    if isinstance(value, list):
        return [_interpolate(item, env, missing) for item in value]
    return value


def _format_errors(exc: ValidationError) -> str:
    """Build a readable message WITHOUT echoing input values (they may be secrets)."""
    lines = ["Manifest is invalid:"]
    for err in exc.errors(include_input=False, include_url=False):
        location = ".".join(str(part) for part in err["loc"]) or "(manifest)"
        message = err["msg"].removeprefix("Value error, ")
        lines.append(f"  - {location}: {message}")
    return "\n".join(lines)


def load_manifest(path: Path, env: Mapping[str, str] | None = None) -> Manifest:
    """Read, interpolate and validate a manifest file."""
    env = os.environ if env is None else env

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"Cannot read manifest {path}: {exc.strerror}") from exc

    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ManifestError(f"{path} is not valid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ManifestError(f"{path}: the top level must be a mapping of key: value pairs")

    missing: list[str] = []
    resolved = _interpolate(raw, env, missing)
    if missing:
        names = ", ".join(sorted(set(missing)))
        raise ManifestError(f"Environment variable(s) not set: {names}")

    try:
        return Manifest.model_validate(resolved)
    except ValidationError as exc:
        raise ManifestError(_format_errors(exc)) from exc

class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    mode: Mode
    source: Endpoint
    target: Endpoint
    settings: Settings = Field(default_factory=Settings)
    tables: list[str] | None = None

    @field_validator("tables")
    @classmethod
    def _tables_must_be_valid(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        if not value:
            raise ValueError(
                "must list at least one table (omit the key to migrate all tables)"
            )
        if any(not name.strip() for name in value):
            raise ValueError("table names must not be empty")
        duplicates = sorted({name for name in value if value.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate table names: {', '.join(duplicates)}")
        return value

    @model_validator(mode="after")
    def _schemes_must_match_mode(self) -> Self:
        source = url_scheme(self.source.url)
        target = url_scheme(self.target.url)

        if source not in SQL_SCHEMES:
            raise ValueError(
                f"source scheme '{source}' is not a supported SQL database "
                f"(supported: {', '.join(sorted(SQL_SCHEMES))})"
            )

        allowed = SQL_SCHEMES if self.mode is Mode.SQL_TO_SQL else NOSQL_SCHEMES
        if target not in allowed:
            raise ValueError(
                f"target scheme '{target}' is not valid for mode '{self.mode.value}' "
                f"(expected: {', '.join(sorted(allowed))})"
            )
        return self