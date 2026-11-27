"""Pipeline manifest: loading, ${ENV_VAR} interpolation and validation."""

from __future__ import annotations

import os
import re
from datetime import timezone, tzinfo
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Mapping, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from polymorph.transform import Rule
from polymorph.urls import NOSQL_SCHEMES, SQL_SCHEMES, url_scheme

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_OVERRIDE_KEY = re.compile(r"^[^.\s]+\.[^.\s]+$")

MAX_EMBED_DEPTH = 5


class ManifestError(Exception):
    """A manifest could not be loaded or is invalid. The message is user-facing."""


class Mode(StrEnum):
    SQL_TO_SQL = "sql-to-sql"
    SQL_TO_NOSQL = "sql-to-nosql"


class TypeOverride(StrEnum):
    UUID = "uuid"
    BOOLEAN = "boolean"
    STRING = "string"


class RowErrorPolicy(StrEnum):
    FAIL = "fail"
    SKIP = "skip"


class Relation(StrEnum):
    ONE = "one"  # 0 or 1 child row: an object, or null
    MANY = "many"  # any number of child rows: an array


def resolve_timezone(name: str) -> tzinfo:
    """Turn 'UTC' or an IANA name such as 'Asia/Kolkata' into a tzinfo."""
    if name.upper() == "UTC":
        return timezone.utc  # no time zone database needed for the default
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
        raise ValueError(
            f"unknown time zone '{name}' (use an IANA name such as 'Asia/Kolkata', or 'UTC')"
        ) from exc


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
    naive_timestamp_timezone: str = "UTC"
    on_row_error: RowErrorPolicy = RowErrorPolicy.FAIL
    dlq_include_rows: bool = False

    @field_validator("naive_timestamp_timezone")
    @classmethod
    def _timezone_must_exist(cls, value: str) -> str:
        resolve_timezone(value)
        return value

    @property
    def naive_tz(self) -> tzinfo:
        return resolve_timezone(self.naive_timestamp_timezone)


# --- folded collections --------------------------------------------------------------


def _valid_field_name(name: str) -> str:
    if not name or name != name.strip():
        raise ValueError("must not be empty or padded with spaces")
    if name == "_id":
        raise ValueError("'_id' is reserved for the primary key")
    if name.startswith("$") or "." in name or "\x00" in name:
        raise ValueError("is not a valid MongoDB field name (no '.', no leading '$')")
    return name


def _unique_aliases(embeds: list[EmbedSpec]) -> list[EmbedSpec]:
    aliases = [embed.as_ for embed in embeds]
    duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})
    if duplicates:
        raise ValueError(f"duplicate 'as' names at the same level: {', '.join(duplicates)}")
    return embeds


def _embed_depth(embeds: list[EmbedSpec]) -> int:
    return 0 if not embeds else 1 + max(_embed_depth(embed.embed) for embed in embeds)


def _embedded_tables(embeds: list[EmbedSpec]) -> set[str]:
    tables: set[str] = set()
    for embed in embeds:
        tables.add(embed.table)
        tables |= _embedded_tables(embed.embed)
    return tables


class EmbedSpec(BaseModel):
    """One child table embedded into its parent's document."""

    model_config = ConfigDict(extra="forbid")

    table: str
    as_: str = Field(alias="as")
    relation: Relation
    join: dict[str, str] | None = None  # {child column: parent column}
    keep_foreign_key: bool = False
    embed: list[EmbedSpec] = Field(default_factory=list)

    @field_validator("table")
    @classmethod
    def _table_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("as_")
    @classmethod
    def _alias_must_be_a_field_name(cls, value: str) -> str:
        return _valid_field_name(value)

    @field_validator("join")
    @classmethod
    def _join_must_not_be_empty(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is not None and not value:
            raise ValueError("must map at least one child column to a parent column")
        return value

    @field_validator("embed")
    @classmethod
    def _sibling_aliases_must_be_unique(cls, value: list[EmbedSpec]) -> list[EmbedSpec]:
        return _unique_aliases(value)


EmbedSpec.model_rebuild()


class CollectionSpec(BaseModel):
    """A MongoDB collection built from a root table and its embedded children."""

    model_config = ConfigDict(extra="forbid")

    from_: str = Field(alias="from")
    embed: list[EmbedSpec] = Field(default_factory=list)

    @field_validator("from_")
    @classmethod
    def _root_table_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("embed")
    @classmethod
    def _check_embeds(cls, value: list[EmbedSpec]) -> list[EmbedSpec]:
        _unique_aliases(value)
        if _embed_depth(value) > MAX_EMBED_DEPTH:
            raise ValueError(f"embedding is nested more than {MAX_EMBED_DEPTH} levels deep")
        return value

    def embedded_tables(self) -> set[str]:
        return _embedded_tables(self.embed)


# --- the manifest ----------------------------------------------------------------------


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    mode: Mode
    source: Endpoint
    target: Endpoint
    settings: Settings = Field(default_factory=Settings)
    tables: list[str] | None = None
    collections: dict[str, CollectionSpec] = Field(default_factory=dict)
    type_overrides: dict[str, TypeOverride] = Field(default_factory=dict)
    transforms: dict[str, list[Rule]] = Field(default_factory=dict)

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

    @field_validator("collections")
    @classmethod
    def _collection_names_must_be_valid(
        cls, value: dict[str, CollectionSpec]
    ) -> dict[str, CollectionSpec]:
        for name in value:
            if not name or name != name.strip():
                raise ValueError("collection names must not be empty or padded with spaces")
            if name.startswith("system.") or "$" in name or "\x00" in name:
                raise ValueError(f"'{name}' is not a valid MongoDB collection name")
        return value

    @field_validator("type_overrides")
    @classmethod
    def _override_keys_must_be_table_dot_column(
        cls, value: dict[str, TypeOverride]
    ) -> dict[str, TypeOverride]:
        for key in value:
            if not _OVERRIDE_KEY.match(key):
                raise ValueError(f"'{key}' must look like table.column")
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

    @model_validator(mode="after")
    def _collections_need_a_document_target(self) -> Self:
        if self.collections and self.mode is not Mode.SQL_TO_NOSQL:
            raise ValueError("collections (folded documents) need mode 'sql-to-nosql'")
        clash = sorted(set(self.collections) & set(self.tables or []))
        if clash:
            raise ValueError(f"collection name(s) also listed in tables: {', '.join(clash)}")
        return self

    def used_tables(self) -> set[str] | None:
        """Every table the pipeline reads, or None when all tables are copied."""
        if self.tables is None and not self.collections:
            return None
        used = set(self.tables or [])
        for spec in self.collections.values():
            used.add(spec.from_)
            used |= spec.embedded_tables()
        return used

    @model_validator(mode="after")
    def _references_must_target_used_tables(self) -> Self:
        used = self.used_tables()
        if used is None:
            return self
        references = [("type_overrides", key.split(".", 1)[0]) for key in self.type_overrides]
        references += [("transforms", table) for table in self.transforms]
        for section, table in references:
            if table not in used:
                raise ValueError(
                    f"{section} refers to table '{table}', "
                    "which is not listed in tables or collections"
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