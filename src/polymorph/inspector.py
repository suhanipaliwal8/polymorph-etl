"""Schema discovery: connect to a SQL database and describe its tables."""

from __future__ import annotations

from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

from sqlalchemy import create_engine, func, inspect, select, table
from sqlalchemy.engine import Connection, Dialect, Engine
from sqlalchemy.exc import ArgumentError, DBAPIError, NoSuchModuleError
from sqlalchemy.engine.reflection import Inspector


from polymorph.urls import NOSQL_SCHEMES, url_scheme

DRIVER_HINTS = {
    "postgresql": 'pip install "polymorph-etl[postgres]" and use a URL like postgresql+psycopg://...',
    "mysql": 'pip install "polymorph-etl[mysql]" and use a URL like mysql+pymysql://...',
    "mariadb": 'pip install "polymorph-etl[mysql]" and use a URL like mariadb+pymysql://...',
}


class InspectError(Exception):
    """Inspection failed. The message is user-facing and never contains secrets."""


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str
    nullable: bool


@dataclass(frozen=True)
class ForeignKeyInfo:
    columns: tuple[str, ...]
    referred_table: str
    referred_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableInfo:
    name: str
    columns: tuple[ColumnInfo, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKeyInfo, ...]
    row_count: int | None


@dataclass(frozen=True)
class DatabaseSchema:
    engine: str
    server_version: str | None
    database: str | None
    schema: str | None
    tables: tuple[TableInfo, ...]  # foreign-key order: parents before children
    warnings: tuple[str, ...]


def create_source_engine(url: str) -> Engine:
    """Create a SQLAlchemy engine, turning common failures into InspectError."""
    scheme = url_scheme(url)
    if scheme in NOSQL_SCHEMES:
        raise InspectError(
            f"'{scheme}' is not a SQL database; inspect works on SQL sources only."
        )

    try:
        engine = create_engine(url)
    except ModuleNotFoundError as exc:
        hint = DRIVER_HINTS.get(scheme, "install the Python driver for this database")
        raise InspectError(
            f"The Python driver for '{scheme}' is not installed "
            f"(missing module: {exc.name}). Try: {hint}"
        ) from exc
    except NoSuchModuleError as exc:  # must come before ArgumentError (its parent)
        raise InspectError(f"Unsupported database type '{scheme}'.") from exc
    except ArgumentError as exc:
        raise InspectError(
            "Malformed connection URL; expected scheme://user:password@host:port/database"
        ) from exc

    # SQLite silently creates an empty file for a mistyped path. Refuse instead.
    database = engine.url.database
    if (
        engine.dialect.name == "sqlite"
        and database
        and database != ":memory:"
        and not Path(database).exists()
    ):
        engine.dispose()
        raise InspectError(f"SQLite file not found: {database}")

    return engine


def inspect_database(
    url: str, schema: str | None = None, count_rows: bool = True
) -> DatabaseSchema:
    """Connect to the database at `url` and describe its tables."""
    engine = create_source_engine(url)
    try:
        with engine.connect() as conn:  # a real connection is made here
            return _inspect_connection(conn, engine.url.database, schema, count_rows)
    except DBAPIError as exc:
        detail = str(exc.orig or exc).strip().splitlines()
        raise InspectError(
            f"Could not read the database: {detail[0] if detail else 'unknown error'}"
        ) from exc
    finally:
        engine.dispose()


def _inspect_connection(
    conn: Connection, database: str | None, schema: str | None, count_rows: bool
) -> DatabaseSchema:
    insp = inspect(conn)
    version_info = conn.dialect.server_version_info
    version = ".".join(str(part) for part in version_info) if version_info else None

    names = sorted(insp.get_table_names(schema=schema))
    infos = [_describe_table(conn, insp, name, schema, count_rows) for name in names]
    ordered, order_warnings = order_by_dependency(infos)

    warnings = [*_table_warnings(ordered), *order_warnings]
    if not ordered:
        where = f" in schema '{schema}'" if schema else ""
        warnings.append(f"No tables found{where}.")

    return DatabaseSchema(
        engine=conn.dialect.name,
        server_version=version,
        database=database,
        schema=schema,
        tables=tuple(ordered),
        warnings=tuple(warnings),
    )


def _describe_table(
    conn: Connection,
    insp: Inspector,
    name: str,
    schema: str | None,
    count_rows: bool,
) -> TableInfo:
    columns = tuple(
        ColumnInfo(
            name=col["name"],
            type=type_name(col["type"], conn.dialect),
            nullable=bool(col["nullable"]),
        )
        for col in insp.get_columns(name, schema=schema)
    )
    pk = insp.get_pk_constraint(name, schema=schema).get("constrained_columns") or []
    foreign_keys = tuple(
        ForeignKeyInfo(
            columns=tuple(fk["constrained_columns"]),
            referred_table=fk["referred_table"],
            referred_columns=tuple(fk["referred_columns"]),
        )
        for fk in insp.get_foreign_keys(name, schema=schema)
    )
    row_count = None
    if count_rows:
        # table() quotes the identifier safely, even for reserved words.
        statement = select(func.count()).select_from(table(name, schema=schema))
        row_count = conn.execute(statement).scalar_one()

    return TableInfo(
        name=name,
        columns=columns,
        primary_key=tuple(pk),
        foreign_keys=foreign_keys,
        row_count=row_count,
    )


def type_name(sql_type: object, dialect: Dialect) -> str:
    """Show the column type the way this database spells it (NUMERIC(19, 4), UUID...)."""
    try:
        return sql_type.compile(dialect=dialect)  # type: ignore[attr-defined]
    except Exception:  # exotic reflected types may not compile; show their class name
        return type(sql_type).__name__


def order_by_dependency(
    tables: list[TableInfo],
) -> tuple[list[TableInfo], list[str]]:
    """Sort tables so every table comes after the tables its foreign keys point to.

    Returns (ordered tables, warnings). On a foreign-key cycle no valid order
    exists, so tables are returned alphabetically with a warning.
    """
    by_name = {t.name: t for t in tables}
    sorter: TopologicalSorter[str] = TopologicalSorter()
    for name in sorted(by_name):
        parents = {
            fk.referred_table
            for fk in by_name[name].foreign_keys
            # Ignore self-references (employee.manager_id -> employee.id) and
            # references to tables outside this schema.
            if fk.referred_table in by_name and fk.referred_table != name
        }
        sorter.add(name, *sorted(parents))

    try:
        order = list(sorter.static_order())
    except CycleError as exc:
        cycle = " -> ".join(exc.args[1])
        warning = (
            f"foreign key cycle between tables ({cycle}); no safe load order exists, "
            "listing tables alphabetically"
        )
        return [by_name[name] for name in sorted(by_name)], [warning]

    return [by_name[name] for name in order], []


def _table_warnings(tables: list[TableInfo]) -> list[str]:
    warnings = []
    for t in tables:
        if not t.primary_key:
            warnings.append(
                f"{t.name}: no primary key; keyset pagination and safe resume "
                "need a unique, ordered key"
            )
        elif len(t.primary_key) > 1:
            warnings.append(
                f"{t.name}: composite primary key ({', '.join(t.primary_key)}); "
                "needs special handling when streaming"
            )
    return warnings


def render_text(schema: DatabaseSchema) -> str:
    """Human-readable description of a database schema."""
    version = f" {schema.server_version}" if schema.server_version else ""
    lines = [
        f"Engine:   {schema.engine}{version}",
        f"Database: {schema.database or '(unknown)'}",
    ]
    if schema.schema:
        lines.append(f"Schema:   {schema.schema}")
    lines.append(f"Tables:   {len(schema.tables)} (foreign-key order, parents first)")

    for position, t in enumerate(schema.tables, start=1):
        count = "?" if t.row_count is None else f"{t.row_count:,}"
        lines.append("")
        lines.append(f"{position}. {t.name}  ({count} rows)")
        for col in t.columns:
            if col.name in t.primary_key:
                flag = "PK"
            elif not col.nullable:
                flag = "NOT NULL"
            else:
                flag = ""
            lines.append(f"     {col.name:<20} {col.type:<26} {flag}".rstrip())
        for fk in t.foreign_keys:
            lines.append(
                f"     FK ({', '.join(fk.columns)}) -> "
                f"{fk.referred_table}({', '.join(fk.referred_columns)})"
            )

    if schema.warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"  ! {warning}" for warning in schema.warnings)

    return "\n".join(lines)