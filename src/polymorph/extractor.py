"""Keyset-paginated table extraction: bounded memory and resumable."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Column, MetaData, Table, and_, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, NoSuchTableError
from sqlalchemy.sql.elements import ColumnElement

# The primary-key values of the last row seen, e.g. (500,) or (3, 12).
Cursor = tuple[Any, ...]


class ExtractError(Exception):
    """Extraction failed. The message is user-facing and never contains secrets."""


@dataclass(frozen=True)
class Batch:
    rows: list[dict[str, Any]]
    cursor: Cursor  # primary key of the last row in this batch


def _describe(exc: DBAPIError) -> str:
    lines = str(exc.orig or exc).strip().splitlines()
    return f"Could not read the database: {lines[0] if lines else 'unknown error'}"


def reflect_table(engine: Engine, name: str, schema: str | None = None) -> Table:
    """Load a table's definition (columns, types, keys) from the database."""
    try:
        return Table(name, MetaData(), schema=schema, autoload_with=engine)
    except NoSuchTableError as exc:
        where = f" in schema '{schema}'" if schema else ""
        raise ExtractError(f"Table not found: {name}{where}") from exc
    except DBAPIError as exc:
        raise ExtractError(_describe(exc)) from exc


def keyset_condition(
    pk_columns: Sequence[Column[Any]], cursor: Cursor
) -> ColumnElement[bool]:
    """SQL condition for 'rows strictly after `cursor` in primary-key order'.

    One column:  id > 5
    Two columns: a > 1 OR (a = 1 AND b > 2)   (portable form of (a, b) > (1, 2))
    """
    clauses = []
    for i, column in enumerate(pk_columns):
        same_prefix = [pk_columns[j] == cursor[j] for j in range(i)]
        clauses.append(and_(*same_prefix, column > cursor[i]))
    return or_(*clauses)


def extract_batches(
    engine: Engine,
    table_name: str,
    batch_size: int = 1000,
    schema: str | None = None,
    after: Cursor | None = None,
) -> Iterator[Batch]:
    """Read a table in primary-key order, one bounded batch at a time.

    Pass `after=<cursor>` to resume strictly after a previously seen key.

    This is a normal function that validates first and then returns a
    generator, so mistakes (unknown table, no primary key) are raised
    immediately instead of on the first iteration.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    table = reflect_table(engine, table_name, schema)
    pk_columns = list(table.primary_key.columns)
    if not pk_columns:
        raise ExtractError(
            f"Table '{table_name}' has no primary key; keyset pagination "
            "needs a unique, ordered key"
        )
    if after is not None and len(after) != len(pk_columns):
        raise ExtractError(
            f"Cursor has {len(after)} value(s) but the primary key of "
            f"'{table_name}' has {len(pk_columns)} column(s)"
        )

    return _stream(engine, table, pk_columns, batch_size, after)


def _stream(
    engine: Engine,
    table: Table,
    pk_columns: list[Column[Any]],
    batch_size: int,
    after: Cursor | None,
) -> Iterator[Batch]:
    cursor = after
    while True:
        query = select(table).order_by(*pk_columns).limit(batch_size)
        if cursor is not None:
            query = query.where(keyset_condition(pk_columns, cursor))

        try:
            # One short transaction per batch. The connection goes back to the
            # pool BEFORE we yield, so a slow consumer never holds it.
            with engine.connect() as conn:
                rows = [dict(row) for row in conn.execute(query).mappings()]
        except DBAPIError as exc:
            raise ExtractError(_describe(exc)) from exc

        if not rows:
            return

        cursor = tuple(rows[-1][column.name] for column in pk_columns)
        yield Batch(rows=rows, cursor=cursor)

        if len(rows) < batch_size:
            return  # a short batch means the table is exhausted