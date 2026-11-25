"""Schema-driven type translation: SQL column types to BSON-safe Python values."""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from bson import Decimal128
from sqlalchemy import Table
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.engine import Dialect

from polymorph.inspector import type_name

Converter = Callable[[Any], Any]

MAX_DECIMAL128_DIGITS = 34
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
SAFE_FLOAT_INT = 2**53
UTC = dt.timezone.utc


class TranslationError(Exception):
    """A value does not fit its column type. Never contains the value itself."""


def _expected(what: str, value: Any) -> ValueError:
    # Only the Python type name is reported: the value may be personal data.
    return ValueError(f"expected {what}, got {type(value).__name__}")


# --- converters: each takes ONE non-null value, returns a BSON-safe value, or raises ValueError


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise ValueError("expected a boolean or 0/1")


def to_bool_lenient(value: Any) -> bool:
    """For type_overrides: boolean. Also understands '0', '1', 'true', 'false'."""
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true"):
            return True
        if lowered in ("0", "false"):
            return False
        raise ValueError("expected 0, 1, true or false")
    return to_bool(value)


def to_int(value: Any) -> int:
    if not isinstance(value, int):
        raise _expected("an integer", value)
    if not INT64_MIN <= value <= INT64_MAX:
        raise ValueError("integer is outside MongoDB's 64-bit range")
    return int(value)


def to_double(value: Any) -> float:
    if isinstance(value, float):
        return value
    if isinstance(value, int) and abs(value) <= SAFE_FLOAT_INT:
        return float(value)
    raise _expected("a floating-point number", value)


def to_decimal128(value: Any) -> Decimal128:
    if isinstance(value, int) and not isinstance(value, bool):
        value = Decimal(value)
    if not isinstance(value, Decimal):
        raise _expected("a decimal number", value)
    digits = len(value.as_tuple().digits)
    if digits > MAX_DECIMAL128_DIGITS:
        raise ValueError(
            f"{digits} digits exceed Decimal128's limit of {MAX_DECIMAL128_DIGITS}"
        )
    try:
        return Decimal128(value)
    except ArithmeticError as exc:
        raise ValueError("value cannot be represented as Decimal128") from exc


def to_text(value: Any) -> str:
    if not isinstance(value, str):
        raise _expected("text", value)
    return value


def to_string(value: Any) -> str:
    """For type_overrides: string. Turns scalars into text."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("binary value is not valid UTF-8 text") from exc
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, Decimal, uuid.UUID, dt.date, dt.time)):
        return str(value)
    raise _expected("a scalar value", value)


def to_bytes(value: Any) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise _expected("binary data", value)
    return bytes(value)


def to_uuid_string(value: Any) -> str:
    """Canonical lowercase hyphenated UUID from a UUID, 32/36-char text or 16 bytes."""
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, str):
        try:
            return str(uuid.UUID(value))
        except ValueError:
            raise ValueError("not a valid UUID") from None
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) == 16:
            return str(uuid.UUID(bytes=raw))
        raise ValueError("a binary UUID must be exactly 16 bytes")
    raise _expected("a UUID", value)


def utc_datetime(value: dt.datetime, naive_tz: dt.tzinfo) -> dt.datetime:
    """Return `value` as UTC, truncated to milliseconds (the precision of BSON dates)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=naive_tz)
    try:
        value = value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError("timestamp cannot be converted to UTC (out of range)") from exc
    return value.replace(microsecond=value.microsecond - value.microsecond % 1000)


def make_timestamp_converter(naive_tz: dt.tzinfo) -> Converter:
    def convert(value: Any) -> dt.datetime:
        if not isinstance(value, dt.datetime):
            raise _expected("a timestamp", value)
        return utc_datetime(value, naive_tz)

    return convert


def to_date(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime) or not isinstance(value, dt.date):
        raise _expected("a date", value)
    return dt.datetime(value.year, value.month, value.day, tzinfo=UTC)


def to_time_string(value: Any) -> str:
    if not isinstance(value, dt.time):
        raise _expected("a time of day", value)
    return value.isoformat()


def generic_to_bson(value: Any) -> Any:
    """Convert by Python type: for JSON, arrays and types without a dedicated mapping."""
    if value is None or isinstance(value, (bool, float, str)):
        return value
    if isinstance(value, int):
        return to_int(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, Decimal):
        return to_decimal128(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dt.datetime):  # must be checked before dt.date
        return utc_datetime(value, UTC)
    if isinstance(value, dt.date):
        return to_date(value)
    if isinstance(value, dt.time):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): generic_to_bson(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [generic_to_bson(item) for item in value]
    raise ValueError(f"values of type {type(value).__name__} are not supported")


# --- choosing a converter from a column type ---------------------------------

OVERRIDES: dict[str, tuple[Converter, str]] = {
    "uuid": (to_uuid_string, "string (UUID)"),
    "boolean": (to_bool_lenient, "bool"),
    "string": (to_string, "string"),
}


def _choose(
    column_type: Any, label: str, sql_type: str, naive_tz: dt.tzinfo
) -> tuple[Converter, str, str | None]:
    """Return (converter, BSON type label, optional note about an assumption)."""
    if isinstance(column_type, sqltypes.Boolean):
        return to_bool, "bool", None

    if isinstance(column_type, TINYINT) and getattr(column_type, "display_width", None) == 1:
        note = f"{label}: {sql_type} is treated as a boolean (only 0 and 1 are accepted)"
        return to_bool, "bool", note

    if isinstance(column_type, sqltypes.Integer):
        return to_int, "int", None

    if isinstance(column_type, sqltypes.Float):  # Float subclasses Numeric: check it first
        return to_double, "double", None

    if isinstance(column_type, sqltypes.Numeric):
        precision = getattr(column_type, "precision", None)
        note = None
        if precision is None or precision > MAX_DECIMAL128_DIGITS:
            note = (
                f"{label}: {sql_type} may hold more than {MAX_DECIMAL128_DIGITS} digits; "
                "values that do are rejected (Decimal128 limit)"
            )
        return to_decimal128, "Decimal128", note

    if isinstance(column_type, sqltypes.DateTime):
        note = None
        if not getattr(column_type, "timezone", False):
            note = (
                f"{label}: {sql_type} has no time zone; values are read as "
                f"{naive_tz} and stored as UTC"
            )
        return make_timestamp_converter(naive_tz), "date (UTC)", note

    if isinstance(column_type, sqltypes.Date):
        return to_date, "date (midnight UTC)", None

    if isinstance(column_type, sqltypes.Time):
        return to_time_string, "string (ISO time)", None

    if isinstance(column_type, sqltypes.Uuid):
        return to_uuid_string, "string (UUID)", None

    if isinstance(column_type, (sqltypes.LargeBinary, sqltypes.BINARY, sqltypes.VARBINARY)):
        return to_bytes, "binary", None

    if isinstance(column_type, (sqltypes.JSON, sqltypes.ARRAY)):
        return generic_to_bson, "document/array", None

    if isinstance(column_type, sqltypes.String):
        return to_text, "string", None

    note = (
        f"{label}: type {sql_type} has no dedicated mapping; "
        "values are converted by their Python type"
    )
    return generic_to_bson, "by Python type", note


@dataclass(frozen=True)
class ColumnMapping:
    name: str
    sql_type: str
    bson_type: str
    note: str | None
    convert: Converter


class Translator:
    """Translates the rows of one table into BSON-safe values."""

    def __init__(self, table: str, mappings: Sequence[ColumnMapping]) -> None:
        self.table = table
        self.mappings = tuple(mappings)

    @property
    def notes(self) -> list[str]:
        return [m.note for m in self.mappings if m.note is not None]

    def translate_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        translated: dict[str, Any] = {}
        for mapping in self.mappings:
            value = row[mapping.name]
            if value is None:
                translated[mapping.name] = None
                continue
            try:
                translated[mapping.name] = mapping.convert(value)
            except ValueError as exc:
                raise TranslationError(f"{self.table}.{mapping.name}: {exc}") from exc
        return translated


def build_translator(
    table: Table,
    dialect: Dialect,
    naive_tz: dt.tzinfo,
    overrides: Mapping[str, str] | None = None,
) -> Translator:
    """Plan the translation of every column of a reflected table."""
    overrides = overrides or {}
    mappings = []
    for column in table.columns:
        sql_type = type_name(column.type, dialect)
        label = f"{table.name}.{column.name}"
        kind = overrides.get(column.name)
        if kind is not None:
            convert, bson_type = OVERRIDES[kind]
            note = f"{label}: {sql_type} is read as {kind} (type_overrides)"
        else:
            convert, bson_type, note = _choose(column.type, label, sql_type, naive_tz)
        mappings.append(ColumnMapping(column.name, sql_type, bson_type, note, convert))
    return Translator(table.name, mappings)