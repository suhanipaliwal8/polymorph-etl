import datetime as dt
import uuid
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from bson import Decimal128
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.types import NullType

from polymorph.extractor import reflect_table
from polymorph.inspector import create_source_engine
from polymorph.translate import (
    TranslationError,
    build_translator,
    generic_to_bson,
    make_timestamp_converter,
    to_bool,
    to_bytes,
    to_date,
    to_decimal128,
    to_int,
    to_time_string,
    to_uuid_string,
)

UTC = dt.timezone.utc
IST = ZoneInfo("Asia/Kolkata")  # UTC+05:30, no daylight saving
CANONICAL = "12345678-1234-5678-1234-567812345678"


def table_with(*columns):
    return Table("t", MetaData(), *columns)


# --- decimals ----------------------------------------------------------------


def test_decimal_is_stored_exactly():
    value = Decimal("12345678901234.5678")

    converted = to_decimal128(value)

    assert isinstance(converted, Decimal128)
    assert converted.to_decimal() == value
    # A float cannot hold this value exactly: this is why Decimal128 matters.
    assert Decimal(str(float(value))) != value


def test_decimal_keeps_trailing_zeros():
    assert str(to_decimal128(Decimal("19.9900")).to_decimal()) == "19.9900"


def test_decimal_with_more_than_34_digits_is_rejected():
    with pytest.raises(ValueError, match="34"):
        to_decimal128(Decimal("1" * 35))


def test_decimal_with_exactly_34_digits_is_accepted():
    value = Decimal("1" * 34)
    assert to_decimal128(value).to_decimal() == value


def test_floats_are_not_silently_accepted_as_decimals():
    with pytest.raises(ValueError, match="float"):
        to_decimal128(1.1)


# --- integers and booleans ---------------------------------------------------


def test_integers_must_fit_in_64_bits():
    assert to_int(2**63 - 1) == 2**63 - 1
    with pytest.raises(ValueError, match="64-bit"):
        to_int(2**63)


def test_text_in_an_integer_column_is_rejected():
    with pytest.raises(ValueError, match="expected an integer, got str"):
        to_int("abc")


@pytest.mark.parametrize(
    ("raw", "expected"), [(True, True), (False, False), (1, True), (0, False)]
)
def test_booleans(raw, expected):
    assert to_bool(raw) is expected


def test_values_other_than_0_and_1_are_not_booleans():
    with pytest.raises(ValueError):
        to_bool(2)


# --- timestamps, dates, times --------------------------------------------------


def test_aware_timestamps_are_converted_to_utc():
    value = dt.datetime(2024, 1, 1, 12, 0, tzinfo=IST)
    assert make_timestamp_converter(UTC)(value) == dt.datetime(2024, 1, 1, 6, 30, tzinfo=UTC)


def test_naive_timestamps_use_the_configured_time_zone():
    value = dt.datetime(2024, 1, 1, 12, 0)
    assert make_timestamp_converter(IST)(value) == dt.datetime(2024, 1, 1, 6, 30, tzinfo=UTC)
    assert make_timestamp_converter(UTC)(value) == dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC)


def test_microseconds_are_truncated_to_milliseconds():
    value = dt.datetime(2024, 1, 1, 12, 0, 0, 123_999, tzinfo=UTC)
    assert make_timestamp_converter(UTC)(value).microsecond == 123_000


def test_text_in_a_timestamp_column_is_rejected():
    with pytest.raises(ValueError, match="timestamp"):
        make_timestamp_converter(UTC)("2024-01-01")


def test_dates_become_midnight_utc():
    assert to_date(dt.date(2024, 5, 1)) == dt.datetime(2024, 5, 1, tzinfo=UTC)


def test_a_datetime_is_not_a_date():
    with pytest.raises(ValueError, match="date"):
        to_date(dt.datetime(2024, 5, 1, 10, 0))


def test_times_become_iso_strings():
    assert to_time_string(dt.time(9, 15)) == "09:15:00"
    assert to_time_string(dt.time(9, 15, 0, 500_000)) == "09:15:00.500000"


# --- uuid, binary, generic -----------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        uuid.UUID(CANONICAL),
        CANONICAL,
        CANONICAL.replace("-", ""),
        CANONICAL.upper(),
        uuid.UUID(CANONICAL).bytes,
    ],
)
def test_uuid_accepts_every_common_storage_format(raw):
    assert to_uuid_string(raw) == CANONICAL


def test_invalid_uuid_is_rejected():
    with pytest.raises(ValueError, match="UUID"):
        to_uuid_string("not-a-uuid")


def test_binary_accepts_memoryview():
    assert to_bytes(memoryview(b"ab")) == b"ab"


def test_generic_converts_nested_structures():
    converted = generic_to_bson(
        {"a": [Decimal("1.5"), {"b": uuid.UUID(CANONICAL)}], "n": None}
    )
    assert isinstance(converted["a"][0], Decimal128)
    assert converted["a"][1]["b"] == CANONICAL
    assert converted["n"] is None


def test_generic_rejects_unsupported_types():
    with pytest.raises(ValueError, match="timedelta"):
        generic_to_bson(dt.timedelta(seconds=5))


# --- mapping column types, without any database ------------------------------


def test_postgres_types_map_to_exact_bson_types():
    table = table_with(
        Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        Column("amount", postgresql.NUMERIC(19, 4)),
        Column("seen_at", postgresql.TIMESTAMP(timezone=True)),
        Column("payload", postgresql.JSONB()),
        Column("blob", postgresql.BYTEA()),
        Column("flag", postgresql.BOOLEAN()),
        Column("n", postgresql.BIGINT()),
    )

    translator = build_translator(table, postgresql.dialect(), UTC)

    assert {m.name: m.bson_type for m in translator.mappings} == {
        "id": "string (UUID)",
        "amount": "Decimal128",
        "seen_at": "date (UTC)",
        "payload": "document/array",
        "blob": "binary",
        "flag": "bool",
        "n": "int",
    }
    assert translator.notes == []  # nothing was assumed


def test_mysql_types_and_the_assumptions_behind_them():
    table = table_with(
        Column("id", mysql.INTEGER(), primary_key=True),
        Column("flag", mysql.TINYINT(1)),
        Column("small", mysql.TINYINT()),
        Column("created", mysql.DATETIME()),
        Column("code", mysql.CHAR(32)),
    )

    translator = build_translator(table, mysql.dialect(), IST)

    bson_types = {m.name: m.bson_type for m in translator.mappings}
    assert bson_types["flag"] == "bool"
    assert bson_types["small"] == "int"
    assert bson_types["created"] == "date (UTC)"
    assert bson_types["code"] == "string"
    notes = "\n".join(translator.notes)
    assert "TINYINT(1)" in notes
    assert "no time zone" in notes
    assert "Asia/Kolkata" in notes


def test_wide_or_unbounded_numerics_get_a_note():
    table = table_with(
        Column("a", postgresql.NUMERIC(40, 2)),
        Column("b", postgresql.NUMERIC()),
    )
    translator = build_translator(table, postgresql.dialect(), UTC)
    assert len(translator.notes) == 2
    assert all("34" in note for note in translator.notes)


def test_unmapped_types_get_a_note():
    table = table_with(Column("x", NullType()))
    translator = build_translator(table, mysql.dialect(), UTC)
    assert "no dedicated mapping" in translator.notes[0]


def test_override_reads_a_char_column_as_uuid():
    table = table_with(
        Column("id", mysql.INTEGER(), primary_key=True),
        Column("external_id", mysql.CHAR(32)),
    )

    translator = build_translator(
        table, mysql.dialect(), UTC, {"external_id": "uuid"}
    )

    row = translator.translate_row({"id": 1, "external_id": CANONICAL.replace("-", "")})
    assert row["external_id"] == CANONICAL
    assert "type_overrides" in translator.notes[0]


# --- translating rows -----------------------------------------------------------


def test_null_values_stay_null():
    table = table_with(Column("id", Integer, primary_key=True), Column("qty", Integer))
    translator = build_translator(table, mysql.dialect(), UTC)
    assert translator.translate_row({"id": 1, "qty": None}) == {"id": 1, "qty": None}


def test_errors_name_the_table_and_column_but_never_the_value():
    table = table_with(Column("id", Integer, primary_key=True), Column("qty", Integer))
    translator = build_translator(table, mysql.dialect(), UTC)

    with pytest.raises(TranslationError) as excinfo:
        translator.translate_row({"id": 1, "qty": "secret-text"})

    assert "t.qty" in str(excinfo.value)
    assert "secret-text" not in str(excinfo.value)


def test_a_reflected_shop_table_translates_end_to_end(shop_url):
    engine = create_source_engine(shop_url)
    try:
        table = reflect_table(engine, "orders")
        translator = build_translator(table, engine.dialect, UTC)
    finally:
        engine.dispose()

    document = translator.translate_row(
        {
            "id": 1,
            "customer_id": 2,
            "status": "paid",
            "total_amount": Decimal("19.9900"),
            "placed_at": dt.datetime(2024, 1, 1, 10, 0),
        }
    )

    assert isinstance(document["total_amount"], Decimal128)
    assert document["placed_at"] == dt.datetime(2024, 1, 1, 10, 0, tzinfo=UTC)
    assert any("no time zone" in note for note in translator.notes)