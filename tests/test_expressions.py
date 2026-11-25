import datetime as dt
from decimal import Decimal

import pytest

from polymorph.expressions import ExpressionError, compile_expression


def ev(source, **fields):
    return compile_expression(source).evaluate(fields.__getitem__)


def test_arithmetic_keeps_decimal_precision():
    assert ev("a * b + 1", a=Decimal("2.5"), b=2) == Decimal("6.0")


def test_chained_comparison():
    assert ev("1 < x <= 3", x=3) is True
    assert ev("1 < x <= 3", x=4) is False


def test_and_or_short_circuit_before_a_failing_comparison():
    assert ev("x is not None and x > 1", x=None) is False
    assert ev("x is None or x > 1", x=None) is True


def test_conditional_expression():
    assert ev("'big' if x > 5 else 'small'", x=9) == "big"
    assert ev("'big' if x > 5 else 'small'", x=1) == "small"


def test_membership():
    assert ev("status in ['paid', 'shipped']", status="paid") is True
    assert ev("status not in ['paid', 'shipped']", status="paid") is False


def test_is_works_with_none():
    assert ev("x is None", x=None) is True
    assert ev("x is not None", x=1) is True


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("lower('AbC')", "abc"),
        ("upper('ab')", "AB"),
        ("trim('  a ')", "a"),
        ("len('abcd')", 4),
        ("abs(-3)", 3),
        ("round(2.567, 1)", 2.6),
        ("coalesce(None, None, 5)", 5),
        ("str(12)", "12"),
        ("int('42')", 42),
        ("float(2)", 2.0),
        ("decimal('1.10')", Decimal("1.10")),
        ("contains('abc', 'b')", True),
        ("startswith('abc', 'a')", True),
        ("endswith('abc', 'c')", True),
    ],
)
def test_functions(source, expected):
    assert ev(source) == expected


def test_decimal_function_for_money_arithmetic():
    assert ev("x * decimal('1.18')", x=Decimal("100")) == Decimal("118.00")


def test_datetime_function_returns_utc():
    expected = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    assert ev("datetime('2024-01-01')") == expected
    assert ev("datetime('2024-01-01T05:30:00+05:30')") == expected


def test_field_names_exclude_function_names():
    assert compile_expression("lower(name) + city").names == {"name", "city"}


@pytest.mark.parametrize(
    "source",
    [
        "x.real",
        "x[0]",
        "__import__('os')",
        "lambda: 1",
        "[i for i in x]",
        "f'{x}'",
        "x ** 2",
        "{1: 2}",
        "(y := 1)",
        "print(x)",
        "x if",
        "b'abc'",
        "1j",
        "x is 5",
        "lower(*x)",
    ],
)
def test_unsafe_or_invalid_expressions_are_rejected(source):
    with pytest.raises(ExpressionError):
        compile_expression(source)


@pytest.mark.parametrize("source", ["", "   ", "x" * 501])
def test_empty_or_oversized_expressions_are_rejected(source):
    with pytest.raises(ExpressionError):
        compile_expression(source)


@pytest.mark.parametrize(
    "source",
    ["lower()", "lower('a', 'b')", "round(1, 2, 3)", "round(x, ndigits=2)", "coalesce()"],
)
def test_wrong_function_arguments_are_rejected(source):
    with pytest.raises(ExpressionError):
        compile_expression(source)


@pytest.mark.parametrize("source", ["'ab' * 3", "x * 1000000000", "'%s' % x", "x > 1"])
def test_type_problems_raise_type_error(source):
    with pytest.raises(TypeError):
        ev(source, x="a")


def test_division_by_zero_is_an_arithmetic_error():
    with pytest.raises(ArithmeticError):
        ev("1 / x", x=0)