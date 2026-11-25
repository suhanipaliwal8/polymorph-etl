"""A small, safe expression language for filters, requirements and computed fields.

Expressions look like Python (`quantity * unit_price`, `status != 'cancelled'`)
but they are parsed with the `ast` module and run by our own interpreter that
knows only a short whitelist of operations. `eval()` is never used, so an
expression can never import modules, read files or call arbitrary code.
"""

from __future__ import annotations

import ast
import datetime as dt
import operator
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from functools import lru_cache
from typing import Any

MAX_EXPRESSION_LENGTH = 500


class ExpressionError(Exception):
    """The expression is invalid. The message is safe to show to users."""


# --- functions available inside expressions -----------------------------------


def _text(value: Any, function: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{function}() needs text")
    return value


def _number(value: Any, function: str) -> Any:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError(f"{function}() needs a number")
    return value


def _round(value: Any, digits: Any = None) -> Any:
    number = _number(value, "round")
    return round(number) if digits is None else round(number, digits)


def _coalesce(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _length(value: Any) -> int:
    if not isinstance(value, (str, bytes, list)):
        raise TypeError("len() needs text, binary data or a list")
    return len(value)


def _to_str(value: Any) -> str:
    if value is None:
        raise TypeError("str() of null")
    return str(value)


def _to_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TypeError("int() needs a number or text")
    return int(value)


def _to_float(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TypeError("float() needs a number or text")
    return float(value)


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise TypeError("decimal() needs a number or text")
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, (str, int, Decimal)):
        return Decimal(value)
    raise TypeError("decimal() needs a number or text")


def _to_datetime(value: Any) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(_text(value, "datetime"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


# name -> (minimum arguments, maximum arguments or None, implementation)
FUNCTIONS: dict[str, tuple[int, int | None, Callable[..., Any]]] = {
    "lower": (1, 1, lambda v: _text(v, "lower").lower()),
    "upper": (1, 1, lambda v: _text(v, "upper").upper()),
    "trim": (1, 1, lambda v: _text(v, "trim").strip()),
    "len": (1, 1, _length),
    "abs": (1, 1, lambda v: abs(_number(v, "abs"))),
    "round": (1, 2, _round),
    "coalesce": (1, None, _coalesce),
    "str": (1, 1, _to_str),
    "int": (1, 1, _to_int),
    "float": (1, 1, _to_float),
    "decimal": (1, 1, _to_decimal),
    "datetime": (1, 1, _to_datetime),
    "contains": (2, 2, lambda s, p: _text(p, "contains") in _text(s, "contains")),
    "startswith": (2, 2, lambda s, p: _text(s, "startswith").startswith(_text(p, "startswith"))),
    "endswith": (2, 2, lambda s, p: _text(s, "endswith").endswith(_text(p, "endswith"))),
}


# --- operators ------------------------------------------------------------------

_SEQUENCES = (str, bytes, list, tuple)


def _multiply(a: Any, b: Any) -> Any:
    # 'x' * 1000000000 would eat all memory.
    if isinstance(a, _SEQUENCES) or isinstance(b, _SEQUENCES):
        raise TypeError("repeating text or lists is not allowed")
    return a * b


def _modulo(a: Any, b: Any) -> Any:
    # '%999999999d' % 1 would eat all memory.
    if isinstance(a, (str, bytes)):
        raise TypeError("text formatting with % is not allowed")
    return a % b


_BINARY: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: _multiply,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: _modulo,
}

_COMPARE: dict[type, Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: lambda a, b: a is b,
    ast.IsNot: lambda a, b: a is not b,
}

_ALLOWED_NODES = (
    ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.USub, ast.UAdd,
    ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod,
    ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.In, ast.NotIn, ast.Is, ast.IsNot,
    ast.Constant, ast.Name, ast.Load, ast.Call, ast.IfExp, ast.List, ast.Tuple,
)

_FRIENDLY_NAMES = {
    ast.Attribute: "attribute access (a.b)",
    ast.Subscript: "indexing (a[0])",
    ast.Lambda: "lambda",
    ast.ListComp: "comprehensions",
    ast.JoinedStr: "f-strings",
    ast.Pow: "the ** operator",
    ast.Dict: "dictionaries",
    ast.Set: "sets",
    ast.NamedExpr: "assignment expressions (:=)",
    ast.Starred: "star arguments",
}


def _describe(node: ast.AST) -> str:
    return _FRIENDLY_NAMES.get(type(node), type(node).__name__)


def _arity(low: int, high: int | None) -> str:
    if high is None:
        return f"at least {low}"
    return str(low) if low == high else f"{low} to {high}"


# --- compiling and evaluating ------------------------------------------------------


@dataclass(frozen=True)
class Expression:
    source: str
    names: frozenset[str]  # the field names the expression reads
    _tree: ast.Expression = field(repr=False, compare=False)

    def evaluate(self, lookup: Callable[[str], Any]) -> Any:
        """Evaluate with `lookup(field_name)` supplying field values.

        Type problems and arithmetic errors propagate as TypeError, ValueError,
        ArithmeticError or AttributeError; the caller decides what they mean.
        """
        return _evaluate(self._tree.body, lookup)


@lru_cache(maxsize=512)
def compile_expression(source: str) -> Expression:
    """Parse and validate an expression. Raises ExpressionError if it is not allowed."""
    if not isinstance(source, str) or not source.strip():
        raise ExpressionError("expression must not be empty")
    if len(source) > MAX_EXPRESSION_LENGTH:
        raise ExpressionError(f"expression is longer than {MAX_EXPRESSION_LENGTH} characters")

    try:
        tree = ast.parse(source.strip(), mode="eval")
    except (SyntaxError, ValueError, RecursionError) as exc:
        raise ExpressionError("syntax error in expression (check quotes and parentheses)") from exc

    function_names: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ExpressionError(f"{_describe(node)} is not allowed in expressions")

        if isinstance(node, ast.Constant):
            value = node.value
            if not (value is None or isinstance(value, (str, int, float, bool))):
                raise ExpressionError("only text, number, True, False and None constants are allowed")

        elif isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ExpressionError("only plain function calls are allowed")
            name = node.func.id
            if name not in FUNCTIONS:
                raise ExpressionError(
                    f"unknown function '{name}' (available: {', '.join(sorted(FUNCTIONS))})"
                )
            if node.keywords:
                raise ExpressionError("keyword arguments are not supported")
            low, high, _ = FUNCTIONS[name]
            if len(node.args) < low or (high is not None and len(node.args) > high):
                raise ExpressionError(f"{name}() takes {_arity(low, high)} argument(s)")
            function_names.add(id(node.func))

        elif isinstance(node, ast.Compare):
            for op, comparator in zip(node.ops, node.comparators):
                if isinstance(op, (ast.Is, ast.IsNot)) and not (
                    isinstance(comparator, ast.Constant) and comparator.value is None
                ):
                    raise ExpressionError("'is' and 'is not' can only be used with None")

    names = frozenset(
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and id(node) not in function_names
    )
    return Expression(source=source.strip(), names=names, _tree=tree)


def _evaluate(node: ast.AST, lookup: Callable[[str], Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        return lookup(node.id)

    if isinstance(node, ast.BoolOp):
        is_and = isinstance(node.op, ast.And)
        result: Any = None
        for operand in node.values:
            result = _evaluate(operand, lookup)
            if is_and and not result:
                return result
            if not is_and and result:
                return result
        return result

    if isinstance(node, ast.UnaryOp):
        operand = _evaluate(node.operand, lookup)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.USub):
            return -operand
        return +operand

    if isinstance(node, ast.BinOp):
        left = _evaluate(node.left, lookup)
        right = _evaluate(node.right, lookup)
        return _BINARY[type(node.op)](left, right)

    if isinstance(node, ast.Compare):
        left = _evaluate(node.left, lookup)
        for op, comparator in zip(node.ops, node.comparators):
            right = _evaluate(comparator, lookup)
            if not _COMPARE[type(op)](left, right):
                return False
            left = right
        return True

    if isinstance(node, ast.IfExp):
        branch = node.body if _evaluate(node.test, lookup) else node.orelse
        return _evaluate(branch, lookup)

    if isinstance(node, ast.Call):
        _, _, function = FUNCTIONS[node.func.id]  # type: ignore[attr-defined]
        return function(*[_evaluate(arg, lookup) for arg in node.args])

    if isinstance(node, (ast.List, ast.Tuple)):
        return [_evaluate(item, lookup) for item in node.elts]

    raise ExpressionError(f"{_describe(node)} is not allowed in expressions")