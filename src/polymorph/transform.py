"""The transform stage: declarative rules applied to every row between extract and load.

A rule has two jobs:
  check(fields, protected)  static: validate the rule against the fields that
                            exist at this point and return the fields that
                            exist afterwards (runs once, before any write)
  apply(row)                runtime: change one row in place
                            (returns False when the row must be dropped)
"""

from __future__ import annotations

import difflib
import hashlib
import hmac
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Any, Literal, Union

from bson import Decimal128
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from polymorph.expressions import Expression, ExpressionError, compile_expression
from polymorph.translate import generic_to_bson

REDACTED = "[REDACTED]"


class RuleConfigError(Exception):
    """A rule does not fit the table it is applied to."""


class TransformConfigError(Exception):
    """A table's rules are invalid. The message names the table and the rule."""


class RowRejected(Exception):
    """One row cannot be processed. The message never contains the row's data."""


# --- helpers ---------------------------------------------------------------------


def _suggest(name: str, fields: frozenset[str]) -> str:
    close = difflib.get_close_matches(name, sorted(fields), n=1)
    return f" (did you mean '{close[0]}'?)" if close else ""


def _require_fields(fields: frozenset[str], names: Sequence[str]) -> None:
    for name in names:
        if name not in fields:
            raise RuleConfigError(f"unknown field '{name}'{_suggest(name, fields)}")


def _require_changeable(protected: frozenset[str], names: Sequence[str]) -> None:
    for name in names:
        if name in protected:
            raise RuleConfigError(f"'{name}' is part of the primary key and cannot be changed")


def _check_new_name(name: str, existing: frozenset[str]) -> None:
    if not name or name != name.strip():
        raise RuleConfigError("field names must not be empty or padded with spaces")
    if name == "_id":
        raise RuleConfigError("'_id' is reserved for the primary key")
    if name.startswith("$") or "." in name or "\x00" in name:
        raise RuleConfigError(f"'{name}' is not a valid MongoDB field name (no '.', no leading '$')")
    if name in existing:
        raise RuleConfigError(f"field '{name}' already exists")


def _check_expression_fields(expression: Expression, fields: frozenset[str]) -> None:
    unknown = sorted(expression.names - fields)
    if unknown:
        available = ", ".join(sorted(fields)[:20])
        raise RuleConfigError(
            f"unknown field(s) in expression: {', '.join(unknown)} (available: {available})"
        )


def _unique_names(value: list[str]) -> list[str]:
    if any(not name.strip() for name in value):
        raise ValueError("field names must not be empty")
    if len(set(value)) != len(value):
        raise ValueError("field names must be unique")
    return value


def _validated_expression(value: str) -> str:
    try:
        compile_expression(value)
    except ExpressionError as exc:
        raise ValueError(str(exc)) from exc
    return value


def _to_native(value: Any) -> Any:
    """Expressions work with exact Decimals, not BSON Decimal128."""
    return value.to_decimal() if isinstance(value, Decimal128) else value


def _evaluate(expression: Expression, row: dict[str, Any]) -> Any:
    try:
        return expression.evaluate(lambda name: _to_native(row[name]))
    except (TypeError, ValueError, ArithmeticError, AttributeError) as exc:
        # Only the exception class is reported: its message could contain data.
        raise RowRejected(
            f"expression `{expression.source}` failed: {type(exc).__name__}"
        ) from exc


# --- the rules ---------------------------------------------------------------------


class _Rule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def check(
        self, fields: frozenset[str], protected: frozenset[str]
    ) -> tuple[frozenset[str], list[str]]:
        raise NotImplementedError

    def apply(self, row: dict[str, Any]) -> bool:
        raise NotImplementedError


class RenameRule(_Rule):
    type: Literal["rename"]
    mapping: dict[str, str] = Field(min_length=1)

    def check(self, fields, protected):
        _require_fields(fields, list(self.mapping))
        _require_changeable(protected, list(self.mapping))
        new_names = list(self.mapping.values())
        if len(set(new_names)) != len(new_names):
            raise RuleConfigError("two fields cannot be renamed to the same name")
        remaining = fields - set(self.mapping)
        for old, new in self.mapping.items():
            if old == new:
                raise RuleConfigError(f"'{old}' is renamed to itself")
            _check_new_name(new, remaining)
        return remaining | set(new_names), []

    def apply(self, row):
        items = list(row.items())
        row.clear()
        for key, value in items:  # rebuilt so the field order is kept
            row[self.mapping.get(key, key)] = value
        return True


class DropRule(_Rule):
    type: Literal["drop"]
    fields: list[str] = Field(min_length=1)

    @field_validator("fields")
    @classmethod
    def _unique(cls, value: list[str]) -> list[str]:
        return _unique_names(value)

    def check(self, fields, protected):
        _require_fields(fields, self.fields)
        _require_changeable(protected, self.fields)
        return fields - set(self.fields), []

    def apply(self, row):
        for name in self.fields:
            del row[name]
        return True


NormalizeOp = Literal["trim", "collapse_whitespace", "lower", "upper", "nfc", "nullify_empty"]

_NORMALIZERS = {
    "trim": str.strip,
    "collapse_whitespace": lambda s: " ".join(s.split()),
    "lower": str.lower,
    "upper": str.upper,
    "nfc": lambda s: unicodedata.normalize("NFC", s),
    "nullify_empty": lambda s: s or None,
}


class NormalizeRule(_Rule):
    type: Literal["normalize"]
    fields: list[str] = Field(min_length=1)
    ops: list[NormalizeOp] = Field(min_length=1)

    @field_validator("fields")
    @classmethod
    def _unique(cls, value: list[str]) -> list[str]:
        return _unique_names(value)

    def check(self, fields, protected):
        _require_fields(fields, self.fields)
        _require_changeable(protected, self.fields)
        return fields, []

    def apply(self, row):
        for name in self.fields:
            value = row[name]
            if value is None:
                continue
            if not isinstance(value, str):
                raise RowRejected(f"{name}: normalize needs text, got {type(value).__name__}")
            for op in self.ops:
                value = _NORMALIZERS[op](value)
                if value is None:  # only nullify_empty can return None
                    break
            row[name] = value
        return True


def _mask_email(value: str, field: str) -> str:
    local, at, domain = value.partition("@")
    labels = domain.split(".")
    valid = (
        at == "@"
        and "@" not in domain
        and local != ""
        and not any(char.isspace() for char in value)
        and len(labels) >= 2
        and all(labels)
    )
    if not valid:
        raise RowRejected(f"{field}: not a valid email address")
    return f"{local[0]}***@***.{labels[-1]}"


def _mask_partial(value: str, keep_last: int) -> str:
    keep = min(keep_last, len(value) // 2)  # never reveal more than half
    return "*" * (len(value) - keep) + value[len(value) - keep :]


class MaskRule(_Rule):
    type: Literal["mask"]
    field: str
    style: Literal["email", "partial", "redact"]
    keep_last: int | None = Field(default=None, ge=0, le=64)

    @model_validator(mode="after")
    def _keep_last_only_for_partial(self):
        if self.keep_last is not None and self.style != "partial":
            raise ValueError("keep_last only applies to style 'partial'")
        return self

    def check(self, fields, protected):
        _require_fields(fields, [self.field])
        _require_changeable(protected, [self.field])
        return fields, []

    def apply(self, row):
        value = row[self.field]
        if value is None:
            return True
        if self.style == "redact":
            row[self.field] = REDACTED
            return True
        if not isinstance(value, str):
            raise RowRejected(
                f"{self.field}: cannot mask a {type(value).__name__} with style '{self.style}'"
            )
        if self.style == "email":
            row[self.field] = _mask_email(value, self.field)
        else:
            row[self.field] = _mask_partial(value, 4 if self.keep_last is None else self.keep_last)
        return True


class HashRule(_Rule):
    type: Literal["hash"]
    field: str
    into: str | None = None
    salt: str | None = Field(default=None, min_length=8)

    def check(self, fields, protected):
        _require_fields(fields, [self.field])
        if self.into is None:
            _require_changeable(protected, [self.field])
            result = fields
        else:
            _check_new_name(self.into, fields)
            result = fields | {self.into}

        notes = []
        if self.salt is None:
            notes.append(
                f"hash of '{self.field}' is unsalted: guessable values (emails, phone "
                "numbers) can be recovered by trying candidates; set a secret "
                "'salt' (salt: ${HASH_SALT})"
            )
        return result, notes

    def apply(self, row):
        value = row[self.field]
        target = self.into or self.field
        if value is None:
            row[target] = None
            return True
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise RowRejected(
                f"{self.field}: can only hash text or integers, got {type(value).__name__}"
            )
        try:
            data = str(value).encode("utf-8")
        except UnicodeEncodeError:
            raise RowRejected(f"{self.field}: text cannot be encoded as UTF-8") from None

        if self.salt is None:
            row[target] = hashlib.sha256(data).hexdigest()
        else:
            row[target] = hmac.new(self.salt.encode("utf-8"), data, hashlib.sha256).hexdigest()
        return True


class ComputeRule(_Rule):
    type: Literal["compute"]
    into: str
    expression: str

    @field_validator("expression")
    @classmethod
    def _valid_expression(cls, value: str) -> str:
        return _validated_expression(value)

    def check(self, fields, protected):
        _check_expression_fields(compile_expression(self.expression), fields)
        _check_new_name(self.into, fields)
        return fields | {self.into}, []

    def apply(self, row):
        value = _evaluate(compile_expression(self.expression), row)
        try:
            row[self.into] = generic_to_bson(value)
        except ValueError as exc:
            raise RowRejected(f"{self.into}: computed value cannot be stored ({exc})") from exc
        return True


class FilterRule(_Rule):
    type: Literal["filter"]
    expression: str

    @field_validator("expression")
    @classmethod
    def _valid_expression(cls, value: str) -> str:
        return _validated_expression(value)

    def check(self, fields, protected):
        _check_expression_fields(compile_expression(self.expression), fields)
        return fields, []

    def apply(self, row):
        result = _evaluate(compile_expression(self.expression), row)
        if result is True:
            return True
        if result is False or result is None:
            return False
        raise RowRejected(
            f"filter `{self.expression}` must give true or false, got {type(result).__name__}"
        )


class RequireRule(_Rule):
    type: Literal["require"]
    expression: str
    message: str | None = None

    @field_validator("expression")
    @classmethod
    def _valid_expression(cls, value: str) -> str:
        return _validated_expression(value)

    def check(self, fields, protected):
        _check_expression_fields(compile_expression(self.expression), fields)
        return fields, []

    def apply(self, row):
        result = _evaluate(compile_expression(self.expression), row)
        if result is True:
            return True
        if result is False or result is None:
            raise RowRejected(self.message or f"requirement not met: {self.expression}")
        raise RowRejected(
            f"requirement `{self.expression}` must give true or false, got {type(result).__name__}"
        )


# The manifest uses this union: the `type` key picks the rule class.
Rule = Annotated[
    Union[
        RenameRule,
        DropRule,
        NormalizeRule,
        MaskRule,
        HashRule,
        ComputeRule,
        FilterRule,
        RequireRule,
    ],
    Field(discriminator="type"),
]


# --- planning and applying a table's rules -----------------------------------------


@dataclass(frozen=True)
class TransformPlan:
    table: str
    rules: tuple[_Rule, ...]
    output_fields: frozenset[str]
    notes: tuple[str, ...]

    def apply(self, row: dict[str, Any]) -> dict[str, Any] | None:
        """Apply every rule in order.

        Returns the changed row, or None if a rule dropped it.
        Raises RowRejected if a rule refuses the row.
        """
        for rule in self.rules:
            if not rule.apply(row):
                return None
        return row


def build_transform_plan(
    table: str,
    columns: Sequence[str],
    primary_key: Sequence[str],
    rules: Sequence[_Rule],
) -> TransformPlan:
    """Validate a table's rules against its columns, before anything runs."""
    fields = frozenset(columns)
    protected = frozenset(primary_key)
    notes: list[str] = []

    for index, rule in enumerate(rules, start=1):
        try:
            fields, rule_notes = rule.check(fields, protected)
        except RuleConfigError as exc:
            raise TransformConfigError(
                f"transforms.{table} rule {index} ({rule.type}): {exc}"
            ) from exc
        notes.extend(f"{table}: {note}" for note in rule_notes)

    if "_id" in fields and "_id" not in protected:
        raise TransformConfigError(
            f"table '{table}' has a field named '_id' that is not its primary key; "
            "rename it with a 'rename' rule (it would collide with MongoDB's _id)"
        )

    return TransformPlan(
        table=table, rules=tuple(rules), output_fields=fields, notes=tuple(notes)
    )