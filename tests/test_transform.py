import hashlib
import hmac
from decimal import Decimal

import pytest
from bson import Decimal128
from pydantic import ValidationError

from polymorph.transform import (
    ComputeRule,
    DropRule,
    FilterRule,
    HashRule,
    MaskRule,
    NormalizeRule,
    RenameRule,
    RequireRule,
    RowRejected,
    RuleConfigError,
    TransformConfigError,
    build_transform_plan,
)

FIELDS = frozenset({"id", "name", "email", "qty", "price"})
PK = frozenset({"id"})
SALT = "unit-test-salt"


# --- rename -------------------------------------------------------------------


def test_rename_keeps_field_order_and_values():
    rule = RenameRule(type="rename", mapping={"name": "full_name"})
    row = {"id": 1, "name": "Asha", "email": "a@x.com"}

    assert rule.apply(row) is True

    assert list(row.items()) == [("id", 1), ("full_name", "Asha"), ("email", "a@x.com")]


def test_rename_can_swap_two_fields():
    rule = RenameRule(type="rename", mapping={"name": "email", "email": "name"})
    fields, _ = rule.check(FIELDS, PK)
    assert fields == FIELDS

    row = {"name": "n", "email": "e"}
    rule.apply(row)
    assert row == {"email": "n", "name": "e"}


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        ({"id": "pk"}, "primary key"),
        ({"nope": "x"}, "unknown field"),
        ({"name": "email"}, "already exists"),
        ({"name": "same", "email": "same"}, "same name"),
        ({"name": "name"}, "itself"),
        ({"name": "_id"}, "reserved"),
        ({"name": "a.b"}, "valid MongoDB field name"),
    ],
)
def test_invalid_renames(mapping, message):
    with pytest.raises(RuleConfigError, match=message):
        RenameRule(type="rename", mapping=mapping).check(FIELDS, PK)


def test_unknown_field_suggests_a_close_match():
    with pytest.raises(RuleConfigError, match="did you mean 'email'"):
        MaskRule(type="mask", field="emial", style="email").check(FIELDS, PK)


# --- drop ---------------------------------------------------------------------


def test_drop_removes_fields_and_protects_the_primary_key():
    rule = DropRule(type="drop", fields=["email"])
    fields, _ = rule.check(FIELDS, PK)
    assert "email" not in fields

    row = {"id": 1, "email": "a@x.com"}
    rule.apply(row)
    assert row == {"id": 1}

    with pytest.raises(RuleConfigError, match="primary key"):
        DropRule(type="drop", fields=["id"]).check(FIELDS, PK)


def test_duplicate_field_lists_are_rejected():
    with pytest.raises(ValidationError):
        DropRule(type="drop", fields=["email", "email"])


# --- normalize ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("ops", "raw", "expected"),
    [
        (["trim"], "  a b  ", "a b"),
        (["collapse_whitespace"], "  a   b \n c ", "a b c"),
        (["lower"], "AbC", "abc"),
        (["upper"], "AbC", "ABC"),
        (["nullify_empty"], "", None),
        (["nullify_empty"], "x", "x"),
        (["collapse_whitespace", "nullify_empty"], "   ", None),
        (["nfc"], "e\u0301", "\u00e9"),
    ],
)
def test_normalize_operations(ops, raw, expected):
    rule = NormalizeRule(type="normalize", fields=["name"], ops=ops)
    row = {"name": raw}
    rule.apply(row)
    assert row["name"] == expected


def test_normalize_passes_null_through_and_rejects_non_text():
    rule = NormalizeRule(type="normalize", fields=["name"], ops=["trim"])
    row = {"name": None}
    rule.apply(row)
    assert row["name"] is None

    with pytest.raises(RowRejected, match="needs text"):
        rule.apply({"name": 5})


def test_normalize_cannot_change_a_primary_key():
    with pytest.raises(RuleConfigError, match="primary key"):
        NormalizeRule(type="normalize", fields=["id"], ops=["trim"]).check(FIELDS, PK)


# --- mask ---------------------------------------------------------------------


def test_email_masking_keeps_first_letter_and_top_level_domain():
    rule = MaskRule(type="mask", field="email", style="email")
    row = {"email": "asha.sharma@example.com"}
    rule.apply(row)
    assert row["email"] == "a***@***.com"


@pytest.mark.parametrize(
    "bad",
    ["not-an-email", "a@b", "@x.com", "a@@x.com", "a b@x.com", "a@.com", "a@x..com", ""],
)
def test_invalid_emails_are_rejected_without_echoing_the_value(bad):
    rule = MaskRule(type="mask", field="email", style="email")
    with pytest.raises(RowRejected) as excinfo:
        rule.apply({"email": bad})
    assert "not a valid email" in str(excinfo.value)
    if bad:
        assert bad not in str(excinfo.value)


def test_partial_masking_never_reveals_more_than_half():
    rule = MaskRule(type="mask", field="phone", style="partial", keep_last=4)
    for raw, expected in [("9876543210", "******3210"), ("12", "*2"), ("1234", "**34"), ("", "")]:
        row = {"phone": raw}
        rule.apply(row)
        assert row["phone"] == expected


def test_partial_masking_defaults_to_the_last_four_characters():
    rule = MaskRule(type="mask", field="phone", style="partial")
    row = {"phone": "9876543210"}
    rule.apply(row)
    assert row["phone"] == "******3210"


def test_redact_replaces_any_non_null_value():
    rule = MaskRule(type="mask", field="x", style="redact")
    for raw in ("secret", 42, True):
        row = {"x": raw}
        rule.apply(row)
        assert row["x"] == "[REDACTED]"
    row = {"x": None}
    rule.apply(row)
    assert row["x"] is None


def test_keep_last_only_applies_to_the_partial_style():
    with pytest.raises(ValidationError):
        MaskRule(type="mask", field="email", style="email", keep_last=2)


def test_text_styles_reject_non_text_values():
    with pytest.raises(RowRejected, match="cannot mask"):
        MaskRule(type="mask", field="x", style="email").apply({"x": 5})


# --- hash ---------------------------------------------------------------------


def test_unsalted_hash_is_plain_sha256():
    row = {"email": "abc"}
    HashRule(type="hash", field="email").apply(row)
    assert row["email"] == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_salted_hash_is_hmac_sha256_and_depends_on_the_salt():
    first = {"email": "abc"}
    HashRule(type="hash", field="email", salt=SALT).apply(first)
    second = {"email": "abc"}
    HashRule(type="hash", field="email", salt="another-salt").apply(second)

    assert first["email"] == hmac.new(SALT.encode(), b"abc", hashlib.sha256).hexdigest()
    assert first["email"] != second["email"]


def test_hash_into_keeps_the_original_field():
    rule = HashRule(type="hash", field="email", into="email_hash", salt=SALT)
    fields, _ = rule.check(FIELDS, PK)
    assert fields == FIELDS | {"email_hash"}

    row = {"email": "abc"}
    rule.apply(row)
    assert row["email"] == "abc"
    assert len(row["email_hash"]) == 64


def test_hash_handles_null_and_integers_and_rejects_other_types():
    rule = HashRule(type="hash", field="x", salt=SALT)

    row = {"x": None}
    rule.apply(row)
    assert row["x"] is None

    as_int, as_text = {"x": 5}, {"x": "5"}
    rule.apply(as_int)
    rule.apply(as_text)
    assert as_int["x"] == as_text["x"]

    with pytest.raises(RowRejected, match="can only hash"):
        rule.apply({"x": True})


def test_unsalted_hash_adds_a_note_and_salted_does_not():
    _, notes = HashRule(type="hash", field="email").check(FIELDS, PK)
    assert "unsalted" in notes[0]
    _, notes = HashRule(type="hash", field="email", salt=SALT).check(FIELDS, PK)
    assert notes == []


def test_short_salts_are_rejected():
    with pytest.raises(ValidationError):
        HashRule(type="hash", field="email", salt="short")


def test_hash_in_place_cannot_change_a_primary_key_but_into_can_read_it():
    with pytest.raises(RuleConfigError, match="primary key"):
        HashRule(type="hash", field="id", salt=SALT).check(FIELDS, PK)
    HashRule(type="hash", field="id", into="id_hash", salt=SALT).check(FIELDS, PK)


# --- compute ------------------------------------------------------------------


def test_compute_with_decimal128_operands_gives_an_exact_decimal128():
    rule = ComputeRule(type="compute", into="line_total", expression="qty * price")
    fields, _ = rule.check(FIELDS, PK)
    assert "line_total" in fields

    row = {"qty": 3, "price": Decimal128(Decimal("19.9900"))}
    rule.apply(row)

    assert isinstance(row["line_total"], Decimal128)
    assert row["line_total"].to_decimal() == Decimal("59.9700")


def test_compute_validation_errors():
    with pytest.raises(RuleConfigError, match="already exists"):
        ComputeRule(type="compute", into="qty", expression="1").check(FIELDS, PK)
    with pytest.raises(RuleConfigError, match="unknown field"):
        ComputeRule(type="compute", into="x", expression="nope + 1").check(FIELDS, PK)


def test_unsafe_expressions_fail_when_the_rule_is_created():
    with pytest.raises(ValidationError):
        ComputeRule(type="compute", into="x", expression="__import__('os')")


def test_compute_rejects_results_that_cannot_be_stored():
    expression = "decimal('" + "1" * 35 + "')"  # 35 digits: more than Decimal128 holds
    rule = ComputeRule(type="compute", into="big", expression=expression)
    with pytest.raises(RowRejected, match="cannot be stored"):
        rule.apply({})


def test_compute_with_a_null_operand_rejects_the_row():
    rule = ComputeRule(type="compute", into="line_total", expression="qty * price")
    with pytest.raises(RowRejected, match="TypeError"):
        rule.apply({"qty": None, "price": Decimal128("1")})


def test_expression_errors_name_the_exception_but_not_the_value():
    rule = RequireRule(type="require", expression="x > 1")
    with pytest.raises(RowRejected) as excinfo:
        rule.apply({"x": "secret"})
    assert "TypeError" in str(excinfo.value)
    assert "secret" not in str(excinfo.value)


# --- filter and require --------------------------------------------------------


def test_filter_keeps_true_and_drops_false_and_null():
    rule = FilterRule(type="filter", expression="qty > 1")
    assert rule.apply({"qty": 5}) is True
    assert rule.apply({"qty": 1}) is False
    assert FilterRule(type="filter", expression="flag").apply({"flag": None}) is False


def test_filter_rejects_non_boolean_results():
    with pytest.raises(RowRejected, match="true or false"):
        FilterRule(type="filter", expression="name").apply({"name": "x"})


def test_require_passes_and_fails_with_a_custom_message():
    rule = RequireRule(type="require", expression="qty > 0", message="quantity must be positive")
    assert rule.apply({"qty": 1}) is True
    with pytest.raises(RowRejected, match="quantity must be positive"):
        rule.apply({"qty": -3})


def test_require_has_a_default_message_and_treats_null_as_failure():
    with pytest.raises(RowRejected, match="requirement not met"):
        RequireRule(type="require", expression="flag").apply({"flag": None})


# --- planning a table's rules ----------------------------------------------------

COLUMNS = ["id", "name", "email", "qty", "price"]


def plan(rules):
    return build_transform_plan("t", COLUMNS, ["id"], rules)


def test_field_names_flow_through_the_rule_chain():
    result = plan(
        [
            RenameRule(type="rename", mapping={"qty": "quantity"}),
            ComputeRule(type="compute", into="total", expression="quantity * price"),
            DropRule(type="drop", fields=["email"]),
        ]
    )
    assert result.output_fields == frozenset({"id", "name", "quantity", "price", "total"})


def test_a_renamed_field_cannot_be_referenced_by_its_old_name():
    with pytest.raises(TransformConfigError, match=r"t rule 2 \(filter\).*unknown field"):
        plan(
            [
                RenameRule(type="rename", mapping={"qty": "quantity"}),
                FilterRule(type="filter", expression="qty > 1"),
            ]
        )


def test_an_id_column_that_is_not_the_primary_key_must_be_renamed():
    with pytest.raises(TransformConfigError, match="_id"):
        build_transform_plan("t", ["pk", "_id"], ["pk"], [])

    fixed = build_transform_plan(
        "t", ["pk", "_id"], ["pk"], [RenameRule(type="rename", mapping={"_id": "legacy_id"})]
    )
    assert "_id" not in fixed.output_fields


def test_plan_applies_rules_in_order_and_signals_dropped_rows():
    result = plan(
        [
            NormalizeRule(type="normalize", fields=["name"], ops=["trim", "upper"]),
            FilterRule(type="filter", expression="qty > 1"),
        ]
    )
    kept = result.apply({"id": 1, "name": " a ", "email": "e", "qty": 2, "price": 1})
    assert kept["name"] == "A"
    assert result.apply({"id": 2, "name": "b", "email": "e", "qty": 1, "price": 1}) is None


def test_plan_notes_carry_the_table_name():
    result = plan([HashRule(type="hash", field="email", into="h")])
    assert result.notes[0].startswith("t: ")