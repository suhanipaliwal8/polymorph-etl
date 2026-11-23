from decimal import Decimal

import pytest

from polymorph.demo.seed import build_data


def test_build_data_is_deterministic():
    assert build_data(100) == build_data(100)


def test_rejects_too_few_customers():
    with pytest.raises(ValueError):
        build_data(10)


def test_every_tenth_customer_has_no_profile():
    data = build_data(100)
    profiled = {p["customer_id"] for p in data["customer_profiles"]}
    assert 1 in profiled
    assert 10 not in profiled


def test_all_orders_reference_existing_customers():
    data = build_data(100)
    customer_ids = {c["id"] for c in data["customers"]}
    assert all(o["customer_id"] in customer_ids for o in data["orders"])


def test_high_precision_price_is_exact_decimal():
    data = build_data(100)
    item = next(i for i in data["order_items"] if i["id"] == 3)
    assert item["unit_price"] == Decimal("12345678901234.5678")


def test_poison_records_are_present():
    data = build_data(100)
    customers = {c["id"]: c for c in data["customers"]}
    assert customers[13]["email"] == "not-an-email"
    assert customers[27]["email"] is None
    assert customers[41]["name"].strip() == ""
    item = next(i for i in data["order_items"] if i["id"] == 5)
    assert item["quantity"] < 0