from decimal import Decimal

import pytest
from bson import Decimal128
from sqlalchemy import create_engine, text

from polymorph.extractor import extract_batches
from polymorph.inspector import create_source_engine
from polymorph.pipeline import (
    PipelineError,
    make_counters,
    plan_collection,
    prefetch_children,
    process_document,
)


def make_sqlite(tmp_path, name, *statements):
    path = tmp_path / name
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    engine.dispose()
    return f"sqlite:///{path.as_posix()}"


def query(url, sql):
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            return [tuple(row) for row in conn.execute(text(sql))]
    finally:
        engine.dispose()


def scalar(url, sql):
    return query(url, sql)[0][0]


def fold(url, manifest, collection="customers", batch_size=7):
    """Build every document of a collection. Returns (plan, outcomes by root key, counters)."""
    engine = create_source_engine(url)
    try:
        plan = plan_collection(engine, collection, manifest)
        counters = make_counters(plan)
        outcomes = {}
        for batch in extract_batches(engine, plan.root.name, batch_size=batch_size):
            groups = {}
            prefetch_children(engine, plan.embeds, batch.rows, groups)
            for row in batch.rows:
                key = tuple(row[name] for name in plan.root.pk_names)
                outcomes[key[0] if len(key) == 1 else key] = process_document(
                    plan, row, groups, counters
                )
        return plan, outcomes, counters
    finally:
        engine.dispose()


def documents(url, make_manifest, spec, **extra):
    manifest = make_manifest(url, collections=spec, **extra)
    plan, outcomes, counters = fold(url, manifest)
    return plan, {key: outcome.document for key, outcome in outcomes.items()}, counters


CUSTOMER_WITH_ORDERS = (
    "SELECT customer_id FROM orders WHERE customer_id % 10 != 0 ORDER BY customer_id LIMIT 1"
)


# --- shape of the documents -------------------------------------------------------------------


def test_a_customer_becomes_one_document_with_nested_rows(shop_url, make_manifest, folding_spec):
    customer = scalar(shop_url, CUSTOMER_WITH_ORDERS)
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)
    doc = docs[customer]

    assert doc["_id"] == customer
    assert "id" not in doc
    assert doc["profile"]["city"]
    order_ids = [
        row[0]
        for row in query(shop_url, f"SELECT id FROM orders WHERE customer_id = {customer} ORDER BY id")
    ]
    assert [order["id"] for order in doc["orders"]] == order_ids
    for order in doc["orders"]:
        item_ids = [
            row[0]
            for row in query(
                shop_url, f"SELECT id FROM order_items WHERE order_id = {order['id']} ORDER BY id"
            )
        ]
        assert [item["id"] for item in order["items"]] == item_ids


def test_foreign_key_columns_are_not_repeated_inside_nested_rows(
    shop_url, make_manifest, folding_spec
):
    customer = scalar(shop_url, CUSTOMER_WITH_ORDERS)
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)
    doc = docs[customer]

    assert "customer_id" not in doc["profile"]
    order = doc["orders"][0]
    assert "customer_id" not in order
    assert "id" in order
    assert "order_id" not in order["items"][0]
    assert "id" in order["items"][0]


def test_keep_foreign_key_keeps_the_link_column(shop_url, make_manifest, folding_spec):
    folding_spec["customers"]["embed"][1]["keep_foreign_key"] = True
    customer = scalar(shop_url, CUSTOMER_WITH_ORDERS)
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)

    assert docs[customer]["orders"][0]["customer_id"] == customer


def test_a_missing_one_to_one_row_becomes_null(shop_url, make_manifest, folding_spec):
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)
    assert docs[10]["profile"] is None  # every 10th customer has no profile


def test_a_parent_without_children_gets_an_empty_array(shop_url, make_manifest, folding_spec):
    lonely = scalar(
        shop_url, "SELECT MIN(id) FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)"
    )
    assert lonely is not None
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)
    assert docs[lonely]["orders"] == []


def test_every_source_row_is_embedded_exactly_once(shop_url, make_manifest, folding_spec):
    _, docs, counters = documents(shop_url, make_manifest, folding_spec)

    profiles = scalar(shop_url, "SELECT COUNT(*) FROM customer_profiles")
    orders = scalar(shop_url, "SELECT COUNT(*) FROM orders")
    items = scalar(shop_url, "SELECT COUNT(*) FROM order_items")

    assert sum(1 for doc in docs.values() if doc["profile"] is not None) == profiles
    assert sum(len(doc["orders"]) for doc in docs.values()) == orders
    assert sum(len(o["items"]) for doc in docs.values() for o in doc["orders"]) == items
    assert counters["customers.profile"].read == profiles
    assert counters["customers.orders"].read == orders
    assert counters["customers.orders.items"].read == items


def test_decimals_inside_nested_documents_stay_exact(shop_url, make_manifest, folding_spec):
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)
    items = [item for doc in docs.values() for o in doc["orders"] for item in o["items"]]

    assert all(isinstance(item["unit_price"], Decimal128) for item in items)
    big = next(item for item in items if item["id"] == 3)
    assert big["unit_price"].to_decimal() == Decimal("12345678901234.5684")


def test_embedded_rows_are_ordered_by_primary_key(shop_url, make_manifest, folding_spec):
    _, docs, _ = documents(shop_url, make_manifest, folding_spec)
    for doc in docs.values():
        order_ids = [order["id"] for order in doc["orders"]]
        assert order_ids == sorted(order_ids)
        for order in doc["orders"]:
            item_ids = [item["id"] for item in order["items"]]
            assert item_ids == sorted(item_ids)


def test_a_table_without_a_collection_definition_is_a_flat_collection(shop_url, make_manifest):
    plan, outcomes, _ = fold(shop_url, make_manifest(shop_url))
    assert plan.embeds == ()
    assert "orders" not in outcomes[1].document


# --- rules and folding ---------------------------------------------------------------------------


def test_rules_can_read_embedded_fields(shop_url, make_manifest, folding_spec):
    transforms = {
        "customers": [
            {"type": "compute", "into": "order_count", "expression": "len(orders)"},
            {"type": "compute", "into": "has_profile", "expression": "profile is not None"},
        ],
        "orders": [{"type": "compute", "into": "item_count", "expression": "len(items)"}],
    }
    _, docs, _ = documents(shop_url, make_manifest, folding_spec, transforms=transforms)

    for doc in docs.values():
        assert doc["order_count"] == len(doc["orders"])
        assert doc["has_profile"] is (doc["profile"] is not None)
        for order in doc["orders"]:
            assert order["item_count"] == len(order["items"])


def test_a_rejected_embedded_row_rejects_the_whole_document(shop_url, make_manifest, folding_spec):
    transforms = {
        "order_items": [
            {"type": "require", "expression": "quantity > 0", "message": "quantity must be positive"}
        ]
    }
    manifest = make_manifest(shop_url, collections=folding_spec, transforms=transforms)
    _, outcomes, counters = fold(shop_url, manifest)
    owner = scalar(
        shop_url,
        "SELECT o.customer_id FROM order_items i JOIN orders o ON o.id = i.order_id WHERE i.id = 5",
    )

    rejected = {key for key, outcome in outcomes.items() if outcome.rejection}
    assert rejected == {owner}
    assert "quantity must be positive" in outcomes[owner].rejection
    assert f"document rejected: customers id={owner}" in outcomes[owner].rejection
    assert counters["customers.orders.items"].rejected == 1
    assert counters["customers.orders"].rejected == 0  # the order itself was fine


def test_filtered_embedded_rows_are_left_out_and_counted(shop_url, make_manifest, folding_spec):
    transforms = {"order_items": [{"type": "filter", "expression": "quantity > 2"}]}
    manifest = make_manifest(shop_url, collections=folding_spec, transforms=transforms)
    _, outcomes, counters = fold(shop_url, manifest)
    docs = [outcome.document for outcome in outcomes.values()]

    kept = sum(len(o["items"]) for doc in docs for o in doc["orders"])
    assert kept == scalar(shop_url, "SELECT COUNT(*) FROM order_items WHERE quantity > 2")
    assert counters["customers.orders.items"].filtered == scalar(
        shop_url, "SELECT COUNT(*) FROM order_items WHERE quantity <= 2"
    )
    assert all(outcome.rejection is None for outcome in outcomes.values())


def test_a_filtered_one_to_one_row_becomes_null(shop_url, make_manifest, folding_spec):
    transforms = {
        "customer_profiles": [{"type": "filter", "expression": "loyalty_tier != 'bronze'"}]
    }
    bronze = scalar(
        shop_url,
        "SELECT MIN(customer_id) FROM customer_profiles WHERE loyalty_tier = 'bronze'",
    )
    _, docs, _ = documents(shop_url, make_manifest, folding_spec, transforms=transforms)
    assert docs[bronze]["profile"] is None


def test_removed_foreign_key_columns_are_not_visible_to_rules(
    shop_url, make_manifest, folding_spec
):
    transforms = {"orders": [{"type": "filter", "expression": "customer_id > 0"}]}
    with pytest.raises(PipelineError, match="unknown field.*customer_id"):
        fold(shop_url, make_manifest(shop_url, collections=folding_spec, transforms=transforms))

    folding_spec["customers"]["embed"][1]["keep_foreign_key"] = True
    fold(shop_url, make_manifest(shop_url, collections=folding_spec, transforms=transforms))


# --- finding the join ---------------------------------------------------------------------------------


def test_a_one_to_one_relation_with_two_matching_rows_rejects_the_parent(tmp_path, make_manifest):
    url = make_sqlite(
        tmp_path,
        "badges.db",
        "CREATE TABLE author (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE badge (id INTEGER PRIMARY KEY, author_id INTEGER REFERENCES author(id), label TEXT)",
        "INSERT INTO author VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO badge VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'z')",
    )
    collections = {
        "authors": {"from": "author", "embed": [{"table": "badge", "as": "badge", "relation": "one"}]}
    }

    plan, outcomes, _ = fold(url, make_manifest(url, collections=collections), collection="authors")

    assert "expected at most one" in outcomes[1].rejection
    assert outcomes[2].document["badge"] == {"id": 3, "label": "z"}
    assert any("not enforced" in note for note in plan.notes)
    assert outcomes[1].detail.stage == "relation"


def test_composite_foreign_keys_are_joined_on_all_columns(tmp_path, make_manifest):
    url = make_sqlite(
        tmp_path,
        "composite.db",
        "CREATE TABLE course (a INTEGER, b INTEGER, title TEXT, PRIMARY KEY (a, b))",
        "CREATE TABLE lesson (id INTEGER PRIMARY KEY, course_a INTEGER, course_b INTEGER, name TEXT, "
        "FOREIGN KEY (course_a, course_b) REFERENCES course (a, b))",
        "INSERT INTO course VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')",
        "INSERT INTO lesson VALUES (1, 1, 2, 'l1'), (2, 1, 2, 'l2'), (3, 2, 1, 'l3')",
    )
    collections = {
        "courses": {"from": "course", "embed": [{"table": "lesson", "as": "lessons", "relation": "many"}]}
    }

    _, outcomes, _ = fold(url, make_manifest(url, collections=collections), collection="courses")

    assert outcomes[(1, 2)].document["_id"] == {"a": 1, "b": 2}
    assert [lesson["name"] for lesson in outcomes[(1, 2)].document["lessons"]] == ["l1", "l2"]
    assert outcomes[(1, 1)].document["lessons"] == []
    assert [lesson["name"] for lesson in outcomes[(2, 1)].document["lessons"]] == ["l3"]


def test_a_join_without_a_declared_foreign_key_needs_join(tmp_path, make_manifest):
    url = make_sqlite(
        tmp_path,
        "legacy.db",
        "CREATE TABLE team (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE player (id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT)",
        "INSERT INTO team VALUES (1, 'red'), (2, 'blue')",
        "INSERT INTO player VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')",
    )
    embed = {"table": "player", "as": "players", "relation": "many"}

    with pytest.raises(PipelineError, match="no foreign key"):
        fold(
            url,
            make_manifest(url, collections={"teams": {"from": "team", "embed": [embed]}}),
            collection="teams",
        )

    with_join = {"teams": {"from": "team", "embed": [{**embed, "join": {"team_id": "id"}}]}}
    _, outcomes, _ = fold(url, make_manifest(url, collections=with_join), collection="teams")
    assert [player["name"] for player in outcomes[1].document["players"]] == ["a", "b"]


def test_two_foreign_keys_to_the_same_parent_must_be_disambiguated(tmp_path, make_manifest):
    url = make_sqlite(
        tmp_path,
        "tickets.db",
        "CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE ticket (id INTEGER PRIMARY KEY, created_by INTEGER REFERENCES person(id), "
        "assigned_to INTEGER REFERENCES person(id), title TEXT)",
        "INSERT INTO person VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO ticket VALUES (1, 1, 2, 't1'), (2, 2, 2, 't2')",
    )
    embed = {"table": "ticket", "as": "assigned", "relation": "many"}

    with pytest.raises(PipelineError, match="more than one foreign key"):
        fold(
            url,
            make_manifest(url, collections={"people": {"from": "person", "embed": [embed]}}),
            collection="people",
        )

    chosen = {"people": {"from": "person", "embed": [{**embed, "join": {"assigned_to": "id"}}]}}
    _, outcomes, _ = fold(url, make_manifest(url, collections=chosen), collection="people")
    assert [ticket["title"] for ticket in outcomes[2].document["assigned"]] == ["t1", "t2"]
    assert outcomes[1].document["assigned"] == []


def test_an_alias_that_collides_with_a_column_is_rejected(shop_url, make_manifest, folding_spec):
    folding_spec["customers"]["embed"][0]["as"] = "name"
    with pytest.raises(PipelineError, match="collide"):
        fold(shop_url, make_manifest(shop_url, collections=folding_spec))


def test_children_that_python_cannot_match_are_reported_not_lost(tmp_path, make_manifest):
    url = make_sqlite(
        tmp_path,
        "nocase.db",
        "CREATE TABLE parent (code TEXT PRIMARY KEY)",
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code TEXT COLLATE NOCASE)",
        "INSERT INTO parent VALUES ('ABC')",
        "INSERT INTO child VALUES (1, 'abc')",
    )
    collections = {
        "parents": {
            "from": "parent",
            "embed": [
                {
                    "table": "child",
                    "as": "children",
                    "relation": "many",
                    "join": {"parent_code": "code"},
                }
            ],
        }
    }

    # The database matches 'abc' to 'ABC' (case-insensitive); Python does not.
    with pytest.raises(PipelineError, match="could not be matched|not in Python"):
        fold(url, make_manifest(url, collections=collections), collection="parents")

def test_the_cause_of_a_rejected_document_has_its_own_table_and_key(
    shop_url, make_manifest, folding_spec
):
    transforms = {
        "order_items": [
            {"type": "require", "expression": "quantity > 0", "message": "quantity must be positive"}
        ]
    }
    _, outcomes, _ = fold(
        shop_url, make_manifest(shop_url, collections=folding_spec, transforms=transforms)
    )
    owner = scalar(
        shop_url,
        "SELECT o.customer_id FROM order_items i JOIN orders o ON o.id = i.order_id WHERE i.id = 5",
    )

    detail = outcomes[owner].detail

    assert (detail.stage, detail.table, detail.key) == ("transform", "order_items", {"id": 5})
    assert detail.reason == "quantity must be positive"