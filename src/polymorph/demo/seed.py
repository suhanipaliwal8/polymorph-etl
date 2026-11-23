"""Seed demo data into a SQL database.

Usage:
    python -m polymorph.demo.seed --db postgres
    python -m polymorph.demo.seed --db mysql --customers 500
    python -m polymorph.demo.seed --url sqlite:///demo.db
"""

from __future__ import annotations

import argparse
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Uuid,
    create_engine,
    insert,
)
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

DEFAULT_URLS = {
    "postgres": "postgresql+psycopg://polymorph:polymorph@localhost:5433/shop",
    "mysql": "mysql+pymysql://polymorph:polymorph@localhost:3307/shop",
}

# --- Schema: defined once, created on any SQL database ---------------------

metadata = MetaData()

customers = Table(
    "customers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("external_id", Uuid, nullable=False),
    Column("name", String(120)),
    Column("email", String(255)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("is_active", Boolean, nullable=False),
)

customer_profiles = Table(
    "customer_profiles",
    metadata,
    Column(
        "customer_id",
        Integer,
        ForeignKey("customers.id"),
        primary_key=True,
        autoincrement=False,
    ),
    Column("city", String(80)),
    Column("loyalty_tier", String(20)),
)

orders = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("customer_id", Integer, ForeignKey("customers.id"), nullable=False, index=True),
    Column("status", String(20), nullable=False),
    Column("total_amount", Numeric(19, 4), nullable=False),
    Column("placed_at", DateTime(timezone=True), nullable=False),
)

order_items = Table(
    "order_items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("order_id", Integer, ForeignKey("orders.id"), nullable=False, index=True),
    Column("sku", String(40), nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("unit_price", Numeric(19, 4), nullable=False),
)

# Parents first, children later (foreign key order).
TABLES_IN_INSERT_ORDER = (customers, customer_profiles, orders, order_items)

# --- Deterministic data generation -----------------------------------------

MIN_CUSTOMERS = 50
PRICE_QUANTUM = Decimal("0.0001")
FIRST_NAMES = ["Asha", "Rohan", "Meera", "Kabir", "Isha", "Arjun", "Neha", "Vikram", "Priya", "Sanjay"]
LAST_NAMES = ["Sharma", "Verma", "Iyer", "Patel", "Khan", "Singh", "Reddy", "Das", "Gupta", "Nair"]
CITIES = ["Bhopal", "Pune", "Chennai", "Jaipur", "Kochi", "Delhi"]
TIERS = ["bronze", "silver", "gold"]
STATUSES = ["pending", "paid", "shipped", "cancelled"]
SKUS = [f"SKU-{n:03d}" for n in range(1, 41)]

Rows = dict[str, list[dict[str, Any]]]


def build_data(n_customers: int, seed: int = 42) -> Rows:
    """Build the same demo rows every time for a given (n_customers, seed)."""
    if n_customers < MIN_CUSTOMERS:
        raise ValueError(f"--customers must be at least {MIN_CUSTOMERS}")

    rng = random.Random(seed)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    customer_rows: list[dict[str, Any]] = []
    profile_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []
    item_rows: list[dict[str, Any]] = []
    order_id = 0
    item_id = 0

    for cid in range(1, n_customers + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        created = base + timedelta(minutes=rng.randint(0, 500_000))
        customer_rows.append(
            {
                "id": cid,
                "external_id": uuid.UUID(int=rng.getrandbits(128), version=4),
                "name": f"{first} {last}",
                "email": f"{first}.{last}{cid}@example.com".lower(),
                "created_at": created,
                "is_active": rng.random() < 0.9,
            }
        )

        # Every 10th customer has no profile (tests LEFT JOIN behaviour later).
        if cid % 10 != 0:
            profile_rows.append(
                {
                    "customer_id": cid,
                    "city": rng.choice(CITIES),
                    "loyalty_tier": rng.choice(TIERS),
                }
            )

        # 0 to 4 orders per customer (some customers have none).
        for _ in range(rng.randint(0, 4)):
            order_id += 1
            order_rows.append(
                {
                    "id": order_id,
                    "customer_id": cid,
                    "status": rng.choice(STATUSES),
                    "total_amount": Decimal("0"),  # filled in below
                    "placed_at": created + timedelta(minutes=rng.randint(1, 100_000)),
                }
            )
            for _ in range(rng.randint(1, 5)):
                item_id += 1
                cents = rng.randint(100, 99_999)
                item_rows.append(
                    {
                        "id": item_id,
                        "order_id": order_id,
                        "sku": rng.choice(SKUS),
                        "quantity": rng.randint(1, 5),
                        "unit_price": (Decimal(cents) / 100).quantize(PRICE_QUANTUM),
                    }
                )

    # Precision edge case: a float64 cannot represent this value exactly.
    item_rows[2]["quantity"] = 1
    item_rows[2]["unit_price"] = Decimal("12345678901234.5678")

    # Order totals = sum of items (computed before poison is applied).
    totals: dict[int, Decimal] = {}
    for item in item_rows:
        totals[item["order_id"]] = totals.get(item["order_id"], Decimal("0")) + (
            item["quantity"] * item["unit_price"]
        )
    for order in order_rows:
        order["total_amount"] = totals.get(order["id"], Decimal("0")).quantize(PRICE_QUANTUM)

    # --- Deliberate "poison" records: valid for the DB, bad for our pipeline ---
    by_id = {row["id"]: row for row in customer_rows}
    by_id[13]["email"] = "not-an-email"
    by_id[27]["email"] = None
    by_id[41]["name"] = "   "
    item_rows[4]["quantity"] = -3  # item id 5; its order total no longer matches

    return {
        "customers": customer_rows,
        "customer_profiles": profile_rows,
        "orders": order_rows,
        "order_items": item_rows,
    }


# --- Writing to a database -------------------------------------------------


def seed(url: str, n_customers: int) -> None:
    data = build_data(n_customers)
    engine = create_engine(url)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        print(f"Could not connect to {engine.url.render_as_string(hide_password=True)}")
        print("Is the database running? Try: docker compose ps")
        print(f"Details: {exc.orig}")
        sys.exit(1)

    # Only drops/recreates OUR four tables, nothing else in the database.
    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.begin() as conn:
        for table in TABLES_IN_INSERT_ORDER:
            rows = data[table.name]
            conn.execute(insert(table), rows)
            print(f"  {table.name:<18} {len(rows):>6} rows")

    engine.dispose()
    print("Seeded. Poison records: customers 13, 27, 41 and order_items 5.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed PolyMorph demo data.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--db", choices=sorted(DEFAULT_URLS), help="Preset demo database")
    target.add_argument("--url", help="Any SQLAlchemy URL, e.g. sqlite:///demo.db")
    parser.add_argument("--customers", type=int, default=200)
    args = parser.parse_args()

    url = args.url or DEFAULT_URLS[args.db]
    try:
        seed(url, args.customers)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()