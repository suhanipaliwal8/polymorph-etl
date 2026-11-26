import os
import uuid

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from polymorph.demo.seed import seed
from polymorph.manifest import Manifest

MONGO_BASE_URL = os.environ.get("POLYMORPH_TEST_MONGO_URL", "mongodb://localhost:27018")


@pytest.fixture()
def shop_url(tmp_path):
    """A freshly seeded SQLite 'shop' database. No Docker needed."""
    url = f"sqlite:///{(tmp_path / 'shop.db').as_posix()}"
    seed(url, 50)
    return url


@pytest.fixture()
def make_manifest():
    """Factory for validated manifests, so tests do not need YAML files."""

    def build(
        source_url,
        target_url="mongodb://localhost:27018/unused",
        tables=None,
        mode="sql-to-nosql",
        type_overrides=None,
        settings=None,
        transforms=None,
        collections=None,
    ) -> Manifest:
        data = {
            "version": 1,
            "name": "test-pipeline",
            "mode": mode,
            "source": {"url": source_url},
            "target": {"url": target_url},
            "settings": {"batch_size": 7, **(settings or {})},
        }
        if tables is not None:
            data["tables"] = tables
        if type_overrides is not None:
            data["type_overrides"] = type_overrides
        if transforms is not None:
            data["transforms"] = transforms
        if collections is not None:
            data["collections"] = collections
        return Manifest.model_validate(data)

    return build


@pytest.fixture()
def folding_spec():
    """customers with profile (1:1), orders (1:N) and each order's items (1:N)."""
    return {
        "customers": {
            "from": "customers",
            "embed": [
                {"table": "customer_profiles", "as": "profile", "relation": "one"},
                {
                    "table": "orders",
                    "as": "orders",
                    "relation": "many",
                    "embed": [{"table": "order_items", "as": "items", "relation": "many"}],
                },
            ],
        }
    }


@pytest.fixture()
def masking_rules():
    """The same rules as examples/pipelines/postgres-to-mongo-masked.yaml."""
    return {
        "customers": [
            {
                "type": "normalize",
                "fields": ["name", "email"],
                "ops": ["nfc", "collapse_whitespace", "nullify_empty"],
            },
            {"type": "normalize", "fields": ["email"], "ops": ["lower"]},
            {
                "type": "require",
                "expression": "name is not None",
                "message": "customer name must not be blank",
            },
            {
                "type": "require",
                "expression": "email is not None",
                "message": "customer email is required",
            },
            {"type": "hash", "field": "email", "into": "email_hash", "salt": "unit-test-salt"},
            {"type": "mask", "field": "email", "style": "email"},
            {"type": "rename", "mapping": {"is_active": "active"}},
        ],
        "customer_profiles": [{"type": "filter", "expression": "loyalty_tier != 'bronze'"}],
        "orders": [
            {
                "type": "compute",
                "into": "total_with_tax",
                "expression": "round(total_amount * decimal('1.18'), 2)",
            }
        ],
        "order_items": [
            {
                "type": "require",
                "expression": "quantity > 0",
                "message": "quantity must be positive",
            }
        ],
    }


@pytest.fixture()
def mongo_target():
    """A throwaway MongoDB database: yields (url, database)."""
    client = MongoClient(MONGO_BASE_URL, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except PyMongoError:
        client.close()
        message = "MongoDB is not reachable (start it with: docker compose up -d)"
        if os.environ.get("POLYMORPH_REQUIRE_MONGO"):
            pytest.fail(message)  # CI: a missing MongoDB must be a failure, not a skip
        pytest.skip(message)

    name = f"polymorph_test_{uuid.uuid4().hex[:8]}"
    try:
        yield f"{MONGO_BASE_URL.rstrip('/')}/{name}", client[name]
    finally:
        client.drop_database(name)
        client.close()