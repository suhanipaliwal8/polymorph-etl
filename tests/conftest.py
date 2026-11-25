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
        return Manifest.model_validate(data)

    return build


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