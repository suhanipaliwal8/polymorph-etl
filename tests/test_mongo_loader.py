import datetime as dt
import uuid
from decimal import Decimal
from types import SimpleNamespace

import pytest
from bson import Decimal128
from bson.errors import InvalidDocument
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError

from polymorph.mongo_loader import (
    LoadError,
    LoadStats,
    build_document,
    connect,
    write_documents,
)


class FakeCollection:
    """Records bulk_write calls so we can test without a MongoDB server."""

    name = "customers"

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    def bulk_write(self, operations, ordered=True):
        self.calls.append((list(operations), ordered))
        if self.error:
            raise self.error
        return self.result


def result(created=0, modified=0, matched=0):
    return SimpleNamespace(
        upserted_count=created, modified_count=modified, matched_count=matched
    )


# --- documents ---------------------------------------------------------------


def test_single_primary_key_becomes_id_and_is_not_repeated():
    row = {"id": 5, "name": "Asha", "email": None}
    assert build_document(row, ["id"]) == {"_id": 5, "name": "Asha", "email": None}


def test_composite_primary_key_becomes_a_subdocument_in_key_order():
    row = {"course_id": 2, "grade": "A", "student_id": 1}

    document = build_document(row, ["student_id", "course_id"])

    assert document["_id"] == {"student_id": 1, "course_id": 2}
    assert list(document["_id"]) == ["student_id", "course_id"]
    assert document["grade"] == "A"


def test_non_key_column_named_id_is_rejected():
    with pytest.raises(LoadError, match="_id"):
        build_document({"id": 1, "_id": "x"}, ["id"])


# --- writing -----------------------------------------------------------------


def test_documents_are_written_as_unordered_upserts():
    collection = FakeCollection(result(created=1, modified=1, matched=1))
    documents = [{"_id": 1, "a": 1}, {"_id": 2, "a": 2}]

    stats = write_documents(collection, documents)

    operations, ordered = collection.calls[0]
    assert operations == [
        ReplaceOne({"_id": 1}, {"_id": 1, "a": 1}, upsert=True),
        ReplaceOne({"_id": 2}, {"_id": 2, "a": 2}, upsert=True),
    ]
    assert ordered is False
    assert stats == LoadStats(created=1, updated=1, unchanged=0)


def test_unchanged_is_matched_minus_modified():
    collection = FakeCollection(result(created=0, modified=2, matched=5))
    stats = write_documents(collection, [{"_id": i} for i in range(5)])
    assert stats == LoadStats(created=0, updated=2, unchanged=3)


def test_empty_batch_does_not_call_mongodb():
    collection = FakeCollection()
    assert write_documents(collection, []) == LoadStats(0, 0, 0)
    assert collection.calls == []


def test_write_errors_become_load_errors():
    error = BulkWriteError({"writeErrors": [{"errmsg": "boom"}]})
    with pytest.raises(LoadError, match="boom"):
        write_documents(FakeCollection(error=error), [{"_id": 1}])


def test_unencodable_values_become_load_errors():
    error = InvalidDocument("cannot encode object")
    with pytest.raises(LoadError, match="BSON"):
        write_documents(FakeCollection(error=error), [{"_id": 1}])


def test_load_stats_can_be_added():
    assert LoadStats(1, 2, 3) + LoadStats(10, 20, 30) == LoadStats(11, 22, 33)


# --- connecting --------------------------------------------------------------


def test_invalid_url_is_reported():
    with pytest.raises(LoadError, match="Invalid MongoDB URL"):
        connect("not a url")


def test_url_without_database_name_is_reported():
    with pytest.raises(LoadError, match="database name"):
        connect("mongodb://localhost:27018")


def test_unreachable_server_is_reported_without_leaking_the_password(monkeypatch):
    monkeypatch.setattr("polymorph.mongo_loader.CONNECT_TIMEOUT_MS", 300)
    with pytest.raises(LoadError, match="Could not connect") as excinfo:
        connect("mongodb://user:topsecret@127.0.0.1:1/shop")
    assert "topsecret" not in str(excinfo.value)