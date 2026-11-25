"""MongoDB loading: document building and idempotent bulk upserts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from bson.errors import InvalidDocument
from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    PyMongoError,
    ServerSelectionTimeoutError,
)
from pymongo.uri_parser import parse_uri

CONNECT_TIMEOUT_MS = 5000


class LoadError(Exception):
    """Loading failed. The message is user-facing and never contains secrets."""


@dataclass(frozen=True)
class LoadStats:
    created: int
    updated: int
    unchanged: int

    def __add__(self, other: LoadStats) -> LoadStats:
        return LoadStats(
            self.created + other.created,
            self.updated + other.updated,
            self.unchanged + other.unchanged,
        )


def connect(url: str) -> tuple[MongoClient, Database]:
    """Connect to MongoDB; return (client, default database named in the URL).

    Fails fast: the server is pinged here, so a wrong host or port is reported
    before any data is read.
    """
    try:
        parse_uri(url)
        client: MongoClient = MongoClient(url, serverSelectionTimeoutMS=CONNECT_TIMEOUT_MS)
    except ConfigurationError as exc:  # InvalidURI is a subclass
        raise LoadError(
            "Invalid MongoDB URL; expected mongodb://user:password@host:27017/database"
        ) from exc

    try:
        database = client.get_default_database()
    except ConfigurationError as exc:
        client.close()
        raise LoadError(
            "The target URL must include a database name, e.g. mongodb://host:27017/shop"
        ) from exc

    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as exc:
        client.close()
        raise LoadError(
            f"Could not connect to MongoDB within {CONNECT_TIMEOUT_MS / 1000:g} seconds. "
            "Check the host, port, network and that the server is running."
        ) from exc
    except PyMongoError as exc:
        client.close()
        raise LoadError(
            "MongoDB rejected the connection (check credentials and permissions)."
        ) from exc

    return client, database


def build_document(row: Mapping[str, Any], pk_names: Sequence[str]) -> dict[str, Any]:
    """Turn an already-translated row into a MongoDB document.

    The primary key becomes `_id`; the key columns are not repeated as fields.
    """
    if "_id" in row and "_id" not in pk_names:
        raise LoadError(
            "A source column named '_id' collides with MongoDB's own _id field; "
            "rename it in the source or wait for the rename transform (Phase 7)"
        )

    if len(pk_names) == 1:
        doc_id = row[pk_names[0]]
    else:
        # Field order inside a subdocument _id matters in MongoDB:
        # always follow the primary-key order, never the row's column order.
        doc_id = {name: row[name] for name in pk_names}

    document: dict[str, Any] = {"_id": doc_id}
    for name, value in row.items():
        if name not in pk_names:
            document[name] = value
    return document


def write_documents(collection: Collection, documents: Sequence[dict[str, Any]]) -> LoadStats:
    """Upsert documents by `_id`. Safe to repeat: the result is always the same."""
    if not documents:
        return LoadStats(0, 0, 0)

    operations = [ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in documents]
    try:
        result = collection.bulk_write(operations, ordered=False)
    except BulkWriteError as exc:
        errors = exc.details.get("writeErrors", [])
        first = errors[0].get("errmsg", "unknown error") if errors else "unknown error"
        raise LoadError(
            f"MongoDB rejected {len(errors)} write(s) to '{collection.name}'; "
            f"first error: {first}"
        ) from exc
    except InvalidDocument as exc:
        raise LoadError(
            f"A value in '{collection.name}' cannot be stored as BSON: {exc}"
        ) from exc
    except PyMongoError as exc:
        raise LoadError(
            f"MongoDB write to '{collection.name}' failed: {exc.__class__.__name__}"
        ) from exc

    updated = result.modified_count
    return LoadStats(
        created=result.upserted_count,
        updated=updated,
        unchanged=result.matched_count - updated,
    )