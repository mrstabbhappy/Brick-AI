"""Verify connectivity to MongoDB / Cosmos DB (Mongo API).

Usage (PowerShell):
  $env:MONGODB_URI='...'; $env:MONGODB_DB='brickdb'; python tools/verify_db_connection.py

Prints basic info and does a ping.
"""

from __future__ import annotations

import os

from pymongo import MongoClient


def main() -> None:
    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or os.getenv("MONGO_URL")
    if not mongo_uri:
        raise RuntimeError("MONGODB_URI is required")

    db_name = os.getenv("MONGODB_DB") or os.getenv("DB_NAME") or "brickdb"

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=int(os.getenv("MONGO_TIMEOUT_MS", "10000")))
    db = client[db_name]

    ping = db.command("ping")
    colls = db.list_collection_names()

    print({"ok": True, "db": db_name, "ping": ping, "collections": sorted(colls)})


if __name__ == "__main__":
    main()
