from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from pymongo import MongoClient


def get_mongo_uri() -> str:
                                                
                                                             
    uri = (
        os.getenv("COSMOS_MONGODB_URI")
        or os.getenv("MONGODB_URI")
        or os.getenv("AZURE_COSMOSDB_MONGO_URI")
        or os.getenv("DEST_MONGODB_URI")
        or os.getenv("MONGO_URI")
        or os.getenv("MONGO_URL")
    )
    if not uri:
        raise RuntimeError("COSMOS_MONGODB_URI is required (or MONGODB_URI / DEST_MONGODB_URI / legacy MONGO_URI)")
    return uri


def get_db_name() -> str:
    return os.getenv("MONGODB_DB") or os.getenv("DB_NAME") or "brickdb"


@lru_cache(maxsize=1)
def get_mongo_client(uri: Optional[str] = None) -> MongoClient:
    uri = uri or get_mongo_uri()
    return MongoClient(uri, serverSelectionTimeoutMS=int(os.getenv("MONGO_TIMEOUT_MS", "5000")))


def get_db(db_name: Optional[str] = None):
    name = db_name or get_db_name()
    return get_mongo_client()[name]
