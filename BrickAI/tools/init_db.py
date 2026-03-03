"""Initialize Mongo/Cosmos collections + indexes for Brick AI.

Usage (PowerShell):
  $env:MONGODB_URI='...'; $env:MONGODB_DB='brickdb'; python tools/init_db.py

This is safe to run multiple times.
"""

from __future__ import annotations

import os

from pymongo import MongoClient


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v:
        return v
    if default is not None:
        return default
    raise RuntimeError(f"Missing required env var: {name}")


def main() -> None:
    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or os.getenv("MONGO_URL")
    if not mongo_uri:
        raise RuntimeError("MONGODB_URI is required")

    db_name = os.getenv("MONGODB_DB") or os.getenv("DB_NAME") or "brickdb"

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=int(os.getenv("MONGO_TIMEOUT_MS", "10000")))
    db = client[db_name]

                                                             
    db.command("ping")

    features = db[os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")]
    users = db["users"]
    bu_locations = db["bu_locations"]

                            
                          
    try:
        features.create_index("processed_at")
    except Exception:
        pass

                                                 
    for key in (
        "metadata.brick_name",
        "metadata.item_number",
        "metadata.brick_colour",
        "metadata.brick_type",
        "metadata.brand",
        "metadata.material",
        "factory_code",
        "factory_region",
    ):
        try:
            features.create_index(key)
        except Exception:
            pass

                                                        
    try:
        features.create_index(
            [
                ("metadata.brick_name", "text"),
                ("metadata.item_number", "text"),
                ("metadata.brick_colour", "text"),
                ("metadata.brick_type", "text"),
                ("metadata.brand", "text"),
            ],
            name="brick_text",
        )
    except Exception:
        pass

                   
    try:
        users.create_index("username", unique=True)
    except Exception:
        pass
    try:
        users.create_index("email", unique=True)
    except Exception:
        pass

                          
    try:
        bu_locations.create_index("name", unique=True)
    except Exception:
        pass
    try:
        bu_locations.create_index("bu_code")
    except Exception:
        pass
    try:
        bu_locations.create_index("active")
    except Exception:
        pass

    print(
        {
            "ok": True,
            "db": db_name,
            "collections": {
                "brick_features": features.name,
                "users": users.name,
                "bu_locations": bu_locations.name,
            },
        }
    )


if __name__ == "__main__":
    main()
