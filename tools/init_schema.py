"""Initialize Cosmos/Mongo schema (collections + indexes).

Note: Cosmos partition keys must be configured at container creation time.
Mongo API does not allow setting partition keys via createCollection.
"""

from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

                                             
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib.mongo import get_db_name, get_mongo_client, get_mongo_uri


def ensure_indexes(db) -> None:
            
    try:
        db.bricks.create_index("brick_id", unique=True)
    except Exception:
        pass
    try:
        db.bricks.create_index("image_path")
    except Exception:
        pass
    for f in ("dataset", "manufacturer", "region_id"):
        try:
            db.bricks.create_index(f)
        except Exception:
            pass

                    
    try:
        db.brick_features.create_index("_id", unique=True)
    except Exception:
        pass
    try:
        db.brick_features.create_index("brick_id")
    except Exception:
        pass
    try:
        db.brick_features.create_index("dataset")
    except Exception:
        pass

             
    try:
        db.regions.create_index("region_id", unique=True)
    except Exception:
        pass

                    
    try:
        db.business_units.create_index("bu_code", unique=True)
    except Exception:
        pass
    try:
        db.business_units.create_index("region_id")
    except Exception:
        pass

                   
    try:
        db.brick_pricing.create_index("brick_id", unique=True)
    except Exception:
        pass
    try:
        db.brick_pricing.create_index("factory_code")
    except Exception:
        pass

               
    try:
        db.brick_co2.create_index("brick_id", unique=True)
    except Exception:
        pass
    try:
        db.brick_co2.create_index("region_id")
    except Exception:
        pass

               
    try:
        db.factories.create_index("factory_code", unique=True)
    except Exception:
        pass
    try:
        db.factories.create_index("status")
    except Exception:
        pass
    try:
        db.factories.create_index("manufacturer")
    except Exception:
        pass


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Initialize collections + indexes")
    p.add_argument("--db", default=None, help="Override DB name")
    args = p.parse_args(argv)

                                                               
    load_dotenv(interpolate=False)

    uri = get_mongo_uri()
    db_name = args.db or get_db_name()

    client = get_mongo_client(uri)
    db = client[db_name]

    ensure_indexes(db)

    print("Schema init complete")
    print(f"- URI: {uri.split('@')[-1] if '@' in uri else uri}")
    print(f"- DB: {db_name}")
    print("- Collections ensured: bricks, brick_features, regions, business_units, brick_pricing, brick_co2")
    print("- Reminder: set Cosmos partition keys in Azure when creating containers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
