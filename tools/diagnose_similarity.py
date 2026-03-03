import argparse
import hashlib
import os
from typing import Any, Dict, List

import numpy as np
from pymongo import MongoClient


def _digest(vec: Any, n: int = 256) -> str:
    if vec is None:
        return "<none>"
    arr = np.asarray(vec, dtype=np.float32).ravel()
    return hashlib.sha1(arr[:n].tobytes()).hexdigest()


def _cos(a: Any, b: Any) -> float:
    a = np.asarray(a, dtype=np.float32).ravel()
    b = np.asarray(b, dtype=np.float32).ravel()
    m = min(a.shape[0], b.shape[0])
    if m == 0:
        return 0.0
    a = a[:m]
    b = b[:m]
    den = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-8
    return float(np.dot(a, b) / den)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    uri = os.getenv("COSMOS_MONGODB_URI") or os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
    if not uri:
        raise SystemExit("Missing COSMOS_MONGODB_URI (or MONGODB_URI/MONGO_URI)")

    db_name = os.getenv("MONGODB_DB", "brickdb")
    bricks_name = os.getenv("MONGODB_COLLECTION", "bricks")
    features_name = os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")

    client = MongoClient(uri, connectTimeoutMS=10000, serverSelectionTimeoutMS=10000)
    db = client[db_name]

    bricks = db[bricks_name]
    features = db[features_name]

    docs: List[Dict[str, Any]] = list(
        features.find(
            {},
            {
                "_id": 1,
                "dataset": 1,
                "deep_features": 1,
                "color_features": 1,
                "texture_features": 1,
            },
        ).limit(args.limit)
    )

    print(f"db={db_name} features={features_name} bricks={bricks_name} docs={len(docs)}")

    for key in ("deep_features", "color_features", "texture_features"):
        digests = [_digest(d.get(key)) for d in docs]
        print(f"{key}: unique_digests={len(set(digests))}")

    if not docs:
        return 0

    base = docs[0]
    for key in ("deep_features", "color_features", "texture_features"):
        sims = [_cos(base.get(key), d.get(key)) for d in docs if d.get(key) is not None]
        print(f"{key}: cos(min/mean/max)={min(sims):.4f}/{(sum(sims)/len(sims)):.4f}/{max(sims):.4f}")

    ids = [d.get("_id") for d in docs[:10]]
    meta_by_id = {
        b.get("brick_id"): (b.get("metadata") or {})
        for b in bricks.find({"brick_id": {"$in": ids}}, {"brick_id": 1, "metadata": 1})
    }

    print("sample metadata:")
    for brick_id in ids:
        m = meta_by_id.get(brick_id, {})
        name = m.get("display_name") or m.get("brick_name") or m.get("item_code")
        colour = m.get("colour") or m.get("brick_colour")
        print(f"- {str(brick_id)[:8]} name={name!r} colour={colour!r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
