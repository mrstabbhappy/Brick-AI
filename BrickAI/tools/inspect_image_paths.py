"""Inspect brick image path conventions in the database.

This helps decide how to migrate disk images to Azure Blob.

Usage (PowerShell):
  $env:MONGODB_URI='...'
  $env:MONGODB_DB='brickdb'
  # Optional:
  $env:MONGODB_FEATURES_COLLECTION='brick_features'
  $env:UPLOAD_FOLDER='C:\\Brick AI\\BRICK_AI_DEV\\uploads'

  python tools/inspect_image_paths.py --limit 50

It prints:
- example image_path values
- pattern counts (basename vs paths)
- how many files exist under UPLOAD_FOLDER by direct join and by basename
"""

from __future__ import annotations

import argparse
import os
from collections import Counter

from pymongo import MongoClient


def _get_mongo_uri() -> str:
    uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or os.getenv("MONGO_URL")
    if not uri:
        raise RuntimeError("MONGODB_URI is required")
    return uri


def _classify(value: str) -> str:
    if not value:
        return "empty"
    v = str(value)
    if os.path.isabs(v):
        return "abs_path"
    if v.startswith("/"):
        return "rooted_posix"
    if "\\" in v:
        return "has_backslash"
    if "/" in v:
        return "has_slash"
    return "basename"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    uri = _get_mongo_uri()
    db_name = os.getenv("MONGODB_DB") or os.getenv("DB_NAME") or "brickdb"
    features_name = os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")

    upload_folder = os.getenv("UPLOAD_FOLDER")
    if upload_folder and not os.path.isabs(upload_folder):
        upload_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), upload_folder)

    db = MongoClient(uri, serverSelectionTimeoutMS=10000)[db_name]
    coll = db[features_name]

    cur = coll.find({"image_path": {"$exists": True}}, {"image_path": 1}).limit(args.limit)

    examples: list[str] = []
    classes = Counter()
    exists_direct = 0
    exists_basename = 0

    for doc in cur:
        v = doc.get("image_path")
        if v is None:
            continue
        v = str(v)
        examples.append(v)
        classes[_classify(v)] += 1

        if upload_folder:
            p1 = v
            if not os.path.isabs(p1):
                p1 = os.path.join(upload_folder, p1)
            if os.path.exists(p1):
                exists_direct += 1

            base = os.path.basename(v.replace("\\", "/"))
            p2 = os.path.join(upload_folder, base)
            if os.path.exists(p2):
                exists_basename += 1

    print({
        "db": db_name,
        "collection": features_name,
        "upload_folder": upload_folder,
        "sample_size": len(examples),
        "class_counts": dict(classes),
        "exists_under_upload_folder_direct": exists_direct,
        "exists_under_upload_folder_by_basename": exists_basename,
        "examples": examples[: min(12, len(examples))],
    })


if __name__ == "__main__":
    main()
