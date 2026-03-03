"""Migrate existing local images referenced by brick_features into Azure Blob Storage.

This helps complete the migration when you move from local filesystem uploads to
Blob-backed `/uploads/<key>` URLs.

Usage (PowerShell):
  # Configure DB
  $env:MONGODB_URI='...'
  $env:MONGODB_DB='brickdb'

  # Configure blob
  $env:AZURE_STORAGE_CONNECTION_STRING='...'
  $env:AZURE_BLOB_UPLOADS_CONTAINER='uploads'

  # Optionally set where local uploads live
  $env:UPLOAD_FOLDER='uploads'

  python tools/migrate_images_to_blob.py --dry-run
  python tools/migrate_images_to_blob.py

Notes:
- Idempotent-ish: skips docs where `image_path` already looks like a blob key.
- Writes `legacy_image_path` (original) and `image_migrated_at` on update.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from pymongo import MongoClient

from lib.blob_storage import BlobStorage


def _looks_like_local_path(value: str) -> bool:
    if not value:
        return False
    if os.path.isabs(value):
        return True
    return ("/" in value) or ("\\" in value)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print actions without uploading/updating")
    ap.add_argument("--limit", type=int, default=0, help="Optional max docs to process")
    ap.add_argument(
        "--strip-prefix",
        action="append",
        default=[],
        help="If image_path starts with this prefix, strip it before resolving locally. Can be repeated.",
    )
    args = ap.parse_args()

    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or os.getenv("MONGO_URL")
    if not mongo_uri:
        raise RuntimeError("MONGODB_URI is required")
    db_name = os.getenv("MONGODB_DB") or os.getenv("DB_NAME") or "brickdb"
    features_name = os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")

    upload_folder = os.getenv("UPLOAD_FOLDER", "uploads")
    if not os.path.isabs(upload_folder):
        upload_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), upload_folder)

    strip_prefixes = list(args.strip_prefix)
    env_strip = os.getenv("IMAGE_PATH_STRIP_PREFIX")
    if env_strip:
        strip_prefixes.append(env_strip)

    blob = BlobStorage()
    if not blob.is_configured():
        raise RuntimeError("Azure Blob is not configured (set AZURE_STORAGE_CONNECTION_STRING or Managed Identity vars)")

    container = os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads")

    db = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)[db_name]
    coll = db[features_name]

    q = {"image_path": {"$exists": True, "$ne": None}}
    cursor = coll.find(q, {"image_path": 1}).batch_size(200)

    processed = 0
    migrated = 0
    skipped = 0
    missing = 0

    for doc in cursor:
        if args.limit and processed >= args.limit:
            break
        processed += 1

        image_path = doc.get("image_path")
        if not image_path:
            skipped += 1
            continue

        normalized = str(image_path)
        for prefix in strip_prefixes:
            if prefix and normalized.startswith(prefix):
                normalized = normalized[len(prefix) :].lstrip("/\\")
                break

                                                                               
        if not _looks_like_local_path(normalized):
            skipped += 1
            continue

        local_path = normalized
        if not os.path.isabs(local_path):
            local_path = os.path.join(upload_folder, local_path)

        if not os.path.exists(local_path):
            missing += 1
            continue

        blob_name = os.path.basename(local_path).replace(" ", "_")
                          
        blob_name = f"{doc.get('_id')}_{blob_name}"

        if args.dry_run:
            print({"_id": doc.get("_id"), "from": local_path, "to": blob_name})
            continue

        with open(local_path, "rb") as f:
            data = f.read()

        blob.upload_bytes(container=container, blob_name=blob_name, data=data, content_type="application/octet-stream")

        coll.update_one(
            {"_id": doc.get("_id")},
            {
                "$set": {
                    "legacy_image_path": doc.get("image_path"),
                    "image_path": blob_name,
                    "image_migrated_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )
        migrated += 1

    print(
        {
            "ok": True,
            "processed": processed,
            "migrated": migrated,
            "skipped": skipped,
            "missing": missing,
            "dry_run": bool(args.dry_run),
        }
    )


if __name__ == "__main__":
    main()
