from __future__ import annotations

import sys

import argparse
import os
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import parse_qsl, urlsplit


                                                                               
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pymongo import MongoClient

from lib.blob_storage import BlobStorage
from lib.mongo import get_db_name, get_mongo_uri


try:                         
    from dotenv import load_dotenv

    load_dotenv(interpolate=False)
except Exception:
    pass


def _redact_mongo_uri(uri: str) -> dict:
    parts = urlsplit(uri)
    netloc = parts.netloc
    username = None
    host = netloc
    if "@" in netloc:
        userinfo, host = netloc.rsplit("@", 1)
        username = userinfo.split(":", 1)[0] if userinfo else None

    query_keys = sorted({k for k, _ in parse_qsl(parts.query, keep_blank_values=True)})
    return {
        "scheme": parts.scheme,
        "host": host,
        "username": username,
        "db_path": (parts.path or "/").lstrip("/"),
        "query_keys": query_keys,
    }


def _normalize_image_key(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = str(value).strip().replace("\\", "/")
    if not v:
        return None
    if "uploads/" in v:
        v = v.split("uploads/", 1)[1]
    return os.path.basename(v)


def _iter_collection_names(db) -> Iterable[str]:
    for name in db.list_collection_names():
        if name.startswith("system."):
            continue
        yield name


@dataclass
class CheckResult:
    checked: int = 0
    ok: int = 0
    missing: int = 0
    no_image_field: int = 0


def main() -> int:
    p = argparse.ArgumentParser(description="Validate DB image_path keys exist in Azure Blob uploads container")
    p.add_argument("--limit", type=int, default=200, help="How many docs to check")
    p.add_argument("--skip", type=int, default=0, help="How many docs to skip")
    p.add_argument(
        "--collection",
        default=(os.getenv("MONGODB_COLLECTION") or "bricks"),
    )
    p.add_argument("--uploads-container", default=os.getenv("AZURE_BLOB_UPLOADS_CONTAINER") or "uploads")
    p.add_argument("--prefix", default="", help="Optional blob prefix (for listing)")
    p.add_argument("--list-blobs", action="store_true", help="Also list a sample of blobs in the container")
    p.add_argument("--list-blobs-limit", type=int, default=20, help="How many blob names to list")
    p.add_argument("--sample-missing", type=int, default=20, help="How many missing keys to print")
    args = p.parse_args()

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()

    print({"mongo": _redact_mongo_uri(mongo_uri), "db": db_name, "collection": args.collection})

    blob = BlobStorage()
    print({
        "blob": {
            "configured": blob.is_configured(),
            "uploads_container": args.uploads_container,
        }
    })
    if not blob.is_configured():
        print("ERROR: Blob is not configured. Set AZURE_STORAGE_CONNECTION_STRING (or Managed Identity vars).")
        return 2

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    db = client[db_name]
    try:
        db.command("ping")
    except Exception as e:
        print(f"ERROR: Could not connect to Mongo/Cosmos: {e}")
        return 3

    if args.collection not in set(_iter_collection_names(db)):
        print(f"WARNING: Collection '{args.collection}' not found. Available: {sorted(list(_iter_collection_names(db)))[:20]}")

    coll = db[args.collection]

    if args.list_blobs:
        try:
            print("Sample blobs:")
            for name in blob.list_blob_names(container=args.uploads_container, prefix=args.prefix, limit=int(args.list_blobs_limit)):
                print(f"  - {name}")
        except Exception as e:
            print(f"WARNING: Could not list blobs: {e}")
            print("TIP: If you're using a SAS token, listing requires 'l' permission. A 403 here usually means your credentials can write but cannot list/read.")

    result = CheckResult()
    missing_samples: list[str] = []

    cursor = coll.find({}, {"image_path": 1, "relpath": 1, "metadata": 1}).skip(int(args.skip)).limit(int(args.limit))
    for doc in cursor:
        result.checked += 1

        image_value = doc.get("image_path") or doc.get("relpath")
        if not image_value:
            md = doc.get("metadata") or {}
            if isinstance(md, dict):
                image_value = md.get("relpath") or md.get("image_path")

        key = _normalize_image_key(image_value)
        if not key:
            result.no_image_field += 1
            continue

        try:
            ok = blob.exists(container=args.uploads_container, blob_name=key)
        except Exception as e:
            print(f"ERROR: Blob permission error while checking existence: {e}")
            print("TIP: Ensure AZURE_STORAGE_CONNECTION_STRING is an account key connection string, or SAS has at least 'r' permission for the container.")
            return 4
        if ok:
            result.ok += 1
        else:
            result.missing += 1
            if len(missing_samples) < int(args.sample_missing):
                missing_samples.append(key)

    print({
        "checked": result.checked,
        "ok": result.ok,
        "missing": result.missing,
        "no_image_field": result.no_image_field,
    })

    if missing_samples:
        print("Missing blob keys (sample):")
        for k in missing_samples:
            print(f"  - {k}")

    if result.missing > 0:
        print("NOTE: If you uploaded blobs but did not ingest/create DB documents that reference those blob keys, they won't show in the UI.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
