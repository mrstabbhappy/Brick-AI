r"""Full migration: MongoDB -> Cosmos (Mongo API) + disk images -> Azure Blob.

This is a *straight copy* helper that:
  1) Upserts all collections from SOURCE to DEST (idempotent).
  2) Creates core indexes on DEST.
  3) Uploads images from disk to Azure Blob and updates DEST `brick_features.image_path` to the blob key.

Usage (PowerShell) from repo root (C:\\TW Apps\\Brick AI):

  # DBs
  $env:SOURCE_MONGODB_URI='...'
  $env:DEST_MONGODB_URI='...'
  $env:MONGODB_DB='brickdb'

  # Where images live on disk (typically BRICK_AI_DEV uploads)
  $env:UPLOAD_FOLDER='C:\\Brick AI\\BRICK_AI_DEV\\uploads'

  # Blob
  $env:AZURE_STORAGE_CONNECTION_STRING='...'
  $env:AZURE_BLOB_UPLOADS_CONTAINER='uploads'

  python tools/full_migrate_to_cosmos_and_blob.py --dry-run
  python tools/full_migrate_to_cosmos_and_blob.py

If your DB stores image paths like /app/uploads/<file>, add:
  python tools/full_migrate_to_cosmos_and_blob.py --strip-prefix /app/uploads/

Notes:
- Does not print secrets.
- Safe to re-run; it uses upserts and overwrites blobs by key.
"""

from __future__ import annotations

import argparse
import os
from collections import Counter
from datetime import datetime, timezone
import getpass
import re
from urllib.parse import quote
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pymongo import MongoClient
from pymongo import ReplaceOne
from pymongo.errors import OperationFailure

from lib.blob_storage import BlobStorage

                                                      
try:                    
    from dotenv import load_dotenv

    load_dotenv(interpolate=False)
except Exception:
    pass


def _env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _prompt_value(label: str, *, secret: bool, default: str | None = None) -> str:
    prompt = f"{label}"
    if default is not None and default != "":
        prompt += f" [{default}]"
    prompt += ": "

    if secret:
        v = getpass.getpass(prompt)
    else:
        v = input(prompt)

    v = (v or "").strip()
    if not v and default is not None:
        v = default
    return (v or "").strip()


def _sanitize_mongo_uri(uri: str) -> str:
    u = (uri or "").strip()
    if (u.startswith("\"") and u.endswith("\"")) or (u.startswith("'") and u.endswith("'")):
        u = u[1:-1].strip()

                         
                                               
                                        
                                    
                                                                                 
                                                                       
    u = re.sub(r"^mongodb\s+srv://", "mongodb+srv://", u, flags=re.IGNORECASE)
    if u.lower().startswith("mongodb+srv:/") and not u.lower().startswith("mongodb+srv://"):
        u = "mongodb+srv://" + u[len("mongodb+srv:/") :]
    if u.lower().startswith("mongodb:/") and not u.lower().startswith("mongodb://"):
        u = "mongodb://" + u[len("mongodb:/") :]

                                                                           
    if "://" not in u and "@" in u and not u.startswith("mongodb"):
        u = "mongodb+srv://" + u

                                                                      
    if u.startswith("mongodb+srv://"):
        rest = u[len("mongodb+srv://") :]
        if "/" not in rest:
            u = u + "/"

    return u


def _is_plausible_mongo_uri(uri: str) -> bool:
    u = (uri or "").strip()
    if not u:
        return False
    if " " in u or "\t" in u or "\n" in u or "\r" in u:
        return False
    return u.startswith("mongodb://") or u.startswith("mongodb+srv://")


def _validate_mongo_uri(uri: str) -> tuple[bool, str | None]:
    """Return (ok, error_message)."""
    try:
        from pymongo.uri_parser import parse_uri

        parse_uri(uri)
        return True, None
    except Exception as e:
        return False, str(e)


def _build_mongo_uri_from_parts(*, default_srv: bool = True) -> str:
    srv_in = _prompt_value("Use SRV (mongodb+srv://)? y/n", secret=False, default="y" if default_srv else "n")
    use_srv = (srv_in or "").strip().lower().startswith("y")

    host = _prompt_value("Host (e.g. cluster.example.mongodb.net)", secret=False)
    if not host:
        raise RuntimeError("Host is required")
    host = host.strip().strip("/")

    username = _prompt_value("Username", secret=False)
    password = _prompt_value("Password", secret=True)

                                                                                               
                                                                                                                   
    already_encoded = _prompt_value("Is password already URL-encoded? y/n", secret=False, default="n")
    u = quote(username, safe="")
    if (already_encoded or "").strip().lower().startswith("y"):
        p = password
    else:
        p = quote(password, safe="")

    db_in_uri = _prompt_value("Database name in URI path (optional)", secret=False, default="")
    db_in_uri = (db_in_uri or "").strip().lstrip("/")

    opts = _prompt_value("Options query (optional, without leading '?')", secret=False, default="")
    opts = (opts or "").strip().lstrip("?")

    scheme = "mongodb+srv" if use_srv else "mongodb"
    uri = f"{scheme}://{u}:{p}@{host}/"
    if db_in_uri:
        uri += db_in_uri
    if opts:
        uri += ("?" + opts)
    return uri


def _prompt_mongo_uri(name: str, label: str) -> str:
                   
    env_v = os.getenv(name)
    if env_v:
        env_v = _sanitize_mongo_uri(env_v)
        if _is_plausible_mongo_uri(env_v):
            os.environ[name] = env_v
            return env_v

                               
    last_err = None
    for attempt in range(3):
        v = _prompt_value(label, secret=True)
        if (v or "").strip().lower() in {"parts", "build", "builder"}:
            built = _build_mongo_uri_from_parts(default_srv=True)
            built = _sanitize_mongo_uri(built)
            ok2, err2 = _validate_mongo_uri(built)
            if ok2:
                os.environ[name] = built
                return built
            last_err = f"Built URI invalid: {err2}"
            continue
        v = _sanitize_mongo_uri(v)

        if not _is_plausible_mongo_uri(v):
            last_err = "Mongo URI must start with mongodb:// or mongodb+srv:// (no whitespace)."
            continue

        ok, err = _validate_mongo_uri(v)
        if ok:
            os.environ[name] = v
            return v

        last_err = (
            f"Could not parse Mongo URI: {err}. "
            "Double-check you pasted the full connection string. "
            "If the password contains special characters, URL-encode them (e.g. '@'->%40, '$'->%24)."
        )

                                                      
        if attempt == 2:
            yn = _prompt_value("Build connection string from parts instead? y/n", secret=False, default="y")
            if (yn or "").strip().lower().startswith("y"):
                built = _build_mongo_uri_from_parts(default_srv=True)
                built = _sanitize_mongo_uri(built)
                ok2, err2 = _validate_mongo_uri(built)
                if ok2:
                    os.environ[name] = built
                    return built
                last_err = f"Built URI still invalid: {err2}"

    raise RuntimeError(f"Invalid {name}. {last_err}")


def _upsert_query_param(uri: str, key: str, value: str) -> str:
    parts = urlsplit(uri)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    new_query = urlencode(query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _redact_uri(uri: str) -> dict:
    parts = urlsplit(uri)
    netloc = parts.netloc
    username = None
    host = netloc
    if "@" in netloc:
        userinfo, host = netloc.rsplit("@", 1)
        if ":" in userinfo:
            username = userinfo.split(":", 1)[0]
        else:
            username = userinfo

    query_keys = sorted({k for k, _ in parse_qsl(parts.query, keep_blank_values=True)})
    db_path = (parts.path or "/").lstrip("/")
    return {
        "scheme": parts.scheme,
        "host": host,
        "username": username,
        "db_path": db_path,
        "query_keys": query_keys,
    }


def _connect_and_ping(uri: str, db_name: str, *, label: str):
    """Connect to db and run ping. Retries common authSource mismatch."""
    print({"step": "ping", "target": label, "db": db_name, "uri": _redact_uri(uri)})
    db = MongoClient(uri, serverSelectionTimeoutMS=10000)[db_name]
    try:
        db.command("ping")
        return db, uri
    except OperationFailure as e:
                                    
        if getattr(e, "code", None) == 18:
                                                                                            
            parts = urlsplit(uri)
            query = dict(parse_qsl(parts.query, keep_blank_values=True))
            retry_uris: list[tuple[str, str]] = []

            if query.get("authSource") != "admin":
                retry_uris.append(("authSource=admin", _upsert_query_param(uri, "authSource", "admin")))

                                                                 
            if query.get("authMechanism") not in {"SCRAM-SHA-256", "SCRAM-SHA-1"}:
                retry_uris.append(("authMechanism=SCRAM-SHA-256", _upsert_query_param(uri, "authMechanism", "SCRAM-SHA-256")))
                retry_uris.append(("authMechanism=SCRAM-SHA-1", _upsert_query_param(uri, "authMechanism", "SCRAM-SHA-1")))

                                                           
            if query.get("authSource") != "admin" and query.get("authMechanism") not in {"SCRAM-SHA-256", "SCRAM-SHA-1"}:
                base = _upsert_query_param(uri, "authSource", "admin")
                retry_uris.append(("authSource=admin&authMechanism=SCRAM-SHA-256", _upsert_query_param(base, "authMechanism", "SCRAM-SHA-256")))
                retry_uris.append(("authSource=admin&authMechanism=SCRAM-SHA-1", _upsert_query_param(base, "authMechanism", "SCRAM-SHA-1")))

            last_exc: Exception | None = None
            for note, retry_uri in retry_uris:
                try:
                    retry_db = MongoClient(retry_uri, serverSelectionTimeoutMS=10000)[db_name]
                    retry_db.command("ping")
                    print({"step": "ping_retry", "target": label, "note": f"Retried with {note}"})
                    return retry_db, retry_uri
                except OperationFailure as e2:
                    last_exc = e2
                    if getattr(e2, "code", None) != 18:
                        raise

                                                      
            raise RuntimeError(
                f"Authentication failed for {label}. "
                "If you used a URL-encoded password, do NOT encode it again (double-encoding breaks auth). "
                "Re-run and type 'parts' at the prompt; when asked 'already URL-encoded', answer correctly."
            ) from (last_exc or e)
        raise

def _get_or_ask(name: str, current: str | None, *, label: str, secret: bool, default: str | None = None) -> str:
    v = (current or "").strip()
    if v:
        return v

    env_v = (os.getenv(name) or "").strip()
    if env_v:
        return env_v

    v = _prompt_value(label, secret=secret, default=default)
    v = (v or "").strip()
    if not v:
        raise RuntimeError(f"Missing required value: {name}")

                                                                       
    os.environ[name] = v
    return v


def _iter_collection_names(db):
    for name in db.list_collection_names():
        if name.startswith("system."):
            continue
        yield name


def _init_indexes(dest_db) -> None:
    features = dest_db[os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")]
    users = dest_db["users"]
    bu_locations = dest_db["bu_locations"]

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


def _guess_content_type(path: str) -> str:
    p = (path or "").lower()
    if p.endswith(".png"):
        return "image/png"
    if p.endswith(".jpg") or p.endswith(".jpeg"):
        return "image/jpeg"
    if p.endswith(".webp"):
        return "image/webp"
    if p.endswith(".bmp"):
        return "image/bmp"
    return "application/octet-stream"


def _resolve_local_path(image_path: str, *, upload_folder: str, strip_prefixes: list[str]) -> str | None:
    if not image_path:
        return None

    normalized = str(image_path)
    for prefix in strip_prefixes:
        if prefix and normalized.startswith(prefix):
            normalized = normalized[len(prefix) :].lstrip("/\\")
            break

                   
    if os.path.isabs(normalized):
        return normalized

                   
    p1 = os.path.join(upload_folder, normalized)
    if os.path.exists(p1):
        return p1

                                                                  
    base = os.path.basename(normalized.replace("\\", "/"))
    p2 = os.path.join(upload_folder, base)
    if os.path.exists(p2):
        return p2

    return None


def migrate_db(source_db, dest_db, *, dry_run: bool) -> None:
    for coll_name in _iter_collection_names(source_db):
        src = source_db[coll_name]
        dst = dest_db[coll_name]

        cursor = src.find({}, no_cursor_timeout=True)
        ops: list[ReplaceOne] = []
        processed = 0

        for doc in cursor:
            _id = doc.get("_id")
            if _id is None:
                continue
            processed += 1
            if not dry_run:
                ops.append(ReplaceOne({"_id": _id}, doc, upsert=True))

            if len(ops) >= 500:
                dst.bulk_write(ops, ordered=False)
                ops = []

        if ops:
            dst.bulk_write(ops, ordered=False)

        print({"collection": coll_name, "docs_seen": processed, "dry_run": dry_run})


def migrate_images(dest_db, *, upload_folder: str, blob: BlobStorage, container: str, dry_run: bool, strip_prefixes: list[str], limit: int) -> None:
    features_name = os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")
    coll = dest_db[features_name]

    q = {"image_path": {"$exists": True, "$ne": None}}
    cur = coll.find(q, {"image_path": 1}).batch_size(200)

    stats = Counter()
    processed = 0

    for doc in cur:
        if limit and processed >= limit:
            break
        processed += 1

        image_path = doc.get("image_path")
        if not image_path:
            stats["skipped_empty"] += 1
            continue

                                                                              
        s = str(image_path)
        if not (os.path.isabs(s) or "/" in s or "\\" in s):
            stats["skipped_already_blob_key"] += 1
            continue

        local_path = _resolve_local_path(s, upload_folder=upload_folder, strip_prefixes=strip_prefixes)
        if not local_path:
            stats["missing_on_disk"] += 1
            continue

        blob_name = os.path.basename(local_path).replace(" ", "_")
        blob_name = f"{doc.get('_id')}_{blob_name}"

        if dry_run:
            stats["would_migrate"] += 1
            continue

        with open(local_path, "rb") as f:
            data = f.read()

        blob.upload_bytes(container=container, blob_name=blob_name, data=data, content_type=_guess_content_type(local_path))
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
        stats["migrated"] += 1

    print({"features_collection": features_name, "processed": processed, **dict(stats), "dry_run": dry_run})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit-images", type=int, default=0, help="Optional max images to migrate")
    ap.add_argument("--source-uri", default=None, help="Override SOURCE_MONGODB_URI")
    ap.add_argument("--dest-uri", default=None, help="Override DEST_MONGODB_URI")
    ap.add_argument("--db", default=None, help="Override MONGODB_DB (default brickdb)")
    ap.add_argument("--upload-folder", default=None, help="Override UPLOAD_FOLDER")
    ap.add_argument(
        "--strip-prefix",
        action="append",
        default=[],
        help="Strip this prefix from image_path before resolving locally. Can be repeated.",
    )
    args = ap.parse_args()

                                                
                                                                                      
    if args.source_uri:
        os.environ["SOURCE_MONGODB_URI"] = _sanitize_mongo_uri(args.source_uri)
    if args.dest_uri:
        os.environ["DEST_MONGODB_URI"] = _sanitize_mongo_uri(args.dest_uri)

    source_uri = _prompt_mongo_uri(
        "SOURCE_MONGODB_URI",
        "SOURCE_MONGODB_URI (source Mongo connection string; or type 'parts')",
    )
    dest_uri = _prompt_mongo_uri(
        "DEST_MONGODB_URI",
        "DEST_MONGODB_URI (Cosmos Mongo API connection string; or type 'parts')",
    )
    default_db_name = (os.getenv("MONGODB_DB") or "").strip() or "brickdb"
    db_name = _get_or_ask(
        "MONGODB_DB",
        args.db,
        label="MONGODB_DB (database name)",
        secret=False,
        default=default_db_name,
    )
    upload_folder = _get_or_ask(
        "UPLOAD_FOLDER",
        args.upload_folder,
        label="UPLOAD_FOLDER (path to existing disk images)",
        secret=False,
        default=(os.getenv("UPLOAD_FOLDER") or "").strip() or "",
    )

    if not os.path.isabs(upload_folder):
        upload_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), upload_folder)

                                                  
    if not (os.getenv("AZURE_STORAGE_CONNECTION_STRING") or os.getenv("AZURE_STORAGE_ACCOUNT_URL") or os.getenv("AZURE_STORAGE_ACCOUNT")):
        _get_or_ask(
            "AZURE_STORAGE_CONNECTION_STRING",
            None,
            label="AZURE_STORAGE_CONNECTION_STRING (for image upload)",
            secret=True,
        )

    blob = BlobStorage()
    if not blob.is_configured():
        raise RuntimeError("Azure Blob is not configured (set AZURE_STORAGE_CONNECTION_STRING or Managed Identity vars)")

    container = os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads")

    strip_prefixes = list(args.strip_prefix)
    env_strip = os.getenv("IMAGE_PATH_STRIP_PREFIX")
    if env_strip:
        strip_prefixes.append(env_strip)

    started = datetime.now(timezone.utc)

                                                                                 
    for _ in range(2):
        try:
            source_db, source_uri = _connect_and_ping(source_uri, db_name, label="source")
            break
        except RuntimeError as e:
            if "Authentication failed for source" in str(e):
                print({"step": "auth_retry", "target": "source", "note": "Re-enter SOURCE_MONGODB_URI (or type parts)"})
                source_uri = _prompt_mongo_uri(
                    "SOURCE_MONGODB_URI",
                    "SOURCE_MONGODB_URI (source Mongo connection string; or type 'parts')",
                )
                continue
            raise

    for _ in range(2):
        try:
            dest_db, dest_uri = _connect_and_ping(dest_uri, db_name, label="dest")
            break
        except RuntimeError as e:
            if "Authentication failed for dest" in str(e):
                print({"step": "auth_retry", "target": "dest", "note": "Re-enter DEST_MONGODB_URI (or type parts)"})
                dest_uri = _prompt_mongo_uri(
                    "DEST_MONGODB_URI",
                    "DEST_MONGODB_URI (Cosmos Mongo API connection string; or type 'parts')",
                )
                continue
            raise

    print({"step": "copy_db", "db": db_name, "dry_run": bool(args.dry_run)})
    migrate_db(source_db, dest_db, dry_run=bool(args.dry_run))

    if not args.dry_run:
        print({"step": "init_indexes", "db": db_name})
        _init_indexes(dest_db)

    print({"step": "migrate_images", "upload_folder": upload_folder, "container": container, "dry_run": bool(args.dry_run)})
    migrate_images(
        dest_db,
        upload_folder=upload_folder,
        blob=blob,
        container=container,
        dry_run=bool(args.dry_run),
        strip_prefixes=strip_prefixes,
        limit=int(args.limit_images or 0),
    )

    ended = datetime.now(timezone.utc)
    dur = (ended - started).total_seconds()
    print({"ok": True, "seconds": dur})


if __name__ == "__main__":
    main()
