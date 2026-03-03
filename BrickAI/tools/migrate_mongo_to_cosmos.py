"""Copy data from an existing MongoDB to Cosmos DB (Mongo API).

This script intentionally mirrors the older, "known-good" behavior:
- Straight document copy using batched `insert_many(..., ordered=False)`
- Preserves `_id` values
- Optional index copy when `MIGRATE_CREATE_INDEXES=1`

Usage (PowerShell):
    $env:SOURCE_MONGODB_URI='...'
    $env:DEST_MONGODB_URI='...'
    $env:MONGODB_DB='brickdb'
    python tools/migrate_mongo_to_cosmos.py

If your connection string password contains reserved URI characters, you can
type `parts` at the prompt and the script will build a safely URL-encoded URI.
"""

from __future__ import annotations

import getpass
import os
import re
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, OperationFailure


                                                      
try:                    
    from dotenv import load_dotenv

    load_dotenv()
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
    if any(ws in u for ws in (" ", "\t", "\n", "\r")):
        return False
    return u.startswith("mongodb://") or u.startswith("mongodb+srv://")


def _validate_mongo_uri(uri: str) -> tuple[bool, str | None]:
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
    host = (host or "").strip().strip("/")
    if not host:
        raise RuntimeError("Host is required")

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


def _prompt_mongo_uri(env_name: str, label: str) -> str:
    env_v = (os.getenv(env_name) or "").strip()
    if env_v:
        env_v = _sanitize_mongo_uri(env_v)
        if _is_plausible_mongo_uri(env_v):
            os.environ[env_name] = env_v
            return env_v

    last_err = None
    for attempt in range(3):
        v = _prompt_value(label, secret=True)
        if (v or "").strip().lower() in {"parts", "build", "builder"}:
            built = _sanitize_mongo_uri(_build_mongo_uri_from_parts(default_srv=True))
            ok, err = _validate_mongo_uri(built)
            if ok:
                os.environ[env_name] = built
                return built
            last_err = f"Built URI invalid: {err}"
            continue

        v = _sanitize_mongo_uri(v)
        if not _is_plausible_mongo_uri(v):
            last_err = "Mongo URI must start with mongodb:// or mongodb+srv:// (no whitespace)."
            continue
        ok, err = _validate_mongo_uri(v)
        if ok:
            os.environ[env_name] = v
            return v

        last_err = f"Could not parse Mongo URI: {err}"
        if attempt == 2:
            raise RuntimeError(f"Invalid {env_name}. {last_err}")

    raise RuntimeError(f"Invalid {env_name}. {last_err}")


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
                "Re-run and type 'parts' at the prompt to build a URL-encoded URI, or reset the DB user password."
            ) from (last_exc or e)
        raise


def iter_collection_names(db) -> Iterable[str]:
    for name in db.list_collection_names():
        if name.startswith("system."):
            continue
        yield name


def main() -> None:
                                           
    source_uri = _prompt_mongo_uri("SOURCE_MONGODB_URI", "SOURCE_MONGODB_URI (or type 'parts')")
    dest_uri = _prompt_mongo_uri("DEST_MONGODB_URI", "DEST_MONGODB_URI (or type 'parts')")
    db_name = (os.getenv("MONGODB_DB") or "").strip() or "brickdb"

    source, source_uri = _connect_and_ping(source_uri, db_name, label="source")
    dest, dest_uri = _connect_and_ping(dest_uri, db_name, label="dest")

    create_indexes = os.getenv("MIGRATE_CREATE_INDEXES", "0") == "1"

    started = datetime.now(timezone.utc)
    for coll_name in iter_collection_names(source):
        src_coll = source[coll_name]
        dst_coll = dest[coll_name]

        cursor = src_coll.find({}, no_cursor_timeout=True)
        batch: list[dict] = []
        copied = 0
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= 500:
                try:
                    res = dst_coll.insert_many(batch, ordered=False)
                    copied += len(res.inserted_ids)
                except BulkWriteError as bwe:
                                                                                         
                    details = bwe.details or {}
                    copied += int(details.get("nInserted") or 0)
                batch = []

        if batch:
            try:
                res = dst_coll.insert_many(batch, ordered=False)
                copied += len(res.inserted_ids)
            except BulkWriteError as bwe:
                details = bwe.details or {}
                copied += int(details.get("nInserted") or 0)

        if create_indexes:
            try:
                for idx in src_coll.list_indexes():
                    if idx.get("name") == "_id_":
                        continue
                    keys = idx.get("key")
                    if not keys:
                        continue
                    dst_coll.create_index(list(keys.items()), name=idx.get("name"))
            except Exception:
                pass

        print(f"Copied {copied} docs: {coll_name}")

    ended = datetime.now(timezone.utc)
    dur = (ended - started).total_seconds()
    print(f"Done in {dur:.1f}s")


if __name__ == "__main__":
    main()
