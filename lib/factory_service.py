from __future__ import annotations

from typing import Any, Dict, Optional

from lib.mongo import get_db


def ensure_factory_indexes() -> None:
    db = get_db()
    try:
        db.factories.create_index("factory_code", unique=True)
    except Exception:
        pass
    try:
        db.factories.create_index("status")
    except Exception:
        pass


def upsert_factory(doc: Dict[str, Any]) -> None:
    if not doc:
        return
    _id = str(doc.get("_id") or "").strip()
    factory_code = str(doc.get("factory_code") or "").strip()
    if not _id and not factory_code:
        raise ValueError("factory must have _id or factory_code")

    if not _id:
        _id = factory_code
    doc = {**doc, "_id": _id}

    db = get_db()
    db.factories.replace_one({"_id": _id}, doc, upsert=True)


def get_factory_by_code(factory_code: str) -> Optional[Dict[str, Any]]:
    code = (factory_code or "").strip()
    if not code:
        return None

    db = get_db()
    return db.factories.find_one({"$or": [{"factory_code": code}, {"_id": code}]})


def factory_address_string(factory_doc: Optional[Dict[str, Any]]) -> Optional[str]:
    if not factory_doc:
        return None

    addr = factory_doc.get("address")
    if isinstance(addr, dict):
        parts = [
            addr.get("line1"),
            addr.get("line2"),
            addr.get("town"),
            addr.get("city"),
            addr.get("county"),
            addr.get("postcode"),
            addr.get("country"),
        ]
        parts = [str(p).strip() for p in parts if p and str(p).strip()]
        return ", ".join(parts) if parts else None

    if isinstance(addr, str):
        s = addr.strip()
        return s or None

    return None
