from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from bson.decimal128 import Decimal128

from lib.mongo import get_db


def _to_decimal128(v: Any) -> Optional[Decimal128]:
    if v is None:
        return None
    if isinstance(v, Decimal128):
        return v
    s = str(v).strip()
    if not s:
        return None
                                                                     
    return Decimal128(s)


def upsert_brick_pricing(
    *,
    brick_id: str,
    band_asps: Dict[str, Any],
    factory_code: Optional[str] = None,
    factory_region: Optional[str] = None,
) -> None:
    """Upsert pricing for a brick.

    Stores Decimal128 (Cosmos shows as NumberDecimal).

    band_asps keys accepted: asp_T1..asp_T4 (strings or numbers).
    """
    db = get_db()

    asps: Dict[str, Decimal128] = {}
    for k, v in (band_asps or {}).items():
                              
        if k.startswith("asp_"):
            band = k.replace("asp_", "")
        else:
            band = k
        d = _to_decimal128(v)
        if d is not None:
            asps[band] = d

    doc = {
        "_id": brick_id,
        "brick_id": brick_id,
        "band_asps": asps,
        "factory_code": factory_code,
        "factory_region": factory_region,
        "updated_at": datetime.utcnow().isoformat(),
    }

    db.brick_pricing.replace_one({"_id": brick_id}, doc, upsert=True)


def get_brick_pricing(brick_id: str) -> Optional[dict]:
    db = get_db()
    return db.brick_pricing.find_one({"_id": brick_id})


def get_asp_for_band(brick_id: str, band: str) -> Optional[float]:
    """Return ASP as float for UI display."""
    doc = get_brick_pricing(brick_id)
    if not doc:
        return None
    asps = doc.get("band_asps") or {}
    v = asps.get(band)
    if isinstance(v, Decimal128):
        try:
            return float(v.to_decimal())
        except Exception:
            return None
    try:
        return float(v)
    except Exception:
        return None
