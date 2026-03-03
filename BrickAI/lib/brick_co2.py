from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

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


def upsert_brick_co2(
    *,
    brick_id: str,
    co2_kg: Any,
    region_id: Optional[str] = None,
    source: Optional[str] = None,
) -> None:
    """Upsert CO2 figure for a brick.

    Stores Decimal128 so Cosmos displays as NumberDecimal.
    """
    if not brick_id:
        raise ValueError("brick_id is required")

    d = _to_decimal128(co2_kg)
    db = get_db()
    db.brick_co2.replace_one(
        {"_id": brick_id},
        {
            "_id": brick_id,
            "brick_id": brick_id,
            "co2_kg": d,
            "region_id": region_id,
            "source": source,
            "updated_at": datetime.utcnow().isoformat(),
        },
        upsert=True,
    )


def get_brick_co2(brick_id: str) -> Optional[dict]:
    if not brick_id:
        return None
    db = get_db()
    return db.brick_co2.find_one({"_id": brick_id})
