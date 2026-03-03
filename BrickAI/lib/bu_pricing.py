import logging

from lib.mongo import get_db
from lib.brick_pricing import get_asp_for_band

logger = logging.getLogger(__name__)


def _candidate_bu_codes(value: str) -> list[str]:
    v = (value or "").strip()
    if not v:
        return []
    out: list[str] = []
    out.append(v)

                                                                             
    upper = v.upper().replace("-", "_")
    if upper not in out:
        out.append(upper)

                                 
    upper2 = upper.replace(" ", "_")
    if upper2 not in out:
        out.append(upper2)

                                      
    return out


def get_bu_pricing(bu_code: str, brick: dict):
    brick_id = brick.get("brick_id") or brick.get("_id")
    factory = brick.get("factory_code") or brick.get("metadata", {}).get("factory_code")
    if not factory:
        return {"bu_band": None, "bu_asp": None, "in_region": False}

    db = get_db()
    rule = None
    for code in _candidate_bu_codes(bu_code):
        rule = db.bu_factory_pricing.find_one(
            {
                "bu_code": code,
                "factory_code": factory,
                "$or": [{"effective_to": None}, {"effective_to": {"$exists": False}}],
            }
        )
        if rule:
            break

    if not rule:
        default_band = "T3"
        return {
            "bu_band": default_band,
            "bu_asp": get_asp_for_band(str(brick_id), default_band) if brick_id else None,
            "in_region": False,
            "tw_region": brick.get("factory_region") or brick.get("metadata", {}).get("factory_region"),
        }

    band = rule.get("price_band")
    asp = get_asp_for_band(str(brick_id), band) if (brick_id and band) else None

    return {
        "bu_band": band,
        "bu_asp": asp,
        "in_region": True,
        "tw_region": rule.get("tw_region"),
    }
