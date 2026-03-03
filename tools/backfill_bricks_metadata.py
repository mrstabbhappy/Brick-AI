from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv
from bson import json_util

                                             
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

                                                           
load_dotenv(os.path.join(_ROOT, ".env"), interpolate=False)

from lib.mongo import get_db_name, get_mongo_client, get_mongo_uri              
from lib.brick_pricing import upsert_brick_pricing              


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apply bulkWrite-style metadata backfill updates to bricks")
    p.add_argument("json_path", help="Path to JSON or Extended JSON file containing operations[]")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--also-write-brick-pricing", action="store_true", help="Also write tier prices into brick_pricing")
    return p.parse_args()


def _load_ops(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    doc = json_util.loads(raw)

    if isinstance(doc, dict) and "operations" in doc:
        ops = doc.get("operations")
    else:
        ops = doc

    if not isinstance(ops, list):
        raise ValueError("JSON must be a list of operations or an object with 'operations': []")

    return ops


def _extract_tier_prices(update_doc: dict) -> dict | None:
                                                                                                         
    try:
        md = (update_doc.get("$set") or {}).get("metadata") or {}
        pricing = md.get("pricing") or {}
        tier = pricing.get("tier_prices_gbp_per_th") or {}
        if not isinstance(tier, dict):
            return None
        band_asps = {}
        for k in ("T1", "T2", "T3", "T4"):
            if k in tier:
                band_asps[f"asp_{k}"] = tier.get(k)
        return band_asps or None
    except Exception:
        return None


def main() -> int:
    args = _parse_args()

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()

    client = get_mongo_client(mongo_uri)
    db = client[db_name]
    bricks = db.bricks

    ops = _load_ops(args.json_path)

    applied = 0
    skipped = 0
    failed = 0

    for i, wrapper in enumerate(ops, start=1):
        if not isinstance(wrapper, dict) or "updateOne" not in wrapper:
            skipped += 1
            continue

        op = wrapper.get("updateOne") or {}
        flt = op.get("filter") or {}
        upd = op.get("update") or {}
        upsert = bool(op.get("upsert"))

        if args.dry_run:
            applied += 1
        else:
            try:
                res = bricks.update_one(flt, upd, upsert=upsert)
                applied += 1

                if args.also_write_brick_pricing:
                                                                                                 
                    band_asps = _extract_tier_prices(upd)
                    if band_asps:
                        img_path = flt.get("image_path")
                        if img_path:
                            b = bricks.find_one({"image_path": img_path}, {"brick_id": 1, "_id": 1, "metadata": 1})
                            brick_id = (b or {}).get("brick_id") or (b or {}).get("_id")
                            if brick_id:
                                md = (b or {}).get("metadata") or {}
                                                                                 
                                factory_code = None
                                try:
                                    proc = md.get("procurement") or {}
                                    factory_code = proc.get("home_factory_code")
                                except Exception:
                                    factory_code = None

                                upsert_brick_pricing(
                                    brick_id=str(brick_id),
                                    band_asps=band_asps,
                                    factory_code=factory_code,
                                    factory_region=None,
                                )

            except Exception as e:
                failed += 1
                print(f"ERROR: op {i} failed: {e}")

        if i % 25 == 0 or i == len(ops):
            print(f"[{i}/{len(ops)}] applied={applied} skipped={skipped} failed={failed}")

    print({"applied": applied, "skipped": skipped, "failed": failed, "dry_run": bool(args.dry_run)})
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
