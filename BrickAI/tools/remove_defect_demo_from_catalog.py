from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

                                             
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

                                                           
load_dotenv(os.path.join(_ROOT, ".env"), interpolate=False)

from lib.mongo import get_db, get_db_name              


def _norm(v: Any) -> str:
    return ("" if v is None else str(v)).strip()


def _build_query() -> dict:
                                            
                          
                           
                                               
                                           
                                                      
    return {
        "dataset": "catalog",
        "$or": [
            {"metadata.demo": True},
            {"metadata.brand": "Defect Demo"},
            {"metadata.item_code": {"$regex": r"^defect_demo_"}},
            {"image_path": {"$regex": r"^defect_demo/"}},
        ],
    }


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Remove defect-demo bricks from the catalog")
    p.add_argument("--dry-run", action="store_true", help="Report what would be deleted")
    p.add_argument(
        "--delete-files",
        action="store_true",
        help="Also delete local files under uploads/defect_demo referenced by matching docs",
    )
    p.add_argument(
        "--uploads-folder",
        default=os.getenv("UPLOAD_FOLDER") or os.path.join(_ROOT, "uploads"),
        help="Override uploads folder (default: UPLOAD_FOLDER env or ./uploads)",
    )
    args = p.parse_args(argv)

    db = get_db()
    print(f"DB: {get_db_name()}")

    q = _build_query()

                                                                           
    docs = list(db.bricks.find(q, {"brick_id": 1, "_id": 1, "image_path": 1, "metadata": 1}))
    brick_ids: list[str] = []
    image_paths: list[str] = []

    for d in docs:
        bid = _norm(d.get("brick_id") or d.get("_id"))
        if bid:
            brick_ids.append(bid)
        ip = _norm(d.get("image_path"))
        if ip:
            image_paths.append(ip)

    print({"matching_docs": len(docs), "distinct_brick_ids": len(set(brick_ids)), "dry_run": bool(args.dry_run)})

    if not docs:
        return 0

                    
    for d in docs[:10]:
        md = d.get("metadata") or {}
        print(
            {
                "brick_id": _norm(d.get("brick_id") or d.get("_id"))[:12],
                "image_path": d.get("image_path"),
                "display_name": md.get("display_name") or md.get("brick_name"),
                "item_code": md.get("item_code"),
                "brand": md.get("brand"),
                "demo": md.get("demo"),
            }
        )

    if args.dry_run:
        return 0

                                                                   
    ids = list(dict.fromkeys(brick_ids))

    res_bricks = db.bricks.delete_many({"brick_id": {"$in": ids}})
    res_features = db.brick_features.delete_many({"_id": {"$in": ids}})

                                       
    res_pricing = None
    res_co2 = None
    try:
        res_pricing = db.brick_pricing.delete_many({"_id": {"$in": ids}})
    except Exception:
        res_pricing = None
    try:
        res_co2 = db.brick_co2.delete_many({"brick_id": {"$in": ids}})
    except Exception:
        res_co2 = None

    print(
        {
            "deleted": {
                "bricks": getattr(res_bricks, "deleted_count", None),
                "brick_features": getattr(res_features, "deleted_count", None),
                "brick_pricing": getattr(res_pricing, "deleted_count", None) if res_pricing else None,
                "brick_co2": getattr(res_co2, "deleted_count", None) if res_co2 else None,
            }
        }
    )

    if args.delete_files:
        uploads_folder = Path(args.uploads_folder)
        deleted_files = 0
        missing_files = 0

        for rel in image_paths:
                                                                   
            if not rel.replace("\\", "/").startswith("defect_demo/"):
                continue
            pth = uploads_folder / Path(rel)
            try:
                if pth.exists() and pth.is_file():
                    pth.unlink()
                    deleted_files += 1
                else:
                    missing_files += 1
            except Exception:
                missing_files += 1

                                                  
        try:
            demo_dir = uploads_folder / "defect_demo"
            if demo_dir.is_dir() and not any(demo_dir.iterdir()):
                demo_dir.rmdir()
        except Exception:
            pass

        print({"files_deleted": deleted_files, "files_missing": missing_files, "uploads_folder": str(uploads_folder)})

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
