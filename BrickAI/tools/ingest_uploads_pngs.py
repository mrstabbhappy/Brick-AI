from __future__ import annotations

import argparse
import hashlib
import os
import sys
from typing import Optional

from PIL import Image
from dotenv import load_dotenv

                                             
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

                                                           
load_dotenv(os.path.join(_ROOT, ".env"), interpolate=False)

from brick_analyzer import BrickImageAnalyzer              
from lib.mongo import get_db_name, get_mongo_uri              


def _brick_id_from_relpath(relpath: str) -> str:
    basis = relpath or ""
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _iter_pngs(upload_folder: str) -> list[str]:
    items: list[str] = []
    for name in os.listdir(upload_folder):
        if name.lower().endswith(".png"):
            items.append(name)
    items.sort()
    return items


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest local uploads/*.png into bricks + brick_features")
    p.add_argument(
        "--uploads-dir",
        default=os.getenv("UPLOAD_FOLDER") or os.path.join(_ROOT, "uploads"),
        help="Path to local uploads folder",
    )
    p.add_argument("--dataset", default="catalog", help="Dataset tag to store (default: catalog)")
    p.add_argument("--skip", type=int, default=0)
    p.add_argument("--limit", type=int, default=0, help="0 = no limit")
    p.add_argument("--skip-existing", action="store_true", help="Skip if brick_features already exists")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    uploads_dir = args.uploads_dir
    if not os.path.isabs(uploads_dir):
        uploads_dir = os.path.abspath(uploads_dir)

    if not os.path.isdir(uploads_dir):
        print(f"ERROR: uploads dir not found: {uploads_dir}")
        return 2

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()
    analyzer = BrickImageAnalyzer(mongo_uri=mongo_uri, db_name=db_name)

    names = _iter_pngs(uploads_dir)
    names = names[int(args.skip) :]
    if int(args.limit or 0) > 0:
        names = names[: int(args.limit)]

    if not names:
        print("No .png files found to ingest.")
        return 0

    print({"uploads_dir": uploads_dir, "count": len(names), "dataset": args.dataset, "dry_run": bool(args.dry_run)})

    ingested = 0
    skipped = 0
    failed = 0

    for idx, filename in enumerate(names, start=1):
        relpath = filename.replace("\\", "/")
        brick_id = _brick_id_from_relpath(relpath)

        if args.skip_existing:
            try:
                exists = analyzer.brick_features_collection.find_one({"_id": brick_id}, {"_id": 1})
                if exists:
                    skipped += 1
                    if idx % 25 == 0:
                        print(f"[{idx}/{len(names)}] skipped_existing={skipped} ingested={ingested} failed={failed}")
                    continue
            except Exception:
                pass

        image_path = os.path.join(uploads_dir, filename)
        try:
            img = Image.open(image_path).convert("RGB")
        except Exception as e:
            failed += 1
            print(f"ERROR: failed to open {filename}: {e}")
            continue

                                                                                                       
        item_code = os.path.splitext(filename)[0]
        metadata = {
            "item_code": item_code,
            "display_name": item_code,
            "brick_name": item_code,
        }

        if args.dry_run:
            ingested += 1
        else:
            try:
                analyzer.process_and_store_brick(img, relpath=relpath, metadata=metadata, dataset=args.dataset)
                ingested += 1
            except Exception as e:
                failed += 1
                print(f"ERROR: ingest failed for {filename}: {e}")
                continue

        if idx % 10 == 0 or idx == len(names):
            print(f"[{idx}/{len(names)}] ingested={ingested} skipped={skipped} failed={failed}")

    print({"ingested": ingested, "skipped": skipped, "failed": failed})
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
