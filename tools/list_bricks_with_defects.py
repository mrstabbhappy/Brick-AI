from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

                                                         
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dotenv import load_dotenv

from brick_analyzer import BrickImageAnalyzer
from lib.blob_storage import BlobStorage
from lib.image_store import load_image
from lib.mongo import get_db_name, get_mongo_uri
from lib.yolo_defect_detector import get_defect_detector


def _display_name(doc: dict) -> str:
    md = doc.get("metadata") or {}
    return md.get("display_name") or md.get("brick_name") or md.get("item_code") or str(doc.get("brick_id") or doc.get("_id") or "")


def main() -> int:
                                                        
    load_dotenv(interpolate=False)

    parser = argparse.ArgumentParser(description="List catalog bricks with YOLO-detected defects")
    parser.add_argument("--dataset", default="catalog", help="Dataset to scan (default: catalog)")
    parser.add_argument("--limit", type=int, default=80, help="Max bricks to scan")
    parser.add_argument("--min-count", type=int, default=1, help="Minimum defect detections to include")
    parser.add_argument("--min-conf", type=float, default=0.20, help="Minimum confidence threshold for YOLO detections")
    parser.add_argument("--write", action="store_true", help="Write defect counts back to bricks collection")

    args = parser.parse_args()

    analyzer = BrickImageAnalyzer(mongo_uri=get_mongo_uri(), db_name=get_db_name())
    blob = BlobStorage()
    yolo = get_defect_detector()
    status = yolo.get_status()

    if not status.get("available"):
        print("YOLO not available:")
        print(status)
        return 2

    uploads_container = os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads")
    upload_folder = os.getenv("UPLOAD_FOLDER", "uploads")

    cursor = analyzer.bricks_collection.find(
        {"dataset": args.dataset},
        {
            "_id": 1,
            "brick_id": 1,
            "image_path": 1,
            "metadata.display_name": 1,
            "metadata.brick_name": 1,
            "metadata.item_code": 1,
            "metadata.brand": 1,
            "metadata.colour": 1,
            "metadata.brick_colour": 1,
        },
    ).limit(int(args.limit))

    found = 0
    scanned = 0
    for doc in cursor:
        scanned += 1
        image_key = doc.get("image_path")
        if not image_key:
            continue

        try:
            img = load_image(
                str(image_key),
                blob=blob,
                uploads_container=uploads_container,
                upload_folder=upload_folder,
            )
        except Exception:
            continue

        detections = yolo.detect_defects(img, conf_threshold=float(args.min_conf))
        if len(detections) < int(args.min_count):
            if args.write:
                analyzer.bricks_collection.update_one(
                    {"_id": doc.get("_id")},
                    {"$set": {"yolo_defects": {"count": 0, "classes": {}, "min_conf": float(args.min_conf)}}},
                )
            continue

        counts = Counter([str(d.get("class") or "unknown") for d in detections])
        found += 1

        md = doc.get("metadata") or {}
        name = _display_name(doc)
        brand = md.get("brand")
        colour = md.get("colour") or md.get("brick_colour")
        item_code = md.get("item_code")
        brick_id = str(doc.get("brick_id") or doc.get("_id") or "")

        parts = [name]
        if brand:
            parts.append(str(brand))
        if colour:
            parts.append(str(colour))
        if item_code:
            parts.append(str(item_code))

        print(f"- {', '.join(parts)}")
        print(f"  id: {brick_id}")
        print(f"  defects: {dict(counts)}")

        if args.write:
            analyzer.bricks_collection.update_one(
                {"_id": doc.get("_id")},
                {
                    "$set": {
                        "yolo_defects": {
                            "count": int(len(detections)),
                            "classes": dict(counts),
                            "min_conf": float(args.min_conf),
                        }
                    }
                },
            )

    print()
    print(f"Scanned: {scanned} | With defects: {found} | min_conf={args.min_conf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
