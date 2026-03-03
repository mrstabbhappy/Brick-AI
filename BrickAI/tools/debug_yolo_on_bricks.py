from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def _load_dotenv_best_effort() -> None:
    try:
        from dotenv import load_dotenv                

        try:
            load_dotenv(os.path.join(ROOT_DIR, ".env"), override=False, interpolate=False)
        except TypeError:
            load_dotenv(os.path.join(ROOT_DIR, ".env"), override=False)
    except Exception:
        pass


@dataclass
class BrickRef:
    brick_id: str
    image_path: str
    name: str


def _find_brick(analyzer: Any, query_text: str) -> BrickRef:
    needle = (query_text or "").strip()
    if not needle:
        raise ValueError("query_text is empty")

    safe = re.sub(r"\s+", ".*", re.escape(needle))

    query = {
        "$or": [
            {"metadata.display_name": {"$regex": safe, "$options": "i"}},
            {"metadata.brick_name": {"$regex": safe, "$options": "i"}},
            {"metadata.item_code": {"$regex": safe, "$options": "i"}},
            {"metadata.item_number": {"$regex": safe, "$options": "i"}},
        ]
    }

    doc = analyzer.bricks_collection.find_one(query)
    if not doc:
                                       
        rx = re.compile(needle, re.I)
        for d in analyzer.bricks_collection.find({}, {"brick_id": 1, "image_path": 1, "metadata": 1}).limit(500):
            md = d.get("metadata") or {}
            hay = " ".join(
                [
                    str(md.get("display_name") or ""),
                    str(md.get("brick_name") or ""),
                    str(md.get("item_code") or ""),
                    str(md.get("item_number") or ""),
                ]
            )
            if rx.search(hay):
                doc = d
                break

    if not doc:
        raise RuntimeError(f"Could not find a brick matching: {query_text!r}")

    brick_id = str(doc.get("brick_id") or doc.get("_id") or "").strip()
    image_path = str(doc.get("image_path") or "").strip()
    if not brick_id:
        raise RuntimeError(f"Found a doc for {query_text!r} but it has no brick_id/_id")
    if not image_path:
        raise RuntimeError(f"Found brick {brick_id} for {query_text!r} but it has no image_path")

    md = doc.get("metadata") or {}
    name = (
        md.get("display_name")
        or md.get("brick_name")
        or md.get("item_code")
        or md.get("item_number")
        or brick_id
    )

    return BrickRef(brick_id=brick_id, image_path=image_path, name=str(name))


def _open_local_image(image_path: str) -> Image.Image:
    """Load an image from DB image_path using the same local/blob logic as the app."""

    from lib.blob_storage import BlobStorage
    from lib.image_store import load_image

    uploads_container = os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads")
    upload_folder = os.getenv("UPLOAD_FOLDER") or os.path.join(ROOT_DIR, "uploads")
    if not os.path.isabs(upload_folder):
        upload_folder = os.path.join(ROOT_DIR, upload_folder)

    blob = BlobStorage()
    return load_image(
        image_path,
        blob=blob,
        uploads_container=uploads_container,
        upload_folder=upload_folder,
    )


def _composite_overlay(base: Image.Image, overlay_png_bytes: bytes) -> Image.Image:
    from io import BytesIO

    overlay = Image.open(BytesIO(overlay_png_bytes)).convert("RGBA")
    base_rgba = base.convert("RGBA")
    return Image.alpha_composite(base_rgba, overlay)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Debug YOLO detections for two bricks and save annotated images")
    p.add_argument("--left", default="Aston Red", help="Text to find the left brick (regex-like)")
    p.add_argument("--right", default="Aston Weathered Red", help="Text to find the right brick (regex-like)")
    p.add_argument(
        "--thresholds",
        default="0.25,0.10,0.05,0.02,0.01",
        help="Comma-separated confidence thresholds to try",
    )
    p.add_argument(
        "--out",
        default=str(Path("tools") / "_out_yolo_debug"),
        help="Output directory for annotated images",
    )
    return p.parse_args()


def main() -> int:
    _load_dotenv_best_effort()

    from brick_analyzer import BrickImageAnalyzer
    from lib.mongo import get_db_name, get_mongo_uri
    from lib.yolo_defect_detector import get_defect_detector
    from lib.compare_analysis import render_defects_overlay_png

    args = _parse_args()

    analyzer = BrickImageAnalyzer(mongo_uri=get_mongo_uri(), db_name=get_db_name())

    left = _find_brick(analyzer, args.left)
    right = _find_brick(analyzer, args.right)
    thresholds = []
    for t in str(args.thresholds).split(","):
        t = t.strip()
        if not t:
            continue
        thresholds.append(float(t))

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    yolo = get_defect_detector()
    status = yolo.get_status() if yolo else None
    print("YOLO status:", status)
    if not yolo or not yolo.is_available():
        print("YOLO detector is not available; cannot run debug.")
        return 2

    left_img = _open_local_image(left.image_path)
    right_img = _open_local_image(right.image_path)

    print("LEFT:", {"id": left.brick_id, "name": left.name, "image_path": left.image_path})
    print("RIGHT:", {"id": right.brick_id, "name": right.name, "image_path": right.image_path})

    for conf in thresholds:
        l_det = yolo.detect_defects(left_img, conf_threshold=conf)
        r_det = yolo.detect_defects(right_img, conf_threshold=conf)

        l_counts = Counter([d.get("class") for d in l_det if d.get("class")])
        r_counts = Counter([d.get("class") for d in r_det if d.get("class")])

        print(f"\n=== conf >= {conf:.3f} ===")
        print("left_count:", len(l_det), "classes:", dict(l_counts))
        print("right_count:", len(r_det), "classes:", dict(r_counts))

                                                            
        l_overlay = render_defects_overlay_png(left_img, l_det)
        r_overlay = render_defects_overlay_png(right_img, r_det)

        if l_overlay:
            l_out = _composite_overlay(left_img, l_overlay)
            l_path = out_dir / f"left_{left.brick_id[:8]}_conf{conf:.3f}.png"
            l_out.save(l_path)
            print("saved:", str(l_path))
        if r_overlay:
            r_out = _composite_overlay(right_img, r_overlay)
            r_path = out_dir / f"right_{right.brick_id[:8]}_conf{conf:.3f}.png"
            r_out.save(r_path)
            print("saved:", str(r_path))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
