from __future__ import annotations

import argparse
import os
import random
import shutil
import sys
from collections import Counter
from pathlib import Path

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


def _resolve_dataset_root(cli_root: str | None) -> Path:
    candidates: list[str] = []
    if cli_root:
        candidates.append(cli_root)

    env_root = os.getenv("BRICK_DEFECT_DATASET_ROOT")
    if env_root:
        candidates.append(env_root)

                                   
    candidates.append(r"C:\Brick AI\BRICK_AI_DEV\data\brick_defects")

                                                                       
    candidates.append(os.path.abspath(os.path.join(ROOT_DIR, "..", "BRICK_AI_DEV", "data", "brick_defects")))

    for c in candidates:
        p = Path(c)
        if p.exists() and p.is_dir():
            return p

    raise FileNotFoundError(
        "Could not find brick defect dataset root. "
        "Pass --dataset-root or set BRICK_DEFECT_DATASET_ROOT."
    )


def _pick_split_root(dataset_root: Path) -> Path:
    for split in ("train", "val", "test"):
        p = dataset_root / split
        if (p / "images").is_dir() and (p / "labels").is_dir():
            return p
    raise FileNotFoundError(f"No split found under {dataset_root} (expected train/val/test with images/labels)")


def _iter_labeled_images(images_dir: Path, labels_dir: Path) -> list[Path]:
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    imgs: list[Path] = []
    for p in images_dir.iterdir():
        if p.suffix.lower() not in exts:
            continue
        label = labels_dir / (p.stem + ".txt")
        if not label.exists():
            continue
        try:
            txt = label.read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            continue
        if not txt:
            continue
        imgs.append(p)
    imgs.sort()
    return imgs


def _parse_label_counts(label_file: Path) -> dict[str, int]:
    from lib.yolo_defect_detector import DEFECT_CLASSES

    counts: Counter[str] = Counter()
    try:
        lines = label_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return {}

    for ln in lines:
        ln = (ln or "").strip()
        if not ln:
            continue
        parts = ln.split()
        if not parts:
            continue
        try:
            cls_id = int(float(parts[0]))
        except Exception:
            continue
        counts[DEFECT_CLASSES.get(cls_id, str(cls_id))] += 1

    return dict(counts)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest defect-positive demo bricks into the catalog from the labeled dataset")
    p.add_argument("--dataset-root", default=None, help="Path to BRICK_AI_DEV/data/brick_defects")
    p.add_argument("--limit", type=int, default=12, help="How many demo bricks to ingest")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dataset", default="catalog", help="Dataset tag to store in Mongo (default: catalog)")
    p.add_argument(
        "--uploads-subdir",
        default="defect_demo",
        help="Subfolder under UPLOAD_FOLDER to copy images into",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> int:
    _load_dotenv_best_effort()

    from brick_analyzer import BrickImageAnalyzer
    from lib.mongo import get_db_name, get_mongo_uri

    args = _parse_args()

    dataset_root = _resolve_dataset_root(args.dataset_root)
    split_root = _pick_split_root(dataset_root)
    images_dir = split_root / "images"
    labels_dir = split_root / "labels"

    all_imgs = _iter_labeled_images(images_dir, labels_dir)
    if not all_imgs:
        print("No labeled images found under:", str(images_dir))
        return 2

    rng = random.Random(int(args.seed))
    rng.shuffle(all_imgs)
    chosen = all_imgs[: max(1, int(args.limit))]

    upload_folder = os.getenv("UPLOAD_FOLDER") or os.path.join(ROOT_DIR, "uploads")
    uploads_subdir = str(args.uploads_subdir).strip().strip("/\\")
    dest_dir = Path(upload_folder) / uploads_subdir
    dest_dir.mkdir(parents=True, exist_ok=True)

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()
    analyzer = BrickImageAnalyzer(mongo_uri=mongo_uri, db_name=db_name)

    print(
        {
            "dataset_root": str(dataset_root),
            "split": str(split_root.name),
            "selected": len(chosen),
            "dest_dir": str(dest_dir),
            "dataset": args.dataset,
            "dry_run": bool(args.dry_run),
        }
    )

    ingested = 0
    failed = 0

    for idx, src_img in enumerate(chosen, start=1):
        label_file = labels_dir / (src_img.stem + ".txt")
        counts = _parse_label_counts(label_file)

        dest_path = dest_dir / src_img.name
        relpath = f"{uploads_subdir}/{src_img.name}".replace("\\", "/")

                                              
        try:
            if not args.dry_run:
                shutil.copy2(src_img, dest_path)
        except Exception as e:
            failed += 1
            print(f"[{idx}/{len(chosen)}] COPY FAIL {src_img.name}: {e}")
            continue

        try:
            img = Image.open(dest_path if dest_path.exists() else src_img).convert("RGB")
        except Exception as e:
            failed += 1
            print(f"[{idx}/{len(chosen)}] OPEN FAIL {src_img.name}: {e}")
            continue

        display_name = f"Defect Demo: {src_img.stem}"
        metadata = {
            "display_name": display_name,
            "brick_name": display_name,
            "item_code": f"defect_demo_{src_img.stem}",
            "brand": "Defect Demo",
            "source": "brick_defects_dataset",
            "demo": True,
            "defect_labels": counts,
        }

        try:
            if args.dry_run:
                brick_id = "(dry-run)"
            else:
                brick_id = analyzer.process_and_store_brick(img, relpath=relpath, metadata=metadata, dataset=args.dataset)
            ingested += 1
            print(f"[{idx}/{len(chosen)}] ingested id={str(brick_id)[:10]} relpath={relpath} labels={counts}")
        except Exception as e:
            failed += 1
            print(f"[{idx}/{len(chosen)}] INGEST FAIL {src_img.name}: {e}")
            continue

    print({"ingested": ingested, "failed": failed})
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
