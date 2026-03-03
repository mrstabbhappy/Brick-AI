import argparse
import glob
import os
import sys
from datetime import datetime

from PIL import Image

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def _load_dotenv_best_effort() -> None:
    try:
        from dotenv import load_dotenv                

        try:
            load_dotenv(".env", override=False, interpolate=False)
        except TypeError:
            load_dotenv(".env", override=False)
    except Exception:
        pass


def main() -> int:
    _load_dotenv_best_effort()

    parser = argparse.ArgumentParser(description="Backfill DINOv2 embeddings into brick_features")
    parser.add_argument("--dataset", default="catalog")
    parser.add_argument("--limit", type=int, default=0, help="0 = no limit")
    parser.add_argument("--use-blob", action="store_true", help="Fetch images from Azure Blob when configured")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    os.environ["ENABLE_DINOV2"] = "1"                              

    from brick_analyzer import BrickImageAnalyzer
    from lib.image_store import load_image
    from lib.blob_storage import BlobStorage

    analyzer = BrickImageAnalyzer(db_name=os.getenv("MONGODB_DB", "brickdb"))

    blob = BlobStorage() if args.use_blob else None

    uploads_container = os.getenv("UPLOADS_CONTAINER") or os.getenv("AZURE_BLOB_UPLOADS_CONTAINER") or "uploads"
    upload_folder = os.getenv("UPLOAD_FOLDER") or os.path.join(os.getcwd(), "uploads")

    q = {"$or": [{"dataset": args.dataset}, {"dataset": {"$exists": False}}]}

    cursor = analyzer.bricks_collection.find(q, {"brick_id": 1, "image_path": 1}).sort("updated_at", -1)

    updated = 0
    skipped = 0
    failed = 0

    for doc in cursor:
        if args.limit and (updated + skipped + failed) >= args.limit:
            break

        brick_id = str(doc.get("brick_id") or doc.get("_id"))
        relpath = doc.get("image_path")
        if not brick_id or not relpath:
            skipped += 1
            continue

        feat = analyzer.brick_features_collection.find_one({"_id": brick_id}, {"dinov2_embedding": 1, "dinov2_model": 1})
        if feat and feat.get("dinov2_embedding"):
            skipped += 1
            continue

        try:
            local_path = relpath
            if not os.path.isabs(local_path):
                local_path = os.path.join(upload_folder, relpath)
            if os.path.exists(local_path):
                img = Image.open(local_path).convert("RGB")
            else:
                img = load_image(
                    relpath,
                    blob=blob,
                    uploads_container=uploads_container,
                    upload_folder=upload_folder,
                )
            emb = analyzer.embed_dinov2(img)
            if args.dry_run:
                updated += 1
                continue

            analyzer.brick_features_collection.update_one(
                {"_id": brick_id},
                {
                    "$set": {
                        "dinov2_embedding": emb.tolist(),
                        "dinov2_model": os.getenv("DINOV2_MODEL", "dinov2_vits14"),
                        "dinov2_updated_at": datetime.utcnow().isoformat(),
                    }
                },
                upsert=True,
            )
            updated += 1
        except Exception as e:
            failed += 1
            print(f"[FAIL] {brick_id} {relpath}: {e}")

    print(f"done updated={updated} skipped={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
