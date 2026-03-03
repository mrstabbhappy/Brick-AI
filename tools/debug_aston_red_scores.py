import os
import re
import sys


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

                                           
    os.environ["ENABLE_DINOV2"] = "1"

    from brick_analyzer import BrickImageAnalyzer
    from PIL import Image

    analyzer = BrickImageAnalyzer(db_name=os.getenv("MONGODB_DB", "brickdb"))

    needle = re.compile(r"aston\s*red", re.I)

                                        
    query = {
        "$or": [
            {"metadata.display_name": {"$regex": "aston.*red", "$options": "i"}},
            {"metadata.brick_name": {"$regex": "aston.*red", "$options": "i"}},
            {"metadata.item_code": {"$regex": "aston.*red", "$options": "i"}},
            {"metadata.item_number": {"$regex": "aston.*red", "$options": "i"}},
        ]
    }

    doc = analyzer.bricks_collection.find_one(query)

                                
    if not doc:
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
            if needle.search(hay):
                doc = d
                break

    if not doc:
        raise SystemExit('Could not find any brick matching "aston red" in metadata.')

    brick_id = str(doc.get("brick_id") or doc.get("_id"))
    image_path = doc.get("image_path")
    if not image_path:
        raise SystemExit(f"Found Aston Red brick {brick_id} but it has no image_path")

    upload_folder = os.getenv("UPLOAD_FOLDER") or "uploads"
    local_path = image_path if os.path.isabs(image_path) else os.path.join(upload_folder, image_path)

    img = Image.open(local_path).convert("RGB")

    results = analyzer.search_similar_bricks(img, top_k=50, dataset="catalog")
    results = [r for r in results if str(r._id) != brick_id]

    print("ASTON RED brick_id:", brick_id)
    print("ASTON RED image_path:", image_path)
    print("--- MATCHES (excluding self) ---")

    for r in results:
        md = r.metadata or {}
        name = (
            md.get("display_name")
            or md.get("brick_name")
            or md.get("item_code")
            or md.get("item_number")
            or (r.image_path or "")
            or r._id
        )
        print(
            f"{name}\t"
            f"OVERALL={r.overall_similarity:.4f}\t"
            f"COLOUR={r.colour_similarity:.4f}\t"
            f"TEXTURE={r.texture_similarity:.4f}\t"
            f"VISUAL={r.visual_similarity:.4f}\t"
            f"ID={r._id[:8]}\t"
            f"IMG={r.image_path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
