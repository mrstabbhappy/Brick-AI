import argparse
import os
import sys

from dotenv import load_dotenv
from PIL import Image

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

                                                           
load_dotenv(os.path.join(REPO_ROOT, ".env"), interpolate=False)

from brick_analyzer import BrickImageAnalyzer              
from lib.mongo import get_db_name, get_mongo_uri              
from lib.blob_storage import BlobStorage              


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest a reference image (used by AI Search) into the DB")
    p.add_argument("image_path", help="Path to an image file")
    p.add_argument("--name", default=None, help="brick_name")
    p.add_argument("--item-number", default=None)
    p.add_argument("--colour", default=None)
    p.add_argument("--type", dest="brick_type", default=None)
    p.add_argument("--brand", default=None)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    img = Image.open(args.image_path).convert("RGB")

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()
    analyzer = BrickImageAnalyzer(mongo_uri=mongo_uri, db_name=db_name)

                                                                                                                 
    blob = BlobStorage()
    relpath = os.path.basename(args.image_path)
    if blob.is_configured():
        from io import BytesIO
        import secrets

        buf = BytesIO()
        img.save(buf, format="PNG")
        blob_name = f"ref_{secrets.token_hex(8)}_{os.path.splitext(relpath)[0]}.png"
        relpath = blob.upload_bytes(
            container=os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads"),
            blob_name=blob_name,
            data=buf.getvalue(),
            content_type="image/png",
        )

    metadata = {
        "brick_name": args.name,
        "item_number": args.item_number,
        "brick_colour": args.colour,
        "brick_type": args.brick_type,
        "brand": args.brand,
    }

    brick_id = analyzer.process_and_store_brick(img, relpath=relpath, metadata=metadata, dataset="reference")
    print(f"Ingested reference image: id={brick_id} image_path={relpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
