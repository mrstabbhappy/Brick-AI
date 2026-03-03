import argparse
import os
import sys

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

                                                           
load_dotenv(os.path.join(REPO_ROOT, ".env"), interpolate=False)

from lib.mongo import get_db, get_db_name              


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Single-table reset: drop brick_features and clear all docs from bricks (keeps users)"
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Actually perform destructive actions (otherwise dry-run)",
    )
    p.add_argument(
        "--bricks-collection",
        default=os.getenv("MONGODB_COLLECTION") or "bricks",
        help="Collection that will store all brick docs",
    )
    p.add_argument(
        "--features-collection",
        default="brick_features",
        help="Collection to drop entirely",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    db = get_db()
    db_name = get_db_name()

    bricks_name = args.bricks_collection
    features_name = args.features_collection

                                       
    bricks_coll = db.get_collection(bricks_name)
    features_coll = db.get_collection(features_name)

    try:
        bricks_count = bricks_coll.count_documents({})
    except Exception:
        bricks_count = None

    try:
        features_count = features_coll.count_documents({})
    except Exception:
        features_count = None

    print(f"Database: {db_name}")
    print(f"Will DROP collection: {features_name} (current docs={features_count})")
    print(f"Will CLEAR collection: {bricks_name} (delete all docs; current docs={bricks_count})")
    print("Will KEEP collection: users")

    if not args.yes:
        print("\nDry-run only. Re-run with --yes to execute.")
        return 0

                                    
    try:
        db.drop_collection(features_name)
        print(f"Dropped: {features_name}")
    except Exception as e:
        print(f"Failed to drop {features_name}: {e}", file=sys.stderr)
        return 2

                                         
    try:
        res = bricks_coll.delete_many({})
        print(f"Cleared: {bricks_name} (deleted {getattr(res, 'deleted_count', '?')} docs)")
    except Exception as e:
        print(f"Failed to clear {bricks_name}: {e}", file=sys.stderr)
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
