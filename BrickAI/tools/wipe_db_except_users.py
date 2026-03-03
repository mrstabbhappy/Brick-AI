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
        description="Drop all collections in the configured DB except 'users' (DANGEROUS)"
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Actually perform the wipe (otherwise dry-run only)",
    )
    p.add_argument(
        "--keep",
        action="append",
        default=[],
        help="Additional collection name to keep (can be specified multiple times)",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    db = get_db()
    db_name = get_db_name()

    keep = {"users", *args.keep}

    try:
        collections = db.list_collection_names()
    except Exception as e:
        print(f"Could not list collections in '{db_name}': {e}", file=sys.stderr)
        return 2

                             
    droppable = [c for c in collections if not c.startswith("system.") and c not in keep]
    droppable = sorted(droppable)

    print(f"Database: {db_name}")
    print(f"Keep: {sorted(keep)}")
    print("Will drop:")
    if not droppable:
        print("  (nothing)")
        return 0
    for c in droppable:
        print(f"  {c}")

    if not args.yes:
        print("\nDry-run only. Re-run with --yes to actually drop these collections.")
        return 0

             
    failures = 0
    for c in droppable:
        try:
            db.drop_collection(c)
        except Exception as e:
            failures += 1
            print(f"Failed to drop {c}: {e}", file=sys.stderr)

    if failures:
        print(f"\nCompleted with {failures} failures.", file=sys.stderr)
        return 3

    print("\nWipe complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
