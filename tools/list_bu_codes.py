import argparse
import os
import sys

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

load_dotenv(os.path.join(REPO_ROOT, ".env"), interpolate=False)

from lib.mongo import get_db              


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="List BU codes from the database")
    p.add_argument("--include-users", action="store_true", help="Also show distinct bu_code values from users")
    p.add_argument(
        "--inactive",
        action="store_true",
        help="Include inactive BU locations (default: active only)",
    )
    return p.parse_args()


def _safe_distinct(coll, field, filter_doc=None):
    try:
        return coll.distinct(field, filter_doc or {})
    except Exception:
        return []


def main() -> int:
    args = _parse_args()
    db = get_db()

    locations = db.get_collection("bu_locations")
    filt = {} if args.inactive else {"active": True}

    codes = _safe_distinct(locations, "bu_code", filt)
    codes = sorted([c for c in codes if isinstance(c, str) and c.strip()])

    print("BU codes (from bu_locations):")
    if not codes:
        print("  (none found)")
    else:
        for c in codes:
            print(f"  {c}")

    if args.include_users:
        users = db.get_collection("users")
        user_codes = _safe_distinct(users, "bu_code", {})
        user_codes = sorted([c for c in user_codes if isinstance(c, str) and c.strip()])
        print("\nBU codes (from users):")
        if not user_codes:
            print("  (none found)")
        else:
            for c in user_codes:
                print(f"  {c}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
