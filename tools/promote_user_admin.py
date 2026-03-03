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
    p = argparse.ArgumentParser(description="Promote an existing user to admin")
    p.add_argument("--username", default=None)
    p.add_argument("--email", default=None)
    p.add_argument("--yes", action="store_true", help="Actually write changes")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.username and not args.email:
        print("Provide --username or --email", file=sys.stderr)
        return 2

    q = []
    if args.username:
        q.append({"username": args.username})
    if args.email:
        q.append({"email": args.email.lower()})

    db = get_db()
    users = db["users"]

    user = users.find_one({"$or": q}, {"_id": 1, "username": 1, "email": 1, "roles": 1})
    if not user:
        print("User not found", file=sys.stderr)
        return 3

    roles = user.get("roles") or []
    if isinstance(roles, str):
        roles = [roles]
    if not isinstance(roles, list):
        roles = []
    if "admin" not in roles:
        roles = list(roles) + ["admin"]

    print(f"User: {user.get('username')} {user.get('email')} id={user.get('_id')}")
    print(f"New roles: {roles}")

    if not args.yes:
        print("Dry-run only. Re-run with --yes to apply.")
        return 0

    users.update_one({"_id": user["_id"]}, {"$set": {"roles": roles}})
    print("Updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
