import argparse
import os
import sys
from getpass import getpass

from dotenv import load_dotenv

                                                    
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

                                                             
                                                                    
load_dotenv(os.path.join(REPO_ROOT, ".env"), interpolate=False)

from lib.mongo import get_db_name, get_mongo_uri              
from lib.user_service import UserService              


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create a user in the Cosmos(Mongo API)/MongoDB users collection")
    p.add_argument("--username", required=True)
    p.add_argument("--email", required=True)
    p.add_argument("--bu-code", default=None)
    p.add_argument("--password", default=None, help="If omitted, you'll be prompted securely")
    p.add_argument("--admin", action="store_true", help="Also add roles=['admin']")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    password = args.password
    if not password:
        password = getpass("Password (min 8 chars): ")

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()

    users = UserService(mongo_uri=mongo_uri, db_name=db_name)

    user_id = users.create_user(
        username=args.username,
        email=args.email,
        password=password,
        bu_code=args.bu_code,
    )

                                                       
    if not user_id:
        existing = users.users.find_one(
            {"$or": [{"username": args.username}, {"email": args.email.lower()}]},
            {"_id": 1, "roles": 1, "username": 1, "email": 1},
        )
        if existing and args.admin:
            roles = existing.get("roles") or []
            if isinstance(roles, str):
                roles = [roles]
            if not isinstance(roles, list):
                roles = []
            if "admin" not in roles:
                roles = list(roles) + ["admin"]
            users.users.update_one({"_id": existing["_id"]}, {"$set": {"roles": roles}})
            print(f"User already exists; promoted to admin: {existing.get('username') or args.username}")
            return 0

        print("User not created (already exists or invalid input).", file=sys.stderr)
        return 2

    if args.admin:
        users.users.update_one({"_id": user_id}, {"$set": {"roles": ["admin"]}})

    print(f"Created user: {args.username} ({args.email}) id={user_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
