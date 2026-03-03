import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import bcrypt
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


class UserService:
    def __init__(self, mongo_uri: str, db_name: str = "brickdb"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.users = self.db["users"]
        self.ensure_indexes()

    def ensure_indexes(self) -> None:
        try:
            self.users.create_index("username", unique=True)
            self.users.create_index("email", unique=True)
        except Exception:
                                                                              
                                                                    
            return

    def _serialize(self, user: Dict[str, Any]) -> Dict[str, Any]:
        if not user:
            return None
        user = dict(user)
        if "created_at" in user and hasattr(user["created_at"], "isoformat"):
            user["created_at"] = user["created_at"].isoformat()
        if "updated_at" in user and hasattr(user["updated_at"], "isoformat"):
            user["updated_at"] = user["updated_at"].isoformat()
        return user

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        username = (username or "").strip()
        password = password or ""
        user = self.users.find_one({"username": username})
        if not user:
            return None
        ph = user.get("password_hash") or ""
        if not ph.startswith("$2"):
            return None
        if bcrypt.checkpw(password.encode("utf-8"), ph.encode("utf-8")):
            return self._serialize(user)
        return None

    def create_user(self, *, username: str, email: str, password: str, bu_code: str | None = None) -> Optional[str]:
        username = (username or "").strip()
        email = (email or "").strip().lower()
        password = password or ""
        bu_code = (bu_code or "").strip() or None

        if not username or not email or not password:
            return None
        if len(password) < 8:
            return None

        user_id = str(uuid.uuid4())
        password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

        doc = {
            "_id": user_id,
            "username": username,
            "email": email,
            "password_hash": password_hash,
            "bu_code": bu_code,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        try:
            self.users.insert_one(doc)
            return user_id
        except DuplicateKeyError:
            return None
        except Exception:
                                                                                     
            if self.users.find_one({"$or": [{"username": username}, {"email": email}]}):
                return None
            raise

    def ensure_admin_from_env(
        self,
        *,
        username: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        bu_code: Optional[str] = None,
    ) -> Optional[str]:
        """Create a bootstrap admin user if one does not exist.

        Safe/idempotent: if the username or email already exists, does nothing.
        """
        username = (username or "").strip()
        email = (email or "").strip().lower()
        password = password or ""
        bu_code = (bu_code or "").strip() or None

        if not username or not email or not password:
            return None
        if self.users.find_one({"$or": [{"username": username}, {"email": email}]}):
            return None

        user_id = self.create_user(username=username, email=email, password=password, bu_code=bu_code)
        if user_id:
                                                         
            self.users.update_one({"_id": user_id}, {"$set": {"roles": ["admin"]}})
        return user_id

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        user = self.users.find_one({"_id": user_id})
        return self._serialize(user) if user else None

    def update_user(self, user_id: str, updates: Dict[str, Any]) -> bool:
        if not updates:
            return False
        updates = dict(updates)
        updates["updated_at"] = datetime.now(timezone.utc)
        res = self.users.update_one({"_id": user_id}, {"$set": updates})
        return res.modified_count > 0
