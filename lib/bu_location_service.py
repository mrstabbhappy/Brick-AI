import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import MongoClient


class BULocationService:
    def __init__(self, mongo_uri: str, db_name: str = "brickdb"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.locations = self.db["bu_locations"]
        try:
            self.locations.create_index("name", unique=True)
            self.locations.create_index("active")
        except Exception:
            pass

    def create_location(self, *, name: str, address: str, lat: float, lng: float, bu_code: str, created_by: str) -> Optional[str]:
        if self.locations.find_one({"name": name}):
            return None
        _id = str(uuid.uuid4())
        doc = {
            "_id": _id,
            "name": name,
            "address": address,
            "lat": lat,
            "lng": lng,
            "bu_code": bu_code,
            "active": True,
            "created_at": datetime.now(timezone.utc),
            "created_by": created_by,
        }
        self.locations.insert_one(doc)
        return _id

    def list_active_locations(self) -> List[Dict[str, Any]]:
        return list(self.locations.find({"active": True}).sort("name", 1))
