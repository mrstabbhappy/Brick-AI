from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

                                             
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib.factory_service import ensure_factory_indexes, upsert_factory


FACTORIES = [
    {
        "_id": "ATLAS",
        "factory_code": "ATL",
        "name": "Atlas Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Stubbers Green Road",
            "town": "Aldridge",
            "city": "Walsall",
            "county": "West Midlands",
            "postcode": "WS9 8BL",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "CATTYBROOK",
        "factory_code": "CAT",
        "name": "Cattybrook Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Over Lane",
            "town": "Almondsbury",
            "city": "Bristol",
            "postcode": "BS32 4BX",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "CHESTERTON",
        "factory_code": "CHE",
        "name": "Chesterton Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Audley Road",
            "town": "Chesterton",
            "city": "Newcastle-under-Lyme",
            "postcode": "ST5 7ES",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "DORKET_HEAD",
        "factory_code": "DOR",
        "name": "Dorket Head Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Lime Lane",
            "town": "Arnold",
            "city": "Nottingham",
            "postcode": "NG5 8PZ",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "ELLISTOWN",
        "factory_code": "ELL",
        "name": "Ellistown Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Whitehill Road",
            "town": "Ellistown",
            "city": "Coalville",
            "county": "Leicestershire",
            "postcode": "LE67 1HY",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "ECLIPSE",
        "factory_code": "LE3",
        "name": "Eclipse Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Leicester Road",
            "town": "Ibstock",
            "county": "Leicestershire",
            "postcode": "LE67 6HS",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "LODGE_LANE",
        "factory_code": "LOD",
        "name": "Lodge Lane Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Lodge Close, off Hawkins Drive",
            "city": "Cannock",
            "county": "Staffordshire",
            "postcode": "WS11 0LW",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "PARKHOUSE",
        "factory_code": "PAR",
        "name": "Parkhouse Brick Factory",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Speedwell Road, Parkhouse Industrial Estate",
            "city": "Newcastle-under-Lyme",
            "postcode": "ST5 7RZ",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "THROCKLEY",
        "factory_code": "THR",
        "name": "Throckley Brickworks",
        "manufacturer": "Ibstock",
        "address": {
            "line1": "Ponteland Road",
            "town": "Throckley",
            "city": "Newcastle upon Tyne",
            "postcode": "NE15 9EQ",
            "country": "United Kingdom",
        },
        "status": "active",
    },
    {
        "_id": "DENTON",
        "factory_code": "DENTON",
        "name": "Denton Brickworks",
        "manufacturer": "Wienerberger",
        "address": {
            "line1": "Windmill Lane",
            "city": "Denton",
            "county": "Greater Manchester",
            "postcode": "M34 2JF",
            "country": "United Kingdom",
        },
        "status": "active",
    },
]


def main() -> int:
                                                               
    load_dotenv(os.path.join(_ROOT, ".env"), interpolate=False)

    ensure_factory_indexes()

    for f in FACTORIES:
        upsert_factory(f)

    print(f"Seeded factories: {len(FACTORIES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
