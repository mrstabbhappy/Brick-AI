from __future__ import annotations

from typing import Dict, List, Optional


                                               
                                                                           
BU_LOCATIONS: List[Dict[str, str]] = [
    {
        "id": "bristol",
        "region": "England - South West",
        "name": "Bristol",
        "address": "Ground Floor, 730 Waterside Drive, Aztec West, Almondsbury, BS32 4UE, UK",
    },
    {
        "id": "exeter",
        "region": "England - South West",
        "name": "Exeter",
        "address": "4 Capital Court, Bittern Road, Sowton Industrial Estate, Exeter, Devon, EX2 7FW, UK",
    },
    {
        "id": "central-london",
        "region": "England - South East",
        "name": "Central London",
        "address": "Ground Floor East Wing, BT Brentwood, Essex, CM14 4QP, UK",
    },
    {
        "id": "south-east",
        "region": "England - South East",
        "name": "South East",
        "address": "Weald Court, 103 Tonbridge Road, Hildenborough, Tonbridge, Kent, TN11 9HL, UK",
    },
    {
        "id": "south-thames",
        "region": "England - South East",
        "name": "South Thames",
        "address": "The Arc, Office Park, Springfield Drive, Leatherhead, KT22 7LP, UK",
    },
    {
        "id": "southern-counties",
        "region": "England - South East",
        "name": "Southern Counties",
        "address": "Colvedene Court, Wessex Business Park, Wessex Way, Colden Common, Winchester, SO21 1WP, UK",
    },
    {
        "id": "west-london",
        "region": "England - South East",
        "name": "West London",
        "address": "Form 1, 17 Bartley Wood Business Park, Bartley Way, Hook, Hampshire, RG27 9XA, UK",
    },
    {
        "id": "east-anglia",
        "region": "England - Eastern",
        "name": "East Anglia",
        "address": "Castle House, Kempson Way, Bury St Edmunds, Suffolk, IP32 7AR, UK",
    },
    {
        "id": "london",
        "region": "England - Eastern",
        "name": "London",
        "address": "Ground Floor East Wing, BT Brentwood, Essex, CM14 4QP, UK",
    },
    {
        "id": "north-thames",
        "region": "England - Eastern",
        "name": "North Thames",
        "address": "The Dock, Station Road, Kings Langley, WD4 8LZ, UK",
    },
    {
        "id": "east-midlands",
        "region": "England - Midlands",
        "name": "East Midlands",
        "address": "Unit 2, The Osiers Business Park, Laversall Way, Leicester, Leicestershire, LE19 1DX, UK",
    },
    {
        "id": "midlands",
        "region": "England - Midlands",
        "name": "Midlands",
        "address": "Unit 2 - Tournament Court, Edgehill Drive, Warwick, Warwickshire, CV34 6LG, UK",
    },
    {
        "id": "north-midlands",
        "region": "England - Midlands",
        "name": "North Midlands",
        "address": "2 Trinity Court, Broadlands, Wolverhampton, WV10 6UH, UK",
    },
    {
        "id": "south-midlands",
        "region": "England - Midlands",
        "name": "South Midlands",
        "address": "Newton House, 2 Sark Drive, Newton Leys, Bletchley, Milton Keynes, Buckinghamshire, MK3 5SD, UK",
    },
    {
        "id": "west-midlands",
        "region": "England - Midlands",
        "name": "West Midlands",
        "address": "Second Floor, Fore 2, Fore Business Park, Huskisson Way, Shirley, Solihull, West Midlands, B90 4SS, UK",
    },
    {
        "id": "east-scotland",
        "region": "Scotland",
        "name": "East Scotland",
        "address": "1 Masterton Park, Dunfermline, Fife, KY11 8NX, UK",
    },
    {
        "id": "west-scotland",
        "region": "Scotland",
        "name": "West Scotland",
        "address": "Unit C - Lightyear Building, Glasgow Airport Business Park, Marchburn Drive, Abbotsinch, Paisley, PA3 2SJ, UK",
    },
    {
        "id": "head-office",
        "region": "Head Office",
        "name": "Head Office",
        "address": "Gate House, Turnpike Road, High Wycombe, Buckinghamshire, HP12 3NR, UK",
    },
    {
        "id": "manchester",
        "region": "England - North West",
        "name": "Manchester",
        "address": "1 Lumsdale Road, Stretford, Manchester, Greater Manchester, M32 0UT, UK",
    },
    {
        "id": "north-west",
        "region": "England - North West",
        "name": "North West",
        "address": "Washington House, Birchwood Park Avenue, Warrington, Cheshire, WA3 6GR, UK",
    },
    {
        "id": "north-east",
        "region": "England - Yorkshire and North East",
        "name": "North East",
        "address": "Rapier House, Colima Avenue, Sunderland, Tyne And Wear, SR5 3XB, UK",
    },
    {
        "id": "north-yorkshire",
        "region": "England - Yorkshire and North East",
        "name": "North Yorkshire",
        "address": "Taylor Wimpey House, Lockheed Court, Preston Farm Industrial Estate, Stockton on Tees, Cleveland, TS18 3SH, UK",
    },
    {
        "id": "yorkshire",
        "region": "England - Yorkshire and North East",
        "name": "Yorkshire",
        "address": "Sandpiper House, Peel Avenue, Calder Park, Wakefield, West Yorkshire, WF2 7UA, UK",
    },
    {
        "id": "south-wales",
        "region": "Wales",
        "name": "South Wales",
        "address": "Building 2, Eastern Business Park, Wern Fawr Lane, St Mellons, Cardiff, CF3 5EA, UK",
    },
]


def list_bu_locations() -> List[Dict[str, str]]:
    return list(BU_LOCATIONS)


def get_bu_location(bu_id: str) -> Optional[Dict[str, str]]:
    v = (bu_id or "").strip()
    if not v:
        return None
    for loc in BU_LOCATIONS:
        if loc.get("id") == v:
            return loc
    return None
