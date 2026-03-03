from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


class GoogleMapsError(RuntimeError):
    pass


def _http_get_json(url: str, *, timeout_s: float = 5.0) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "BrickAI/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise GoogleMapsError(f"Invalid JSON response: {e}")


def _http_post_json(
    url: str,
    *,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    timeout_s: float = 5.0,
) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    hdrs = {"User-Agent": "BrickAI/1.0", "Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raw = e.read() if hasattr(e, "read") else b""
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            data = {}
        msg = (
            (data.get("error") or {}).get("message")
            or data.get("message")
            or (raw.decode("utf-8", errors="ignore")[:500] if raw else str(e))
        )
        raise GoogleMapsError(msg)

    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as ex:
        raise GoogleMapsError(f"Invalid JSON response: {ex}")


def _get_driving_distance_km_routes_api(
    *,
    api_key: str,
    origin: str,
    destination: str,
    timeout_s: float = 5.0,
) -> Optional[float]:
    """Return driving distance in km using Google Routes API (preferred)."""
    key = (api_key or "").strip()
    if not key:
        raise GoogleMapsError("Missing GOOGLE_MAPS_API_KEY")

    o = (origin or "").strip()
    d = (destination or "").strip()
    if not o or not d:
        return None

    url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    payload = {
        "origin": {"address": o},
        "destination": {"address": d},
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_UNAWARE",
        "computeAlternativeRoutes": False,
        "units": "METRIC",
    }
    headers = {
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": "routes.distanceMeters",
    }

    data = _http_post_json(url, headers=headers, payload=payload, timeout_s=timeout_s)
    routes = data.get("routes") or []
    if not routes:
        return None
    meters = (routes[0] or {}).get("distanceMeters")
    if meters is None:
        return None
    try:
        return float(meters) / 1000.0
    except Exception as e:
        raise GoogleMapsError(f"Unexpected distanceMeters value: {e}")


def _get_driving_distance_km_distance_matrix(
    *,
    api_key: str,
    origin: str,
    destination: str,
    timeout_s: float = 5.0,
) -> Optional[float]:
    """Return driving distance in km using Google Distance Matrix (legacy fallback)."""
    key = (api_key or "").strip()
    if not key:
        raise GoogleMapsError("Missing GOOGLE_MAPS_API_KEY")

    o = (origin or "").strip()
    d = (destination or "").strip()
    if not o or not d:
        return None

    params = {
        "origins": o,
        "destinations": d,
        "mode": "driving",
        "units": "metric",
        "key": key,
    }
    url = "https://maps.googleapis.com/maps/api/distancematrix/json?" + urllib.parse.urlencode(params)

    data = _http_get_json(url, timeout_s=timeout_s)
    if data.get("status") != "OK":
        msg = data.get("error_message") or data.get("status")
        raise GoogleMapsError(f"Distance Matrix error: {msg}")

    rows = data.get("rows") or []
    if not rows:
        return None
    elements = (rows[0] or {}).get("elements") or []
    if not elements:
        return None

    el = elements[0] or {}
    if el.get("status") != "OK":
        return None

    meters = ((el.get("distance") or {}).get("value"))
    if meters is None:
        return None

    try:
        km = float(meters) / 1000.0
    except Exception as e:
        raise GoogleMapsError(f"Unexpected distance value: {e}")

    return km


def get_driving_distance_km(
    *,
    api_key: str,
    origin: str,
    destination: str,
    timeout_s: float = 5.0,
) -> Optional[float]:
    """Return driving distance in km.

    Prefers the newer Routes API, with Distance Matrix as a fallback.
    Returns None on no-route / not-found.
    Raises GoogleMapsError on API / transport errors.
    """
    last_err: Optional[Exception] = None
    try:
        return _get_driving_distance_km_routes_api(
            api_key=api_key,
            origin=origin,
            destination=destination,
            timeout_s=timeout_s,
        )
    except Exception as e:
        last_err = e

    try:
        return _get_driving_distance_km_distance_matrix(
            api_key=api_key,
            origin=origin,
            destination=destination,
            timeout_s=timeout_s,
        )
    except Exception as e:
                                                                     
        raise GoogleMapsError(str(last_err or e))


def embed_directions_iframe_url(*, api_key: str, origin: str, destination: str) -> str:
    key = (api_key or "").strip()
    o = (origin or "").strip()
    d = (destination or "").strip()
    params = {
        "key": key,
        "origin": o,
        "destination": d,
        "mode": "driving",
    }
    return "https://www.google.com/maps/embed/v1/directions?" + urllib.parse.urlencode(params)
