from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


class ValidationError(ValueError):
    pass


_ALLOWED_CALC_MODES = {"wall_area", "house_preset", "density"}
_ALLOWED_WALL_TYPES = {"single_skin", "double_skin"}
_ALLOWED_PRICE_MODES = {"asp_all_regions", "tiered_region"}
_ALLOWED_TIERS = {"T1", "T2", "T3", "T4"}


_FACTORY_NAME_TO_CODE = {
    "ATLAS": "ATL",
    "CATTYBROOK": "CAT",
    "CHESTERTON": "CHE",
    "DORKET HEAD": "DOR",
    "ELLISTOWN": "ELL",
    "ECLIPSE": "LE3",
    "LODGE LANE": "LOD",
    "PARKHOUSE": "PAR",
    "THROCKLEY": "THR",
}


def _get_nested(d: dict, path: str, default=None):
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        if part not in cur:
            return default
        cur = cur.get(part)
    return cur


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None


def _ceil_div(n: float, d: float) -> int:
    return int(math.ceil(n / d))


def _validate_payload(payload: dict) -> None:
    sku = (payload.get("brick_sku") or "").strip()
    if not sku:
        raise ValidationError("brick_sku is required")

    mode = payload.get("calc_mode")
    if mode not in _ALLOWED_CALC_MODES:
        raise ValidationError('calc_mode must be one of: "wall_area" | "house_preset" | "density"')

    plots = payload.get("plots", 1)
    plots_i = _to_int(plots)
    if plots_i is None or plots_i < 1:
        raise ValidationError("plots must be >= 1")

    waste = payload.get("waste_pct", 0.10)
    waste_f = _to_float(waste)
    if waste_f is None or waste_f < 0.0 or waste_f > 0.25:
        raise ValidationError("waste_pct must be between 0.00 and 0.25")

    cps = payload.get("custom_pack_size")
    if cps is not None:
        cps_i = _to_int(cps)
        if cps_i is None or cps_i < 1:
            raise ValidationError("custom_pack_size must be an integer >= 1")

    pricing = payload.get("pricing") or {}
    price_mode = pricing.get("price_mode")
    if price_mode not in _ALLOWED_PRICE_MODES:
        raise ValidationError('pricing.price_mode must be "asp_all_regions" | "tiered_region"')

    if price_mode == "tiered_region":
        if not (pricing.get("tw_region_name") and pricing.get("selected_factory")):
            raise ValidationError("tiered_region requires tw_region_name and selected_factory")

    override_tier = pricing.get("override_tier")
    if override_tier:
        if str(override_tier).strip() not in _ALLOWED_TIERS:
            raise ValidationError("override_tier must be one of T1..T4")

    if mode == "wall_area":
        wa = _to_float(payload.get("wall_area_m2"))
        if wa is None or wa <= 0:
            raise ValidationError("wall_area_m2 must be > 0")
        wt = payload.get("wall_type")
        if wt not in _ALLOWED_WALL_TYPES:
            raise ValidationError('wall_type must be "single_skin" or "double_skin"')

    if mode == "house_preset":
        hp = payload.get("house_preset") or {}
        band = (hp.get("size_band") or "").strip().lower()
        if band not in {"small", "medium", "large"}:
            raise ValidationError("house_preset.size_band must be small/medium/large")

    if mode == "density":
        density = payload.get("density") or {}
        if not density.get("enabled"):
            raise ValidationError("density.enabled must be true when calc_mode=density")
        acres = _to_float(density.get("net_developable_acres"))
        hpa = _to_float(density.get("houses_per_acre"))
        if acres is None or acres <= 0:
            raise ValidationError("density.net_developable_acres must be > 0")
        if hpa is None or hpa <= 0:
            raise ValidationError("density.houses_per_acre must be > 0")

                                                                              
                       
        wa = payload.get("wall_area_m2")
        wt = payload.get("wall_type")
        hp = payload.get("house_preset") or {}
        band = (hp.get("size_band") or "").strip().lower()

        has_wall = _to_float(wa) is not None and _to_float(wa) > 0 and wt in _ALLOWED_WALL_TYPES
        has_preset = bool(hp.get("enabled")) and band in {"small", "medium", "large"}
        if not (has_wall or has_preset):
            raise ValidationError(
                "density mode requires either (wall_area_m2 + wall_type) or (house_preset.enabled + size_band)"
            )


def _pack_size(brick_doc: dict, custom_pack_size: Optional[int]) -> Tuple[int, str]:
    if custom_pack_size is not None:
        return int(custom_pack_size), "custom"

    db_pack = _to_int(_get_nested(brick_doc, "metadata.pack_size"))
    if db_pack and db_pack >= 1:
        return int(db_pack), "db"

    return 475, "default"


def _bricks_per_plot_wall_area(wall_area_m2: float, wall_type: str, waste_pct: float) -> Tuple[int, int, float]:
    rate = 60 if wall_type == "single_skin" else 120
    base = float(wall_area_m2) * float(rate)
    bricks = int(math.ceil(base * (1.0 + float(waste_pct))))
    return rate, bricks, base


def _bricks_per_plot_house_preset(size_band: str, waste_pct: float) -> Tuple[int, float]:
    band = (size_band or "").strip().lower()
    preset = {"small": 10000, "medium": 20000, "large": 50000}.get(band)
    if preset is None:
        raise ValidationError("house_preset.size_band must be small/medium/large")
    bricks = int(math.ceil(float(preset) * (1.0 + float(waste_pct))))
    return bricks, float(preset)


def _select_price(brick_doc: dict, pricing: dict) -> Tuple[float, dict]:
    custom = pricing.get("custom_asp_gbp_per_th")
    custom_f = _to_float(custom)
    if custom_f is not None:
        return float(custom_f), {
            "price_mode": pricing.get("price_mode"),
            "asp_used_gbp_per_th": float(custom_f),
            "price_source": "custom",
            "tw_region_name": pricing.get("tw_region_name"),
            "selected_factory": pricing.get("selected_factory"),
            "tier_used": None,
            "approval_required": False,
        }

    mode = pricing.get("price_mode")
    if mode == "asp_all_regions":
        asp = _to_float(_get_nested(brick_doc, "metadata.pricing.asp_gbp_per_th"))
        if asp is None:
            raise ValidationError("ASP (all regions) not available for this brick; provide custom_asp_gbp_per_th")
        return float(asp), {
            "price_mode": mode,
            "asp_used_gbp_per_th": float(asp),
            "price_source": "db_asp_all_regions",
            "tw_region_name": None,
            "selected_factory": None,
            "tier_used": None,
            "approval_required": False,
        }

    if mode == "tiered_region":
        tw_region = (pricing.get("tw_region_name") or "").strip()
        selected_factory = (pricing.get("selected_factory") or "").strip()
        if not tw_region or not selected_factory:
            raise ValidationError("tiered_region requires tw_region_name and selected_factory")

        override = (pricing.get("override_tier") or "").strip() or None

        tiers_by_region = _get_nested(brick_doc, "metadata.pricing.tw_regions") or []
        if not isinstance(tiers_by_region, list):
            tiers_by_region = []

        region_entry = None
        for r in tiers_by_region:
            if isinstance(r, dict) and (r.get("tw_region_name") or "").strip().lower() == tw_region.lower():
                region_entry = r
                break
        if not region_entry:
            raise ValidationError("tiered_region selection where region not found")

        factory_tiers = region_entry.get("factory_tiers") or {}
        if not isinstance(factory_tiers, dict):
            raise ValidationError("tiered_region selection where factory tier not found")

        tier = override or factory_tiers.get(selected_factory)
        if tier not in _ALLOWED_TIERS:
            raise ValidationError("tiered_region selection where factory tier not found")

        tier_prices = _get_nested(brick_doc, "metadata.pricing.tier_prices_gbp_per_th") or {}
        if not isinstance(tier_prices, dict):
            raise ValidationError("Tier prices not available for this brick")

        asp = _to_float(tier_prices.get(tier))
        if asp is None:
            raise ValidationError("Tier price missing for selected tier")

                        
        allowed = _get_nested(brick_doc, "metadata.procurement.allowed_tiers_without_approval")
        approval_required = _get_nested(brick_doc, "metadata.procurement.approval_required_tiers")
        if not isinstance(allowed, list):
            allowed = ["T1", "T2"]
        if not isinstance(approval_required, list):
            approval_required = ["T3", "T4"]

        needs_approval = tier in set(str(x) for x in approval_required)

        pricing_out = {
            "price_mode": mode,
            "asp_used_gbp_per_th": float(asp),
            "price_source": "db_tiered",
            "tw_region_name": tw_region,
            "selected_factory": selected_factory,
            "tier_used": tier,
            "approval_required": bool(needs_approval),
        }
        if needs_approval:
            pricing_out["message"] = "Approval required from Category Lead, Group Procurement"

        return float(asp), pricing_out

    raise ValidationError("Unsupported pricing mode")


def calculate_bricks_and_cost(payload: dict, brick_doc: dict) -> dict:
    """Pure calculation function.

    Expects the caller to provide brick_doc with (at minimum) metadata.pack_size and
    metadata.pricing fields if those modes are used.
    """
    _validate_payload(payload)

    sku = (payload.get("brick_sku") or "").strip()
    mode = payload.get("calc_mode")

    waste_pct = float(payload.get("waste_pct", 0.10))

                                        
    plots = int(payload.get("plots", 1))

    if mode == "density":
        density = payload.get("density") or {}
        acres = float(density.get("net_developable_acres"))
        hpa = float(density.get("houses_per_acre"))
        plots = int(math.ceil(acres * hpa))

    custom_pack = payload.get("custom_pack_size")
    custom_pack_i = _to_int(custom_pack) if custom_pack is not None else None
    pack_size_used, pack_size_source = _pack_size(brick_doc, custom_pack_i)

    rate_per_m2 = None
    base_bricks_per_plot = None

    if mode in ("wall_area", "density"):
                                                                              
        if payload.get("wall_area_m2") is not None and payload.get("wall_type") in _ALLOWED_WALL_TYPES:
            wall_area_m2 = float(payload.get("wall_area_m2"))
            wall_type = str(payload.get("wall_type"))
            rate_per_m2, bricks_per_plot, base_bricks_per_plot = _bricks_per_plot_wall_area(
                wall_area_m2, wall_type, waste_pct
            )
        else:
            hp = payload.get("house_preset") or {}
            band = (hp.get("size_band") or "").strip().lower()
            bricks_per_plot, preset_base = _bricks_per_plot_house_preset(band, waste_pct)
            base_bricks_per_plot = preset_base
    elif mode == "house_preset":
        hp = payload.get("house_preset") or {}
        band = (hp.get("size_band") or "").strip().lower()
        bricks_per_plot, preset_base = _bricks_per_plot_house_preset(band, waste_pct)
        base_bricks_per_plot = preset_base
    else:
        raise ValidationError("Unsupported calc_mode")

    pallets_per_plot = int(math.ceil(float(bricks_per_plot) / float(pack_size_used)))

    total_bricks = int(bricks_per_plot) * int(plots)
    total_pallets = int(math.ceil(float(total_bricks) / float(pack_size_used)))

    asp_used, pricing_out = _select_price(brick_doc, payload.get("pricing") or {})

    cost_per_plot = (float(bricks_per_plot) / 1000.0) * float(asp_used)
    cost_total = (float(total_bricks) / 1000.0) * float(asp_used)

    out = {
        "brick_sku": sku,
        "pack_size_used": int(pack_size_used),
        "pack_size_source": pack_size_source,
        "calculation_mode": mode,
        "inputs": {
            "wall_area_m2": payload.get("wall_area_m2"),
            "wall_type": payload.get("wall_type"),
            "waste_pct": waste_pct,
            "plots": plots,
        },
        "quantity": {
            "rate_per_m2": rate_per_m2,
            "base_bricks_per_plot": base_bricks_per_plot,
            "bricks_per_plot": int(bricks_per_plot),
            "pallets_per_plot": int(pallets_per_plot),
            "total_bricks": int(total_bricks),
            "total_pallets": int(total_pallets),
        },
        "pricing": pricing_out,
        "cost": {
            "cost_per_plot_gbp": round(cost_per_plot, 2),
            "cost_total_gbp": round(cost_total, 2),
        },
    }

    return out
