from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
from PIL import Image
from PIL import ImageDraw, ImageFont

try:
    import cv2                
except Exception:                    
    cv2 = None                


@dataclass(frozen=True)
class ColourStats:
    avg_rgb: Tuple[float, float, float]
    avg_lab: Tuple[float, float, float]


@dataclass(frozen=True)
class TextureStats:
    laplacian_var: float
    edge_density: float


def _img_to_rgb_array(img: Image.Image, *, size: int = 384) -> np.ndarray:
    rgb = img.convert("RGB")
    w, h = rgb.size
    if w == 0 or h == 0:
        return np.zeros((1, 1, 3), dtype=np.uint8)

                                                
    scale = float(size) / float(max(w, h))
    if scale < 1.0:
        rgb = rgb.resize((max(1, int(w * scale)), max(1, int(h * scale))))

    return np.asarray(rgb)


def _bgr(img: Image.Image, *, size: int = 512) -> np.ndarray:
    arr = _img_to_rgb_array(img, size=size)
    if cv2 is None:
                                        
        return arr
    return cv2.cvtColor(arr, cv2.COLOR_RGB2BGR)


def compute_colour_stats(img: Image.Image) -> ColourStats:
    arr = _img_to_rgb_array(img, size=384).astype(np.float32)
    if arr.size == 0:
        return ColourStats(avg_rgb=(0.0, 0.0, 0.0), avg_lab=(0.0, 0.0, 0.0))

    avg_rgb = tuple(float(x) for x in arr.reshape(-1, 3).mean(axis=0))

    if cv2 is None:
                                                    
        return ColourStats(avg_rgb=avg_rgb, avg_lab=(0.0, 0.0, 0.0))

    bgr = cv2.cvtColor(arr.astype(np.uint8), cv2.COLOR_RGB2BGR)
    lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB).astype(np.float32)
    avg_lab = tuple(float(x) for x in lab.reshape(-1, 3).mean(axis=0))
    return ColourStats(avg_rgb=avg_rgb, avg_lab=avg_lab)


def delta_e76(lab_a: Tuple[float, float, float], lab_b: Tuple[float, float, float]) -> float:
    a = np.array(lab_a, dtype=np.float32)
    b = np.array(lab_b, dtype=np.float32)
    return float(np.linalg.norm(a - b))


def rgb_hex(rgb: Tuple[float, float, float]) -> str:
    r = int(max(0, min(255, round(rgb[0]))))
    g = int(max(0, min(255, round(rgb[1]))))
    b = int(max(0, min(255, round(rgb[2]))))
    return f"#{r:02x}{g:02x}{b:02x}"


def describe_colour_delta(delta_e: float) -> str:
    if delta_e < 6:
        return "Very close"
    if delta_e < 12:
        return "Close"
    if delta_e < 20:
        return "Noticeably different"
    return "Very different"


def compute_texture_stats(img: Image.Image) -> TextureStats:
    if cv2 is None:
        arr = _img_to_rgb_array(img, size=384).astype(np.float32)
        gray = arr.mean(axis=2)
                                                                         
        lap = (
            -4 * gray
            + np.roll(gray, 1, axis=0)
            + np.roll(gray, -1, axis=0)
            + np.roll(gray, 1, axis=1)
            + np.roll(gray, -1, axis=1)
        )
        lap_var = float(lap.var())
        edges = np.abs(lap) > np.percentile(np.abs(lap), 90)
        edge_density = float(edges.mean())
        return TextureStats(laplacian_var=lap_var, edge_density=edge_density)

    bgr = _bgr(img, size=512)
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)

    lap = cv2.Laplacian(gray, cv2.CV_64F)
    lap_var = float(lap.var())

    edges = cv2.Canny(gray, threshold1=80, threshold2=160)
    edge_density = float((edges > 0).mean())

    return TextureStats(laplacian_var=lap_var, edge_density=edge_density)


def _safe_resize_to_match(a: np.ndarray, b: np.ndarray, *, max_side: int = 640) -> Tuple[np.ndarray, np.ndarray]:
    if cv2 is None:
                                    
        return a, b

    ha, wa = a.shape[:2]
    hb, wb = b.shape[:2]
    h = min(ha, hb)
    w = min(wa, wb)
    h = max(1, h)
    w = max(1, w)

                                       
    scale = float(max_side) / float(max(h, w))
    if scale < 1.0:
        h = max(1, int(h * scale))
        w = max(1, int(w * scale))

    a2 = cv2.resize(a, (w, h), interpolation=cv2.INTER_AREA)
    b2 = cv2.resize(b, (w, h), interpolation=cv2.INTER_AREA)
    return a2, b2


def make_difference_heatmap_overlay_png(
    left_img: Image.Image,
    right_img: Image.Image,
    *,
    base: str = "left",
    alpha: float = 0.55,
) -> Optional[bytes]:
    """Return a PNG (bytes) of a heatmap overlay showing pixel-level differences.

    Uses abs-diff + colormap, blended onto the chosen base image.
    """

    if cv2 is None:
        return None

    left_bgr = _bgr(left_img, size=640)
    right_bgr = _bgr(right_img, size=640)
    left_bgr, right_bgr = _safe_resize_to_match(left_bgr, right_bgr, max_side=640)

    diff = cv2.absdiff(left_bgr, right_bgr)
    diff_gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
                                                
    p95 = float(np.percentile(diff_gray, 95))
    scale = 255.0 / max(1.0, p95)
    diff_norm = np.clip(diff_gray.astype(np.float32) * scale, 0, 255).astype(np.uint8)

    heat = cv2.applyColorMap(diff_norm, cv2.COLORMAP_JET)

    base_img_bgr = left_bgr if base == "left" else right_bgr
    blended = cv2.addWeighted(base_img_bgr, 1.0, heat, float(alpha), 0)

    ok, buf = cv2.imencode(".png", blended)
    if not ok:
        return None
    return buf.tobytes()


def png_bytes_to_data_url(png: Optional[bytes]) -> Optional[str]:
    if not png:
        return None
    b64 = base64.b64encode(png).decode("ascii")
    return f"data:image/png;base64,{b64}"


def render_defects_overlay_png(
    base_img: Image.Image,
    detections: list[dict],
    *,
    box_width: int = 3,
) -> Optional[bytes]:
    """Render YOLO-style detections as a transparent overlay PNG.

    Expects detections with keys: class, confidence, bbox=[x1,y1,x2,y2].
    """

    if not detections:
        return None

    w, h = base_img.size
    if w <= 0 or h <= 0:
        return None

    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

                                               
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

                                                                           
    stroke = (198, 26, 65, 220)              
    fill = (198, 26, 65, 50)
    label_bg = (26, 16, 32, 200)
    label_fg = (255, 255, 255, 255)

    for d in detections:
        bbox = d.get("bbox") or []
        if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
            continue
        try:
            x1, y1, x2, y2 = [float(x) for x in bbox]
        except Exception:
            continue

               
        x1 = max(0.0, min(float(w - 1), x1))
        x2 = max(0.0, min(float(w - 1), x2))
        y1 = max(0.0, min(float(h - 1), y1))
        y2 = max(0.0, min(float(h - 1), y2))
        if x2 <= x1 or y2 <= y1:
            continue

                   
        draw.rectangle([x1, y1, x2, y2], outline=stroke, width=box_width, fill=fill)

        cls = str(d.get("class") or "defect")
        conf = d.get("confidence")
        try:
            conf_s = f"{float(conf):.2f}" if conf is not None else ""
        except Exception:
            conf_s = ""
        label = cls + (f" {conf_s}" if conf_s else "")

                          
        if font is not None:
            tw, th = draw.textbbox((0, 0), label, font=font)[2:]
        else:
            tw, th = (len(label) * 6, 11)
        pad_x, pad_y = 4, 2
        lx1 = x1
        ly1 = max(0.0, y1 - (th + pad_y * 2 + 2))
        lx2 = min(float(w), lx1 + tw + pad_x * 2)
        ly2 = ly1 + th + pad_y * 2
        draw.rectangle([lx1, ly1, lx2, ly2], fill=label_bg)
        draw.text((lx1 + pad_x, ly1 + pad_y), label, fill=label_fg, font=font)

    out = None
    try:
        from io import BytesIO

        buf = BytesIO()
        overlay.save(buf, format="PNG")
        out = buf.getvalue()
    except Exception:
        out = None
    return out


def build_pricing_summary(*, bu_code: Optional[str], pricing: Optional[dict]) -> str:
    if not bu_code:
        return "Set a BU code to see region-aware pricing."
    if not pricing or pricing.get("bu_band") is None:
        return f"No region pricing rule found for BU {bu_code}."

    band = pricing.get("bu_band")
    asp = pricing.get("bu_asp")
    in_region = bool(pricing.get("in_region"))

    bits = [f"Band {band}"]
    if asp is not None:
        bits.append(f"ASP {asp:.2f}")
    if not in_region:
        bits.append("default/out-of-region")

    return " • ".join(bits)


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default
