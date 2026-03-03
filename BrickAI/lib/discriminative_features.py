                                    

import logging
from typing import Dict, Tuple

import cv2
import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)


def compute_color_variance(img: Image.Image) -> Tuple[float, float, float]:
    arr = np.array(img.resize((256, 256)))
    r_std = np.std(arr[:, :, 0]) / 255.0
    g_std = np.std(arr[:, :, 1]) / 255.0
    b_std = np.std(arr[:, :, 2]) / 255.0
    return r_std, g_std, b_std


def compute_local_color_variance(img: Image.Image, tile_size: int = 32) -> float:
    arr = np.array(img.resize((256, 256)))
    h, w = arr.shape[:2]

    local_vars = []
    for i in range(0, h, tile_size):
        for j in range(0, w, tile_size):
            tile = arr[i : min(i + tile_size, h), j : min(j + tile_size, w)]
            if tile.size > 0:
                tile_var = np.var(tile) / (255.0**2)
                local_vars.append(tile_var)

    return float(np.mean(local_vars)) if local_vars else 0.0


def compute_spot_density(img: Image.Image) -> float:
    arr = np.array(img.resize((256, 256)))
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)

    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
    blackhat = cv2.morphologyEx(gray, cv2.MORPH_BLACKHAT, kernel)
    _, spots = cv2.threshold(blackhat, 15, 255, cv2.THRESH_BINARY)

    spot_density = np.sum(spots > 0) / spots.size
    return float(spot_density)


def compute_texture_complexity(img: Image.Image) -> float:
    arr = np.array(img.resize((256, 256)))
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)

    gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    magnitude = np.sqrt(gx**2 + gy**2)

    p95 = np.percentile(magnitude, 95.0)
    if p95 > 0:
        magnitude = magnitude / p95

    complexity = float(np.mean(magnitude))
    return min(complexity, 1.0)


def compute_discriminative_features(img: Image.Image) -> Dict[str, float]:
    try:
        r_var, g_var, b_var = compute_color_variance(img)
        local_var = compute_local_color_variance(img, tile_size=32)
        spot_dens = compute_spot_density(img)
        tex_complex = compute_texture_complexity(img)

        return {
            "color_variance_r": r_var,
            "color_variance_g": g_var,
            "color_variance_b": b_var,
            "avg_color_variance": (r_var + g_var + b_var) / 3.0,
            "local_variance": local_var,
            "spot_density": spot_dens,
            "texture_complexity": tex_complex,
        }
    except Exception as e:
        logger.error(f"Failed to compute discriminative features: {e}")
        return {
            "color_variance_r": 0.0,
            "color_variance_g": 0.0,
            "color_variance_b": 0.0,
            "avg_color_variance": 0.0,
            "local_variance": 0.0,
            "spot_density": 0.0,
            "texture_complexity": 0.0,
        }


def compare_discriminative_features(features_a: Dict[str, float], features_b: Dict[str, float]) -> float:
    var_diff_r = abs(features_a["color_variance_r"] - features_b["color_variance_r"])
    var_diff_g = abs(features_a["color_variance_g"] - features_b["color_variance_g"])
    var_diff_b = abs(features_a["color_variance_b"] - features_b["color_variance_b"])
    avg_var_diff = (var_diff_r + var_diff_g + var_diff_b) / 3.0

    local_var_diff = abs(features_a["local_variance"] - features_b["local_variance"])
    spot_diff = abs(features_a["spot_density"] - features_b["spot_density"])
    tex_diff = abs(features_a["texture_complexity"] - features_b["texture_complexity"])

    uniformity_sim = np.exp(-avg_var_diff * 5.0)
    mottling_sim = np.exp(-local_var_diff * 8.0)
    spot_sim = np.exp(-spot_diff * 6.0)
    texture_sim = np.exp(-tex_diff * 4.0)

    discriminative_similarity = uniformity_sim * 0.35 + mottling_sim * 0.30 + spot_sim * 0.20 + texture_sim * 0.15
    return float(np.clip(discriminative_similarity, 0.0, 1.0))
