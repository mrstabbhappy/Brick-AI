                                  
                                                                    

                                                                                           
import os
import uuid
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import numpy as np
from PIL import Image
from pymongo import MongoClient
from pymongo.collection import Collection
import cv2

                                                                      
WEIGHT_VISUAL = 0.10                                       
WEIGHT_COLOUR = 0.50                                                  
WEIGHT_TEXTURE = 0.40                  

SUPPORTED_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp")


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")


                                                             
def _as_np(x, dtype=np.float32) -> Optional[np.ndarray]:
    if x is None:
        return None
    arr = np.asarray(x, dtype=dtype)
    if arr.ndim == 0:
        return None
    return arr

def _l2_normalize(x: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(x) + 1e-8
    return (x / n).astype(np.float32)

def _cos_sim_shift01(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity mapped from [-1,1] to [0,1]. Assumes same length."""
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-8
    val = float(a.dot(b) / denom)          
    return float(np.clip((val + 1.0) * 0.5, 0.0, 1.0))

def _cos_sim_clip01(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity clipped to [0,1] (use for non-negative histograms)."""
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-8
    val = float(a.dot(b) / denom)
    return float(np.clip(val, 0.0, 1.0))

def _cos_sim_shift01_lenient(a: np.ndarray, b: np.ndarray) -> float:
    """
    Cosine similarity mapped from [-1,1] to [0,1], tolerating length mismatches
    (truncates to min length). Prevents crashes on legacy docs.
    """
    if a is None or b is None:
        return 0.0
    la, lb = a.shape[0], b.shape[0]
    if la == 0 or lb == 0:
        return 0.0
    m = min(la, lb)
    a2 = a[:m]
    b2 = b[:m]
    return _cos_sim_shift01(a2, b2)

def _cos_sim_clip01_lenient(a: np.ndarray, b: np.ndarray) -> float:
    """
    Cosine similarity clipped to [0,1], tolerating length mismatches.
    Intended for non-negative histogram-like vectors.
    """
    if a is None or b is None:
        return 0.0
    la, lb = a.shape[0], b.shape[0]
    if la == 0 or lb == 0:
        return 0.0
    m = min(la, lb)
    a2 = a[:m]
    b2 = b[:m]
    return _cos_sim_clip01(a2, b2)


                                                          
@dataclass
class BrickMatch:
    _id: str
    overall_similarity: float
    visual_similarity: float
    colour_similarity: float
    texture_similarity: float
    image_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


                                                        
class BrickImageAnalyzer:
    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        db_name: str = "brickdb",
        bricks_collection_name: Optional[str] = None,
        features_collection_name: Optional[str] = None,
    ):
                           
        mongo_uri = (
            mongo_uri
            or os.getenv("COSMOS_MONGODB_URI")
            or os.getenv("MONGODB_URI")
            or os.getenv("MONGO_URI")
        )
        if not mongo_uri:
            raise RuntimeError("COSMOS_MONGODB_URI (or MONGODB_URI / MONGO_URI) is required (brick_analyzer)")

        self.client = MongoClient(
            mongo_uri,
            connectTimeoutMS=10000,
            serverSelectionTimeoutMS=10000
        )
        self.db = self.client[db_name]
        self.bricks_collection: Collection = self.db[
            bricks_collection_name or os.getenv("MONGODB_COLLECTION", "bricks")
        ]
        self.brick_features_collection: Collection = self.db[
            features_collection_name or os.getenv("MONGODB_FEATURES_COLLECTION", "brick_features")
        ]

        self.device = "cpu"                                           

                                                                                  
        self._torch = None
        self._models = None
        self._transforms = None
        self.model = None
        self.preprocess = None

                                              
        self._dinov2_model = None
        self._dinov2_preprocess = None
        self._dinov2_name = os.getenv("DINOV2_MODEL", "dinov2_vits14").strip() or "dinov2_vits14"

    def get_database_stats(self) -> dict:
        try:
            return {
                "db": getattr(self.db, "name", None),
                "bricks_collection": getattr(self.bricks_collection, "name", None),
                "brick_features_collection": getattr(self.brick_features_collection, "name", None),
                "bricks_docs": self.bricks_collection.count_documents({}),
                "brick_features_docs": self.brick_features_collection.count_documents({}),
            }
        except Exception as e:
            return {"error": str(e)}

    def ensure_indexes(self) -> None:
        """Best-effort index creation (Cosmos Mongo API may restrict options)."""
        try:
            self.bricks_collection.create_index("brick_id", unique=True)
        except Exception:
            pass
        try:
            self.bricks_collection.create_index("image_path")
        except Exception:
            pass
        for f in ("dataset", "manufacturer", "region_id"):
            try:
                self.bricks_collection.create_index(f)
            except Exception:
                pass
        try:
                                                             
            self.brick_features_collection.create_index("_id", unique=True)
        except Exception:
            pass
        try:
            self.brick_features_collection.create_index("dataset")
        except Exception:
            pass

    def _normalize_len(self, vec: np.ndarray, length: int) -> np.ndarray:
        vec = np.asarray(vec, dtype=np.float32).ravel()
        if vec.shape[0] == length:
            return vec
        if vec.shape[0] > length:
            return vec[:length]
        out = np.zeros((length,), dtype=np.float32)
        out[: vec.shape[0]] = vec
        return out

                                                                              
    def _ensure_torch(self) -> None:
        if self._torch is not None:
            return
        try:
            import torch                
            from torchvision import models, transforms                

            self._torch = torch
            self._models = models
            self._transforms = transforms
        except Exception as e:
            raise RuntimeError(
                "Deep feature extractor unavailable (torch/torchvision import failed). "
                "Install torch+torchvision or set up a compatible environment. "
                f"Details: {e}"
            )

    def _init_feature_extractor(self):
        self._ensure_torch()
        torch = self._torch
        models = self._models
        transforms = self._transforms

        try:
            weights = models.ResNet152_Weights.DEFAULT                       
            model = models.resnet152(weights=weights)
        except Exception:
            model = models.resnet152(pretrained=True)                                    
        model.fc = torch.nn.Identity()
        model.eval()
                     
        self.model = model

        self.preprocess = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),         
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225]),
        ])

                                                                     
    def _ensure_dinov2(self) -> None:
        """Lazy-load DINOv2 via torch hub. Opt-in via ENABLE_DINOV2=1."""
        if self._dinov2_model is not None and self._dinov2_preprocess is not None:
            return
        self._ensure_torch()
        torch = self._torch
        transforms = self._transforms

                                                                    
        try:
            model = torch.hub.load("facebookresearch/dinov2", self._dinov2_name)
            model.eval()
            self._dinov2_model = model
        except Exception as e:
            raise RuntimeError(
                "DINOv2 model load failed. Internet access may be required on first run (or use a cached torch hub). "
                f"DINOV2_MODEL={self._dinov2_name}. Details: {e}"
            )

                                                                                                
        self._dinov2_preprocess = transforms.Compose(
            [
                transforms.Resize(518),
                transforms.CenterCrop(518),
                transforms.ToTensor(),
                transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
            ]
        )

    def _prep_face_rgb_for_embedding(self, pil_img: Image.Image) -> Image.Image:
        """Fill mortar regions and return a face-focused RGB image for embedding."""
        rgb, gray = self._prep_rgb(pil_img)
        face = self._brick_face_mask_from_gray(gray)
        out = rgb.copy()
        if face.any():
            fill = np.median(out[face], axis=0)
        else:
            fill = np.array([127, 127, 127], dtype=np.float32)
        out[~face] = fill.astype(np.uint8)
        return Image.fromarray(out, mode="RGB")

    def embed_dinov2(self, img: Image.Image) -> np.ndarray:
        self._ensure_dinov2()
        torch = self._torch

        x_img = self._prep_face_rgb_for_embedding(img)
        t = self._dinov2_preprocess(x_img).unsqueeze(0)

        with torch.no_grad():
            out = self._dinov2_model(t)

                                     
        if isinstance(out, (list, tuple)) and len(out) > 0:
            out = out[0]
        if isinstance(out, dict):
            out = out.get("x_norm_clstoken") or out.get("last_hidden_state") or next(iter(out.values()))
        if hasattr(out, "detach"):
            out = out.detach()
        vec = out.squeeze(0).cpu().numpy().astype(np.float32).ravel()
        return _l2_normalize(vec)

    def embed_deep(self, img: Image.Image) -> np.ndarray:
        if self.model is None or self.preprocess is None:
            self._init_feature_extractor()
        torch = self._torch

        with torch.no_grad():
            t = self.preprocess(img).unsqueeze(0)                 
            feat = self.model(t).squeeze(0).cpu().numpy().astype(np.float32)
        return _l2_normalize(feat)

                                                                                 
    def _crop_nonblack(self, rgb: np.ndarray) -> np.ndarray:
        if rgb.ndim != 3:
            return rgb
        mask = (rgb > 8).any(axis=2)
        if not mask.any():
            return rgb
        ys, xs = np.where(mask)
        return rgb[ys.min():ys.max()+1, xs.min():xs.max()+1]

    def _prep_rgb(self, pil_img: Image.Image, target_w: int = 512) -> Tuple[np.ndarray, np.ndarray]:
        rgb = np.array(pil_img.convert("RGB"))
        rgb = self._crop_nonblack(rgb)
        h, w = rgb.shape[:2]
        if w > target_w:
            s = target_w / float(w)
            rgb = cv2.resize(rgb, (target_w, int(h*s)), interpolation=cv2.INTER_AREA)
        gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY).astype(np.float32)/255.0
        return rgb, gray

    def _brick_face_mask_from_gray(self, gray: np.ndarray) -> np.ndarray:
        thr = cv2.adaptiveThreshold(
            (gray*255).astype(np.uint8), 255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 5
        )
        mortar = (thr == 0)
        mortar = cv2.dilate(mortar.astype(np.uint8), np.ones((3,3), np.uint8), 1).astype(bool)
        face = ~mortar
        if face.sum() < 50:
            face[:] = True
        return face

    def _gray_world(self, rgb: np.ndarray) -> np.ndarray:
        rgb_f = rgb.astype(np.float32) + 1e-6
        means = rgb_f.reshape(-1, 3).mean(axis=0)
        gain = means.mean() / means
        return np.clip(rgb_f * gain, 0, 255).astype(np.uint8)

    def _ab_hist_descriptor(self, lab: np.ndarray, face_mask: np.ndarray, bins: int = 32) -> np.ndarray:
        a = lab[:, :, 1].astype(np.float32) - 128.0
        b = lab[:, :, 2].astype(np.float32) - 128.0
        a = a[face_mask]; b = b[face_mask]
        if a.size == 0:
            return np.zeros((bins*bins,), dtype=np.float32)
        hist, _, _ = np.histogram2d(a, b, bins=bins,
                                    range=[[-128.0,127.0],[-128.0,127.0]], density=False)
        hist = hist.astype(np.float32)
        hist /= (hist.sum() + 1e-8)
        return hist.ravel()

    def _lab_means(self, lab: np.ndarray, face_mask: np.ndarray) -> Tuple[float,float,float]:
        L = lab[:, :, 0].astype(np.float32)*(100.0/255.0)
        a = lab[:, :, 1].astype(np.float32)-128.0
        b = lab[:, :, 2].astype(np.float32)-128.0
        L = L[face_mask]; a = a[face_mask]; b = b[face_mask]
        if L.size == 0:
            return 0.0, 0.0, 0.0
        return float(L.mean()), float(a.mean()), float(b.mean())

    def _ab_kmeans(self, lab: np.ndarray, face_mask: np.ndarray, k: int = 3) -> np.ndarray:
        ab = np.stack([lab[:, :, 1].astype(np.float32)-128.0,
                       lab[:, :, 2].astype(np.float32)-128.0], axis=2)
        ab = ab[face_mask]
        if ab.size == 0:
            return np.zeros((1,2), dtype=np.float32)
        if ab.shape[0] < max(10, k):
            return ab.mean(axis=0, keepdims=True).astype(np.float32)
        Z = ab.astype(np.float32)
        criteria = (cv2.TERM_CRITERIA_EPS+cv2.TERM_CRITERIA_MAX_ITER, 50, 0.1)
        _ret, _labels, centers = cv2.kmeans(Z, k, None, criteria, 3, cv2.KMEANS_PP_CENTERS)
        centers = np.array(sorted(centers, key=lambda v: np.arctan2(float(v[1]), float(v[0]))), dtype=np.float32)
        return centers

    def _ciede2000(self, L1, a1, b1, L2, a2, b2) -> float:
        L1, a1, b1, L2, a2, b2 = float(L1), float(a1), float(b1), float(L2), float(a2), float(b2)
        kL = kC = kH = 1.0
        C1 = np.hypot(a1, b1); C2 = np.hypot(a2, b2)
        Lm = 0.5*(L1+L2); Cm = 0.5*(C1+C2)
        G = 0.5*(1.0 - (Cm**7/(Cm**7+25.0**7))**0.5) if Cm>0 else 0.0
        a1p=(1.0+G)*a1; a2p=(1.0+G)*a2
        C1p=np.hypot(a1p,b1); C2p=np.hypot(a2p,b2)
        def _hp(a,b):
            ang=np.degrees(np.arctan2(b,a)); return ang+360.0 if ang<0 else ang
        h1p=0.0 if C1p==0 else _hp(a1p,b1); h2p=0.0 if C2p==0 else _hp(a2p,b2)
        dLp=L2-L1; dCp=C2p-C1p
        if C1p*C2p==0: dhp=0.0
        else:
            dh=h2p-h1p
            if dh>180.0: dh-=360.0
            if dh<-180.0: dh+=360.0
            dhp=2.0*(C1p*C2p)**0.5*np.sin(np.radians(dh)*0.5)
        Lpm=Lm; Cpm=0.5*(C1p+C2p)
        if C1p*C2p==0: hpm=h1p+h2p
        else:
            hd=abs(h1p-h2p)
            if hd>180.0:
                hpm=(h1p+h2p+360.0)*0.5 if (h1p+h2p)<360.0 else (h1p+h2p-360.0)*0.5
            else:
                hpm=(h1p+h2p)*0.5
        T=(1 - 0.17*np.cos(np.radians(hpm-30))
             + 0.24*np.cos(np.radians(2*hpm))
             + 0.32*np.cos(np.radians(3*hpm+6))
             - 0.20*np.cos(np.radians(4*hpm-63)))
        dRo=30.0*np.exp(-((hpm-275.0)/25.0)**2)
        Rc=2.0*(Cpm**7/(Cpm**7+25.0**7))**0.5
        Sl=1.0 + (0.015*(Lpm-50.0)**2)/((20.0+(Lpm-50.0)**2)**0.5)
        Sc=1.0 + 0.045*Cpm
        Sh=1.0 + 0.015*Cpm*T
        Rt=-np.sin(np.radians(2.0*dRo))*Rc
        dE=((dLp/(kL*Sl))**2 + (dCp/(kC*Sc))**2 + (dhp/(kH*Sh))**2 + Rt*(dCp/(kC*Sc))*(dhp/(kH*Sh)))
        return float(np.sqrt(max(dE, 0.0)))

    def color_similarity_ab(self, img_a: Image.Image, img_b: Image.Image) -> float:
           
        rgbA, grayA = self._prep_rgb(img_a); faceA = self._brick_face_mask_from_gray(grayA)
        labA = cv2.cvtColor(self._gray_world(rgbA), cv2.COLOR_RGB2LAB)
        descA = self._ab_hist_descriptor(labA, faceA, bins=32)
        LA, aA, bA = self._lab_means(labA, faceA); centA = self._ab_kmeans(labA, faceA, k=3)
           
        rgbB, grayB = self._prep_rgb(img_b); faceB = self._brick_face_mask_from_gray(grayB)
        labB = cv2.cvtColor(self._gray_world(rgbB), cv2.COLOR_RGB2LAB)
        descB = self._ab_hist_descriptor(labB, faceB, bins=32)
        LB, aB, bB = self._lab_means(labB, faceB); centB = self._ab_kmeans(labB, faceB, k=3)

                                         
        descA = _l2_normalize(descA); descB = _l2_normalize(descB)
        bc = float(np.sum(np.sqrt(np.clip(descA,0,1) * np.clip(descB,0,1))))
        bc = float(np.clip(bc, 0.0, 1.0))

                        
        de00 = self._ciede2000(LA,aA,bA, LB,aB,bB)
        dL = abs(LA-LB)

                                 
        if centA.size == 0 or centB.size == 0:
            d_clusters = 0.0
        else:
            dists = np.linalg.norm(centA[:,None,:] - centB[None,:,:], axis=2)
            dA = dists.min(axis=1).mean(); dB = dists.min(axis=0).mean()
            d_clusters = float(0.5*(dA+dB))

        D = 0.6*de00 + 0.3*d_clusters + 0.1*dL
        sim_core = float(np.exp(- (D/12.0)**2))
        return float(np.clip(sim_core * (bc**0.5), 0.0, 1.0))

                                                                            
    def _prep_gray(self, pil_img: Image.Image, target_w: int = 512) -> np.ndarray:
        rgb = np.array(pil_img.convert("RGB"))
        rgb = self._crop_nonblack(rgb)
        h, w = rgb.shape[:2]
        if w > target_w:
            s = target_w / float(w)
            rgb = cv2.resize(rgb, (target_w, int(h*s)), interpolation=cv2.INTER_AREA)
        gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY).astype(np.float32)/255.0
        return gray

    def _mask_mortar(self, gray: np.ndarray) -> np.ndarray:
        thr = cv2.adaptiveThreshold(
            (gray*255).astype(np.uint8), 255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 5
        )
        mortar = (thr == 0)
        mortar = cv2.dilate(mortar.astype(np.uint8), np.ones((3,3), np.uint8), 1).astype(bool)
        if mortar.any():
            g = gray.copy()
            fill = float(np.median(gray[~mortar])) if (~mortar).any() else 0.5
            g[mortar] = fill
            return g
        return gray

    def _orientation_hist_grid(self, gray: np.ndarray, tiles=(3,6), bins: int = 8) -> np.ndarray:
        gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
        gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
        mag = np.sqrt(gx*gx + gy*gy) + 1e-8
        ang = (np.arctan2(gy, gx) + np.pi)           
        H, W = gray.shape
        th, tw = max(8, H//tiles[0]), max(8, W//tiles[1])
        feats = []
        for i in range(tiles[0]):
            for j in range(tiles[1]):
                y0, y1 = i*th, min((i+1)*th, H)
                x0, x1 = j*tw, min((j+1)*tw, W)
                patch_mag = mag[y0:y1, x0:x1].ravel()
                patch_ang = ang[y0:y1, x0:x1].ravel()
                hist, _ = np.histogram(patch_ang, bins=bins, range=(0, 2*np.pi), weights=patch_mag)
                hist = hist.astype(np.float32)
                hist /= (np.linalg.norm(hist) + 1e-8)
                feats.append(hist)
        f = np.concatenate(feats).astype(np.float32)
        return _l2_normalize(f)

    def _orb_match_ratio(self, gray_a: np.ndarray, gray_b: np.ndarray) -> float:
        a8 = (np.clip(gray_a, 0, 1)*255).astype(np.uint8)
        b8 = (np.clip(gray_b, 0, 1)*255).astype(np.uint8)
        orb = cv2.ORB_create(nfeatures=800, scaleFactor=1.2, nlevels=8)
        kpa, desa = orb.detectAndCompute(a8, None)
        kpb, desb = orb.detectAndCompute(b8, None)
        if desa is None or desb is None or len(desa) == 0 or len(desb) == 0:
            return 0.0
        bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
        matches = bf.match(desa, desb)
        if not matches:
            return 0.0
        denom = float(min(len(desa), len(desb)))
        return float(np.clip(len(matches)/denom, 0.0, 1.0))

    def visual_similarity_strict(self, img_a: Image.Image, img_b: Image.Image) -> float:
        ga = self._mask_mortar(self._prep_gray(img_a))
        gb = self._mask_mortar(self._prep_gray(img_b))
        fa = self._orientation_hist_grid(ga)
        fb = self._orientation_hist_grid(gb)
        vis_cos = float(np.clip(np.dot(fa, fb), 0.0, 1.0))
        orb_ratio = self._orb_match_ratio(ga, gb)
        return float(np.clip(0.65*vis_cos + 0.35*orb_ratio, 0.0, 1.0))

                                                                         
    def _lbp_hist(self, gray: np.ndarray, radius: int = 1, points: int = 8) -> np.ndarray:
        points = int(points); points = min(points, 8)
        H, W = gray.shape
        lbp = np.zeros((H, W), dtype=np.uint8)
        angles = np.linspace(0, 2*np.pi, points, endpoint=False)
        offsets = np.stack([np.round(radius*np.sin(angles)).astype(int),
                            np.round(radius*np.cos(angles)).astype(int)], axis=1)
        g8 = (np.clip(gray, 0, 1)*255).astype(np.uint8)
        for i in range(radius, H-radius):
            row = g8[i]
            for j in range(radius, W-radius):
                c = row[j]
                code = 0
                for k, (dy, dx) in enumerate(offsets):
                    code |= (1 if g8[i+dy, j+dx] >= c else 0) << k
                lbp[i, j] = code
        hist, _ = np.histogram(lbp, bins=256, range=(0, 256))
        hist = hist.astype(np.float32)
        hist /= (hist.sum() + 1e-8)
        return hist

    def _lap_hist(self, gray: np.ndarray, bins: int = 24) -> np.ndarray:
        g = cv2.GaussianBlur(gray, (3,3), 0.8)
        lap = cv2.Laplacian(g, cv2.CV_32F, ksize=3)
        mag = np.abs(lap)
        hi = float(np.percentile(mag, 99.5))
        hist, _ = np.histogram(mag, bins=bins, range=(0.0, hi + 1e-6))
        hist = hist.astype(np.float32)
        hist /= (hist.sum() + 1e-8)
        return hist

    def _blackhat_stats(self, gray: np.ndarray):
        u8 = (np.clip(gray, 0, 1)*255).astype(np.uint8)
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (9,9))
        bh = cv2.morphologyEx(u8, cv2.MORPH_BLACKHAT, kernel).astype(np.float32) / 255.0
        thr = float(np.percentile(bh, 85.0))
        mask = bh > thr
        density = float(mask.mean()) if bh.size else 0.0
        depth_p95 = float(np.percentile(bh, 95.0)) if bh.size else 0.0
        hist, _ = np.histogram(bh, bins=16, range=(0.0, float(bh.max()) + 1e-6))
        hist = hist.astype(np.float32); hist /= (hist.sum() + 1e-8)
        return hist, density, depth_p95

    def _dog_energy(self, gray: np.ndarray):
        g1 = cv2.GaussianBlur(gray, (0,0), 0.8)
        g2 = cv2.GaussianBlur(gray, (0,0), 2.0)
        dog = cv2.absdiff(g1, g2)
        m = float(np.percentile(dog, 99.5)) + 1e-6
        dogn = (dog / m).astype(np.float32)
        return float(dogn.mean()), float(np.percentile(dogn, 95.0))

    def _texture_vec_and_stats(self, image: Image.Image):
        gray = self._mask_mortar(self._prep_gray(image))
        f_lbp1 = self._lbp_hist(gray, radius=1, points=8)
        f_lbp2 = self._lbp_hist(gray, radius=2, points=8)
        f_lap  = self._lap_hist(gray, bins=24)
        f_bh_hist, bh_density, bh_p95 = self._blackhat_stats(gray)
        dog_mean, dog_p95 = self._dog_energy(gray)
        feat = np.concatenate([f_lbp1, f_lbp2, f_lap, f_bh_hist,
                               np.array([bh_density, bh_p95, dog_mean, dog_p95], dtype=np.float32)])
        feat = _l2_normalize(feat.astype(np.float32))
        stats = {"bh_density": bh_density, "bh_p95": bh_p95, "dog_p95": dog_p95}
        return feat, stats

    def texture_similarity_relief(self, img_a: Image.Image, img_b: Image.Image) -> float:
        fa, sa = self._texture_vec_and_stats(img_a)
        fb, sb = self._texture_vec_and_stats(img_b)
        base = float(np.clip(np.dot(fa, fb), 0.0, 1.0))
        d_density = abs(sa["bh_density"] - sb["bh_density"])
        d_depth   = abs(sa["bh_p95"]     - sb["bh_p95"])
        d_dog     = abs(sa["dog_p95"]    - sb["dog_p95"])
        penalty = np.exp(-(3.0*d_density + 2.0*d_depth + 1.0*d_dog))
        return float(np.clip(base * penalty, 0.0, 1.0))

    def discriminative_similarity(self, img_a: Image.Image, img_b: Image.Image) -> float:
        """
        Compare defects/patterns between two bricks.
        Detects cracks, indents, blemishes, spots, and weathering marks.

        Uses YOLOv8 defect detection if available, falls back to traditional image processing.

        Returns:
            Similarity score (0-1) where 1.0 = similar defect patterns
        """
        try:
                                                               
            try:
                from lib.yolo_defect_detector import get_defect_detector
                detector = get_defect_detector()

                if detector.is_available():
                                                      
                    similarity = detector.compare_defects(img_a, img_b)
                    import logging
                    logging.getLogger(__name__).debug(f"YOLOv8 defect similarity: {similarity:.3f}")
                    return float(np.clip(similarity, 0.0, 1.0))
            except Exception as yolo_error:
                import logging
                logging.getLogger(__name__).debug(f"YOLOv8 not available: {yolo_error}, using fallback")

                                                      
            from lib.discriminative_features import (
                compute_discriminative_features,
                compare_discriminative_features
            )

                                              
            features_a = compute_discriminative_features(img_a)
            features_b = compute_discriminative_features(img_b)

                              
            similarity = compare_discriminative_features(features_a, features_b)

            return float(np.clip(similarity, 0.0, 1.0))

        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Discriminative similarity failed: {e}")
                                                                        
            return 0.5

                                                                          
    def process_and_store_brick(
        self,
        img: Image.Image,
        relpath: str,
        metadata: Dict[str, Any],
        dataset: str = "catalog",
    ) -> str:
        """
        Ingest one brick image:
          - Deep descriptor via ResNet-152 (used for search)
          - Cached colour (full-image 16x16 ab hist) + cached LBP (256) for fast search
          - Strict pairwise compare later recomputes masked colour/texture
          - Stores doc in features collection under a stable _id from relpath
        """
                                                                          
        deep = self._normalize_len(self.embed_deep(img), 1536)
        deep = _l2_normalize(deep)

                                                                                             
        rgb = np.array(img.convert("RGB"))
        rgb_balanced = self._gray_world(rgb)
        lab = cv2.cvtColor(rgb_balanced, cv2.COLOR_RGB2LAB)
        H, W = rgb.shape[:2]                                              
        full_face_mask = np.ones((H, W), dtype=bool)
        colour = self._ab_hist_descriptor(lab, full_face_mask, bins=16)
        colour = _l2_normalize(colour)

                                                                                  
        gray = self._prep_gray(img)
        texture = self._lbp_hist(gray, radius=1, points=8)
        texture = _l2_normalize(texture)

                                       
        strict_texture_vector, strict_texture_stats = self._texture_vec_and_stats(img)

                                   
        rgb_s, gray_s = self._prep_rgb(img)
        face_s = self._brick_face_mask_from_gray(gray_s)
        lab_s = cv2.cvtColor(self._gray_world(rgb_s), cv2.COLOR_RGB2LAB)
        Lm, am, bm = self._lab_means(lab_s, face_s)
        strict_colour_lab = {
            "L_mean": float(Lm),
            "a_mean": float(am),
            "b_mean": float(bm),
        }

                                                                  
        dinov2_embedding = None
        dinov2_model_name = None
        if _env_flag("ENABLE_DINOV2", "0"):
            try:
                dinov2_embedding = self.embed_dinov2(img)
                dinov2_model_name = self._dinov2_name
            except Exception:
                dinov2_embedding = None
                dinov2_model_name = None

                         
        basis = relpath or uuid.uuid4().hex
        brick_id = hashlib.sha256(basis.encode("utf-8")).hexdigest()

                                                               
        brick_doc = {
            "_id": brick_id,
            "brick_id": brick_id,
            "dataset": dataset,
            "image_path": relpath,
            "manufacturer": (metadata or {}).get("manufacturer") or (metadata or {}).get("brand"),
            "region_id": (metadata or {}).get("region_id") or (metadata or {}).get("factory_region"),
            "metadata": metadata or {},
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.bricks_collection.replace_one({"brick_id": brick_id}, brick_doc, upsert=True)

                                                                               
        features_doc = {
            "_id": brick_id,
            "brick_id": brick_id,
            "dataset": dataset,
            "deep_features": deep.tolist(),
            "color_features": colour.tolist(),
            "texture_features": texture.tolist(),
            "strict_texture_vector": strict_texture_vector.tolist(),
            "strict_texture_stats": strict_texture_stats,
            "strict_colour_lab": strict_colour_lab,
            "dinov2_embedding": dinov2_embedding.tolist() if isinstance(dinov2_embedding, np.ndarray) else None,
            "dinov2_model": dinov2_model_name,
            "feature_schema_version": 1,
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.brick_features_collection.replace_one({"_id": brick_id}, features_doc, upsert=True)
        return brick_id

                                                                         
    def search_similar_bricks(
        self,
        query_image: Image.Image,
        top_k: int = 10,
        dataset: str = "catalog",
    ) -> List[BrickMatch]:
        """
        Fast kNN over stored features (deep + cached colour/texture).
        Compare view should do strict colour/texture recompute for the chosen pair.
        """
        top_k = max(1, min(int(top_k or 10), 50))
        q_deep = self._normalize_len(self.embed_deep(query_image), 1536)
        q_deep = _l2_normalize(q_deep)

                                                                           
                                                                        
        rgb_s, gray_s = self._prep_rgb(query_image)
        face_s = self._brick_face_mask_from_gray(gray_s)
        lab_s = cv2.cvtColor(self._gray_world(rgb_s), cv2.COLOR_RGB2LAB)
        qLm, qam, qbm = self._lab_means(lab_s, face_s)

                                                                                         
        q_strict_tex, q_strict_stats = self._texture_vec_and_stats(query_image)
        q_strict_tex = _l2_normalize(q_strict_tex)

                                                                      
        q_dino = None
        use_dino = _env_flag("ENABLE_DINOV2", "0")
        if use_dino:
            try:
                q_dino = self.embed_dinov2(query_image)
            except Exception:
                q_dino = None
                use_dino = False

        results: List[BrickMatch] = []

        ds_filter: Dict[str, Any] = {"$or": [{"dataset": dataset}, {"dataset": {"$exists": False}}]}

        cursor = self.brick_features_collection.find(ds_filter, {
            "_id": 1,
            "brick_id": 1,
            "deep_features": 1,
            "strict_texture_vector": 1,
            "strict_texture_stats": 1,
            "strict_colour_lab": 1,
            "dinov2_embedding": 1,
            "dinov2_model": 1,
        })

        for doc in cursor:
            _id = str(doc.get("_id"))
            d = _as_np(doc.get("deep_features"))
            st = _as_np(doc.get("strict_texture_vector"))
            if d is None or st is None:
                continue
            d = _l2_normalize(d)
            st = _l2_normalize(st)

                                                                                         
            v_sim = _cos_sim_clip01_lenient(q_deep, d)

                                                             
            c_sim = 0.0
            sc = doc.get("strict_colour_lab") or {}
            try:
                Lm = float(sc.get("L_mean"))
                am = float(sc.get("a_mean"))
                bm = float(sc.get("b_mean"))
                dE = float(np.sqrt((qLm - Lm) ** 2 + (qam - am) ** 2 + (qbm - bm) ** 2))
                c_sim = float(np.exp(- (dE / 12.0) ** 2))
            except Exception:
                c_sim = 0.0

                                                                                                                   
            t_sim = 0.0
            used_dino_for_row = False
            if use_dino and q_dino is not None:
                cand_dino = _as_np(doc.get("dinov2_embedding"))
                if cand_dino is not None and cand_dino.size > 0:
                    cand_dino = _l2_normalize(cand_dino)
                    t_sim = _cos_sim_clip01_lenient(q_dino, cand_dino)
                    used_dino_for_row = True

            if not used_dino_for_row:
                base_tex = _cos_sim_clip01_lenient(q_strict_tex, st)
                penalty = 1.0
                try:
                    ss = doc.get("strict_texture_stats") or {}
                    d_density = abs(float(q_strict_stats.get("bh_density", 0.0)) - float(ss.get("bh_density", 0.0)))
                    d_depth = abs(float(q_strict_stats.get("bh_p95", 0.0)) - float(ss.get("bh_p95", 0.0)))
                    d_dog = abs(float(q_strict_stats.get("dog_p95", 0.0)) - float(ss.get("dog_p95", 0.0)))
                    penalty = float(np.exp(-(3.0 * d_density + 2.0 * d_depth + 1.0 * d_dog)))
                except Exception:
                    penalty = 1.0
                t_sim = float(np.clip(base_tex * penalty, 0.0, 1.0))

            overall = WEIGHT_VISUAL*v_sim + WEIGHT_COLOUR*c_sim + WEIGHT_TEXTURE*t_sim

            results.append(BrickMatch(
                _id=_id,
                overall_similarity=overall,
                visual_similarity=v_sim,
                colour_similarity=c_sim,
                texture_similarity=t_sim,
                image_path=None,
                metadata=None,
            ))

        results.sort(key=lambda r: r.overall_similarity, reverse=True)
        top = results[:top_k]

                                                              
        ids = [r._id for r in top]
        bricks = list(self.bricks_collection.find({"brick_id": {"$in": ids}}, {"brick_id": 1, "image_path": 1, "metadata": 1}))
        by_id = {str(b.get("brick_id")): b for b in bricks}
        for r in top:
            b = by_id.get(r._id)
            if b:
                r.image_path = b.get("image_path")
                r.metadata = b.get("metadata") or {}
        return top