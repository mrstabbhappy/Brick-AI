                                                                       

import logging
import os
from typing import Dict, List

import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

DEFECT_CLASSES = {
    0: "crack",
    1: "chip",
    2: "hole",
    3: "indent",
    4: "weathering",
    5: "blemish",
    6: "spot",
}

DEFECT_SEVERITY = {
    "crack": 1.0,
    "chip": 0.9,
    "hole": 0.85,
    "indent": 0.7,
    "weathering": 0.6,
    "blemish": 0.5,
    "spot": 0.4,
}


class YOLOv8DefectDetector:
    def __init__(self, model_path: str = "models/yolov8_brick_defects.pt", conf_threshold: float = 0.25):
                                                                      
        self.model_path = os.getenv("YOLO_DEFECT_MODEL_PATH") or model_path
        try:
            env_conf = os.getenv("YOLO_DEFECT_CONF_THRESHOLD")
            conf_threshold = float(env_conf) if env_conf is not None and str(env_conf).strip() else conf_threshold
        except Exception:
            pass
        self.conf_threshold = float(conf_threshold)
        self.model = None
        self.class_names: dict[int, str] = dict(DEFECT_CLASSES)
        self.available = False
        self.status_reason: str | None = None
        self._load_model()

    def _resolve_model_path(self) -> str | None:
        candidates: list[str] = []

        if self.model_path:
            candidates.append(self.model_path)

                                 
        candidates.append("models/yolov8_brick_defects.pt")

                                                                                           
        try:
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            candidates.append(os.path.join(repo_root, "models", "yolov8_brick_defects.pt"))
        except Exception:
            pass

                                                           
        try:
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            models_dir = os.path.join(repo_root, "models")
            if os.path.isdir(models_dir):
                pts = [os.path.join(models_dir, f) for f in os.listdir(models_dir) if f.lower().endswith(".pt")]
                if len(pts) == 1:
                    candidates.insert(0, pts[0])
        except Exception:
            pass

        for p in candidates:
            if not p:
                continue
                                    
            abs_p = p if os.path.isabs(p) else os.path.abspath(p)
            if os.path.exists(abs_p):
                return abs_p
        return None

    def _load_model(self) -> None:
        try:
            from ultralytics import YOLO

            resolved = self._resolve_model_path()
            if not resolved:
                self.status_reason = (
                    "Weights file not found. Place a .pt file at models/yolov8_brick_defects.pt "
                    "or set YOLO_DEFECT_MODEL_PATH."
                )
                logger.info(f"YOLO weights not found (expected {self.model_path})")
                self.available = False
                return

            self.model_path = resolved
            self.model = YOLO(resolved)

                                                                         
            try:
                names = getattr(self.model, "names", None)
                if isinstance(names, dict):
                    parsed: dict[int, str] = {}
                    for k, v in names.items():
                        try:
                            parsed[int(k)] = str(v)
                        except Exception:
                            continue
                    if parsed:
                        self.class_names = parsed
                elif isinstance(names, (list, tuple)):
                    self.class_names = {int(i): str(v) for i, v in enumerate(names)}
            except Exception:
                self.class_names = dict(DEFECT_CLASSES)

            self.available = True
            self.status_reason = None
        except Exception as e:
            logger.info(f"YOLO not available: {e}")
            self.available = False
            self.status_reason = str(e)

    def is_available(self) -> bool:
        return self.available

    def get_status(self) -> Dict[str, str | bool | None]:
        return {
            "available": bool(self.available),
            "model_path": self.model_path,
            "reason": self.status_reason,
        }

    def detect_defects(self, img: Image.Image, *, conf_threshold: float | None = None) -> List[Dict]:
        if not self.available or self.model is None:
            return []

        conf = self.conf_threshold if conf_threshold is None else float(conf_threshold)

        img_np = np.array(img)
        results = self.model(img_np, conf=conf, verbose=False)

        detections: List[Dict] = []
        for result in results:
            boxes = result.boxes
            for i in range(len(boxes)):
                bbox = boxes.xyxy[i].cpu().numpy()
                conf = float(boxes.conf[i].cpu().numpy())
                cls = int(boxes.cls[i].cpu().numpy())

                class_name = (self.class_names or DEFECT_CLASSES).get(cls, "unknown")
                width = bbox[2] - bbox[0]
                height = bbox[3] - bbox[1]
                area = width * height
                severity = DEFECT_SEVERITY.get(class_name, 0.5)

                detections.append(
                    {
                        "class": class_name,
                        "confidence": conf,
                        "bbox": bbox.tolist(),
                        "area": float(area),
                        "severity": severity,
                    }
                )

        return detections

    def compute_defect_features(self, img: Image.Image) -> Dict[str, float]:
        detections = self.detect_defects(img)
        if not detections:
            return {
                "total_defects": 0,
                "total_defect_area": 0.0,
                "avg_severity": 0.0,
                "weighted_defect_score": 0.0,
            }

        total_area = 0.0
        total_severity = 0.0
        for d in detections:
            total_area += float(d.get("area") or 0.0)
            total_severity += float(d.get("severity") or 0.0) * float(d.get("confidence") or 0.0)

        img_area = img.size[0] * img.size[1]
        normalized_area = min(total_area / img_area, 1.0) if img_area else 0.0
        avg_severity = total_severity / len(detections)
        weighted_score = min((len(detections) / 10.0) * 0.4 + normalized_area * 0.3 + avg_severity * 0.3, 1.0)

        return {
            "total_defects": float(len(detections)),
            "total_defect_area": float(normalized_area),
            "avg_severity": float(avg_severity),
            "weighted_defect_score": float(weighted_score),
        }

    def compare_defects(self, img_a: Image.Image, img_b: Image.Image) -> float:
        a = self.compute_defect_features(img_a)
        b = self.compute_defect_features(img_b)
        diff = abs(a["weighted_defect_score"] - b["weighted_defect_score"])
        return float(np.clip(1.0 - diff, 0.0, 1.0))


_detector: YOLOv8DefectDetector | None = None


def get_defect_detector() -> YOLOv8DefectDetector:
    global _detector
    if _detector is None:
        _detector = YOLOv8DefectDetector()
    else:
                                                                        
        if not _detector.is_available():
            try:
                _detector._load_model()
            except Exception:
                pass
    return _detector
