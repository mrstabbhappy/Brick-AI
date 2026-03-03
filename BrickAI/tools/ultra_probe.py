from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image
from ultralytics import YOLO


def main() -> int:
    weights = Path("models/yolov8_brick_defects.pt")
    demo_dir = Path("uploads") / "defect_demo"
    imgs = sorted([p for p in demo_dir.glob("*.*") if p.is_file()])
    if not imgs:
        raise SystemExit(f"No images found under {demo_dir}")

    img_path = imgs[0]

    print("weights:", str(weights.resolve()), "bytes=", weights.stat().st_size)
    print("img:", str(img_path), "bytes=", img_path.stat().st_size)

    model = YOLO(str(weights))
    print("model.names:", getattr(model, "names", None))

    img = Image.open(img_path).convert("RGB")
    arr = np.array(img)

    for conf in [0.25, 0.10, 0.05, 0.02, 0.01, 0.005, 0.001]:
        results = model(arr, conf=conf, verbose=False)
        r0 = results[0]
        boxes = r0.boxes
        n = 0 if boxes is None else len(boxes)
        top = None
        if boxes is not None and n:
            top = float(np.max(boxes.conf.cpu().numpy()))
        print(f"conf>={conf:.3f}: boxes={n} top_conf={top}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
