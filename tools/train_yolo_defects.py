from __future__ import annotations

import argparse
import glob
import os
import shutil
from pathlib import Path


def _latest_best_pt(search_root: Path) -> Path | None:
    candidates: list[Path] = []
                                                    
    direct = search_root / "runs" / "detect" / "weights" / "best.pt"
    if direct.exists():
        candidates.append(direct)

                                                     
    candidates.extend(
        [
            Path(p)
            for p in glob.glob(str(search_root / "runs" / "detect" / "train*" / "weights" / "best.pt"))
        ]
    )
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Train YOLOv8 defect detector and export weights for Brick AI")
    parser.add_argument("--data", default=str(Path("models") / "brick_defects_dataset.yaml"), help="Path to dataset yaml")
    parser.add_argument("--epochs", type=int, default=int(os.getenv("YOLO_EPOCHS", "10")))
    parser.add_argument("--imgsz", type=int, default=int(os.getenv("YOLO_IMGSZ", "640")))
    parser.add_argument("--batch", default=os.getenv("YOLO_BATCH", "auto"))
    parser.add_argument("--model", default=os.getenv("YOLO_BASE_MODEL", "yolov8n.pt"), help="Base model (e.g. yolov8n.pt)")
    parser.add_argument("--device", default=os.getenv("YOLO_DEVICE", "cpu"), help="cpu or cuda")
    parser.add_argument(
        "--out",
        default=str(Path("models") / "yolov8_brick_defects.pt"),
        help="Destination weights path used by the app",
    )

    args = parser.parse_args()
    batch = args.batch
    if isinstance(batch, str):
        b = batch.strip().lower()
        if b in ("auto", "0"):
            batch = 0
        else:
            try:
                batch = int(b)
            except Exception:
                batch = 0

    data_yaml = Path(args.data)
    if not data_yaml.exists():
        raise SystemExit(f"Dataset yaml not found: {data_yaml}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    from ultralytics import YOLO

    model = YOLO(args.model)

                                                          
    results = model.train(
        data=str(data_yaml),
        epochs=int(args.epochs),
        imgsz=int(args.imgsz),
        batch=batch,
        device=args.device,
        project="runs",
        name="detect",
        exist_ok=True,
        verbose=False,
    )

    best = _latest_best_pt(Path.cwd())
    if not best or not best.exists():
        raise SystemExit("Training finished but best.pt was not found under runs/detect/train*/weights")

    shutil.copyfile(best, out_path)
    print(f"Exported weights -> {out_path}")
    try:
        print(f"best.pt source -> {best}")
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
