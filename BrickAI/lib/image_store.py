from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from PIL import Image

from lib.blob_storage import BlobStorage


@dataclass(frozen=True)
class ImageRef:
                                                          
    kind: str                    
    path_or_blob: str
    container: Optional[str] = None


def _is_probable_path(value: str) -> bool:
    if not value:
        return False
    if os.path.isabs(value):
        return True
    if "/" in value or "\\" in value:
        return True
    return False


def load_image(image_path_or_key: str, *, blob: Optional[BlobStorage], uploads_container: str, upload_folder: str) -> Image.Image:
    """Load an image either from local storage or Azure Blob.

    Convention:
    - Prefer local files when present (dev-friendly and avoids blob permission issues).
    - If the local file is not present and Azure Blob is configured, treat stored `image_path` values as blob names (keys)
      unless they look like a local path.
    """
    if not image_path_or_key:
        raise FileNotFoundError("No image reference")

                                                                               
    local_path = image_path_or_key
    if not os.path.isabs(local_path):
        local_path = os.path.join(upload_folder, local_path)
    if os.path.exists(local_path):
        return Image.open(local_path).convert("RGB")

    if blob and blob.is_configured() and not _is_probable_path(image_path_or_key):
        data = blob.download_bytes(container=uploads_container, blob_name=image_path_or_key)
        from io import BytesIO

        return Image.open(BytesIO(data)).convert("RGB")

                                                                        
    return Image.open(local_path).convert("RGB")
