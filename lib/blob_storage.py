from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BlobConfig:
    account_name: Optional[str] = None
    account_url: Optional[str] = None
    connection_string: Optional[str] = None

    uploads_container: str = "uploads"
    tmp_container: str = "tmp"


def load_blob_config() -> BlobConfig:
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT") or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not account_url and account_name:
        account_url = f"https://{account_name}.blob.core.windows.net"

    return BlobConfig(
        account_name=account_name,
        account_url=account_url,
        connection_string=connection_string,
        uploads_container=os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads"),
        tmp_container=os.getenv("AZURE_BLOB_TMP_CONTAINER", "tmp"),
    )


class BlobStorage:
    def __init__(self, cfg: Optional[BlobConfig] = None):
        self.cfg = cfg or load_blob_config()
        self._client = None

    def is_configured(self) -> bool:
        return bool(self.cfg.connection_string or self.cfg.account_url)

    def _get_client(self):
        if self._client is not None:
            return self._client

        try:
            from azure.storage.blob import BlobServiceClient
        except Exception as e:                    
            raise RuntimeError("azure-storage-blob is not installed") from e

        if self.cfg.connection_string:
            self._client = BlobServiceClient.from_connection_string(self.cfg.connection_string)
            return self._client

        if not self.cfg.account_url:
            raise RuntimeError(
                "Azure Blob Storage not configured: set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT(_URL)"
            )

        try:
            from azure.identity import DefaultAzureCredential
        except Exception as e:                    
            raise RuntimeError("azure-identity is required for Managed Identity auth") from e

        credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self._client = BlobServiceClient(account_url=self.cfg.account_url, credential=credential)
        return self._client

    def upload_bytes(
        self, *, container: str, blob_name: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        svc = self._get_client()
        container_client = svc.get_container_client(container)
        try:
            container_client.create_container()
        except Exception:
            pass

        try:
            from azure.storage.blob import ContentSettings
        except Exception as e:                    
            raise RuntimeError("azure-storage-blob is not installed") from e

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )
        return blob_name

    def download_bytes(self, *, container: str, blob_name: str) -> bytes:
        blob = self._get_client().get_blob_client(container=container, blob=blob_name)
        return blob.download_blob().readall()

    def exists(self, *, container: str, blob_name: str) -> bool:
        blob = self._get_client().get_blob_client(container=container, blob=blob_name)
        timeout = float(os.getenv("AZURE_BLOB_TIMEOUT", "10"))
        try:
            blob.get_blob_properties(timeout=timeout)
            return True
        except Exception as e:
                                                              
            try:
                from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

                if isinstance(e, ResourceNotFoundError):
                    return False
                if isinstance(e, HttpResponseError) and getattr(e, "status_code", None) in (401, 403):
                    raise
            except Exception:
                                                                    
                pass
            return False

    def list_blob_names(self, *, container: str, prefix: str = "", limit: int = 100):
        """List blob names (best-effort) without downloading content."""
        svc = self._get_client()
        container_client = svc.get_container_client(container)
        timeout = float(os.getenv("AZURE_BLOB_TIMEOUT", "10"))
        i = 0
        for b in container_client.list_blobs(name_starts_with=prefix or None, timeout=timeout):
            name = getattr(b, "name", None)
            if name:
                yield name
                i += 1
                if i >= limit:
                    return
