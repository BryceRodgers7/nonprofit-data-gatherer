from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

import httpx

from nonprofit_platform.config import StorageSettings


@dataclass(slots=True)
class StoredArtifact:
    bucket: str
    path: str
    checksum: str
    content_length: int
    content_type: str


class SupabaseStorage:
    def __init__(self, settings: StorageSettings) -> None:
        self.settings = settings

    @property
    def enabled(self) -> bool:
        return (
            self.settings.enabled
            and bool(self.settings.project_url)
            and bool(self.settings.service_role_key)
            and bool(self.settings.bucket)
        )

    def _headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.settings.service_role_key}",
            "apikey": self.settings.service_role_key,
        }
        if content_type:
            headers["Content-Type"] = content_type
            headers["x-upsert"] = "true"
        return headers

    def upload_bytes(
        self,
        path: str,
        content: bytes,
        content_type: str = "application/xml",
    ) -> StoredArtifact:
        if not self.enabled:
            raise RuntimeError("Supabase storage is not fully configured.")
        checksum = hashlib.sha256(content).hexdigest()
        url = f"{self.settings.project_url.rstrip('/')}/storage/v1/object/{self.settings.bucket}/{path}"
        response = httpx.put(
            url,
            headers=self._headers(content_type),
            content=content,
            timeout=120.0,
        )
        response.raise_for_status()
        return StoredArtifact(
            bucket=self.settings.bucket,
            path=path,
            checksum=checksum,
            content_length=len(content),
            content_type=content_type,
        )

    def download_bytes(self, path: str) -> bytes:
        if not self.enabled:
            raise RuntimeError("Supabase storage is not fully configured.")
        url = f"{self.settings.project_url.rstrip('/')}/storage/v1/object/{self.settings.bucket}/{path}"
        response = httpx.get(url, headers=self._headers(), timeout=120.0)
        response.raise_for_status()
        return response.content
