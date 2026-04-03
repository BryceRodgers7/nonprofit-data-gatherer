from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

from nonprofit_platform.config import SourceSettings


UTC = timezone.utc


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_key(key: str) -> str:
    return key.strip().lower().replace(" ", "_")


@dataclass(slots=True)
class RegistrySnapshot:
    organizations: list[dict[str, Any]]
    statuses: list[dict[str, Any]]


class IrsRegistryClient:
    def __init__(self, settings: SourceSettings) -> None:
        self.settings = settings

    def _download_text(self, url: str) -> str:
        response = httpx.get(url, timeout=120.0, follow_redirects=True)
        response.raise_for_status()
        return response.text

    def _download_zip_member(self, url: str) -> str:
        response = httpx.get(url, timeout=120.0, follow_redirects=True)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            members = archive.namelist()
            if not members:
                raise RuntimeError(f"No files found in zip archive: {url}")
            with archive.open(members[0]) as handle:
                return handle.read().decode("utf-8", errors="replace")

    def iter_eo_bmf(self, sample_limit: int | None = None) -> Iterable[dict[str, Any]]:
        text = self._download_text(self.settings.eo_bmf_region_url)
        reader = csv.DictReader(io.StringIO(text))
        for index, row in enumerate(reader, start=1):
            normalized = {_normalize_key(key): value.strip() for key, value in row.items() if key}
            yield normalized
            if sample_limit and index >= sample_limit:
                break

    def iter_pipe_delimited_zip(self, url: str, sample_limit: int | None = None) -> Iterable[dict[str, Any]]:
        text = self._download_zip_member(url)
        reader = csv.DictReader(io.StringIO(text), delimiter="|")
        for index, row in enumerate(reader, start=1):
            normalized = {_normalize_key(key): (value.strip() if value else "") for key, value in row.items() if key}
            yield normalized
            if sample_limit and index >= sample_limit:
                break

    def load_registry_snapshot(self, sample_limit: int | None = None) -> RegistrySnapshot:
        observed_at = _utc_now()
        organizations: list[dict[str, Any]] = []
        statuses: list[dict[str, Any]] = []

        for row in self.iter_eo_bmf(sample_limit=sample_limit):
            ein = row.get("ein") or row.get("ein_")
            if not ein:
                continue
            organizations.append(
                {
                    "ein": ein.zfill(9),
                    "legal_name": row.get("name", ""),
                    "doing_business_as_name": row.get("doing_business_as_name", ""),
                    "city": row.get("city", ""),
                    "state": row.get("state", ""),
                    "zip_code": row.get("zip", ""),
                    "country": row.get("country", "US"),
                    "ruling_month": row.get("ruling", ""),
                    "subsection_code": row.get("subsection", ""),
                    "foundation_code": row.get("foundation", ""),
                    "classification_code": row.get("classification", ""),
                    "affiliation_code": row.get("affiliation", ""),
                    "deductibility_code": row.get("deductibility", ""),
                    "organization_type": row.get("organization", ""),
                    "exempt_status_code": row.get("status", ""),
                    "tax_period": row.get("tax_period", ""),
                    "ntee_code": row.get("ntee_cd", ""),
                    "sort_name": row.get("sort_name", ""),
                    "latest_registry_source": "eo_bmf",
                    "latest_registry_updated_at": observed_at,
                    "raw_registry_payload": row,
                }
            )

        for row in self.iter_pipe_delimited_zip(self.settings.publication_78_url, sample_limit=sample_limit):
            ein = row.get("ein")
            if not ein:
                continue
            statuses.append(
                {
                    "ein": ein.zfill(9),
                    "source_name": "publication_78",
                    "status_code": row.get("deductibility_code", "eligible"),
                    "status_label": "Deductibility eligibility",
                    "status_value": row.get("organization_name", ""),
                    "effective_date": None,
                    "observed_at": observed_at,
                    "is_current": True,
                    "payload": row,
                }
            )

        for row in self.iter_pipe_delimited_zip(self.settings.revocation_url, sample_limit=sample_limit):
            ein = row.get("ein")
            if not ein:
                continue
            statuses.append(
                {
                    "ein": ein.zfill(9),
                    "source_name": "automatic_revocation",
                    "status_code": "revoked",
                    "status_label": "Automatic revocation",
                    "status_value": row.get("revocation_date", ""),
                    "effective_date": row.get("revocation_date") or None,
                    "observed_at": observed_at,
                    "is_current": True,
                    "payload": row,
                }
            )

        return RegistrySnapshot(organizations=organizations, statuses=statuses)
