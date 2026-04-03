from __future__ import annotations

import csv
import io
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
class FilingIndexSnapshot:
    year: int
    filings: list[dict[str, Any]]


class IrsFilingIndexClient:
    def __init__(self, settings: SourceSettings) -> None:
        self.settings = settings

    def build_index_url(self, year: int) -> str:
        return f"{self.settings.form_990_base_url.rstrip('/')}/{year}/index_{year}.csv"

    def build_xml_url(self, year: int, object_id: str) -> str:
        return f"{self.settings.form_990_base_url.rstrip('/')}/{year}/{object_id}_public.xml"

    def fetch_index(self, year: int, sample_limit: int | None = None) -> FilingIndexSnapshot:
        url = self.build_index_url(year)
        response = httpx.get(url, timeout=120.0, follow_redirects=True)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        rows: list[dict[str, Any]] = []
        observed_at = _utc_now()
        for index, row in enumerate(reader, start=1):
            normalized = {_normalize_key(key): value.strip() for key, value in row.items() if key}
            object_id = normalized.get("object_id") or normalized.get("objectid")
            return_id = normalized.get("return_id") or normalized.get("returnid")
            if not object_id:
                continue
            rows.append(
                {
                    "object_id": object_id,
                    "return_id": return_id,
                    "ein": (normalized.get("ein") or "").zfill(9),
                    "tax_year": _safe_int(normalized.get("tax_period", "")[:4]) or _safe_int(normalized.get("tax_year")),
                    "filing_year": year,
                    "tax_period": normalized.get("tax_period"),
                    "form_type": normalized.get("sub_date", "") and normalized.get("form_type", normalized.get("return_type")),
                    "taxpayer_name": normalized.get("taxpayer_name", ""),
                    "submitted_on": normalized.get("sub_date") or None,
                    "xml_url": self.build_xml_url(year, object_id),
                    "index_url": url,
                    "source_updated_at": observed_at,
                    "filing_status": "discovered",
                    "payload": normalized,
                }
            )
            if sample_limit and index >= sample_limit:
                break
        return FilingIndexSnapshot(year=year, filings=rows)

    def download_xml(self, xml_url: str) -> bytes:
        response = httpx.get(xml_url, timeout=120.0, follow_redirects=True)
        response.raise_for_status()
        return response.content


def _safe_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return int(digits) if digits else None
