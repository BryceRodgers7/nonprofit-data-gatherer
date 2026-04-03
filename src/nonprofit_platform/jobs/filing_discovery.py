from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.logging import MetricsRecorder
from nonprofit_platform.sources.irs_filings import IrsFilingIndexClient


class FilingDiscoveryJob:
    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        client: IrsFilingIndexClient,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        self.config = config
        self.repository = repository
        self.client = client
        self.metrics = metrics or MetricsRecorder()

    def run(self, years: Sequence[int] | None = None) -> dict[str, Any]:
        target_years = list(years) if years else [self.config.workers.years_backfill_start]
        discovered = 0
        enqueued = 0
        for year in target_years:
            snapshot = self.client.fetch_index(year, sample_limit=self.config.workers.filing_sample_limit)
            discovered += self.repository.upsert_filing_index(snapshot.filings)
            jobs = [
                {
                    "payload": {
                        "object_id": filing["object_id"],
                        "return_id": filing.get("return_id"),
                        "ein": filing.get("ein"),
                        "xml_url": filing["xml_url"],
                        "index_url": filing["index_url"],
                    },
                    "idempotency_key": filing["object_id"],
                }
                for filing in snapshot.filings
            ]
            enqueued += self.repository.enqueue_many_jobs(
                "raw_fetch",
                jobs,
                max_attempts=self.config.workers.max_attempts,
            )
        self.metrics.increment("filing.discovery_rows", discovered)
        self.metrics.increment("filing.fetch_jobs_enqueued", enqueued)
        return {"years": target_years, "discovered": discovered, "raw_fetch_jobs_enqueued": enqueued}
