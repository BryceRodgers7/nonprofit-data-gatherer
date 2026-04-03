from __future__ import annotations

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.enrichment.profile_builder import ProfileEnricher
from nonprofit_platform.jobs.claiming import ClaimedWorker
from nonprofit_platform.logging import MetricsRecorder


class EnrichmentWorker(ClaimedWorker):
    job_type = "enrich"

    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        enricher: ProfileEnricher,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(config, repository, metrics=metrics)
        self.enricher = enricher

    def process_job(self, job: dict) -> dict:
        payload = job["payload"]
        normalized = self.repository.get_latest_normalized_filing(payload["object_id"])
        if not normalized:
            raise RuntimeError(f"Missing normalized filing for object_id={payload['object_id']}")
        built = self.enricher.build_profile(normalized)
        enrichment_run_id = self.repository.insert_enrichment_run(built.enrichment_run_row)
        built.profile_row["enrichment_run_id"] = enrichment_run_id
        profile_id = self.repository.upsert_nonprofit_profile(built.profile_row)
        self.repository.enqueue_job(
            "index",
            payload={"object_id": payload["object_id"], "nonprofit_profile_id": profile_id},
            idempotency_key=f"{payload['object_id']}:{built.profile_row['source_text_hash']}",
            max_attempts=self.config.workers.max_attempts,
        )
        return {"object_id": payload["object_id"], "nonprofit_profile_id": profile_id}
