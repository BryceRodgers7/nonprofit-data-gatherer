from __future__ import annotations

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.jobs.claiming import ClaimedWorker
from nonprofit_platform.logging import MetricsRecorder
from nonprofit_platform.parsers.form990.extractor import Form990Extractor
from nonprofit_platform.storage.supabase import SupabaseStorage


class FilingExtractionWorker(ClaimedWorker):
    job_type = "extract"

    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        storage: SupabaseStorage,
        extractor: Form990Extractor,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(config, repository, metrics=metrics)
        self.storage = storage
        self.extractor = extractor

    def process_job(self, job: dict) -> dict:
        payload = job["payload"]
        object_id = payload["object_id"]
        raw_filing = self.repository.get_raw_filing_by_object_id(object_id)
        if not raw_filing:
            raise RuntimeError(f"Missing raw filing for object_id={object_id}")
        xml_bytes = self.storage.download_bytes(raw_filing["storage_path"])
        extracted = self.extractor.extract(xml_bytes, object_id=object_id, return_id=raw_filing.get("return_id"))
        normalized_id = self.repository.upsert_normalized_filing(extracted.to_record())
        self.repository.mark_filing_status(object_id, "normalized")
        self.repository.enqueue_job(
            "enrich",
            payload={"object_id": object_id, "normalized_filing_id": normalized_id},
            idempotency_key=f"{object_id}:{extracted.parser_version}",
            max_attempts=self.config.workers.max_attempts,
        )
        return {"object_id": object_id, "normalized_filing_id": normalized_id}
