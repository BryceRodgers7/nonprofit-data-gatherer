from __future__ import annotations

from datetime import datetime, timezone

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.jobs.claiming import ClaimedWorker
from nonprofit_platform.logging import MetricsRecorder
from nonprofit_platform.sources.irs_filings import IrsFilingIndexClient
from nonprofit_platform.storage.supabase import SupabaseStorage


UTC = timezone.utc


class RawFilingFetchWorker(ClaimedWorker):
    job_type = "raw_fetch"

    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        client: IrsFilingIndexClient,
        storage: SupabaseStorage,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(config, repository, metrics=metrics)
        self.client = client
        self.storage = storage

    def process_job(self, job: dict) -> dict:
        payload = job["payload"]
        object_id = payload["object_id"]
        xml_url = payload["xml_url"]
        xml_bytes = self.client.download_xml(xml_url)
        storage_path = f"{self.config.storage.base_prefix}/{object_id}.xml"
        artifact = self.storage.upload_bytes(storage_path, xml_bytes, content_type="application/xml")
        raw_filing_id = self.repository.upsert_raw_filing(
            {
                "object_id": object_id,
                "return_id": payload.get("return_id"),
                "ein": payload.get("ein"),
                "xml_url": xml_url,
                "index_url": payload.get("index_url"),
                "storage_provider": "supabase",
                "storage_bucket": artifact.bucket,
                "storage_path": artifact.path,
                "artifact_checksum": artifact.checksum,
                "content_length": artifact.content_length,
                "content_type": artifact.content_type,
                "fetched_at": datetime.now(tz=UTC),
                "fetch_status": "fetched",
                "metadata": {"worker_id": self.worker_id},
                "raw_metadata": payload,
            }
        )
        self.repository.mark_filing_status(object_id, "fetched")
        self.repository.enqueue_job(
            "extract",
            payload={"object_id": object_id, "raw_filing_id": raw_filing_id},
            idempotency_key=object_id,
            max_attempts=self.config.workers.max_attempts,
        )
        return {"object_id": object_id, "raw_filing_id": raw_filing_id, "storage_path": artifact.path}
