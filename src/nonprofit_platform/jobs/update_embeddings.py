from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.indexing.adapter import IndexAdapter, IndexDocument
from nonprofit_platform.jobs.claiming import ClaimedWorker
from nonprofit_platform.logging import MetricsRecorder


UTC = timezone.utc


class EmbeddingIndexWorker(ClaimedWorker):
    job_type = "index"

    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        adapter: IndexAdapter,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(config, repository, metrics=metrics)
        self.adapter = adapter

    def process_job(self, job: dict) -> dict:
        payload = job["payload"]
        pending = self.repository.get_profiles_pending_index(batch_size=1)
        profile = None
        for candidate in pending:
            if candidate["id"] == payload["nonprofit_profile_id"]:
                profile = candidate
                break
        if profile is None:
            raise RuntimeError(f"Missing nonprofit profile id={payload['nonprofit_profile_id']}")

        text = "\n".join(
            part
            for part in [
                profile.get("profile_summary"),
                " ".join(profile.get("program_highlights") or []),
                " ".join(profile.get("fit_notes") or []),
            ]
            if part
        )
        content_hash = sha256(text.encode("utf-8")).hexdigest()
        result = self.adapter.upsert_document(
            IndexDocument(
                document_id=profile["object_id"],
                text=text,
                metadata={
                    "ein": profile.get("ein"),
                    "cause_tags": profile.get("cause_tags"),
                    "location_hints": profile.get("location_hints"),
                },
                content_hash=content_hash,
            )
        )
        index_status_id = self.repository.upsert_embeddings_status(
            {
                "object_id": profile["object_id"],
                "nonprofit_profile_id": profile["id"],
                "adapter_name": result.adapter_name,
                "document_hash": content_hash,
                "status": result.status,
                "indexed_at": datetime.now(tz=UTC),
                "attempt_count": 1,
                "last_error": None,
                "payload": result.payload,
            }
        )
        return {"object_id": profile["object_id"], "index_status_id": index_status_id}
