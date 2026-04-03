from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.connection import Database
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.logging import MetricsRecorder, get_logger


@dataclass(slots=True)
class JobClaimResult:
    claimed: int
    completed: int
    retried: int
    dead_lettered: int


def calculate_retry_delay(attempt_count: int, base_seconds: int = 60, cap_seconds: int = 3600) -> int:
    return min(base_seconds * max(attempt_count, 1), cap_seconds)


class ClaimedWorker:
    job_type: str = ""

    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        self.config = config
        self.repository = repository
        self.metrics = metrics or MetricsRecorder()
        self.logger = get_logger(self.__class__.__name__)
        self.worker_id = self.repository.make_worker_id(self.job_type or "worker")

    def run(self, batch_size: int | None = None) -> dict[str, Any]:
        limit = batch_size or self.config.workers.claim_batch_size
        jobs = self.repository.claim_jobs(
            job_type=self.job_type,
            batch_size=limit,
            worker_id=self.worker_id,
            lease_seconds=self.config.workers.claim_lease_seconds,
        )
        completed = 0
        retried = 0
        dead_lettered = 0
        for job in jobs:
            try:
                with self._job_heartbeat(job["id"]):
                    result = self.process_job(job)
                completed_successfully = self.repository.complete_job(
                    job["id"],
                    result_payload=result,
                    worker_id=self.worker_id,
                )
                if not completed_successfully:
                    self.logger.warning(
                        "job completion skipped because lease was lost",
                        extra={"job_type": self.job_type, "job_id": job["id"], "worker_id": self.worker_id},
                    )
                    continue
                completed += 1
                self.metrics.increment("jobs.completed", job_type=self.job_type)
            except Exception as exc:
                retryable = self.is_retryable(exc)
                failed = self.repository.fail_job(
                    job_id=job["id"],
                    error_message=str(exc),
                    retryable=retryable,
                    retry_delay_seconds=calculate_retry_delay(job["attempt_count"] + 1),
                    worker_id=self.worker_id,
                )
                if not failed:
                    self.logger.warning(
                        "job failure transition skipped because lease was lost",
                        extra={"job_type": self.job_type, "job_id": job["id"], "worker_id": self.worker_id},
                    )
                    continue
                if retryable and job["attempt_count"] + 1 < job["max_attempts"]:
                    retried += 1
                else:
                    dead_lettered += 1
                self.logger.exception(
                    "job failed",
                    extra={"job_type": self.job_type, "job_id": job["id"], "worker_id": self.worker_id},
                )
                self.metrics.increment("jobs.failed", job_type=self.job_type)
        return {
            "worker_id": self.worker_id,
            "job_type": self.job_type,
            "claimed": len(jobs),
            "completed": completed,
            "retried": retried,
            "dead_lettered": dead_lettered,
        }

    def process_job(self, job: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def is_retryable(self, exc: Exception) -> bool:
        return True

    @contextmanager
    def _job_heartbeat(self, job_id: int) -> Iterator[None]:
        interval = self.config.workers.heartbeat_interval_seconds
        if interval <= 0:
            yield
            return

        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(job_id, stop_event),
            name=f"{self.job_type}-heartbeat-{job_id}",
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            thread.join(timeout=max(1.0, interval))

    def _heartbeat_loop(self, job_id: int, stop_event: threading.Event) -> None:
        interval = max(0.1, self.config.workers.heartbeat_interval_seconds)
        try:
            if not self._heartbeat_tick(job_id):
                return
        except Exception:
            self.logger.exception(
                "heartbeat failed",
                extra={"job_type": self.job_type, "job_id": job_id, "worker_id": self.worker_id},
            )
            return
        while not stop_event.wait(interval):
            try:
                if not self._heartbeat_tick(job_id):
                    return
            except Exception:
                self.logger.exception(
                    "heartbeat failed",
                    extra={"job_type": self.job_type, "job_id": job_id, "worker_id": self.worker_id},
                )

    def _heartbeat_tick(self, job_id: int) -> bool:
        with Database(self.config.database) as database:
            heartbeat_repository = PipelineRepository(database)
            return heartbeat_repository.heartbeat_job(
                job_id=job_id,
                lease_seconds=self.config.workers.claim_lease_seconds,
                worker_id=self.worker_id,
            )
