import time

from nonprofit_platform.config import (
    AppConfig,
    DatabaseSettings,
    FeatureFlags,
    LoggingSettings,
    OpenAISettings,
    SourceSettings,
    StorageSettings,
    WorkerSettings,
)
from nonprofit_platform.jobs.claiming import ClaimedWorker, calculate_retry_delay


def make_config() -> AppConfig:
    return AppConfig(
        database=DatabaseSettings(dsn="postgresql://unused"),
        storage=StorageSettings(project_url="", service_role_key="", bucket="test", enabled=False),
        openai=OpenAISettings(api_key="", enabled=False),
        workers=WorkerSettings(
            claim_lease_seconds=1,
            heartbeat_interval_seconds=0.02,
            orchestrator_poll_interval_seconds=0.01,
            registry_sync_interval_seconds=0.01,
            filing_discovery_interval_seconds=0.01,
            stale_claim_recovery_interval_seconds=0.01,
        ),
        features=FeatureFlags(),
        logging=LoggingSettings(),
        sources=SourceSettings(),
    )


class FakeRepository:
    def __init__(self) -> None:
        self.claimed = False
        self.completed: list[tuple[int, dict]] = []
        self.failed: list[int] = []

    def make_worker_id(self, prefix: str) -> str:
        return f"{prefix}-worker"

    def claim_jobs(self, job_type: str, batch_size: int, worker_id: str, lease_seconds: int) -> list[dict]:
        if self.claimed:
            return []
        self.claimed = True
        return [{"id": 1, "attempt_count": 0, "max_attempts": 5, "payload": {"id": "a"}}]

    def complete_job(self, job_id: int, result_payload: dict | None = None, worker_id: str | None = None) -> bool:
        self.completed.append((job_id, result_payload or {}))
        return True

    def fail_job(self, job_id: int, **_: object) -> bool:
        self.failed.append(job_id)
        return True


def test_retry_delay_scales_and_caps() -> None:
    assert calculate_retry_delay(1) == 60
    assert calculate_retry_delay(3) == 180
    assert calculate_retry_delay(100) == 3600


def test_claimed_worker_heartbeats_during_long_running_job() -> None:
    class SlowWorker(ClaimedWorker):
        job_type = "raw_fetch"

        def __init__(self, config: AppConfig, repository: FakeRepository) -> None:
            super().__init__(config, repository)  # type: ignore[arg-type]
            self.heartbeat_ticks = 0

        def process_job(self, job: dict[str, object]) -> dict[str, object]:
            time.sleep(0.08)
            return {"ok": True}

        def _heartbeat_tick(self, job_id: int) -> bool:
            self.heartbeat_ticks += 1
            return True

    repository = FakeRepository()
    worker = SlowWorker(make_config(), repository)

    result = worker.run()

    assert result["completed"] == 1
    assert worker.heartbeat_ticks >= 1
    assert repository.completed[0][0] == 1
