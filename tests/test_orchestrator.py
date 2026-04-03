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
from nonprofit_platform.jobs.orchestrator import ContinuousWorkerService, OrchestratorService


def make_config() -> AppConfig:
    return AppConfig(
        database=DatabaseSettings(dsn="postgresql://unused"),
        storage=StorageSettings(project_url="", service_role_key="", bucket="test", enabled=False),
        openai=OpenAISettings(api_key="", enabled=False),
        workers=WorkerSettings(
            orchestrator_poll_interval_seconds=0.01,
            registry_sync_interval_seconds=0.01,
            filing_discovery_interval_seconds=0.01,
            stale_claim_recovery_interval_seconds=0.01,
            heartbeat_interval_seconds=0.01,
        ),
        features=FeatureFlags(),
        logging=LoggingSettings(),
        sources=SourceSettings(),
    )


class FakeRepository:
    def __init__(self) -> None:
        self.recovered_calls: list[tuple[tuple[str, ...] | None, int]] = []

    def recover_stale_jobs(self, job_types=None, limit: int = 500) -> int:
        normalized = tuple(job_types) if job_types is not None else None
        self.recovered_calls.append((normalized, limit))
        return 2


class FakeJob:
    def __init__(self) -> None:
        self.calls = 0

    def run(self) -> dict[str, int]:
        self.calls += 1
        return {"calls": self.calls}


class FakeWorker:
    def __init__(self, job_type: str) -> None:
        self.job_type = job_type
        self.calls = 0

    def run(self) -> dict[str, int]:
        self.calls += 1
        return {"claimed": 1, "completed": 1}


def test_orchestrator_runs_singletons_workers_and_recovery_once() -> None:
    repository = FakeRepository()
    registry_job = FakeJob()
    filing_job = FakeJob()
    workers = {
        "raw_fetch": FakeWorker("raw_fetch"),
        "extract": FakeWorker("extract"),
    }
    service = OrchestratorService(
        config=make_config(),
        repository=repository,  # type: ignore[arg-type]
        registry_job=registry_job,
        filing_discovery_job=filing_job,
        workers=workers,
    )

    result = service.run(once=True)

    assert result["loops"] == 1
    assert result["recovered"] == 2
    assert registry_job.calls == 1
    assert filing_job.calls == 1
    assert workers["raw_fetch"].calls == 1
    assert workers["extract"].calls == 1


def test_continuous_worker_service_recovers_stale_jobs_before_running() -> None:
    repository = FakeRepository()
    worker = FakeWorker("enrich")
    service = ContinuousWorkerService(
        config=make_config(),
        worker=worker,
        repository=repository,  # type: ignore[arg-type]
    )

    result = service.run(once=True)

    assert result["iterations"] == 1
    assert result["recovered"] == 2
    assert worker.calls == 1
