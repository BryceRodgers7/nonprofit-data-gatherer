from __future__ import annotations

import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.logging import MetricsRecorder, get_logger


@dataclass(slots=True)
class ScheduledTask:
    name: str
    interval_seconds: float
    runner: Callable[[], dict[str, Any]]
    last_run_monotonic: float | None = None

    def due(self, now_monotonic: float) -> bool:
        return self.last_run_monotonic is None or (
            now_monotonic - self.last_run_monotonic >= self.interval_seconds
        )


class OrchestratorService:
    worker_job_types = ("raw_fetch", "extract", "enrich", "index")

    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        registry_job: Any,
        filing_discovery_job: Any,
        workers: dict[str, Any],
        metrics: MetricsRecorder | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.repository = repository
        self.registry_job = registry_job
        self.filing_discovery_job = filing_discovery_job
        self.workers = workers
        self.metrics = metrics or MetricsRecorder()
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.logger = get_logger(self.__class__.__name__)
        self.stop_event = threading.Event()
        self._scheduled_tasks = [
            ScheduledTask(
                name="registry_sync",
                interval_seconds=self.config.workers.registry_sync_interval_seconds,
                runner=self.registry_job.run,
            ),
            ScheduledTask(
                name="filing_discovery",
                interval_seconds=self.config.workers.filing_discovery_interval_seconds,
                runner=self.filing_discovery_job.run,
            ),
        ]
        self._last_recovery_monotonic: float | None = None

    def install_signal_handlers(self) -> None:
        for signame in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, signame, None)
            if sig is not None:
                signal.signal(sig, self._handle_stop_signal)

    def _handle_stop_signal(self, _signum, _frame) -> None:
        self.stop_event.set()

    def run(self, once: bool = False, worker_stages: Sequence[str] | None = None) -> dict[str, Any]:
        worker_names = list(worker_stages) if worker_stages else list(self.workers.keys())
        self.install_signal_handlers()
        loop_count = 0
        total_recovered = 0
        while not self.stop_event.is_set():
            loop_count += 1
            now_monotonic = self.monotonic_fn()
            recovered = self._recover_stale_jobs_if_due(now_monotonic)
            total_recovered += recovered
            singleton_runs = self._run_due_singletons(now_monotonic)
            worker_runs = self._run_workers(worker_names)
            if once:
                break
            idle = recovered == 0 and not singleton_runs and not any(
                run.get("claimed", 0) or run.get("completed", 0) for run in worker_runs.values()
            )
            self.sleep_fn(self.config.workers.orchestrator_poll_interval_seconds if idle else 0.1)
        return {
            "loops": loop_count,
            "recovered": total_recovered,
            "worker_stages": worker_names,
        }

    def _recover_stale_jobs_if_due(self, now_monotonic: float) -> int:
        interval = self.config.workers.stale_claim_recovery_interval_seconds
        due = self._last_recovery_monotonic is None or (
            now_monotonic - self._last_recovery_monotonic >= interval
        )
        if not due:
            return 0
        recovered = self.repository.recover_stale_jobs(job_types=self.worker_job_types)
        self._last_recovery_monotonic = now_monotonic
        if recovered:
            self.metrics.increment("jobs.recovered", recovered)
        return recovered

    def _run_due_singletons(self, now_monotonic: float) -> list[str]:
        ran: list[str] = []
        for task in self._scheduled_tasks:
            if not task.due(now_monotonic):
                continue
            task.runner()
            task.last_run_monotonic = now_monotonic
            ran.append(task.name)
            self.metrics.increment("orchestrator.singleton_runs", stage=task.name)
        return ran

    def _run_workers(self, worker_names: Sequence[str]) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for worker_name in worker_names:
            worker = self.workers[worker_name]
            results[worker_name] = worker.run()
        return results


class ContinuousWorkerService:
    def __init__(
        self,
        config: AppConfig,
        worker: Any,
        repository: PipelineRepository,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.worker = worker
        self.repository = repository
        self.sleep_fn = sleep_fn
        self.stop_event = threading.Event()

    def install_signal_handlers(self) -> None:
        for signame in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, signame, None)
            if sig is not None:
                signal.signal(sig, self._handle_stop_signal)

    def _handle_stop_signal(self, _signum, _frame) -> None:
        self.stop_event.set()

    def run(self, once: bool = False) -> dict[str, Any]:
        self.install_signal_handlers()
        iterations = 0
        total_recovered = 0
        while not self.stop_event.is_set():
            iterations += 1
            total_recovered += self.repository.recover_stale_jobs(job_types=[self.worker.job_type])
            result = self.worker.run()
            if once:
                break
            if result.get("claimed", 0) == 0:
                self.sleep_fn(self.config.workers.orchestrator_poll_interval_seconds)
        return {
            "iterations": iterations,
            "job_type": self.worker.job_type,
            "recovered": total_recovered,
        }
