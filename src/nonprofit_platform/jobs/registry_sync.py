from __future__ import annotations

from typing import Any

from nonprofit_platform.config import AppConfig
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.logging import MetricsRecorder
from nonprofit_platform.sources.irs_registry import IrsRegistryClient


class RegistrySyncJob:
    def __init__(
        self,
        config: AppConfig,
        repository: PipelineRepository,
        client: IrsRegistryClient,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        self.config = config
        self.repository = repository
        self.client = client
        self.metrics = metrics or MetricsRecorder()

    def run(self, sample_limit: int | None = None) -> dict[str, Any]:
        limit = sample_limit or self.config.workers.registry_sample_limit
        snapshot = self.client.load_registry_snapshot(sample_limit=limit)
        organizations = self.repository.upsert_organizations(snapshot.organizations)
        statuses = self.repository.insert_organization_status(snapshot.statuses)
        self.metrics.increment("registry.organizations_upserted", organizations)
        self.metrics.increment("registry.statuses_inserted", statuses)
        return {
            "organizations_upserted": organizations,
            "statuses_inserted": statuses,
            "sample_limit": limit,
        }
