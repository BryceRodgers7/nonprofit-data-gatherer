from __future__ import annotations

import argparse
import json
from typing import Iterable

from nonprofit_platform.config import load_config
from nonprofit_platform.db.connection import Database
from nonprofit_platform.db.repository import PipelineRepository
from nonprofit_platform.enrichment.openai_client import OpenAIEnrichmentClient
from nonprofit_platform.enrichment.profile_builder import ProfileEnricher
from nonprofit_platform.indexing.null_adapter import NullIndexAdapter
from nonprofit_platform.jobs.extract_filings import FilingExtractionWorker
from nonprofit_platform.jobs.filing_discovery import FilingDiscoveryJob
from nonprofit_platform.jobs.orchestrator import ContinuousWorkerService, OrchestratorService
from nonprofit_platform.jobs.raw_fetch import RawFilingFetchWorker
from nonprofit_platform.jobs.registry_sync import RegistrySyncJob
from nonprofit_platform.jobs.update_embeddings import EmbeddingIndexWorker
from nonprofit_platform.jobs.enrich_profiles import EnrichmentWorker
from nonprofit_platform.logging import MetricsRecorder, configure_logging, get_logger
from nonprofit_platform.parsers.form990.extractor import Form990Extractor
from nonprofit_platform.storage.supabase import SupabaseStorage
from nonprofit_platform.sources.irs_filings import IrsFilingIndexClient
from nonprofit_platform.sources.irs_registry import IrsRegistryClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nonprofit data ingestion platform.")
    parser.add_argument("--env-file", default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    registry = subparsers.add_parser("registry-sync", help="Sync the registry subset.")
    registry.add_argument("--sample-limit", type=int, default=None)

    discovery = subparsers.add_parser("filing-discovery", help="Discover filings from IRS indexes.")
    discovery.add_argument("--year", type=int, action="append", dest="years")

    raw_fetch = subparsers.add_parser("raw-fetch", help="Fetch raw XML filings.")
    raw_fetch.add_argument("--batch-size", type=int, default=None)

    extract = subparsers.add_parser("extract-filings", help="Extract structured filing data.")
    extract.add_argument("--batch-size", type=int, default=None)

    enrich = subparsers.add_parser("enrich-profiles", help="Enrich profiles from narrative text.")
    enrich.add_argument("--batch-size", type=int, default=None)

    index = subparsers.add_parser("update-embeddings", help="Update downstream indexing state.")
    index.add_argument("--batch-size", type=int, default=None)

    orchestrator = subparsers.add_parser("run-orchestrator", help="Run the always-on scheduler and worker service.")
    orchestrator.add_argument("--once", action="store_true")
    orchestrator.add_argument("--worker-stage", action="append", dest="worker_stages", choices=["raw_fetch", "extract", "enrich", "index"])

    run_worker = subparsers.add_parser("run-worker", help="Run a single worker stage continuously.")
    run_worker.add_argument("--stage", required=True, choices=["raw-fetch", "extract-filings", "enrich-profiles", "update-embeddings"])
    run_worker.add_argument("--once", action="store_true")

    enqueue = subparsers.add_parser("enqueue-stage", help="Backfill a stage for specific EINs or years.")
    enqueue.add_argument("--stage", required=True)
    enqueue.add_argument("--ein", action="append", dest="eins")
    enqueue.add_argument("--year", type=int, action="append", dest="years")

    inspect = subparsers.add_parser("inspect-job", help="Inspect recent jobs by type.")
    inspect.add_argument("--job-type", required=True)
    inspect.add_argument("--limit", type=int, default=10)

    return parser


def _print_rows(rows: Iterable[dict]) -> None:
    for row in rows:
        print(json.dumps(row, default=str))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.env_file)
    configure_logging(config.logging.level, config.logging.json)
    logger = get_logger(__name__)
    metrics = MetricsRecorder()

    with Database(config.database) as database:
        repository = PipelineRepository(database)
        storage = SupabaseStorage(config.storage)
        registry_client = IrsRegistryClient(config.sources)
        filing_client = IrsFilingIndexClient(config.sources)
        extractor = Form990Extractor()
        openai_client = OpenAIEnrichmentClient(config.openai, metrics=metrics)
        enricher = ProfileEnricher(openai_client)
        index_adapter = NullIndexAdapter()
        registry_job = RegistrySyncJob(config, repository, registry_client, metrics=metrics)
        filing_discovery_job = FilingDiscoveryJob(config, repository, filing_client, metrics=metrics)
        workers = {
            "raw_fetch": RawFilingFetchWorker(config, repository, filing_client, storage, metrics=metrics),
            "extract": FilingExtractionWorker(config, repository, storage, extractor, metrics=metrics),
            "enrich": EnrichmentWorker(config, repository, enricher, metrics=metrics),
            "index": EmbeddingIndexWorker(config, repository, index_adapter, metrics=metrics),
        }

        if args.command == "registry-sync":
            result = registry_job.run(sample_limit=args.sample_limit)
            logger.info("registry sync completed", extra={"stage": "registry-sync"})
            print(json.dumps(result, default=str))
            return

        if args.command == "filing-discovery":
            result = filing_discovery_job.run(years=args.years)
            print(json.dumps(result, default=str))
            return

        if args.command == "raw-fetch":
            result = workers["raw_fetch"].run(batch_size=args.batch_size)
            print(json.dumps(result, default=str))
            return

        if args.command == "extract-filings":
            result = workers["extract"].run(batch_size=args.batch_size)
            print(json.dumps(result, default=str))
            return

        if args.command == "enrich-profiles":
            result = workers["enrich"].run(batch_size=args.batch_size)
            print(json.dumps(result, default=str))
            return

        if args.command == "update-embeddings":
            result = workers["index"].run(batch_size=args.batch_size)
            print(json.dumps(result, default=str))
            return

        if args.command == "run-orchestrator":
            service = OrchestratorService(
                config=config,
                repository=repository,
                registry_job=registry_job,
                filing_discovery_job=filing_discovery_job,
                workers=workers,
                metrics=metrics,
            )
            result = service.run(once=args.once, worker_stages=args.worker_stages)
            print(json.dumps(result, default=str))
            return

        if args.command == "run-worker":
            stage_map = {
                "raw-fetch": "raw_fetch",
                "extract-filings": "extract",
                "enrich-profiles": "enrich",
                "update-embeddings": "index",
            }
            worker = workers[stage_map[args.stage]]
            service = ContinuousWorkerService(config=config, worker=worker, repository=repository)
            result = service.run(once=args.once)
            print(json.dumps(result, default=str))
            return

        if args.command == "enqueue-stage":
            result = repository.enqueue_backfill(
                stage=args.stage,
                eins=args.eins or [],
                years=args.years or [],
                max_attempts=config.workers.max_attempts,
            )
            print(json.dumps(result, default=str))
            return

        if args.command == "inspect-job":
            rows = repository.list_jobs(args.job_type, limit=args.limit)
            _print_rows(rows)
            return

        parser.error(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
