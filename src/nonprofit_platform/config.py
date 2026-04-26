from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _load_dotenv_files(dotenv_path: str | Path | None) -> None:
    """Populate os.environ from .env files. Never overrides keys already set in the environment."""
    if dotenv_path:
        load_dotenv(dotenv_path=Path(dotenv_path), override=False)
        return
    # Repo-root .env (works when the process cwd is not the project directory)
    repo_root = Path(__file__).resolve().parents[2]
    repo_env = repo_root / ".env"
    if repo_env.is_file():
        load_dotenv(dotenv_path=repo_env, override=False)
    # Default: search upward from cwd for .env (python-dotenv behavior)
    load_dotenv(override=False)


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


@dataclass(slots=True)
class DatabaseSettings:
    dsn: str
    application_name: str = "nonprofit-platform"


@dataclass(slots=True)
class StorageSettings:
    project_url: str
    service_role_key: str
    bucket: str
    base_prefix: str = "irs-filings"
    enabled: bool = True


@dataclass(slots=True)
class OpenAISettings:
    api_key: str
    model: str = "gpt-4.1-mini"
    prompt_version: str = "v1"
    request_timeout_seconds: float = 120.0
    enabled: bool = False


@dataclass(slots=True)
class WorkerSettings:
    registry_sample_limit: int = 250
    filing_sample_limit: int = 50
    claim_batch_size: int = 20
    enrich_batch_size: int = 5
    fetch_concurrency: int = 4
    extract_concurrency: int = 4
    enrich_concurrency: int = 2
    index_concurrency: int = 4
    max_attempts: int = 5
    claim_lease_seconds: int = 600
    heartbeat_interval_seconds: float = 30.0
    orchestrator_poll_interval_seconds: float = 5.0
    registry_sync_interval_seconds: float = 3600.0
    filing_discovery_interval_seconds: float = 1800.0
    stale_claim_recovery_interval_seconds: float = 60.0
    openai_requests_per_minute: int = 60
    years_backfill_start: int = 2024


@dataclass(slots=True)
class FeatureFlags:
    enable_enrichment: bool = False
    enable_indexing: bool = False
    dry_run: bool = False


@dataclass(slots=True)
class LoggingSettings:
    level: str = "INFO"
    json: bool = False


@dataclass(slots=True)
class SourceSettings:
    eo_bmf_region_url: str = "https://www.irs.gov/pub/irs-soi/eo1.csv"
    publication_78_url: str = "https://apps.irs.gov/pub/epostcard/data-download-pub78.zip"
    revocation_url: str = "https://apps.irs.gov/pub/epostcard/data-download-revocation.zip"
    form_990_base_url: str = "https://apps.irs.gov/pub/epostcard/990/xml"


@dataclass(slots=True)
class AppConfig:
    database: DatabaseSettings
    storage: StorageSettings
    openai: OpenAISettings
    workers: WorkerSettings
    features: FeatureFlags
    logging: LoggingSettings
    sources: SourceSettings


def load_config(dotenv_path: str | Path | None = None) -> AppConfig:
    _load_dotenv_files(dotenv_path)

    database = DatabaseSettings(
        dsn=os.getenv("DATABASE_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"),
        application_name=os.getenv("DB_APPLICATION_NAME", "nonprofit-platform"),
    )
    storage = StorageSettings(
        project_url=os.getenv("SUPABASE_URL", ""),
        service_role_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""),
        bucket=os.getenv("SUPABASE_STORAGE_BUCKET", "raw-filings"),
        base_prefix=os.getenv("SUPABASE_STORAGE_PREFIX", "irs-filings"),
        enabled=_get_bool("ENABLE_STORAGE", True),
    )
    openai_settings = OpenAISettings(
        api_key=os.getenv("OPENAI_API_KEY", ""),
        model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        prompt_version=os.getenv("OPENAI_PROMPT_VERSION", "v1"),
        request_timeout_seconds=_get_float("OPENAI_TIMEOUT_SECONDS", 120.0),
        enabled=_get_bool("ENABLE_ENRICHMENT", False),
    )
    workers = WorkerSettings(
        registry_sample_limit=_get_int("REGISTRY_SAMPLE_LIMIT", 250),
        filing_sample_limit=_get_int("FILING_SAMPLE_LIMIT", 50),
        claim_batch_size=_get_int("CLAIM_BATCH_SIZE", 20),
        enrich_batch_size=_get_int("ENRICH_BATCH_SIZE", 5),
        fetch_concurrency=_get_int("FETCH_CONCURRENCY", 4),
        extract_concurrency=_get_int("EXTRACT_CONCURRENCY", 4),
        enrich_concurrency=_get_int("ENRICH_CONCURRENCY", 2),
        index_concurrency=_get_int("INDEX_CONCURRENCY", 4),
        max_attempts=_get_int("JOB_MAX_ATTEMPTS", 5),
        claim_lease_seconds=_get_int("JOB_CLAIM_LEASE_SECONDS", 600),
        heartbeat_interval_seconds=_get_float("JOB_HEARTBEAT_INTERVAL_SECONDS", 30.0),
        orchestrator_poll_interval_seconds=_get_float("ORCHESTRATOR_POLL_INTERVAL_SECONDS", 5.0),
        registry_sync_interval_seconds=_get_float("REGISTRY_SYNC_INTERVAL_SECONDS", 3600.0),
        filing_discovery_interval_seconds=_get_float("FILING_DISCOVERY_INTERVAL_SECONDS", 1800.0),
        stale_claim_recovery_interval_seconds=_get_float("STALE_CLAIM_RECOVERY_INTERVAL_SECONDS", 60.0),
        openai_requests_per_minute=_get_int("OPENAI_REQUESTS_PER_MINUTE", 60),
        years_backfill_start=_get_int("FILING_YEARS_BACKFILL_START", 2024),
    )
    flags = FeatureFlags(
        enable_enrichment=_get_bool("ENABLE_ENRICHMENT", False),
        enable_indexing=_get_bool("ENABLE_INDEXING", False),
        dry_run=_get_bool("DRY_RUN", False),
    )
    logging = LoggingSettings(
        level=os.getenv("LOG_LEVEL", "INFO"),
        json=_get_bool("LOG_JSON", False),
    )
    sources = SourceSettings(
        eo_bmf_region_url=os.getenv("IRS_EO_BMF_URL", "https://www.irs.gov/pub/irs-soi/eo1.csv"),
        publication_78_url=os.getenv(
            "IRS_PUBLICATION_78_URL",
            "https://apps.irs.gov/pub/epostcard/data-download-pub78.zip",
        ),
        revocation_url=os.getenv(
            "IRS_REVOCATION_URL",
            "https://apps.irs.gov/pub/epostcard/data-download-revocation.zip",
        ),
        form_990_base_url=os.getenv("IRS_990_BASE_URL", "https://apps.irs.gov/pub/epostcard/990/xml"),
    )
    return AppConfig(
        database=database,
        storage=storage,
        openai=openai_settings,
        workers=workers,
        features=flags,
        logging=logging,
        sources=sources,
    )
