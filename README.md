# Nonprofit Data Gatherer

Python scaffold for a modular nonprofit data ingestion and enrichment platform built around IRS bulk sources, Postgres/Supabase, durable job orchestration, and independently scalable workers.

## What This First Version Does
- syncs a small subset of IRS EO BMF and related status datasets into canonical organization tables
- discovers a small subset of Form 990 filings from IRS index CSVs
- downloads raw XML artifacts to Supabase Storage
- extracts directly available XML fields into normalized relational and JSONB storage without using an LLM
- enriches narrative sections with OpenAI into versioned nonprofit profile outputs
- marks profiles for downstream search/vector indexing through a placeholder adapter
- uses durable `job_runs` rows with claim/lease/retry/dead-letter behavior so each stage can scale independently

## Repository Layout
- `src/nonprofit_platform/config.py`: environment-driven configuration
- `src/nonprofit_platform/db/`: Postgres connection and repository methods
- `src/nonprofit_platform/storage/`: raw artifact storage adapter for Supabase Storage
- `src/nonprofit_platform/sources/`: IRS registry and filing source clients
- `src/nonprofit_platform/parsers/form990/`: XML extraction logic
- `src/nonprofit_platform/enrichment/`: OpenAI narrative-only enrichment
- `src/nonprofit_platform/indexing/`: placeholder indexing adapter and interface
- `src/nonprofit_platform/jobs/`: singleton jobs and horizontally scalable worker entrypoints
- `sql/001_initial_schema.sql`: initial schema definition, not auto-applied
- `tests/`: parser and workflow-focused unit tests

## Architecture
```mermaid
flowchart TD
  orchestrator[OrchestratorService] --> registrySync[RegistrySync]
  orchestrator --> filingDiscovery[FilingDiscovery]
  orchestrator --> rawFetch[RawArtifactFetcher]
  orchestrator --> extractor[StructuredExtractor]
  orchestrator --> enrichmentWorker[EnrichmentWorker]
  orchestrator --> embeddingWorker[EmbeddingIndexingWorker]
  registrySync[RegistrySync] --> organizations[organizations]
  registrySync --> organizationStatus[organization_status]
  filingDiscovery[FilingDiscovery] --> filingIndex[filing_index]
  filingIndex --> rawFetch[RawArtifactFetcher]
  rawFetch --> rawFilings[raw_filings]
  rawFilings --> extractor[StructuredExtractor]
  extractor --> normalizedFilings[normalized_filings]
  normalizedFilings --> enrichmentWorker[EnrichmentWorker]
  enrichmentWorker --> nonprofitProfiles[nonprofit_profiles]
  enrichmentWorker --> enrichmentRuns[enrichment_runs]
  nonprofitProfiles --> embeddingWorker[EmbeddingIndexingWorker]
  embeddingWorker --> embeddingsStatus[embeddings_index_status]
```

## Job Model
Singleton/scheduled:
- `registry-sync`
- `filing-discovery`

Horizontally scalable claim-based workers:
- `raw-fetch`
- `extract-filings`
- `enrich-profiles`
- `update-embeddings`

Always-on orchestration:
- `run-orchestrator`: long-running service that schedules singleton jobs, recovers stale leases, and polls claim-based workers
- `run-worker`: long-running single-stage worker loop for one claim-based stage

Worker claims use `FOR UPDATE SKIP LOCKED`, bounded retries, dead-letter transitions, and periodic lease heartbeats while work is active.

```mermaid
flowchart TD
  pending[PendingJob] --> claim[WorkerClaimsChunk]
  claim --> processing[Processing]
  processing --> heartbeat[HeartbeatRenewal]
  heartbeat --> processing
  processing --> success[Completed]
  processing --> retry[RetryScheduled]
  retry --> pending
  processing --> deadLetter[DeadLettered]
  processing --> leaseExpired[LeaseExpired]
  leaseExpired --> recover[StaleLeaseRecovery]
  recover --> pending
```

### Lease Heartbeats
- each claimed job starts a lightweight heartbeat loop while `process_job()` is running
- the heartbeat renews `claimed_until` before the lease expires
- if the worker crashes and heartbeats stop, the orchestrator's stale-lease recovery pass moves expired `processing` jobs back to `pending`
- heartbeat timing should stay comfortably below `JOB_CLAIM_LEASE_SECONDS`

## Schema
The initial schema script creates these system-of-record tables:
- `organizations`
- `organization_status`
- `filing_index`
- `raw_filings`
- `normalized_filings`
- `nonprofit_profiles`
- `enrichment_runs`
- `embeddings_index_status`
- `job_runs`

Design notes:
- raw XML is stored durably in Supabase Storage, while structured and semi-structured outputs are stored in Postgres
- `normalized_filings` keeps both extracted relational fields and JSONB narrative/section payloads
- `nonprofit_profiles` and `enrichment_runs` include prompt/model/input versioning so reruns do not destroy prior outputs
- `job_runs` provides per-stage idempotency keys, leases, progress payloads, retries, and dead-letter state

## Local Setup
1. Create a Python 3.11+ virtual environment.
2. Install dependencies with `pip install -e .[dev]`.
3. Create a `.env` file with values like:

```env
DATABASE_DSN=postgresql://postgres:postgres@localhost:5432/postgres
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
SUPABASE_STORAGE_BUCKET=raw-filings
SUPABASE_STORAGE_PREFIX=irs-filings
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1-mini
OPENAI_PROMPT_VERSION=v1
ENABLE_STORAGE=true
ENABLE_ENRICHMENT=false
ENABLE_INDEXING=false
REGISTRY_SAMPLE_LIMIT=250
FILING_SAMPLE_LIMIT=25
CLAIM_BATCH_SIZE=20
JOB_MAX_ATTEMPTS=5
JOB_CLAIM_LEASE_SECONDS=600
JOB_HEARTBEAT_INTERVAL_SECONDS=30
ORCHESTRATOR_POLL_INTERVAL_SECONDS=5
REGISTRY_SYNC_INTERVAL_SECONDS=3600
FILING_DISCOVERY_INTERVAL_SECONDS=1800
STALE_CLAIM_RECOVERY_INTERVAL_SECONDS=60
FILING_YEARS_BACKFILL_START=2024
```

4. Review `sql/001_initial_schema.sql` and apply it manually to the target database when ready.

## Running Stages Independently
Registry subset sync:
```bash
nonprofit-platform registry-sync --sample-limit 100
```

Filing discovery for a small year slice:
```bash
nonprofit-platform filing-discovery --year 2024
```

Claim and fetch raw filings:
```bash
nonprofit-platform raw-fetch --batch-size 10
```

Extract normalized filing data:
```bash
nonprofit-platform extract-filings --batch-size 10
```

Enrich profiles:
```bash
nonprofit-platform enrich-profiles --batch-size 5
```

Update downstream indexing status:
```bash
nonprofit-platform update-embeddings --batch-size 10
```

Backfill selected stages:
```bash
nonprofit-platform enqueue-stage --stage raw_fetch --ein 123456789 --year 2024
nonprofit-platform enqueue-stage --stage enrich --ein 123456789 --year 2024
```

Inspect durable job history:
```bash
nonprofit-platform inspect-job --job-type raw_fetch --limit 20
```

## Running Long-Lived Services
Run the combined scheduler/orchestrator service:
```bash
nonprofit-platform run-orchestrator
```

Run the orchestrator once for a single polling pass:
```bash
nonprofit-platform run-orchestrator --once
```

Run only selected worker loops under the orchestrator:
```bash
nonprofit-platform run-orchestrator --worker-stage raw_fetch --worker-stage extract
```

Run a single stage continuously:
```bash
nonprofit-platform run-worker --stage raw-fetch
```

For development, a common flow is:
- use one-shot commands to verify specific stages
- switch to `run-orchestrator` once the schema and credentials are configured
- use `run-worker` when you want to scale one stage independently

## Extraction Rules
The extractor intentionally does not use an LLM for fields already present in XML. First-pass extracted fields include:
- EIN, filer name, address, tax year, filing year, tax period, form type
- organization type and select status-style fields when present
- total revenue, total expenses, total assets, liabilities, net assets
- mission text, employee count, volunteer count
- contributions, program-service revenue, investment income
- officer rows and program accomplishment text

Narrative sections are separated into `narrative_sections` for later enrichment.

## Enrichment Rules
The OpenAI worker only consumes narrative text and selected structured context. First-pass outputs:
- profile summary
- cause tags
- program highlights
- geographic hints
- fit notes
- derived profile JSON for downstream recommendation/search use

When `ENABLE_ENRICHMENT=false`, the client falls back to a deterministic local summary path for development smoke tests.

## Qdrant Readiness
`src/nonprofit_platform/indexing/adapter.py` defines the indexing contract. The initial implementation uses `NullIndexAdapter`, but the code is structured so a Qdrant adapter can later replace it without changing worker orchestration or status tracking.

## Tests
Run:

```bash
pytest
```
