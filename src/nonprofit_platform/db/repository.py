from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any

from psycopg.types.json import Jsonb

from nonprofit_platform.db.connection import Database


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


class PipelineRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def upsert_organizations(self, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        query = """
        insert into organizations (
            ein, legal_name, doing_business_as_name, city, state, zip_code, country,
            ruling_month, subsection_code, foundation_code, classification_code,
            affiliation_code, deductibility_code, organization_type, exempt_status_code,
            tax_period, ntee_code, sort_name, latest_registry_source, latest_registry_updated_at,
            raw_registry_payload
        )
        values (
            %(ein)s, %(legal_name)s, %(doing_business_as_name)s, %(city)s, %(state)s, %(zip_code)s, %(country)s,
            %(ruling_month)s, %(subsection_code)s, %(foundation_code)s, %(classification_code)s,
            %(affiliation_code)s, %(deductibility_code)s, %(organization_type)s, %(exempt_status_code)s,
            %(tax_period)s, %(ntee_code)s, %(sort_name)s, %(latest_registry_source)s, %(latest_registry_updated_at)s,
            %(raw_registry_payload)s
        )
        on conflict (ein) do update
        set legal_name = excluded.legal_name,
            doing_business_as_name = excluded.doing_business_as_name,
            city = excluded.city,
            state = excluded.state,
            zip_code = excluded.zip_code,
            country = excluded.country,
            ruling_month = excluded.ruling_month,
            subsection_code = excluded.subsection_code,
            foundation_code = excluded.foundation_code,
            classification_code = excluded.classification_code,
            affiliation_code = excluded.affiliation_code,
            deductibility_code = excluded.deductibility_code,
            organization_type = excluded.organization_type,
            exempt_status_code = excluded.exempt_status_code,
            tax_period = excluded.tax_period,
            ntee_code = excluded.ntee_code,
            sort_name = excluded.sort_name,
            latest_registry_source = excluded.latest_registry_source,
            latest_registry_updated_at = excluded.latest_registry_updated_at,
            raw_registry_payload = excluded.raw_registry_payload,
            updated_at = now()
        """
        prepared = []
        for row in rows:
            prepared.append(
                {
                    **row,
                    "raw_registry_payload": Jsonb(row.get("raw_registry_payload", {})),
                }
            )
        with self.database.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, prepared)
        return len(rows)

    def insert_organization_status(self, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        query = """
        insert into organization_status (
            ein, source_name, status_code, status_label, status_value,
            effective_date, observed_at, is_current, payload
        )
        values (
            %(ein)s, %(source_name)s, %(status_code)s, %(status_label)s, %(status_value)s,
            %(effective_date)s, %(observed_at)s, %(is_current)s, %(payload)s
        )
        on conflict (ein, source_name, status_code, observed_at) do nothing
        """
        prepared = [{**row, "payload": Jsonb(row.get("payload", {}))} for row in rows]
        with self.database.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, prepared)
        return len(rows)

    def upsert_filing_index(self, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        query = """
        insert into filing_index (
            object_id, return_id, ein, tax_year, filing_year, tax_period, form_type,
            taxpayer_name, submitted_on, xml_url, index_url, source_updated_at, filing_status,
            payload
        )
        values (
            %(object_id)s, %(return_id)s, %(ein)s, %(tax_year)s, %(filing_year)s, %(tax_period)s, %(form_type)s,
            %(taxpayer_name)s, %(submitted_on)s, %(xml_url)s, %(index_url)s, %(source_updated_at)s, %(filing_status)s,
            %(payload)s
        )
        on conflict (object_id) do update
        set return_id = excluded.return_id,
            ein = excluded.ein,
            tax_year = excluded.tax_year,
            filing_year = excluded.filing_year,
            tax_period = excluded.tax_period,
            form_type = excluded.form_type,
            taxpayer_name = excluded.taxpayer_name,
            submitted_on = excluded.submitted_on,
            xml_url = excluded.xml_url,
            index_url = excluded.index_url,
            source_updated_at = excluded.source_updated_at,
            filing_status = excluded.filing_status,
            payload = excluded.payload,
            updated_at = now()
        """
        prepared = [{**row, "payload": Jsonb(row.get("payload", {}))} for row in rows]
        with self.database.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, prepared)
        return len(rows)

    def enqueue_job(
        self,
        job_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        max_attempts: int,
        run_after: datetime | None = None,
        priority: int = 100,
    ) -> None:
        query = """
        insert into job_runs (
            job_type, status, payload, idempotency_key, priority,
            attempt_count, max_attempts, run_after
        )
        values (
            %(job_type)s, 'pending', %(payload)s, %(idempotency_key)s, %(priority)s,
            0, %(max_attempts)s, %(run_after)s
        )
        on conflict (job_type, idempotency_key) do update
        set payload = excluded.payload,
            priority = least(job_runs.priority, excluded.priority),
            run_after = least(job_runs.run_after, excluded.run_after),
            updated_at = now()
        """
        with self.database.transaction() as conn:
            conn.execute(
                query,
                {
                    "job_type": job_type,
                    "payload": Jsonb(payload),
                    "idempotency_key": idempotency_key,
                    "priority": priority,
                    "max_attempts": max_attempts,
                    "run_after": run_after or utc_now(),
                },
            )

    def enqueue_many_jobs(
        self,
        job_type: str,
        items: Sequence[dict[str, Any]],
        max_attempts: int,
        priority: int = 100,
    ) -> int:
        for item in items:
            self.enqueue_job(
                job_type=job_type,
                payload=item["payload"],
                idempotency_key=item["idempotency_key"],
                max_attempts=max_attempts,
                priority=item.get("priority", priority),
                run_after=item.get("run_after"),
            )
        return len(items)

    def claim_jobs(
        self,
        job_type: str,
        batch_size: int,
        worker_id: str,
        lease_seconds: int,
    ) -> list[dict[str, Any]]:
        query = """
        with next_jobs as (
            select id
            from job_runs
            where job_type = %(job_type)s
              and status in ('pending', 'retry_scheduled')
              and run_after <= now()
              and (claimed_until is null or claimed_until < now())
              and dead_lettered_at is null
            order by priority asc, run_after asc, id asc
            for update skip locked
            limit %(batch_size)s
        )
        update job_runs
        set status = 'processing',
            claimed_by = %(worker_id)s,
            claimed_at = now(),
            claimed_until = now() + (%(lease_seconds)s || ' seconds')::interval,
            updated_at = now()
        where id in (select id from next_jobs)
        returning *
        """
        with self.database.transaction() as conn:
            rows = conn.execute(
                query,
                {
                    "job_type": job_type,
                    "batch_size": batch_size,
                    "worker_id": worker_id,
                    "lease_seconds": lease_seconds,
                },
            ).fetchall()
        return list(rows)

    def heartbeat_job(self, job_id: int, lease_seconds: int, worker_id: str) -> bool:
        with self.database.transaction() as conn:
            result = conn.execute(
                """
                update job_runs
                set claimed_until = now() + (%(lease_seconds)s || ' seconds')::interval,
                    updated_at = now()
                where id = %(job_id)s
                  and status = 'processing'
                  and claimed_by = %(worker_id)s
                """,
                {"job_id": job_id, "lease_seconds": lease_seconds, "worker_id": worker_id},
            )
        return result.rowcount > 0

    def recover_stale_jobs(self, job_types: Sequence[str] | None = None, limit: int = 500) -> int:
        params: dict[str, Any] = {"limit": limit, "job_types": list(job_types or [])}
        type_filter = ""
        if job_types:
            type_filter = "and job_type = any(%(job_types)s)"
        query = f"""
        with stale_jobs as (
            select id
            from job_runs
            where status = 'processing'
              and claimed_until is not null
              and claimed_until < now()
              {type_filter}
            order by claimed_until asc, id asc
            limit %(limit)s
            for update skip locked
        )
        update job_runs
        set status = 'pending',
            claimed_by = null,
            claimed_at = null,
            claimed_until = null,
            progress_payload = coalesce(progress_payload, '{{}}'::jsonb) || jsonb_build_object(
                'lease_recovered_at', now()
            ),
            updated_at = now()
        where id in (select id from stale_jobs)
        """
        with self.database.transaction() as conn:
            result = conn.execute(query, params)
        return result.rowcount

    def complete_job(
        self,
        job_id: int,
        result_payload: dict[str, Any] | None = None,
        worker_id: str | None = None,
    ) -> bool:
        where_clause = "where id = %(job_id)s"
        params: dict[str, Any] = {
            "job_id": job_id,
            "result_payload": Jsonb(result_payload or {}),
        }
        if worker_id is not None:
            where_clause += " and status = 'processing' and claimed_by = %(worker_id)s"
            params["worker_id"] = worker_id
        query = f"""
        update job_runs
        set status = 'completed',
            result_payload = %(result_payload)s,
            claimed_by = null,
            claimed_at = null,
            claimed_until = null,
            completed_at = now(),
            updated_at = now()
        {where_clause}
        """
        with self.database.transaction() as conn:
            result = conn.execute(query, params)
        return result.rowcount > 0

    def fail_job(
        self,
        job_id: int,
        error_message: str,
        retryable: bool = True,
        retry_delay_seconds: int = 300,
        worker_id: str | None = None,
    ) -> bool:
        where_clause = "where id = %(job_id)s"
        params: dict[str, Any] = {
            "job_id": job_id,
            "last_error": error_message,
            "retryable": retryable,
            "retry_delay_seconds": retry_delay_seconds,
        }
        if worker_id is not None:
            where_clause += " and status = 'processing' and claimed_by = %(worker_id)s"
            params["worker_id"] = worker_id
        query = f"""
        update job_runs
        set attempt_count = attempt_count + 1,
            last_error = %(last_error)s,
            last_error_at = now(),
            status = case
                when %(retryable)s is true and attempt_count + 1 < max_attempts then 'retry_scheduled'
                else 'dead_letter'
            end,
            dead_lettered_at = case
                when %(retryable)s is true and attempt_count + 1 < max_attempts then null
                else now()
            end,
            run_after = case
                when %(retryable)s is true and attempt_count + 1 < max_attempts then now() + (%(retry_delay_seconds)s || ' seconds')::interval
                else run_after
            end,
            claimed_by = null,
            claimed_at = null,
            claimed_until = null,
            updated_at = now()
        {where_clause}
        """
        with self.database.transaction() as conn:
            result = conn.execute(query, params)
        return result.rowcount > 0

    def touch_job_progress(self, job_id: int, payload: dict[str, Any]) -> None:
        with self.database.transaction() as conn:
            conn.execute(
                """
                update job_runs
                set progress_payload = coalesce(progress_payload, '{}'::jsonb) || %(payload)s,
                    updated_at = now()
                where id = %(job_id)s
                """,
                {"job_id": job_id, "payload": Jsonb(payload)},
            )

    def upsert_raw_filing(self, row: dict[str, Any]) -> int:
        query = """
        insert into raw_filings (
            object_id, return_id, ein, xml_url, index_url, storage_provider,
            storage_bucket, storage_path, artifact_checksum, content_length,
            content_type, fetched_at, fetch_status, metadata, raw_metadata
        )
        values (
            %(object_id)s, %(return_id)s, %(ein)s, %(xml_url)s, %(index_url)s, %(storage_provider)s,
            %(storage_bucket)s, %(storage_path)s, %(artifact_checksum)s, %(content_length)s,
            %(content_type)s, %(fetched_at)s, %(fetch_status)s, %(metadata)s, %(raw_metadata)s
        )
        on conflict (object_id) do update
        set return_id = excluded.return_id,
            ein = excluded.ein,
            xml_url = excluded.xml_url,
            index_url = excluded.index_url,
            storage_provider = excluded.storage_provider,
            storage_bucket = excluded.storage_bucket,
            storage_path = excluded.storage_path,
            artifact_checksum = excluded.artifact_checksum,
            content_length = excluded.content_length,
            content_type = excluded.content_type,
            fetched_at = excluded.fetched_at,
            fetch_status = excluded.fetch_status,
            metadata = excluded.metadata,
            raw_metadata = excluded.raw_metadata,
            updated_at = now()
        returning id
        """
        with self.database.transaction() as conn:
            row = conn.execute(
                query,
                {
                    **row,
                    "metadata": Jsonb(row.get("metadata", {})),
                    "raw_metadata": Jsonb(row.get("raw_metadata", {})),
                },
            ).fetchone()
        return int(row["id"])

    def get_raw_filing_by_object_id(self, object_id: str) -> dict[str, Any] | None:
        with self.database.transaction() as conn:
            row = conn.execute(
                "select * from raw_filings where object_id = %(object_id)s",
                {"object_id": object_id},
            ).fetchone()
        return dict(row) if row else None

    def upsert_normalized_filing(self, row: dict[str, Any]) -> int:
        query = """
        insert into normalized_filings (
            object_id, return_id, ein, tax_year, filing_year, tax_period, form_type,
            organization_name, address_line_1, address_line_2, city, state, zip_code, country,
            organization_type, deductibility_status, public_charity_status,
            total_revenue, total_expenses, total_assets, total_liabilities, net_assets,
            mission_text, employee_count, volunteer_count, contributions_revenue,
            program_service_revenue, investment_income, program_service_accomplishments,
            officers, extracted_sections, narrative_sections, parser_version, extracted_at
        )
        values (
            %(object_id)s, %(return_id)s, %(ein)s, %(tax_year)s, %(filing_year)s, %(tax_period)s, %(form_type)s,
            %(organization_name)s, %(address_line_1)s, %(address_line_2)s, %(city)s, %(state)s, %(zip_code)s, %(country)s,
            %(organization_type)s, %(deductibility_status)s, %(public_charity_status)s,
            %(total_revenue)s, %(total_expenses)s, %(total_assets)s, %(total_liabilities)s, %(net_assets)s,
            %(mission_text)s, %(employee_count)s, %(volunteer_count)s, %(contributions_revenue)s,
            %(program_service_revenue)s, %(investment_income)s, %(program_service_accomplishments)s,
            %(officers)s, %(extracted_sections)s, %(narrative_sections)s, %(parser_version)s, %(extracted_at)s
        )
        on conflict (object_id, parser_version) do update
        set return_id = excluded.return_id,
            ein = excluded.ein,
            tax_year = excluded.tax_year,
            filing_year = excluded.filing_year,
            tax_period = excluded.tax_period,
            form_type = excluded.form_type,
            organization_name = excluded.organization_name,
            address_line_1 = excluded.address_line_1,
            address_line_2 = excluded.address_line_2,
            city = excluded.city,
            state = excluded.state,
            zip_code = excluded.zip_code,
            country = excluded.country,
            organization_type = excluded.organization_type,
            deductibility_status = excluded.deductibility_status,
            public_charity_status = excluded.public_charity_status,
            total_revenue = excluded.total_revenue,
            total_expenses = excluded.total_expenses,
            total_assets = excluded.total_assets,
            total_liabilities = excluded.total_liabilities,
            net_assets = excluded.net_assets,
            mission_text = excluded.mission_text,
            employee_count = excluded.employee_count,
            volunteer_count = excluded.volunteer_count,
            contributions_revenue = excluded.contributions_revenue,
            program_service_revenue = excluded.program_service_revenue,
            investment_income = excluded.investment_income,
            program_service_accomplishments = excluded.program_service_accomplishments,
            officers = excluded.officers,
            extracted_sections = excluded.extracted_sections,
            narrative_sections = excluded.narrative_sections,
            extracted_at = excluded.extracted_at,
            updated_at = now()
        returning id
        """
        prepared = {
            **row,
            "officers": Jsonb(row.get("officers", [])),
            "extracted_sections": Jsonb(row.get("extracted_sections", {})),
            "narrative_sections": Jsonb(row.get("narrative_sections", {})),
        }
        with self.database.transaction() as conn:
            record = conn.execute(query, prepared).fetchone()
        return int(record["id"])

    def get_latest_normalized_filing(self, object_id: str) -> dict[str, Any] | None:
        with self.database.transaction() as conn:
            row = conn.execute(
                """
                select *
                from normalized_filings
                where object_id = %(object_id)s
                order by extracted_at desc, id desc
                limit 1
                """,
                {"object_id": object_id},
            ).fetchone()
        return dict(row) if row else None

    def insert_enrichment_run(self, row: dict[str, Any]) -> int:
        query = """
        insert into enrichment_runs (
            object_id, normalized_filing_id, prompt_version, model_name, input_hash,
            request_payload, response_payload, token_input, token_output, estimated_cost_usd,
            run_status, error_message, completed_at
        )
        values (
            %(object_id)s, %(normalized_filing_id)s, %(prompt_version)s, %(model_name)s, %(input_hash)s,
            %(request_payload)s, %(response_payload)s, %(token_input)s, %(token_output)s, %(estimated_cost_usd)s,
            %(run_status)s, %(error_message)s, %(completed_at)s
        )
        returning id
        """
        prepared = {
            **row,
            "request_payload": Jsonb(row.get("request_payload", {})),
            "response_payload": Jsonb(row.get("response_payload", {})),
        }
        with self.database.transaction() as conn:
            result = conn.execute(query, prepared).fetchone()
        return int(result["id"])

    def upsert_nonprofit_profile(self, row: dict[str, Any]) -> int:
        query = """
        insert into nonprofit_profiles (
            object_id, ein, normalized_filing_id, enrichment_run_id, prompt_version,
            model_name, profile_summary, cause_tags, program_highlights, location_hints,
            fit_notes, derived_profile, source_text_hash, is_current, output_schema_version
        )
        values (
            %(object_id)s, %(ein)s, %(normalized_filing_id)s, %(enrichment_run_id)s, %(prompt_version)s,
            %(model_name)s, %(profile_summary)s, %(cause_tags)s, %(program_highlights)s, %(location_hints)s,
            %(fit_notes)s, %(derived_profile)s, %(source_text_hash)s, %(is_current)s, %(output_schema_version)s
        )
        on conflict (object_id, prompt_version, model_name, source_text_hash) do update
        set normalized_filing_id = excluded.normalized_filing_id,
            enrichment_run_id = excluded.enrichment_run_id,
            profile_summary = excluded.profile_summary,
            cause_tags = excluded.cause_tags,
            program_highlights = excluded.program_highlights,
            location_hints = excluded.location_hints,
            fit_notes = excluded.fit_notes,
            derived_profile = excluded.derived_profile,
            is_current = excluded.is_current,
            output_schema_version = excluded.output_schema_version,
            updated_at = now()
        returning id
        """
        prepared = {
            **row,
            "cause_tags": Jsonb(row.get("cause_tags", [])),
            "program_highlights": Jsonb(row.get("program_highlights", [])),
            "location_hints": Jsonb(row.get("location_hints", [])),
            "fit_notes": Jsonb(row.get("fit_notes", [])),
            "derived_profile": Jsonb(row.get("derived_profile", {})),
        }
        with self.database.transaction() as conn:
            result = conn.execute(query, prepared).fetchone()
            conn.execute(
                """
                update nonprofit_profiles
                set is_current = false, updated_at = now()
                where object_id = %(object_id)s
                  and id <> %(current_id)s
                  and prompt_version = %(prompt_version)s
                """,
                {
                    "object_id": row["object_id"],
                    "current_id": result["id"],
                    "prompt_version": row["prompt_version"],
                },
            )
        return int(result["id"])

    def upsert_embeddings_status(self, row: dict[str, Any]) -> int:
        query = """
        insert into embeddings_index_status (
            object_id, nonprofit_profile_id, adapter_name, document_hash,
            status, indexed_at, attempt_count, last_error, payload
        )
        values (
            %(object_id)s, %(nonprofit_profile_id)s, %(adapter_name)s, %(document_hash)s,
            %(status)s, %(indexed_at)s, %(attempt_count)s, %(last_error)s, %(payload)s
        )
        on conflict (object_id, adapter_name, document_hash) do update
        set nonprofit_profile_id = excluded.nonprofit_profile_id,
            status = excluded.status,
            indexed_at = excluded.indexed_at,
            attempt_count = excluded.attempt_count,
            last_error = excluded.last_error,
            payload = excluded.payload,
            updated_at = now()
        returning id
        """
        with self.database.transaction() as conn:
            result = conn.execute(query, {**row, "payload": Jsonb(row.get("payload", {}))}).fetchone()
        return int(result["id"])

    def list_jobs(self, job_type: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.database.transaction() as conn:
            rows = conn.execute(
                """
                select *
                from job_runs
                where job_type = %(job_type)s
                order by id desc
                limit %(limit)s
                """,
                {"job_type": job_type, "limit": limit},
            ).fetchall()
        return [dict(row) for row in rows]

    def enqueue_backfill(
        self,
        stage: str,
        eins: Sequence[str],
        years: Sequence[int],
        max_attempts: int,
    ) -> dict[str, Any]:
        items = []
        if stage == "registry_sync":
            for ein in eins:
                items.append(
                    {
                        "payload": {"ein": ein},
                        "idempotency_key": f"registry:{ein}",
                    }
                )
        elif stage in {"raw_fetch", "extract", "enrich", "index"}:
            for ein in eins or ["all"]:
                for year in years or [0]:
                    idempotency = f"{stage}:{ein}:{year}"
                    items.append(
                        {
                            "payload": {"ein": ein, "year": year},
                            "idempotency_key": idempotency,
                        }
                    )
        else:
            raise ValueError(f"Unsupported backfill stage: {stage}")
        count = self.enqueue_many_jobs(stage, items, max_attempts=max_attempts)
        return {"stage": stage, "enqueued": count}

    def get_normalized_filings_for_enrichment(self, batch_size: int) -> list[dict[str, Any]]:
        with self.database.transaction() as conn:
            rows = conn.execute(
                """
                select nf.*
                from normalized_filings nf
                left join nonprofit_profiles np
                  on np.object_id = nf.object_id
                 and np.is_current = true
                where np.id is null
                order by nf.extracted_at asc, nf.id asc
                limit %(batch_size)s
                """,
                {"batch_size": batch_size},
            ).fetchall()
        return [dict(row) for row in rows]

    def get_profiles_pending_index(self, batch_size: int) -> list[dict[str, Any]]:
        with self.database.transaction() as conn:
            rows = conn.execute(
                """
                select np.*
                from nonprofit_profiles np
                left join embeddings_index_status eis
                  on eis.nonprofit_profile_id = np.id
                 and eis.adapter_name = 'null'
                 and eis.status = 'indexed'
                where np.is_current = true
                  and eis.id is null
                order by np.updated_at asc, np.id asc
                limit %(batch_size)s
                """,
                {"batch_size": batch_size},
            ).fetchall()
        return [dict(row) for row in rows]

    def get_filing_index_by_object_id(self, object_id: str) -> dict[str, Any] | None:
        with self.database.transaction() as conn:
            row = conn.execute(
                "select * from filing_index where object_id = %(object_id)s",
                {"object_id": object_id},
            ).fetchone()
        return dict(row) if row else None

    def mark_filing_status(self, object_id: str, filing_status: str) -> None:
        with self.database.transaction() as conn:
            conn.execute(
                """
                update filing_index
                set filing_status = %(filing_status)s,
                    updated_at = now()
                where object_id = %(object_id)s
                """,
                {"object_id": object_id, "filing_status": filing_status},
            )

    def dump_json(self, value: dict[str, Any]) -> str:
        return json.dumps(value, default=str)

    def make_worker_id(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4()}"
