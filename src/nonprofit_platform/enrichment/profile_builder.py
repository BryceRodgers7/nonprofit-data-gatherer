from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nonprofit_platform.enrichment.openai_client import EnrichmentResult, OpenAIEnrichmentClient


@dataclass(slots=True)
class BuiltProfile:
    source_payload: dict[str, Any]
    enrichment: EnrichmentResult
    profile_row: dict[str, Any]
    enrichment_run_row: dict[str, Any]


class ProfileEnricher:
    output_schema_version = "v1"

    def __init__(self, client: OpenAIEnrichmentClient) -> None:
        self.client = client

    def build_profile(self, normalized_filing: dict[str, Any]) -> BuiltProfile:
        narrative_sections = normalized_filing.get("narrative_sections") or {}
        structured = {
            "ein": normalized_filing.get("ein"),
            "object_id": normalized_filing.get("object_id"),
            "organization_name": normalized_filing.get("organization_name"),
            "city": normalized_filing.get("city"),
            "state": normalized_filing.get("state"),
            "mission": narrative_sections.get("mission") or normalized_filing.get("mission_text"),
            "program_accomplishments": narrative_sections.get("program_accomplishments") or [],
            "financial_summary": {
                "total_revenue": normalized_filing.get("total_revenue"),
                "total_expenses": normalized_filing.get("total_expenses"),
                "total_assets": normalized_filing.get("total_assets"),
            },
        }
        enrichment = self.client.enrich_narrative(structured)
        response = enrichment.response_json
        profile_row = {
            "object_id": normalized_filing["object_id"],
            "ein": normalized_filing.get("ein"),
            "normalized_filing_id": normalized_filing["id"],
            "enrichment_run_id": None,
            "prompt_version": enrichment.prompt_version,
            "model_name": enrichment.model_name,
            "profile_summary": response.get("summary"),
            "cause_tags": response.get("cause_tags") or [],
            "program_highlights": response.get("program_highlights") or [],
            "location_hints": response.get("location_hints") or [],
            "fit_notes": response.get("fit_notes") or [],
            "derived_profile": {
                "mission_text": normalized_filing.get("mission_text"),
                "total_revenue": normalized_filing.get("total_revenue"),
                "employee_count": normalized_filing.get("employee_count"),
                "confidence_notes": response.get("confidence_notes"),
            },
            "source_text_hash": enrichment.input_hash,
            "is_current": True,
            "output_schema_version": self.output_schema_version,
        }
        enrichment_run_row = {
            "object_id": normalized_filing["object_id"],
            "normalized_filing_id": normalized_filing["id"],
            "prompt_version": enrichment.prompt_version,
            "model_name": enrichment.model_name,
            "input_hash": enrichment.input_hash,
            "request_payload": enrichment.prompt_payload,
            "response_payload": response,
            "token_input": enrichment.token_input,
            "token_output": enrichment.token_output,
            "estimated_cost_usd": enrichment.estimated_cost_usd,
            "run_status": "completed",
            "error_message": None,
            "completed_at": normalized_filing.get("updated_at") or normalized_filing.get("extracted_at"),
        }
        return BuiltProfile(structured, enrichment, profile_row, enrichment_run_row)
