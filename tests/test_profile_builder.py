from nonprofit_platform.config import OpenAISettings
from nonprofit_platform.enrichment.openai_client import OpenAIEnrichmentClient
from nonprofit_platform.enrichment.profile_builder import ProfileEnricher


def test_profile_builder_uses_fallback_when_enrichment_disabled() -> None:
    client = OpenAIEnrichmentClient(OpenAISettings(api_key="", enabled=False))
    enricher = ProfileEnricher(client)

    built = enricher.build_profile(
        {
            "id": 1,
            "object_id": "2024_0001",
            "ein": "123456789",
            "organization_name": "Sample Helping Hands",
            "state": "IL",
            "mission_text": "Provide emergency housing.",
            "narrative_sections": {
                "mission": "Provide emergency housing.",
                "program_accomplishments": ["Operated a shelter."],
            },
            "total_revenue": 100.0,
            "total_expenses": 80.0,
            "total_assets": 200.0,
            "employee_count": 3,
            "updated_at": None,
            "extracted_at": None,
        }
    )

    assert built.profile_row["profile_summary"] == "Provide emergency housing."
    assert built.profile_row["program_highlights"] == ["Operated a shelter."]
    assert built.profile_row["location_hints"] == ["IL"]
    assert built.enrichment_run_row["estimated_cost_usd"] == 0.0
