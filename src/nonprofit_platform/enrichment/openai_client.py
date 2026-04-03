from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from openai import OpenAI

from nonprofit_platform.config import OpenAISettings
from nonprofit_platform.logging import MetricsRecorder


@dataclass(slots=True)
class EnrichmentResult:
    model_name: str
    prompt_version: str
    input_hash: str
    response_text: str
    response_json: dict[str, Any]
    prompt_payload: dict[str, Any]
    token_input: int
    token_output: int
    estimated_cost_usd: float


class OpenAIEnrichmentClient:
    def __init__(self, settings: OpenAISettings, metrics: MetricsRecorder | None = None) -> None:
        self.settings = settings
        self.metrics = metrics or MetricsRecorder()
        self._client: OpenAI | None = None
        if settings.api_key:
            self._client = OpenAI(api_key=settings.api_key, timeout=settings.request_timeout_seconds)

    def enrich_narrative(self, payload: dict[str, Any]) -> EnrichmentResult:
        serialized = json.dumps(payload, sort_keys=True)
        input_hash = sha256(serialized.encode("utf-8")).hexdigest()
        if not self.settings.enabled or not self._client:
            fallback = self._fallback_response(payload)
            return EnrichmentResult(
                model_name=self.settings.model,
                prompt_version=self.settings.prompt_version,
                input_hash=input_hash,
                response_text=json.dumps(fallback),
                response_json=fallback,
                prompt_payload=payload,
                token_input=0,
                token_output=0,
                estimated_cost_usd=0.0,
            )

        response = self._client.responses.create(
            model=self.settings.model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You enrich nonprofit narrative text only. "
                        "Return compact JSON with keys: summary, cause_tags, program_highlights, "
                        "location_hints, fit_notes, confidence_notes."
                    ),
                },
                {
                    "role": "user",
                    "content": serialized,
                },
            ],
        )
        text = response.output_text
        data = json.loads(text)
        usage = getattr(response, "usage", None)
        token_input = getattr(usage, "input_tokens", 0) if usage else 0
        token_output = getattr(usage, "output_tokens", 0) if usage else 0
        estimated_cost = 0.0
        self.metrics.increment("openai.requests", model=self.settings.model)
        return EnrichmentResult(
            model_name=self.settings.model,
            prompt_version=self.settings.prompt_version,
            input_hash=input_hash,
            response_text=text,
            response_json=data,
            prompt_payload=payload,
            token_input=token_input,
            token_output=token_output,
            estimated_cost_usd=estimated_cost,
        )

    def _fallback_response(self, payload: dict[str, Any]) -> dict[str, Any]:
        mission = payload.get("mission") or ""
        accomplishments = payload.get("program_accomplishments") or []
        summary = mission or "No narrative mission available."
        return {
            "summary": summary[:500],
            "cause_tags": [],
            "program_highlights": accomplishments[:5],
            "location_hints": [payload.get("state")] if payload.get("state") else [],
            "fit_notes": [],
            "confidence_notes": "Generated without API call because enrichment is disabled.",
        }
