from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Any


@dataclass(slots=True)
class IndexDocument:
    document_id: str
    text: str
    metadata: dict[str, Any]
    content_hash: str


@dataclass(slots=True)
class IndexResult:
    adapter_name: str
    status: str
    payload: dict[str, Any]


class IndexAdapter(Protocol):
    adapter_name: str

    def upsert_document(self, document: IndexDocument) -> IndexResult:
        ...
