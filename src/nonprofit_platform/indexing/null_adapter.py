from __future__ import annotations

from nonprofit_platform.indexing.adapter import IndexDocument, IndexResult


class NullIndexAdapter:
    adapter_name = "null"

    def upsert_document(self, document: IndexDocument) -> IndexResult:
        return IndexResult(
            adapter_name=self.adapter_name,
            status="indexed",
            payload={
                "document_id": document.document_id,
                "content_hash": document.content_hash,
                "text_length": len(document.text),
                "metadata": document.metadata,
            },
        )
