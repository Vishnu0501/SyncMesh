"""
sync_worker.py
--------------
AI Worker Layer — concrete Kafka consumer that:

  1. Consumes CDC events from Kafka topics.
  2. INSERT / UPDATE → builds a deterministic text context payload,
     calls LLMClient to generate an embedding, upserts into Qdrant.
  3. DELETE → bypasses the LLM entirely and directly removes the
     vector from Qdrant using the record's primary key.

Rate limiting for Gemini is enforced transparently inside LLMClient
via the RateLimiter. This class stays focused on routing logic only.
"""

from __future__ import annotations

from typing import Any

from src.consumers.base_consumer import BaseConsumer
from src.services.llm_client import LLMClient
from src.services.vector_store import VectorStore
from src.utils.logger import get_logger

log = get_logger(__name__)

# Operations that require embedding generation
_EMBED_OPS: frozenset[str] = frozenset({"INSERT", "UPDATE"})


class SyncWorker(BaseConsumer):
    """
    Subscribes to CDC Kafka topics and keeps Qdrant in sync in real-time.

    Parameters
    ----------
    topics : list[str]
        Kafka topics to subscribe to.
        Example: ["syncmesh.cdc.public_customer_profiles"]
    """

    def __init__(self, topics: list[str]) -> None:
        super().__init__(topics)
        self._llm    = LLMClient()
        self._vector = VectorStore()

    # ------------------------------------------------------------------
    # BaseConsumer interface
    # ------------------------------------------------------------------

    def process_event(self, event: dict[str, Any]) -> None:
        """
        Route the CDC event to the correct handler based on `operation`.

        Expected event schema (PRD §5.3):
        {
          "event_id":  "...",
          "table":     "customer_profiles",
          "operation": "INSERT" | "UPDATE" | "DELETE",
          "timestamp": 1716381002,
          "data": { "before": {...}, "after": {...} }
        }
        """
        operation = event.get("operation", "").upper()
        table     = event.get("table", "unknown")
        data      = event.get("data", {})

        log.info(
            "Processing CDC event",
            extra={
                "operation": operation,
                "table":     table,
                "event_id":  event.get("event_id"),
            },
        )

        if operation in _EMBED_OPS:
            self._handle_upsert(event, data)
        elif operation == "DELETE":
            self._handle_delete(event, data)
        else:
            log.warning(
                "Unknown operation — skipping",
                extra={"operation": operation, "event_id": event.get("event_id")},
            )

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _handle_upsert(self, event: dict[str, Any], data: dict[str, Any]) -> None:
        """
        Build a deterministic text context payload from the `after` record,
        generate a dense embedding via LLMClient, and upsert into Qdrant.
        """
        record = data.get("after") or data.get("before") or {}
        if not record:
            log.warning(
                "Upsert event has no record data — skipping",
                extra={"event_id": event.get("event_id")},
            )
            return

        # Build a stable natural-language string for embedding
        text_context = self._build_text_context(event["table"], record)

        # LLMClient internally handles provider routing + rate limiting
        embedding = self._llm.embed(text_context)

        record_id = str(record.get("id") or event["event_id"])
        self._vector.upsert(
            record_id=record_id,
            vector=embedding,
            payload={
                "table":     event["table"],
                "operation": event["operation"],
                "timestamp": event["timestamp"],
                "data":      record,
            },
        )
        log.info(
            "Vector upserted",
            extra={"record_id": record_id, "table": event["table"]},
        )

    def _handle_delete(self, event: dict[str, Any], data: dict[str, Any]) -> None:
        """
        DELETE operations bypass the LLM entirely.
        The vector is removed from Qdrant directly by primary key.
        """
        record    = data.get("before") or {}
        record_id = str(record.get("id") or event["event_id"])

        self._vector.delete(record_id=record_id)
        log.info(
            "Vector deleted",
            extra={"record_id": record_id, "table": event["table"]},
        )

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_text_context(table: str, record: dict[str, Any]) -> str:
        """
        Construct a deterministic natural-language context string.

        The string is built from sorted key=value pairs so identical records
        always produce identical text (ensuring stable embedding inputs).

        Example output:
            "Table: customer_profiles | account_status: Platinum | id: 4022"
        """
        fields = " | ".join(
            f"{k}: {v}"
            for k, v in sorted(record.items())
            if v is not None
        )
        return f"Table: {table} | {fields}"
