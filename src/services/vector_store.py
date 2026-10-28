"""
vector_store.py
---------------
Qdrant Adapter — provides a clean, thin interface over the Qdrant Python
client for all vector memory operations required by SyncMesh:

  - Collection bootstrap  : create the collection on first run if absent
  - upsert()              : insert or overwrite a vector + metadata payload
  - delete()              : remove a vector by its record primary key
  - search()              : nearest-neighbour query (utility for RAG consumers)

The collection is created automatically on startup with Cosine distance
(most appropriate for embedding similarity) and is safe to call multiple
times (idempotent bootstrap).
"""

from __future__ import annotations

from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from config.settings import settings
from src.utils.logger import get_logger

log = get_logger(__name__)


class VectorStore:
    """
    Qdrant adapter used by SyncWorker to persist and delete embeddings.

    Parameters
    ----------
    host : str | None
        Qdrant host override (defaults to settings.qdrant.host).
    port : int | None
        Qdrant port override (defaults to settings.qdrant.port).
    """

    def __init__(self, host: str | None = None, port: int | None = None) -> None:
        self._cfg        = settings.qdrant
        self._collection = self._cfg.collection_name
        self._client     = QdrantClient(
            host=host or self._cfg.host,
            port=port or self._cfg.port,
        )
        self._ensure_collection()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upsert(
        self,
        record_id: str,
        vector: list[float],
        payload: dict[str, Any] | None = None,
    ) -> None:
        """
        Insert or overwrite a vector point in the Qdrant collection.

        Parameters
        ----------
        record_id : str
            Unique identifier for the point (maps to the DB primary key).
        vector : list[float]
            Dense embedding vector produced by LLMClient.
        payload : dict | None
            Arbitrary JSON metadata stored alongside the vector.
            Includes table, operation, timestamp, and the raw record data.
        """
        point = qmodels.PointStruct(
            id=self._to_qdrant_id(record_id),
            vector=vector,
            payload=payload or {},
        )
        self._client.upsert(
            collection_name=self._collection,
            points=[point],
            wait=True,  # Synchronous write acknowledgement from Qdrant
        )
        log.debug(
            "Qdrant upsert OK",
            extra={"id": record_id, "collection": self._collection},
        )

    def delete(self, record_id: str) -> None:
        """
        Remove a vector point from Qdrant by its record ID.

        Called directly for DELETE CDC events — no LLM call is made.

        Parameters
        ----------
        record_id : str
            The primary key of the deleted database record.
        """
        self._client.delete(
            collection_name=self._collection,
            points_selector=qmodels.PointIdsList(
                points=[self._to_qdrant_id(record_id)]
            ),
            wait=True,
        )
        log.debug(
            "Qdrant delete OK",
            extra={"id": record_id, "collection": self._collection},
        )

    def search(
        self,
        query_vector: list[float],
        top_k: int = 5,
        score_threshold: float = 0.7,
    ) -> list[dict[str, Any]]:
        """
        Nearest-neighbour vector search — utility for downstream RAG consumers.

        Parameters
        ----------
        query_vector : list[float]
            Dense query embedding.
        top_k : int
            Maximum number of results to return.
        score_threshold : float
            Minimum cosine similarity score for a result to be included.

        Returns
        -------
        list[dict]
            Each item: {"id": ..., "score": float, "payload": dict}
        """
        results = self._client.search(
            collection_name=self._collection,
            query_vector=query_vector,
            limit=top_k,
            score_threshold=score_threshold,
        )
        log.debug(
            "Qdrant search",
            extra={"results": len(results), "top_k": top_k},
        )
        return [
            {"id": hit.id, "score": hit.score, "payload": hit.payload}
            for hit in results
        ]

    def collection_info(self) -> dict[str, Any]:
        """
        Return basic collection statistics.
        Useful for health-check endpoints.
        """
        info = self._client.get_collection(self._collection)
        return {
            "name":          self._collection,
            "vectors_count": info.vectors_count,
            "status":        str(info.status),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_collection(self) -> None:
        """
        Create the Qdrant collection with Cosine distance metric if absent.

        Cosine similarity is the standard metric for text embedding similarity.
        Safe to call on every startup — is a no-op if the collection exists.
        """
        existing = {c.name for c in self._client.get_collections().collections}
        if self._collection in existing:
            log.info(
                "Qdrant collection ready",
                extra={"collection": self._collection},
            )
            return

        self._client.create_collection(
            collection_name=self._collection,
            vectors_config=qmodels.VectorParams(
                size=self._cfg.vector_size,
                distance=qmodels.Distance.COSINE,
            ),
        )
        log.info(
            "Qdrant collection created",
            extra={
                "collection":  self._collection,
                "vector_size": self._cfg.vector_size,
                "distance":    "COSINE",
            },
        )

    @staticmethod
    def _to_qdrant_id(record_id: str) -> int | str:
        """
        Qdrant point IDs must be unsigned integers or UUID strings.
        Numeric string IDs are coerced to int for storage efficiency.
        Non-numeric strings (UUIDs, slugs) are used as-is.
        """
        try:
            return int(record_id)
        except ValueError:
            return record_id
