"""
llm_client.py
-------------
Adapter Layer — routes embedding requests to either:
  - Ollama  (local, open-source)  when USE_LOCAL_LLM=true
  - Gemini  (cloud API)           when USE_LOCAL_LLM=false

The adapter pattern ensures the rest of the system only ever calls
`embed(text)` and is completely unaware of which backend is active.

Rate limiting for Gemini is enforced via RateLimiter before every API
call.  Exhausted quotas trigger exponential back-off up to the configured
MAX_RETRY_ATTEMPTS.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
import google.generativeai as genai

from config.settings import settings
from src.services.rate_limiter import RateLimiter
from src.utils.logger import get_logger

log = get_logger(__name__)


class LLMClient:
    """
    Unified embedding interface with automatic provider selection.

    The active provider is chosen once at construction time based on
    the USE_LOCAL_LLM environment flag, making the switch cost-free
    at runtime.

    Usage
    -----
    client = LLMClient()
    vector: list[float] = client.embed("customer id=4022 status=Platinum")
    """

    def __init__(self) -> None:
        self._cfg          = settings.llm
        self._rate_limiter = None if self._cfg.use_local_llm else RateLimiter()

        if not self._cfg.use_local_llm:
            # Configure the Gemini SDK once at construction time
            if not self._cfg.gemini_api_key:
                raise ValueError(
                    "GEMINI_API_KEY is required when USE_LOCAL_LLM=false"
                )
            genai.configure(api_key=self._cfg.gemini_api_key)

        log.info(
            "LLMClient initialised",
            extra={
                "provider": "ollama" if self._cfg.use_local_llm else "gemini",
                "model": (
                    self._cfg.ollama_embedding_model
                    if self._cfg.use_local_llm
                    else self._cfg.gemini_embedding_model
                ),
            },
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def embed(self, text: str) -> list[float]:
        """
        Generate a dense embedding vector for the given text string.

        Parameters
        ----------
        text : str
            Natural-language context payload built by SyncWorker.

        Returns
        -------
        list[float]
            Dense embedding vector. Dimension depends on the active model:
            - nomic-embed-text  (Ollama) → 768 dims
            - text-embedding-004 (Gemini) → 768 dims
        """
        if self._cfg.use_local_llm:
            return self._embed_ollama(text)
        return self._embed_gemini(text)

    # ------------------------------------------------------------------
    # Provider adapters
    # ------------------------------------------------------------------

    def _embed_ollama(self, text: str) -> list[float]:
        """
        Call the Ollama REST API to generate an embedding.

        Endpoint: POST {OLLAMA_HOST}/api/embeddings
        Payload:  { "model": "...", "prompt": "..." }
        Response: { "embedding": [float, ...] }
        """
        url = f"{self._cfg.ollama_host}/api/embeddings"
        payload: dict[str, Any] = {
            "model":  self._cfg.ollama_embedding_model,
            "prompt": text,
        }
        try:
            response = httpx.post(url, json=payload, timeout=30.0)
            response.raise_for_status()
            embedding: list[float] = response.json()["embedding"]
            log.debug(
                "Ollama embedding generated",
                extra={"dim": len(embedding), "model": self._cfg.ollama_embedding_model},
            )
            return embedding
        except httpx.HTTPStatusError as exc:
            log.error(
                "Ollama HTTP error",
                extra={"status": exc.response.status_code, "error": str(exc)},
            )
            raise
        except httpx.RequestError as exc:
            log.error("Ollama connection error", extra={"error": str(exc)})
            raise

    def _embed_gemini(self, text: str) -> list[float]:
        """
        Call the Gemini Embeddings API with Redis-backed rate limiting.

        Retries up to REDIS_MAX_RETRY_ATTEMPTS with exponential back-off
        when either the RPM or TPM quota is exhausted.
        """
        max_attempts = settings.redis.max_retry_attempts
        base_backoff = settings.redis.base_backoff_seconds

        for attempt in range(1, max_attempts + 1):
            token_estimate = self._estimate_token_count(text)

            # --- Check Redis rate-limit budget before making the call ---
            allowed, reason = self._rate_limiter.check_and_consume(
                rpm_cost=1,
                tpm_cost=token_estimate,
            )

            if not allowed:
                wait = base_backoff * (2 ** (attempt - 1))
                log.warning(
                    "Rate limit hit — backing off",
                    extra={
                        "reason":       reason,
                        "wait_seconds": wait,
                        "attempt":      attempt,
                        "max_attempts": max_attempts,
                    },
                )
                time.sleep(wait)
                continue

            # --- Make the Gemini API call ---
            try:
                result = genai.embed_content(
                    model=self._cfg.gemini_embedding_model,
                    content=text,
                    task_type="RETRIEVAL_DOCUMENT",
                )
                embedding: list[float] = result["embedding"]
                log.debug(
                    "Gemini embedding generated",
                    extra={
                        "dim":   len(embedding),
                        "model": self._cfg.gemini_embedding_model,
                    },
                )
                return embedding

            except Exception as exc:
                log.error(
                    "Gemini API error",
                    extra={"error": str(exc), "attempt": attempt},
                )
                if attempt >= max_attempts:
                    raise
                wait = base_backoff * (2 ** attempt)
                log.info(f"Retrying in {wait:.1f}s…")
                time.sleep(wait)

        raise RuntimeError(
            f"Gemini embed failed after {max_attempts} attempts"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _estimate_token_count(text: str) -> int:
        """
        Rough token count estimate (1 token ≈ 4 characters).
        Used to pre-check TPM budget before making the Gemini API call,
        avoiding the network round-trip for quota checks.
        """
        return max(1, len(text) // 4)
