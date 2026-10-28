"""
rate_limiter.py
---------------
Distributed token-bucket rate limiter backed by Redis.

Implements two independent sliding-window counters per 60-second epoch:
  - RPM  (Requests Per Minute)
  - TPM  (Tokens   Per Minute)

Both buckets must have capacity before a Gemini API call is permitted.
If either is exhausted, `check_and_consume` returns (False, reason) and
the caller backs off before retrying.

Redis keys auto-expire after 60 s so quotas reset naturally each minute
without requiring any explicit cleanup or cron job.

Design choices
--------------
- We use a Redis pipeline (MULTI/EXEC) to read-then-increment atomically,
  preventing race conditions in a multi-worker deployment.
- TTL is set to `seconds_until_next_window + 5` as a small safety margin
  so keys do not expire mid-window under heavy load.
"""

from __future__ import annotations

import time
from typing import Tuple

import redis

from config.settings import settings
from src.utils.logger import get_logger

log = get_logger(__name__)

# Redis key templates — one bucket per 60-second window
_RPM_KEY = "syncmesh:rate:rpm:{window}"
_TPM_KEY = "syncmesh:rate:tpm:{window}"


class RateLimiter:
    """
    Distributed token-bucket rate limiter using Redis atomic pipelines.

    Parameters
    ----------
    rpm_limit : int | None
        Override max requests per minute (default: settings.llm.gemini_rpm_limit).
    tpm_limit : int | None
        Override max tokens per minute   (default: settings.llm.gemini_tpm_limit).
    """

    def __init__(
        self,
        rpm_limit: int | None = None,
        tpm_limit: int | None = None,
    ) -> None:
        self._redis     = redis.from_url(settings.redis.url, decode_responses=True)
        self._rpm_limit = rpm_limit or settings.llm.gemini_rpm_limit
        self._tpm_limit = tpm_limit or settings.llm.gemini_tpm_limit
        log.info(
            "RateLimiter initialised",
            extra={"rpm_limit": self._rpm_limit, "tpm_limit": self._tpm_limit},
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_and_consume(
        self,
        rpm_cost: int = 1,
        tpm_cost: int = 0,
    ) -> Tuple[bool, str]:
        """
        Atomically check quota availability and consume if allowed.

        Parameters
        ----------
        rpm_cost : int
            Number of requests to deduct (typically 1 per API call).
        tpm_cost : int
            Estimated token count to deduct from the TPM bucket.

        Returns
        -------
        (allowed, reason)
            allowed = True  → call may proceed.
            allowed = False → budget exhausted; reason explains which limit.
        """
        window  = self._current_window()
        rpm_key = _RPM_KEY.format(window=window)
        tpm_key = _TPM_KEY.format(window=window)
        ttl     = self._ttl()

        # --- Atomic read of both counters via pipeline ---
        pipe = self._redis.pipeline(transaction=True)
        pipe.get(rpm_key)
        pipe.get(tpm_key)
        current_rpm_raw, current_tpm_raw = pipe.execute()

        current_rpm = int(current_rpm_raw or 0)
        current_tpm = int(current_tpm_raw or 0)

        # --- Guard: check RPM budget ---
        if current_rpm + rpm_cost > self._rpm_limit:
            log.debug(
                "RPM budget exhausted",
                extra={"current": current_rpm, "limit": self._rpm_limit},
            )
            return False, f"RPM limit exhausted ({current_rpm}/{self._rpm_limit})"

        # --- Guard: check TPM budget ---
        if tpm_cost and (current_tpm + tpm_cost > self._tpm_limit):
            log.debug(
                "TPM budget exhausted",
                extra={"current": current_tpm, "limit": self._tpm_limit},
            )
            return False, f"TPM limit exhausted ({current_tpm}/{self._tpm_limit})"

        # --- Consume: atomic increment + refresh TTL ---
        pipe = self._redis.pipeline(transaction=True)
        pipe.incrby(rpm_key, rpm_cost)
        pipe.expire(rpm_key, ttl)
        if tpm_cost:
            pipe.incrby(tpm_key, tpm_cost)
            pipe.expire(tpm_key, ttl)
        pipe.execute()

        return True, "ok"

    def get_usage(self) -> dict[str, int]:
        """
        Return current minute's RPM and TPM usage.
        Useful for health-check endpoints and observability dashboards.
        """
        window  = self._current_window()
        rpm     = int(self._redis.get(_RPM_KEY.format(window=window)) or 0)
        tpm     = int(self._redis.get(_TPM_KEY.format(window=window)) or 0)
        return {
            "rpm":       rpm,
            "rpm_limit": self._rpm_limit,
            "tpm":       tpm,
            "tpm_limit": self._tpm_limit,
        }

    def reset(self) -> None:
        """
        Force-reset the current window's counters.
        Intended for use in tests and emergency operator intervention only.
        """
        window  = self._current_window()
        rpm_key = _RPM_KEY.format(window=window)
        tpm_key = _TPM_KEY.format(window=window)
        self._redis.delete(rpm_key, tpm_key)
        log.info("Rate-limit counters reset", extra={"window": window})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _current_window() -> int:
        """
        Return the current 60-second window bucket identifier (unix minute).
        All requests within the same wall-clock minute share a bucket.
        """
        return int(time.time()) // 60

    @staticmethod
    def _ttl() -> int:
        """
        Seconds until the next 60-second window, plus a 5-second safety margin.
        This prevents keys from expiring mid-window under heavy throughput.
        """
        remaining = 60 - (int(time.time()) % 60)
        return remaining + 5
