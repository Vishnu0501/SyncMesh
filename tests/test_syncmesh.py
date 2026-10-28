"""
tests/test_syncmesh.py
----------------------
Unit + integration smoke tests for SyncMesh.

What is tested (no live infra required — all external I/O is mocked):
  1. Settings loader        — env vars are parsed into the right types.
  2. kafka_config helpers   — topic derivation and config dict shapes.
  3. logger                 — correct handler/formatter attached.
  4. WALListener._build_event — canonical event schema output.
  5. SyncWorker._build_text_context — deterministic text payload.
  6. SyncWorker.process_event — correct routing (upsert vs delete).
  7. RateLimiter             — allow / deny logic with a fake Redis.
  8. LLMClient (Ollama path) — correct HTTP call and response parsing.
  9. LLMClient (Gemini path) — correct rate-limit pre-check then API call.
 10. VectorStore             — upsert / delete / search delegates to client.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

# ---------------------------------------------------------------------------
# Make project root importable without installing the package
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Stub heavy optional dependencies so tests can run in a minimal venv
# (psycopg2, confluent_kafka, qdrant_client, google.generativeai, redis, httpx)

def _make_stub(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


# ── psycopg2 ──────────────────────────────────────────────────────────────
psycopg2_stub = _make_stub("psycopg2")
psycopg2_stub.connect = MagicMock()
psycopg2_extras_stub = _make_stub("psycopg2.extras")
psycopg2_extras_stub.LogicalReplicationConnection = MagicMock
psycopg2_stub.extras = psycopg2_extras_stub
psycopg2_errors_stub = _make_stub("psycopg2.errors")
psycopg2_errors_stub.DuplicateObject = Exception
psycopg2_stub.errors = psycopg2_errors_stub

# ── confluent_kafka ────────────────────────────────────────────────────────
confluent_stub = _make_stub("confluent_kafka")
confluent_stub.Producer       = MagicMock
confluent_stub.Consumer       = MagicMock
confluent_stub.KafkaError     = MagicMock()
confluent_stub.KafkaError._PARTITION_EOF = -191
confluent_stub.KafkaException = Exception
confluent_stub.Message        = MagicMock

# ── qdrant_client ──────────────────────────────────────────────────────────
qdrant_stub = _make_stub("qdrant_client")
qdrant_stub.QdrantClient = MagicMock

qdrant_http_stub = _make_stub("qdrant_client.http")
qdrant_stub.http = qdrant_http_stub

qdrant_models = _make_stub("qdrant_client.http.models")
qdrant_models.PointStruct     = MagicMock
qdrant_models.PointIdsList    = MagicMock
qdrant_models.VectorParams    = MagicMock
_dist_mock                    = MagicMock()
_dist_mock.COSINE             = "Cosine"
qdrant_models.Distance        = _dist_mock
qdrant_http_stub.models       = qdrant_models

# ── redis ──────────────────────────────────────────────────────────────────
redis_stub = _make_stub("redis")
redis_stub.from_url = MagicMock()

# ── httpx ──────────────────────────────────────────────────────────────────
httpx_stub = _make_stub("httpx")
httpx_stub.post           = MagicMock()
httpx_stub.HTTPStatusError = Exception
httpx_stub.RequestError   = Exception

# ── google.generativeai ────────────────────────────────────────────────────
google_stub = _make_stub("google")
genai_stub  = _make_stub("google.generativeai")
genai_stub.configure     = MagicMock()
genai_stub.embed_content = MagicMock(return_value={"embedding": [0.1] * 768})
google_stub.generativeai = genai_stub


# ---------------------------------------------------------------------------
# Now import project modules
# ---------------------------------------------------------------------------
from config.settings import settings, LLMSettings, KafkaSettings   # noqa: E402
from config.kafka_config import cdc_topic_for_table, get_producer_config, get_consumer_config  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402
from src.cdc.wal_listener import _build_event  # noqa: E402
from src.consumers.sync_worker import SyncWorker  # noqa: E402
from src.services.rate_limiter import RateLimiter  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════════
# 1. Settings
# ═══════════════════════════════════════════════════════════════════════════
class TestSettings(unittest.TestCase):

    def test_settings_singleton_is_settings_type(self):
        from config.settings import Settings
        self.assertIsInstance(settings, Settings)

    def test_llm_settings_defaults(self):
        # Default USE_LOCAL_LLM should be True (dev-friendly)
        self.assertIsInstance(settings.llm.use_local_llm, bool)

    def test_kafka_num_partitions_is_int(self):
        self.assertIsInstance(settings.kafka.num_partitions, int)
        self.assertGreater(settings.kafka.num_partitions, 0)

    def test_qdrant_vector_size_is_positive(self):
        self.assertGreater(settings.qdrant.vector_size, 0)

    def test_redis_base_backoff_is_float(self):
        self.assertIsInstance(settings.redis.base_backoff_seconds, float)

    def test_postgres_watched_tables_is_list(self):
        self.assertIsInstance(settings.postgres.watched_tables, list)


# ═══════════════════════════════════════════════════════════════════════════
# 2. Kafka config helpers
# ═══════════════════════════════════════════════════════════════════════════
class TestKafkaConfig(unittest.TestCase):

    def test_cdc_topic_for_table_basic(self):
        topic = cdc_topic_for_table("customer_profiles")
        self.assertIn("customer_profiles", topic)
        self.assertTrue(topic.startswith("syncmesh.cdc"))

    def test_cdc_topic_for_table_normalises_dots(self):
        topic = cdc_topic_for_table("public.orders")
        # Dots in table name should become underscores
        self.assertNotIn(".orders", topic)

    def test_cdc_topic_for_table_lowercase(self):
        topic = cdc_topic_for_table("MyTable")
        self.assertEqual(topic, topic.lower())

    def test_producer_config_has_required_keys(self):
        cfg = get_producer_config()
        for key in ("bootstrap.servers", "acks", "enable.idempotence"):
            self.assertIn(key, cfg)

    def test_consumer_config_has_required_keys(self):
        cfg = get_consumer_config("test-group")
        self.assertEqual(cfg["group.id"], "test-group")
        self.assertFalse(cfg["enable.auto.commit"])

    def test_consumer_config_defaults_group(self):
        cfg = get_consumer_config()
        self.assertEqual(cfg["group.id"], settings.kafka.consumer_group)


# ═══════════════════════════════════════════════════════════════════════════
# 3. Logger
# ═══════════════════════════════════════════════════════════════════════════
class TestLogger(unittest.TestCase):

    def test_get_logger_returns_logger(self):
        log = get_logger("test.logger")
        self.assertIsInstance(log, logging.Logger)

    def test_get_logger_idempotent(self):
        log_a = get_logger("test.idempotent")
        log_b = get_logger("test.idempotent")
        self.assertIs(log_a, log_b)

    def test_logger_has_handler(self):
        log = get_logger("test.has_handler")
        self.assertGreater(len(log.handlers), 0)


# ═══════════════════════════════════════════════════════════════════════════
# 4. WAL Listener — event builder
# ═══════════════════════════════════════════════════════════════════════════
class TestBuildEvent(unittest.TestCase):

    def _make_event(self, op="UPDATE"):
        return _build_event(
            table="public.customer_profiles",
            operation=op,
            before={"id": 1, "status": "Silver"},
            after={"id": 1, "status": "Platinum"},
        )

    def test_event_has_required_keys(self):
        event = self._make_event()
        for key in ("event_id", "table", "operation", "timestamp", "data"):
            self.assertIn(key, event)

    def test_event_operation_preserved(self):
        self.assertEqual(self._make_event("INSERT")["operation"], "INSERT")
        self.assertEqual(self._make_event("DELETE")["operation"], "DELETE")

    def test_event_data_contains_before_after(self):
        event = self._make_event()
        self.assertIn("before", event["data"])
        self.assertIn("after", event["data"])

    def test_event_id_is_uuid_string(self):
        import uuid
        event = self._make_event()
        # Should not raise
        uuid.UUID(event["event_id"])

    def test_event_timestamp_is_int(self):
        event = self._make_event()
        self.assertIsInstance(event["timestamp"], int)
        self.assertGreater(event["timestamp"], 0)


# ═══════════════════════════════════════════════════════════════════════════
# 5. SyncWorker — text context builder
# ═══════════════════════════════════════════════════════════════════════════
class TestBuildTextContext(unittest.TestCase):

    def test_contains_table_name(self):
        ctx = SyncWorker._build_text_context("orders", {"id": 1, "total": 99.9})
        self.assertIn("orders", ctx)

    def test_contains_field_values(self):
        ctx = SyncWorker._build_text_context("tbl", {"id": 42, "name": "Alice"})
        self.assertIn("42", ctx)
        self.assertIn("Alice", ctx)

    def test_none_values_excluded(self):
        ctx = SyncWorker._build_text_context("tbl", {"id": 1, "notes": None})
        self.assertNotIn("notes", ctx)

    def test_deterministic_output(self):
        record = {"z_field": "z", "a_field": "a", "m_field": "m"}
        ctx_a  = SyncWorker._build_text_context("tbl", record)
        ctx_b  = SyncWorker._build_text_context("tbl", record)
        self.assertEqual(ctx_a, ctx_b)

    def test_fields_sorted_alphabetically(self):
        ctx = SyncWorker._build_text_context("tbl", {"z": 1, "a": 2})
        self.assertLess(ctx.index("a:"), ctx.index("z:"))


# ═══════════════════════════════════════════════════════════════════════════
# 6. SyncWorker — process_event routing
# ═══════════════════════════════════════════════════════════════════════════
class TestSyncWorkerRouting(unittest.TestCase):

    def _make_worker(self):
        """Build a SyncWorker with mocked LLMClient and VectorStore."""
        worker = SyncWorker.__new__(SyncWorker)
        worker._llm    = MagicMock()
        worker._vector = MagicMock()
        worker._llm.embed.return_value = [0.1] * 768
        return worker

    def _make_event(self, op: str, record: dict | None = None) -> dict:
        record = record or {"id": 1, "status": "Gold"}
        data   = {
            "INSERT": {"before": None, "after": record},
            "UPDATE": {"before": {"id": 1, "status": "Silver"}, "after": record},
            "DELETE": {"before": record, "after": None},
        }[op]
        return {
            "event_id":  "test-uuid",
            "table":     "public.customer_profiles",
            "operation": op,
            "timestamp": 1716381002,
            "data":      data,
        }

    def test_insert_calls_llm_and_upsert(self):
        worker = self._make_worker()
        worker.process_event(self._make_event("INSERT"))
        worker._llm.embed.assert_called_once()
        worker._vector.upsert.assert_called_once()
        worker._vector.delete.assert_not_called()

    def test_update_calls_llm_and_upsert(self):
        worker = self._make_worker()
        worker.process_event(self._make_event("UPDATE"))
        worker._llm.embed.assert_called_once()
        worker._vector.upsert.assert_called_once()

    def test_delete_skips_llm_calls_delete(self):
        worker = self._make_worker()
        worker.process_event(self._make_event("DELETE"))
        worker._llm.embed.assert_not_called()
        worker._vector.delete.assert_called_once()

    def test_upsert_payload_contains_table(self):
        worker = self._make_worker()
        worker.process_event(self._make_event("INSERT"))
        call_kwargs = worker._vector.upsert.call_args[1]
        self.assertIn("table", call_kwargs.get("payload", {}))

    def test_unknown_op_does_not_call_llm_or_vector(self):
        worker = self._make_worker()
        event  = self._make_event("INSERT")
        event["operation"] = "TRUNCATE"
        worker.process_event(event)
        worker._llm.embed.assert_not_called()
        worker._vector.upsert.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# 7. RateLimiter
# ═══════════════════════════════════════════════════════════════════════════
class TestRateLimiter(unittest.TestCase):

    def _make_limiter(self, rpm_limit=10, tpm_limit=1000, rpm_current=0, tpm_current=0):
        """Build a RateLimiter with a fully mocked Redis client."""
        limiter = RateLimiter.__new__(RateLimiter)
        limiter._rpm_limit = rpm_limit
        limiter._tpm_limit = tpm_limit

        mock_redis = MagicMock()
        # First pipeline call returns current usage
        pipe1 = MagicMock()
        pipe1.execute.return_value = [str(rpm_current), str(tpm_current)]
        # Second pipeline call is the increment — just return success
        pipe2 = MagicMock()
        pipe2.execute.return_value = [rpm_current + 1, 1, tpm_current + 100, 1]

        mock_redis.pipeline.side_effect = [pipe1, pipe2]
        limiter._redis = mock_redis
        return limiter

    def test_allows_when_under_limit(self):
        limiter = self._make_limiter(rpm_current=5)
        allowed, reason = limiter.check_and_consume(rpm_cost=1, tpm_cost=100)
        self.assertTrue(allowed)
        self.assertEqual(reason, "ok")

    def test_denies_when_rpm_exhausted(self):
        limiter = self._make_limiter(rpm_limit=10, rpm_current=10)
        allowed, reason = limiter.check_and_consume(rpm_cost=1, tpm_cost=0)
        self.assertFalse(allowed)
        self.assertIn("RPM", reason)

    def test_denies_when_tpm_exhausted(self):
        limiter = self._make_limiter(tpm_limit=100, tpm_current=95)
        allowed, reason = limiter.check_and_consume(rpm_cost=1, tpm_cost=10)
        self.assertFalse(allowed)
        self.assertIn("TPM", reason)

    def test_current_window_returns_int(self):
        from src.services.rate_limiter import RateLimiter as RL
        self.assertIsInstance(RL._current_window(), int)

    def test_ttl_positive(self):
        from src.services.rate_limiter import RateLimiter as RL
        self.assertGreater(RL._ttl(), 0)


# ═══════════════════════════════════════════════════════════════════════════
# 8. LLMClient — Ollama adapter
# ═══════════════════════════════════════════════════════════════════════════
class TestLLMClientOllama(unittest.TestCase):

    def _make_client(self):
        """Build an LLMClient wired to Ollama (USE_LOCAL_LLM=true)."""
        from src.services.llm_client import LLMClient
        client = LLMClient.__new__(LLMClient)
        client._cfg = MagicMock()
        client._cfg.use_local_llm = True
        client._cfg.ollama_host = "http://localhost:11434"
        client._cfg.ollama_embedding_model = "nomic-embed-text"
        client._rate_limiter = None
        return client

    def test_embed_ollama_returns_vector(self):
        client   = self._make_client()
        fake_vec = [0.5] * 768
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"embedding": fake_vec}
        mock_resp.raise_for_status = MagicMock()

        with patch("src.services.llm_client.httpx.post", return_value=mock_resp) as mock_post:
            result = client._embed_ollama("hello world")

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["json"]["model"], "nomic-embed-text")
        self.assertEqual(result, fake_vec)

    def test_embed_routes_to_ollama_when_local(self):
        client = self._make_client()
        with patch.object(client, "_embed_ollama", return_value=[0.1] * 5) as mock_ollama:
            result = client.embed("test text")
        mock_ollama.assert_called_once_with("test text")
        self.assertEqual(result, [0.1] * 5)


# ═══════════════════════════════════════════════════════════════════════════
# 9. LLMClient — Gemini adapter
# ═══════════════════════════════════════════════════════════════════════════
class TestLLMClientGemini(unittest.TestCase):

    def _make_client(self):
        from src.services.llm_client import LLMClient
        client = LLMClient.__new__(LLMClient)
        client._cfg = MagicMock()
        client._cfg.use_local_llm = False
        client._cfg.gemini_embedding_model = "text-embedding-004"
        client._cfg.gemini_api_key = "fake-key"
        # Mock rate limiter — always allows
        client._rate_limiter = MagicMock()
        client._rate_limiter.check_and_consume.return_value = (True, "ok")
        return client

    def test_embed_gemini_calls_api(self):
        import google.generativeai as genai
        client   = self._make_client()
        fake_vec = [0.2] * 768
        genai.embed_content = MagicMock(return_value={"embedding": fake_vec})

        result = client._embed_gemini("some text")
        genai.embed_content.assert_called_once()
        self.assertEqual(result, fake_vec)

    def test_embed_routes_to_gemini_when_cloud(self):
        client = self._make_client()
        with patch.object(client, "_embed_gemini", return_value=[0.9] * 5) as mock_gemini:
            result = client.embed("cloud text")
        mock_gemini.assert_called_once_with("cloud text")

    def test_embed_gemini_respects_rate_limit_deny(self):
        from src.services.llm_client import LLMClient
        import time

        client = self._make_client()
        # Rate limiter denies on first 2 attempts then allows
        client._rate_limiter.check_and_consume.side_effect = [
            (False, "RPM limit exhausted (60/60)"),
            (True, "ok"),
        ]

        import google.generativeai as genai
        genai.embed_content = MagicMock(return_value={"embedding": [0.3] * 768})

        # Patch out sleep to keep tests fast
        with patch("src.services.llm_client.time.sleep"):
            result = client._embed_gemini("rate limited text")

        self.assertEqual(len(result), 768)
        self.assertEqual(client._rate_limiter.check_and_consume.call_count, 2)

    def test_token_estimate_non_zero(self):
        from src.services.llm_client import LLMClient
        self.assertGreater(LLMClient._estimate_token_count("hello world"), 0)

    def test_token_estimate_proportional(self):
        from src.services.llm_client import LLMClient
        short = LLMClient._estimate_token_count("hi")
        long_ = LLMClient._estimate_token_count("hi " * 100)
        self.assertLess(short, long_)


# ═══════════════════════════════════════════════════════════════════════════
# 10. VectorStore
# ═══════════════════════════════════════════════════════════════════════════
class TestVectorStore(unittest.TestCase):

    def _make_store(self):
        """Build a VectorStore with a mocked Qdrant client."""
        from src.services.vector_store import VectorStore

        store = VectorStore.__new__(VectorStore)
        store._cfg        = settings.qdrant
        store._collection = "syncmesh_vectors"

        mock_client = MagicMock()
        # Simulate collection already existing
        existing_col = MagicMock()
        existing_col.name = "syncmesh_vectors"
        mock_client.get_collections.return_value.collections = [existing_col]
        store._client = mock_client
        return store

    def test_upsert_calls_qdrant_upsert(self):
        store = self._make_store()
        store.upsert("42", [0.1] * 768, payload={"table": "orders"})
        store._client.upsert.assert_called_once()

    def test_delete_calls_qdrant_delete(self):
        store = self._make_store()
        store.delete("42")
        store._client.delete.assert_called_once()

    def test_search_returns_list(self):
        store  = self._make_store()
        hit    = MagicMock()
        hit.id, hit.score, hit.payload = 1, 0.95, {"table": "orders"}
        store._client.search.return_value = [hit]
        results = store.search([0.1] * 768, top_k=5)
        self.assertIsInstance(results, list)
        self.assertEqual(results[0]["score"], 0.95)

    def test_to_qdrant_id_numeric(self):
        from src.services.vector_store import VectorStore
        self.assertEqual(VectorStore._to_qdrant_id("42"), 42)

    def test_to_qdrant_id_uuid_string(self):
        from src.services.vector_store import VectorStore
        uid = "550e8400-e29b-41d4-a716-446655440000"
        self.assertEqual(VectorStore._to_qdrant_id(uid), uid)

    def test_ensure_collection_creates_when_absent(self):
        from src.services.vector_store import VectorStore
        store = self._make_store()
        # Simulate collection NOT existing
        store._client.get_collections.return_value.collections = []
        store._ensure_collection()
        store._client.create_collection.assert_called_once()

    def test_ensure_collection_skips_when_present(self):
        store = self._make_store()
        store._ensure_collection()
        store._client.create_collection.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    unittest.main(verbosity=2)
