"""
settings.py
-----------
Central configuration loader for SyncMesh.
Reads all environment variables from .env and exposes them as typed
settings objects so every module has a single, consistent source of truth.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env from the project root (two levels up from config/)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


# ---------------------------------------------------------------------------
# LLM / Embedding settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LLMSettings:
    use_local_llm: bool = field(
        default_factory=lambda: os.getenv("USE_LOCAL_LLM", "true").lower() == "true"
    )
    # Ollama (local)
    ollama_host: str = field(
        default_factory=lambda: os.getenv("OLLAMA_HOST", "http://localhost:11434")
    )
    ollama_embedding_model: str = field(
        default_factory=lambda: os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")
    )
    # Gemini (cloud)
    gemini_api_key: str = field(
        default_factory=lambda: os.getenv("GEMINI_API_KEY", "")
    )
    gemini_embedding_model: str = field(
        default_factory=lambda: os.getenv("GEMINI_EMBEDDING_MODEL", "text-embedding-004")
    )
    # Rate-limit budgets for Gemini (per minute)
    gemini_rpm_limit: int = field(
        default_factory=lambda: int(os.getenv("GEMINI_RPM_LIMIT", "60"))
    )
    gemini_tpm_limit: int = field(
        default_factory=lambda: int(os.getenv("GEMINI_TPM_LIMIT", "1000000"))
    )


# ---------------------------------------------------------------------------
# Kafka settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class KafkaSettings:
    broker: str = field(
        default_factory=lambda: os.getenv("KAFKA_BROKER", "localhost:9092")
    )
    cdc_topic_prefix: str = field(
        default_factory=lambda: os.getenv("KAFKA_CDC_TOPIC_PREFIX", "syncmesh.cdc")
    )
    dlq_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "syncmesh.dlq")
    )
    consumer_group: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "syncmesh-workers")
    )
    num_partitions: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_NUM_PARTITIONS", "6"))
    )


# ---------------------------------------------------------------------------
# Redis settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RedisSettings:
    url: str = field(
        default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    max_retry_attempts: int = field(
        default_factory=lambda: int(os.getenv("REDIS_MAX_RETRY_ATTEMPTS", "5"))
    )
    base_backoff_seconds: float = field(
        default_factory=lambda: float(os.getenv("REDIS_BASE_BACKOFF_SECONDS", "1.0"))
    )


# ---------------------------------------------------------------------------
# Qdrant settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class QdrantSettings:
    host: str = field(
        default_factory=lambda: os.getenv("QDRANT_HOST", "localhost")
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("QDRANT_PORT", "6333"))
    )
    collection_name: str = field(
        default_factory=lambda: os.getenv("QDRANT_COLLECTION", "syncmesh_vectors")
    )
    # Embedding vector dimension — must match the active model's output
    vector_size: int = field(
        default_factory=lambda: int(os.getenv("QDRANT_VECTOR_SIZE", "768"))
    )


# ---------------------------------------------------------------------------
# PostgreSQL CDC settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PostgresSettings:
    cdc_url: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_CDC_URL", "postgres://user:pass@localhost:5432/maindb"
        )
    )
    replication_slot: str = field(
        default_factory=lambda: os.getenv("POSTGRES_REPLICATION_SLOT", "syncmesh_slot")
    )
    publication_name: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PUBLICATION", "syncmesh_pub")
    )
    # Comma-separated list of tables to watch; empty = all published tables
    watched_tables: list = field(
        default_factory=lambda: [
            t.strip()
            for t in os.getenv("POSTGRES_WATCHED_TABLES", "").split(",")
            if t.strip()
        ]
    )


# ---------------------------------------------------------------------------
# Logging settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LoggingSettings:
    level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )
    json_logs: bool = field(
        default_factory=lambda: os.getenv("LOG_JSON", "false").lower() == "true"
    )


# ---------------------------------------------------------------------------
# Aggregated settings singleton
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    llm: LLMSettings = field(default_factory=LLMSettings)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    qdrant: QdrantSettings = field(default_factory=QdrantSettings)
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)


# Module-level singleton — import `settings` everywhere else
settings = Settings()
