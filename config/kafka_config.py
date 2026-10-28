"""
kafka_config.py
---------------
Factory functions that build confluent-kafka producer and consumer
configuration dictionaries. All callers use these helpers so Kafka
configuration stays in one place and is never duplicated across modules.
"""

from __future__ import annotations
from typing import Any
from config.settings import settings


def get_producer_config() -> dict[str, Any]:
    """
    Return a confluent-kafka Producer configuration dict.

    Key choices
    -----------
    acks=all          : strongest durability guarantee (wait for all ISRs).
    idempotence=true  : exactly-once producer semantics at broker level.
    compression.type  : lz4 for fast compression / decompression.
    """
    return {
        "bootstrap.servers": settings.kafka.broker,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 5,         # micro-batch up to 5 ms to improve throughput
        "batch.size": 65536,    # 64 KB batch
        "retries": 5,
        "retry.backoff.ms": 200,
    }


def get_consumer_config(group_id: str | None = None) -> dict[str, Any]:
    """
    Return a confluent-kafka Consumer configuration dict.

    Key choices
    -----------
    auto.offset.reset=earliest : never skip events on a new consumer group.
    enable.auto.commit=false   : manual commit after successful processing
                                 guarantees at-least-once delivery.
    isolation.level=read_committed : only read fully committed transactions.
    """
    return {
        "bootstrap.servers": settings.kafka.broker,
        "group.id": group_id or settings.kafka.consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "max.poll.interval.ms": 300_000,   # 5 min — generous for slow LLM calls
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 10_000,
    }


def get_dlq_producer_config() -> dict[str, Any]:
    """
    Separate, lighter-weight producer config for the Dead-Letter Queue.
    No batching delay — failed events should land in DLQ as fast as possible.
    """
    return {
        "bootstrap.servers": settings.kafka.broker,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 0,
        "retries": 3,
        "retry.backoff.ms": 500,
    }


def cdc_topic_for_table(table_name: str) -> str:
    """
    Derive the deterministic Kafka topic name for a given source table.

    Example
    -------
    >>> cdc_topic_for_table("customer_profiles")
    'syncmesh.cdc.customer_profiles'
    """
    safe_table = table_name.replace(".", "_").replace("-", "_").lower()
    return f"{settings.kafka.cdc_topic_prefix}.{safe_table}"
