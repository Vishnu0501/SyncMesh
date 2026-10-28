"""
base_consumer.py
----------------
Abstract Kafka consumer that handles all the boilerplate:
  - Connection lifecycle and topic subscription
  - Manual offset commit (at-least-once delivery guarantee)
  - Graceful shutdown on SIGTERM / SIGINT
  - Dead-Letter Queue (DLQ) forwarding for unrecoverable errors

Concrete workers subclass this and implement `process_event()` only.
"""

from __future__ import annotations

import json
import signal
from abc import ABC, abstractmethod
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, Message

from config.kafka_config import get_consumer_config, get_dlq_producer_config
from config.settings import settings
from src.utils.logger import get_logger

log = get_logger(__name__)


class BaseConsumer(ABC):
    """
    Abstract base for all SyncMesh Kafka consumers.

    Subclass and implement `process_event(event: dict) -> None`.
    The base class handles polling, offset management, and DLQ routing.

    Parameters
    ----------
    topics : list[str]
        Kafka topics to subscribe to.
    group_id : str | None
        Override the default consumer group from settings.
    """

    def __init__(self, topics: list[str], group_id: str | None = None) -> None:
        self._topics   = topics
        self._consumer = Consumer(get_consumer_config(group_id))
        self._dlq      = Producer(get_dlq_producer_config())
        self._running  = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Start the blocking consumer poll loop.
        Registers SIGTERM / SIGINT handlers for graceful shutdown.
        """
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT,  self._handle_signal)

        self._consumer.subscribe(self._topics)
        self._running = True
        log.info("Consumer started", extra={"topics": self._topics})

        try:
            while self._running:
                msg: Message | None = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self._handle_kafka_error(msg)
                    continue
                self._handle_message(msg)
        finally:
            self._consumer.close()
            self._dlq.flush()
            log.info("Consumer shut down cleanly")

    def stop(self) -> None:
        """Signal the poll loop to exit on the next iteration."""
        self._running = False

    # ------------------------------------------------------------------
    # Abstract interface — subclasses implement this
    # ------------------------------------------------------------------

    @abstractmethod
    def process_event(self, event: dict[str, Any]) -> None:
        """
        Process a single decoded CDC event dict.
        Raise any exception to route the message to the DLQ.
        """

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_message(self, msg: Message) -> None:
        """
        Decode JSON payload, dispatch to process_event, then manually
        commit the offset only after successful processing.
        """
        raw = msg.value()
        try:
            event = json.loads(raw)
            self.process_event(event)
            # Manual commit — only after successful processing
            self._consumer.commit(message=msg)
            log.debug(
                "Offset committed",
                extra={
                    "topic":     msg.topic(),
                    "partition": msg.partition(),
                    "offset":    msg.offset(),
                },
            )
        except Exception as exc:
            log.error(
                "Processing failed — routing to DLQ",
                extra={"error": str(exc), "topic": msg.topic()},
            )
            self._send_to_dlq(msg, error=str(exc))
            # Commit so the consumer does not get stuck on a poison-pill message
            self._consumer.commit(message=msg)

    def _send_to_dlq(self, original: Message, error: str) -> None:
        """
        Forward a failed message to the Dead-Letter Queue topic with
        full diagnostic metadata so operators can replay or investigate.
        """
        dlq_payload = {
            "original_topic":     original.topic(),
            "original_partition": original.partition(),
            "original_offset":    original.offset(),
            "error":              error,
            "payload":            original.value().decode(errors="replace"),
        }
        self._dlq.produce(
            topic=settings.kafka.dlq_topic,
            value=json.dumps(dlq_payload).encode(),
        )
        self._dlq.poll(0)
        log.warning(
            "Message sent to DLQ",
            extra={
                "dlq_topic": settings.kafka.dlq_topic,
                "error":     error,
            },
        )

    def _handle_kafka_error(self, msg: Message) -> None:
        """Log benign partition-EOF; raise on fatal broker-level errors."""
        if msg.error().code() == KafkaError._PARTITION_EOF:
            log.debug(
                "Reached end of partition",
                extra={"partition": msg.partition()},
            )
        else:
            raise KafkaException(msg.error())

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle OS shutdown signals gracefully."""
        log.info("Shutdown signal received", extra={"signal": signum})
        self.stop()
