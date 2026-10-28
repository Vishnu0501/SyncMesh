"""
wal_listener.py
---------------
CDC Layer — tails the PostgreSQL Write-Ahead Log via logical replication
(pgoutput plugin, built into PostgreSQL 10+).

Every mutation is parsed into the canonical Kafka event payload (§5.3 PRD)
and published to the deterministic topic for that source table.

PostgreSQL prerequisites (run once as superuser)
-------------------------------------------------
    ALTER SYSTEM SET wal_level = logical;
    SELECT pg_reload_conf();
    CREATE PUBLICATION syncmesh_pub FOR ALL TABLES;
    -- Replication slot is created automatically on first run.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer

from config.kafka_config import cdc_topic_for_table, get_producer_config
from config.settings import settings
from src.utils.logger import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_event(
    table: str,
    operation: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Construct the canonical Kafka CDC event payload.

    Schema contract (PRD §5.3):
    {
      "event_id":  "<uuid-v4>",
      "table":     "<table_name>",
      "operation": "INSERT" | "UPDATE" | "DELETE",
      "timestamp": <unix_epoch_int>,
      "data": {
        "before": { ... } | null,
        "after":  { ... } | null
      }
    }
    """
    return {
        "event_id":  str(uuid.uuid4()),
        "table":     table,
        "operation": operation,
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "data": {
            "before": before,
            "after":  after,
        },
    }


def _delivery_report(err: Any, msg: Any) -> None:
    """Confluent-kafka async producer delivery callback."""
    if err:
        log.error("Kafka delivery failed", extra={"error": str(err)})
    else:
        log.debug(
            "Event delivered",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


# ---------------------------------------------------------------------------
# WAL Listener
# ---------------------------------------------------------------------------

class WALListener:
    """
    Streams logical replication messages from PostgreSQL and publishes
    structured CDC events to Kafka.

    Parameters
    ----------
    dsn : str | None
        PostgreSQL DSN with replication support.
        Defaults to settings.postgres.cdc_url.
    """

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn         = dsn or settings.postgres.cdc_url
        self._slot        = settings.postgres.replication_slot
        self._publication = settings.postgres.publication_name
        self._producer    = Producer(get_producer_config())
        self._running     = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Blocking entry point.
        Call from the main process or a dedicated thread.
        """
        log.info(
            "WAL listener starting",
            extra={"slot": self._slot, "publication": self._publication},
        )
        self._running = True
        conn = self._connect()
        try:
            self._stream(conn)
        except KeyboardInterrupt:
            log.info("WAL listener interrupted by user")
        finally:
            conn.close()
            self._producer.flush()
            log.info("WAL listener stopped")

    def stop(self) -> None:
        """Signal the streaming loop to exit on the next iteration."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _connect(self) -> psycopg2.extensions.connection:
        """
        Open a logical replication connection.
        Creates the replication slot if it does not already exist.
        Uses the pgoutput plugin (built into PostgreSQL 10+, no extensions needed).
        """
        conn = psycopg2.connect(
            self._dsn,
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
        )
        cur = conn.cursor()
        try:
            cur.create_replication_slot(self._slot, output_plugin="pgoutput")
            log.info("Replication slot created", extra={"slot": self._slot})
        except psycopg2.errors.DuplicateObject:
            log.info("Replication slot already exists", extra={"slot": self._slot})
        return conn

    def _stream(self, conn: psycopg2.extensions.connection) -> None:
        """
        Start logical replication streaming and process incoming messages.

        Each pgoutput message is decoded, converted into a canonical
        CDC event dict, and published to the appropriate Kafka topic.
        LSN is acknowledged back to PostgreSQL so WAL segments can be recycled.
        """
        cur = conn.cursor()
        cur.start_replication(
            slot_name=self._slot,
            decode=True,
            options={
                "publication_names": self._publication,
                "proto_version":     "1",
            },
        )
        log.info("Streaming WAL changes…")

        while self._running:
            # Non-blocking poll — returns None after 100 ms timeout
            msg = cur.read_message()
            if msg is None:
                cur.send_feedback(reply_requested=False)
                continue

            self._process_message(msg)
            # Acknowledge the LSN so PostgreSQL can free WAL disk space
            cur.send_feedback(flush_lsn=msg.data_start)

    def _process_message(self, msg: Any) -> None:
        """
        Parse a raw pgoutput message and publish to Kafka.

        pgoutput emits: Begin, Commit, Relation, Insert, Update, Delete.
        We only act on the three DML mutation types.
        """
        payload  = msg.payload   # dict decoded by psycopg2
        msg_type = payload.get("type", "")

        op_map = {"insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}
        operation = op_map.get(msg_type)
        if operation is None:
            return  # DDL / Begin / Commit / Relation — skip

        schema = payload.get("schema", "public")
        relation = payload.get("relation", "unknown")
        table  = f"{schema}.{relation}"
        before = payload.get("old_tuple")
        after  = payload.get("new_tuple")

        # Filter by watched_tables if configured
        watched = settings.postgres.watched_tables
        if watched and relation not in watched and table not in watched:
            return

        event = _build_event(table, operation, before, after)
        topic = cdc_topic_for_table(table)

        # Use primary key as Kafka partition key for ordering guarantees
        record = after or before or {}
        key = str(record.get("id", event["event_id"]))

        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=json.dumps(event).encode(),
            callback=_delivery_report,
        )
        self._producer.poll(0)  # Trigger delivery callbacks non-blocking

        log.info(
            "CDC event published",
            extra={
                "operation": operation,
                "table":     table,
                "topic":     topic,
                "key":       key,
            },
        )
