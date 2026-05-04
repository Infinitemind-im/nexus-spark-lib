"""Raw record model — the input shape arriving on m1.int.raw_records.

Published by: nexus-m1-worker (CDC path) and Batch Backfill DAGs (batch path).
Consumed by:  nexus-spark-transformer (this library's pipeline entry point).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SourceOp(str, Enum):
    """Source-side operation type, normalised from Debezium op codes."""

    INSERT = "INSERT"           # Debezium op=c (create)
    UPDATE = "UPDATE"           # Debezium op=u
    DELETE = "DELETE"           # Debezium op=d  — routed to entity_removed topic downstream
    SNAPSHOT_READ = "SNAPSHOT_READ"  # Debezium op=r — treated as INSERT by ER
    RELEVEL = "RELEVEL"         # Materialization tier change — skip ER, re-synthesise only


@dataclass
class RawRecord:
    """One source record arriving from CDC or Batch Backfill.

    This is the Python representation of a row in the m1.int.raw_records topic.
    All fields must be present; optional fields default to None.
    """

    tenant_id: str
    connector_id: str
    source_system: str
    source_table: str
    source_record_id: str
    source_op: SourceOp
    source_ts: datetime
    after_payload: dict[str, Any]

    # before_payload is only set for UPDATE and DELETE — None for INSERT/SNAPSHOT_READ/RELEVEL
    before_payload: dict[str, Any] | None = None

    # message_id is the Kafka message UUID for deduplication
    message_id: str = ""

    # backfill_batch_id is set by Batch Backfill DAGs; None for CDC Streaming records
    backfill_batch_id: str | None = None

    # trace_id is propagated for distributed tracing (OpenTelemetry)
    trace_id: str | None = None

    # Schema version from the NexusMessage envelope
    schema_version: str = "2.0"

    def is_batch(self) -> bool:
        return self.backfill_batch_id is not None

    def natural_key(self) -> tuple[str, str, str, str]:
        """Deduplication key: (tenant_id, connector_id, source_table, source_record_id)."""
        return (self.tenant_id, self.connector_id, self.source_table, self.source_record_id)
