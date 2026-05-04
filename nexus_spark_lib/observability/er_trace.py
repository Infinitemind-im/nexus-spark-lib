"""ER diagnostic trace writer (FR-Dev3-S-01).

When settings.enable_er_trace is True, every ER signal evaluation, score
computation, and threshold check is recorded to nexus_system.er_trace.
Used by data stewards investigating bad merges.

Disabled by default in production — enabling adds significant write overhead.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ErTraceEntry:
    """One row in nexus_system.er_trace."""

    trace_id: str
    tenant_id: str
    cdm_entity_type: str
    source_record_id: str
    signal: str                        # "signal_a" | "signal_b" | "signal_c" | "routing"
    candidate_id: str | None
    score: float | None
    threshold: float | None
    outcome: str                       # "auto_apply" | "review_band" | "new_gr" | "skip"
    details: dict[str, Any] = field(default_factory=dict)
    recorded_at: datetime = field(default_factory=datetime.utcnow)


class ErTraceWriter:
    """Batched async writer for ER trace entries.

    Buffers entries in memory and flushes to DB on each micro-batch boundary
    to avoid per-row roundtrips. Enabled only when settings.enable_er_trace=True.
    """

    def __init__(self) -> None:
        self._buffer: list[ErTraceEntry] = []

    def record(self, entry: ErTraceEntry) -> None:
        self._buffer.append(entry)

    async def flush(self, conn: Any) -> int:
        """Flush buffered trace entries to nexus_system.er_trace. Returns row count."""
        if not self._buffer:
            return 0
        rows = self._buffer[:]
        self._buffer.clear()
        if not rows:
            return 0
        await conn.executemany(
            """
            INSERT INTO nexus_system.er_trace
                (trace_id, tenant_id, cdm_entity_type, source_record_id,
                 signal, candidate_id, score, threshold, outcome, details, recorded_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    r.trace_id, r.tenant_id, r.cdm_entity_type, r.source_record_id,
                    r.signal, r.candidate_id, r.score, r.threshold, r.outcome,
                    r.details, r.recorded_at,
                )
                for r in rows
            ],
        )
        return len(rows)

    def clear(self) -> None:
        self._buffer.clear()
