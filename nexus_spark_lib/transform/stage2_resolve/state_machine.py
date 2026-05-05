"""GoldenRecordStateMachine — manages GR state transitions.

Allowed transitions:
  NEW         → ACTIVE       (first time we see a source)
  ACTIVE      → ACTIVE       (update)
  ACTIVE      → SUPERSEDED   (MERGE: loser)
  ACTIVE      → TOMBSTONED   (all sources removed)
  PROVISIONAL → ACTIVE       (review approved)
  SUPERSEDED  → ACTIVE       (SPLIT: survivor re-activated)
  any         → TOMBSTONED   (all provenance deleted)
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import asyncpg

from nexus_spark_lib.db.golden_records import get_golden_record_state, upsert_golden_record
from nexus_spark_lib.db.redirects import insert_redirect
from nexus_spark_lib.errors.exceptions import ERStateTransitionError
from nexus_spark_lib.models.er_types import GoldenRecordState
from nexus_spark_lib.observability.metrics import ER_STATE_TRANSITIONS
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


class GoldenRecordStateMachine:
    """Applies ER state transitions with audit trail."""

    def __init__(self, conn: asyncpg.Connection, tenant_id: str) -> None:
        self._conn = conn
        self._tenant_id = tenant_id

    async def create_or_activate(
        self,
        cdm_entity_id: str,
        cdm_entity_type: str,
        reason: str = "new_source",
    ) -> GoldenRecordState:
        """Create a new GR or re-activate an existing one."""
        current = await get_golden_record_state(self._conn, cdm_entity_id)
        if current is None:
            state = GoldenRecordState.ACTIVE
        elif current == GoldenRecordState.TOMBSTONED:
            state = GoldenRecordState.ACTIVE
        else:
            state = current

        await upsert_golden_record(
            self._conn, cdm_entity_id, self._tenant_id, cdm_entity_type,
            state=state, state_change_reason=reason,
        )
        ER_STATE_TRANSITIONS.labels(
            from_state=str(current), to_state=state.value, tenant_id=self._tenant_id
        ).inc()
        return state

    async def merge(
        self,
        loser_id: str,
        survivor_id: str,
        cdm_entity_type: str,
    ) -> None:
        """Supersede the loser and install a redirect to the survivor.

        Survivor is chosen as the GR with the earlier created_at (canonical rule).
        """
        await upsert_golden_record(
            self._conn, loser_id, self._tenant_id, cdm_entity_type,
            state=GoldenRecordState.SUPERSEDED,
            state_change_reason=f"merged_into:{survivor_id}",
        )
        await insert_redirect(self._conn, loser_id, survivor_id, self._tenant_id)
        ER_STATE_TRANSITIONS.labels(
            from_state="active", to_state="superseded", tenant_id=self._tenant_id
        ).inc()
        logger.info("MERGE: %s → %s", loser_id, survivor_id)

    async def tombstone(self, cdm_entity_id: str, cdm_entity_type: str) -> None:
        """Mark a Golden Record as TOMBSTONED when all sources are gone."""
        await upsert_golden_record(
            self._conn, cdm_entity_id, self._tenant_id, cdm_entity_type,
            state=GoldenRecordState.TOMBSTONED,
            state_change_reason="all_sources_deleted",
        )
        ER_STATE_TRANSITIONS.labels(
            from_state="active", to_state="tombstoned", tenant_id=self._tenant_id
        ).inc()
        logger.info("TOMBSTONE: %s", cdm_entity_id)

    async def provisional(self, cdm_entity_id: str, cdm_entity_type: str) -> None:
        """Mark a GR as PROVISIONAL when it's pending human ER review."""
        await upsert_golden_record(
            self._conn, cdm_entity_id, self._tenant_id, cdm_entity_type,
            state=GoldenRecordState.PROVISIONAL,
            state_change_reason="review_queued",
        )
        ER_STATE_TRANSITIONS.labels(
            from_state="new", to_state="provisional", tenant_id=self._tenant_id
        ).inc()

    async def split(
        self,
        source_entity_id: str,
        new_entity_id: str,
        cdm_entity_type: str,
    ) -> None:
        """Re-activate the source GR after a SPLIT (SUPERSEDED → ACTIVE).

        Called when two previously merged entities are determined to be distinct.
        The source entity (formerly the MERGE loser) is re-activated as its own
        independent Golden Record.
        """
        await upsert_golden_record(
            self._conn, source_entity_id, self._tenant_id, cdm_entity_type,
            state=GoldenRecordState.ACTIVE,
            state_change_reason=f"split_from:{new_entity_id}",
        )
        ER_STATE_TRANSITIONS.labels(
            from_state="superseded", to_state="active", tenant_id=self._tenant_id
        ).inc()
        logger.info("SPLIT: %s re-activated from %s", source_entity_id, new_entity_id)
