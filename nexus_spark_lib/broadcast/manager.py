"""BroadcastManager — central registry for all Spark broadcast variables.

Each broadcast has a TTL and is refreshed either on a timer or when an
invalidation event (e.g. nexus.materialization_policy.changed) is received.

Usage in a Spark application:
    manager = BroadcastManager(spark, db_pool)
    await manager.initialise()

    # In the streaming loop / foreachBatch:
    policy_bc = manager.get_materialization_policy()   # returns cached broadcast
    await manager.refresh_if_stale()                    # call once per micro-batch
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from nexus_core.db import SYSTEM_TENANT, get_tenant_scoped_connection
from nexus_spark_lib.config.settings import settings
from nexus_spark_lib.db.survivorship_rules import (
    load_materialization_policy,
    load_survivorship_rules,
)
from nexus_spark_lib.errors.exceptions import BroadcastExpiredError, BroadcastRefreshError
from nexus_spark_lib.models.broadcasts import (
    MaterializationPolicyBroadcast,
    SurvivorshipBroadcast,
)
from nexus_spark_lib.observability.metrics import MATERIALIZATION_POLICY_CACHE_AGE
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


class BroadcastManager:
    """Manages lifecycle of all Spark broadcast variables.

    Thread-safe refresh with asyncio Lock. One instance per Spark driver process.
    """

    def __init__(self, spark: SparkSession, db_pool: object) -> None:
        self._spark = spark
        self._db_pool = db_pool
        self._policy_bc: MaterializationPolicyBroadcast | None = None
        self._survivorship_bc: SurvivorshipBroadcast | None = None
        self._policy_loaded_at: datetime | None = None
        self._survivorship_loaded_at: datetime | None = None
        self._lock = asyncio.Lock()

    async def initialise(self) -> None:
        """Load all broadcasts at startup. Must be called before any stage runs."""
        async with self._lock:
            await self._refresh_policy()
            await self._refresh_survivorship()
        logger.info("BroadcastManager: all broadcasts initialised")

    async def refresh_if_stale(self) -> None:
        """Refresh any broadcast that has exceeded its TTL. Called per micro-batch."""
        now = datetime.utcnow()
        policy_ttl = timedelta(seconds=settings.materialization_policy_cache_ttl_seconds)
        surv_ttl = timedelta(seconds=settings.survivorship_cache_ttl_seconds)

        async with self._lock:
            if self._policy_loaded_at is None or (now - self._policy_loaded_at) > policy_ttl:
                await self._refresh_policy()
                MATERIALIZATION_POLICY_CACHE_AGE.set(0)

            if self._survivorship_loaded_at is None or (now - self._survivorship_loaded_at) > surv_ttl:
                await self._refresh_survivorship()

        # Update cache age gauge
        if self._policy_loaded_at:
            age = (datetime.utcnow() - self._policy_loaded_at).total_seconds()
            MATERIALIZATION_POLICY_CACHE_AGE.set(age)

    async def invalidate_policy(self) -> None:
        """Force-refresh the materialization policy broadcast on nexus.materialization_policy.changed."""
        async with self._lock:
            await self._refresh_policy()
        logger.info("MaterializationPolicy broadcast force-refreshed")

    def get_materialization_policy(self) -> MaterializationPolicyBroadcast:
        if self._policy_bc is None:
            raise BroadcastExpiredError("MaterializationPolicy broadcast not initialised")
        return self._policy_bc

    def get_survivorship(self) -> SurvivorshipBroadcast:
        if self._survivorship_bc is None:
            raise BroadcastExpiredError("Survivorship broadcast not initialised")
        return self._survivorship_bc

    async def _refresh_policy(self) -> None:
        try:
            async with get_tenant_scoped_connection(self._db_pool, SYSTEM_TENANT) as conn:
                policy = await load_materialization_policy(conn)
            bc = self._spark.sparkContext.broadcast(policy)
            if self._policy_bc:
                self._policy_bc.broadcast.unpersist()
            self._policy_bc = MaterializationPolicyBroadcast(
                broadcast=bc,
                snapshot_ts=datetime.utcnow().isoformat(),
            )
            self._policy_loaded_at = datetime.utcnow()
            logger.info("MaterializationPolicy broadcast refreshed")
        except Exception as exc:
            raise BroadcastRefreshError(f"Failed to refresh policy broadcast: {exc}") from exc

    async def _refresh_survivorship(self) -> None:
        try:
            async with get_tenant_scoped_connection(self._db_pool, SYSTEM_TENANT) as conn:
                ruleset = await load_survivorship_rules(conn)
            bc = self._spark.sparkContext.broadcast(ruleset)
            if self._survivorship_bc:
                self._survivorship_bc.broadcast.unpersist()
            self._survivorship_bc = SurvivorshipBroadcast(
                broadcast=bc,
                snapshot_ts=datetime.utcnow().isoformat(),
            )
            self._survivorship_loaded_at = datetime.utcnow()
            logger.info("Survivorship broadcast refreshed")
        except Exception as exc:
            raise BroadcastRefreshError(f"Failed to refresh survivorship broadcast: {exc}") from exc
