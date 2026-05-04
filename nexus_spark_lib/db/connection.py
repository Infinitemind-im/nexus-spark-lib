"""PostgreSQL connection pool for Spark executors.

Each executor process maintains its own asyncpg connection pool. The pool is
initialised lazily on first use within a partition and closed at executor shutdown.

RLS enforcement
---------------
Never call pool.acquire() directly. Always use:
    from nexus_core.db import get_tenant_scoped_connection, SYSTEM_TENANT
    async with get_tenant_scoped_connection(pool, tenant_id) as conn:
        ...
This is re-exported from nexus_spark_lib.db for convenience.
nexus_core's implementation sanitises tenant_id against SQL injection and
resets app.current_tenant in the finally block.
"""

from __future__ import annotations

import asyncio
import asyncpg

from nexus_core.db import SYSTEM_TENANT, get_tenant_scoped_connection  # noqa: F401 — re-exported

from nexus_spark_lib.config.settings import settings
from nexus_spark_lib.errors.exceptions import DBConnectionError
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


async def get_pool() -> asyncpg.Pool:
    """Return (or lazily initialise) the per-executor asyncpg connection pool."""
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is not None:
            return _pool
        try:
            _pool = await asyncpg.create_pool(
                dsn=settings.db_dsn,
                min_size=settings.db_pool_min_size,
                max_size=settings.db_pool_max_size,
                max_inactive_connection_lifetime=settings.db_pool_max_inactive_connection_lifetime,
                command_timeout=30,
            )
            logger.info("DB pool created: min=%d max=%d", settings.db_pool_min_size, settings.db_pool_max_size)
        except Exception as exc:
            raise DBConnectionError(f"Failed to create DB pool: {exc}") from exc
    return _pool  # type: ignore[return-value]


async def close_pool() -> None:
    """Close the pool. Called on executor shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ── get_tenant_scoped_connection and SYSTEM_TENANT are imported above from  ──
# ── nexus_core.db and re-exported via db/__init__.py.                        ──
# ──                                                                           ──
# ── Usage:                                                                    ──
# ──   pool = await get_pool()                                                 ──
# ──   async with get_tenant_scoped_connection(pool, tenant_id) as conn:       ──
# ──       rows = await conn.fetch(...)                                         ──
# ──                                                                           ──
# ── For system-level (cross-tenant) queries, pass SYSTEM_TENANT.              ──
