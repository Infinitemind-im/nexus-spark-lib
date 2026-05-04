"""Signal C — Graph-based entity resolution lift using Neo4j 2-hop traversal.

For a candidate cdm_entity_id, this signal computes an additive confidence lift
based on how many shared 1-hop and 2-hop graph neighbours the candidate has with
entities already in the graph:

  depth-1 shared neighbour: +0.05 per shared entity (capped at +0.10 total)
  depth-2 shared neighbour: +0.02 per shared entity (capped as part of +0.10 total)
"""

from __future__ import annotations

from typing import Any

from nexus_spark_lib.config.settings import settings
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_MAX_LIFT = 0.10
_DEPTH1_LIFT = 0.05
_DEPTH2_LIFT = 0.02


def run_signal_c(
    driver: Any,
    cdm_entity_id: str,
    tenant_id: str,
) -> float:
    """Return confidence lift from graph traversal. Returns 0.0 if Neo4j unavailable."""
    if driver is None:
        return 0.0

    try:
        return _compute_graph_lift(driver, cdm_entity_id, tenant_id)
    except Exception as exc:
        logger.warning("Signal C graph lookup failed (non-fatal): %s", exc)
        return 0.0


def _compute_graph_lift(driver: Any, cdm_entity_id: str, tenant_id: str) -> float:
    """Execute 2-hop traversal and compute additive lift."""
    query = """
    MATCH (e {cdm_entity_id: $entity_id, tenant_id: $tenant_id})
    OPTIONAL MATCH (e)-[*1..1]-(n1)
      WHERE n1.tenant_id = $tenant_id
    WITH e, collect(DISTINCT n1.cdm_entity_id) AS depth1_ids
    OPTIONAL MATCH (e)-[*2..2]-(n2)
      WHERE n2.tenant_id = $tenant_id AND NOT n2.cdm_entity_id IN depth1_ids
    RETURN
        size(depth1_ids) AS depth1_count,
        count(DISTINCT n2) AS depth2_count
    """
    with driver.session() as session:
        result = session.run(query, entity_id=cdm_entity_id, tenant_id=tenant_id)
        record = result.single()
        if record is None:
            return 0.0

        depth1 = int(record["depth1_count"] or 0)
        depth2 = int(record["depth2_count"] or 0)

    lift = min(
        (depth1 * _DEPTH1_LIFT) + (depth2 * _DEPTH2_LIFT),
        _MAX_LIFT,
    )
    return round(lift, 4)
