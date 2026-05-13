"""Signal C — Graph-based entity resolution lift using shared neighbours.

The HTML V2 spec defines Signal C as a lift for review-band candidates only.
The lift comes from shared neighbours between the incoming record's resolved FK
targets and the candidate's 1-hop / 2-hop graph neighbourhood, weighted by the
confidence of the traversed edges and capped at +0.10.
"""

from __future__ import annotations

from typing import Any

from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_MAX_LIFT = 0.10
_DEPTH1_LIFT = 0.05
_DEPTH2_LIFT = 0.02
_MIN_EDGE_CONFIDENCE = 0.85


def run_signal_c(
    driver: Any,
    cdm_entity_id: str,
    tenant_id: str,
    cdm_entity_type: str,
    source_system: str,
    fields: dict[str, Any],
    er_index: Any,
) -> float:
    """Return confidence lift from graph traversal. Returns 0.0 if Neo4j unavailable."""
    if driver is None:
        return 0.0

    incoming_neighbours = _resolve_fk_neighbours(
        tenant_id=tenant_id,
        cdm_entity_type=cdm_entity_type,
        source_system=source_system,
        fields=fields,
        er_index=er_index,
    )
    if not incoming_neighbours[1]:
        return 0.0

    try:
        return _compute_graph_lift(driver, cdm_entity_id, tenant_id, incoming_neighbours[1])
    except Exception as exc:
        logger.warning("Signal C graph lookup failed (non-fatal): %s", exc)
        return 0.0


def _compute_graph_lift(
    driver: Any,
    cdm_entity_id: str,
    tenant_id: str,
    incoming_neighbour_ids: set[str],
) -> float:
    """Execute shared-neighbour traversal and compute additive lift."""
    depth1_query = """
    MATCH (candidate {cdm_entity_id: $entity_id, tenant_id: $tenant_id})-[r]-(neighbour)
    WHERE neighbour.tenant_id = $tenant_id
      AND neighbour.cdm_entity_id IN $incoming_ids
    RETURN collect(DISTINCT {
        id: neighbour.cdm_entity_id,
        confidence: coalesce(r.confidence, 1.0)
    }) AS depth1_hits
    """
    depth2_query = """
    MATCH (candidate {cdm_entity_id: $entity_id, tenant_id: $tenant_id})-[r1]-(mid)-[r2]-(neighbour)
    WHERE mid.tenant_id = $tenant_id
      AND neighbour.tenant_id = $tenant_id
      AND neighbour.cdm_entity_id IN $incoming_ids
    RETURN collect(DISTINCT {
        id: neighbour.cdm_entity_id,
        confidence: CASE
            WHEN coalesce(r1.confidence, 1.0) < coalesce(r2.confidence, 1.0)
                THEN coalesce(r1.confidence, 1.0)
            ELSE coalesce(r2.confidence, 1.0)
        END
    }) AS depth2_hits
    """
    with driver.session() as session:
        depth1_record = session.run(
            depth1_query,
            entity_id=cdm_entity_id,
            tenant_id=tenant_id,
            incoming_ids=sorted(incoming_neighbour_ids),
        ).single()
        depth2_record = session.run(
            depth2_query,
            entity_id=cdm_entity_id,
            tenant_id=tenant_id,
            incoming_ids=sorted(incoming_neighbour_ids),
        ).single()

    depth1_hits = _coerce_hits(depth1_record, "depth1_hits")
    depth1_ids = {hit["id"] for hit in depth1_hits}
    depth2_hits = [
        hit
        for hit in _coerce_hits(depth2_record, "depth2_hits")
        if hit["id"] not in depth1_ids
    ]

    lift = 0.0
    for hit in depth1_hits:
        confidence = hit["confidence"]
        if confidence >= _MIN_EDGE_CONFIDENCE:
            lift += _DEPTH1_LIFT * confidence

    for hit in depth2_hits:
        confidence = hit["confidence"]
        if confidence >= _MIN_EDGE_CONFIDENCE:
            lift += _DEPTH2_LIFT * confidence

    return round(min(lift, _MAX_LIFT), 4)


def _resolve_fk_neighbours(
    *,
    tenant_id: str,
    cdm_entity_type: str,
    source_system: str,
    fields: dict[str, Any],
    er_index: Any,
) -> dict[int, set[str]]:
    neighbours: set[str] = set()
    if not source_system:
        return {1: neighbours, 2: set()}

    for attr_name, field in (fields or {}).items():
        if not isinstance(field, dict):
            continue

        attribute_kind = str(field.get("attribute_kind") or "").strip().lower()
        if attribute_kind != "foreign_key":
            continue

        raw_value = field.get("value")
        if raw_value is None:
            continue

        target_entity_type = str(field.get("fk_target_entity_type") or "").strip()
        if not target_entity_type:
            continue

        resolved = None
        if hasattr(er_index, "find_entity_by_source_record"):
            resolved = er_index.find_entity_by_source_record(
                tenant_id=tenant_id,
                cdm_entity_type=target_entity_type,
                source_system=source_system,
                source_record_id=str(raw_value),
            )

        if resolved:
            neighbours.add(str(resolved))

    return {1: neighbours, 2: set()}


def _coerce_hits(record: Any, key: str) -> list[dict[str, float | str]]:
    if not record:
        return []

    raw_hits = record.get(key) if hasattr(record, "get") else record[key]
    if not isinstance(raw_hits, list):
        return []

    hits: list[dict[str, float | str]] = []
    for raw_hit in raw_hits:
        if not isinstance(raw_hit, dict):
            continue
        neighbour_id = str(raw_hit.get("id") or "").strip()
        if not neighbour_id:
            continue
        try:
            confidence = float(raw_hit.get("confidence", 1.0))
        except (TypeError, ValueError):
            confidence = 1.0
        hits.append({"id": neighbour_id, "confidence": confidence})
    return hits
