from __future__ import annotations

"""
Driver-side ER + materialization gate (single record, no Spark).

Use ``transform.stage2_resolve.resolve`` for the Spark DataFrame stage.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from nexus_spark_lib.db.er_index_lookup import ErIndexLookup
from nexus_spark_lib.transform.stage2_resolve.materialization_gate import (
    MaterializationGateOutcome,
    run_materialization_gate,
)
from nexus_spark_lib.transform.stage2_resolve.signals.signal_a_deterministic import run_signal_a
from nexus_spark_lib.transform.stage2_resolve.signals.signal_b_probabilistic import (
    classify_signal_b,
    run_signal_b,
)


class ResolutionAction(str, Enum):
    AUTO_APPLY = "auto_apply"
    REVIEW = "review"
    NEW_ENTITY = "new_entity"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class ResolveOutcome:
    tenant_id: str
    cdm_entity_type: str
    source_connector: str
    source_record_id: str
    cdm_entity_id: str | None
    resolution_method: str | None
    confidence: float
    action: ResolutionAction
    signal_b_score: float | None = None
    materialization: MaterializationGateOutcome | None = None


def resolve_record(
    *,
    tenant_id: str,
    cdm_entity_type: str,
    source_connector: str,
    source_record_id: str,
    fields: dict[str, Any],
    er_index: Any,
    system_dsn: str | None = None,
    fast_path_lookup: Callable[[], str | None] | None = None,
    apply_materialization_gate: bool = True,
    neo4j_driver: Any | None = None,
) -> ResolveOutcome:
    """Run Signal A/B/C then optional ``entity_store_presence`` gate."""
    cdm_entity_id: str | None = None
    method: str | None = None
    confidence = 0.0
    action = ResolutionAction.UNRESOLVED
    signal_b_score: float | None = None

    if fast_path_lookup is not None:
        cdm_entity_id = fast_path_lookup()
    elif system_dsn:
        cdm_entity_id = ErIndexLookup(system_dsn).fast_path(
            tenant_id=tenant_id,
            source_connector=source_connector,
            source_record_id=source_record_id,
        )

    if cdm_entity_id:
        method = "fast_path"
        confidence = 1.0
        action = ResolutionAction.AUTO_APPLY
    else:
        cdm_entity_id = run_signal_a(tenant_id, cdm_entity_type, fields, er_index)
        if cdm_entity_id:
            method = "signal_a"
            confidence = 1.0
            action = ResolutionAction.AUTO_APPLY
        else:
            score, candidate = run_signal_b(tenant_id, cdm_entity_type, fields, er_index)
            signal_b_score = score
            verdict = classify_signal_b(score, candidate, er_index, tenant_id, cdm_entity_type)
            if verdict == "auto_apply" and candidate:
                cdm_entity_id = candidate
                method = "signal_b"
                confidence = score
                action = ResolutionAction.AUTO_APPLY
            elif verdict == "review" and candidate:
                from nexus_spark_lib.transform.stage2_resolve.signals.signal_c_graph import run_signal_c

                lift = run_signal_c(neo4j_driver, candidate, tenant_id) if neo4j_driver else 0.0
                final = (score or 0.0) + lift
                auto = float(
                    er_index.thresholds.get((tenant_id, cdm_entity_type), {}).get(
                        "auto_apply_threshold", 0.92
                    )
                )
                if final >= auto:
                    cdm_entity_id = candidate
                    method = "signal_c" if lift else "signal_b"
                    confidence = final
                    action = ResolutionAction.AUTO_APPLY
                else:
                    cdm_entity_id = candidate
                    method = "signal_b"
                    confidence = score
                    action = ResolutionAction.REVIEW
            else:
                action = ResolutionAction.NEW_ENTITY
                cdm_entity_id = None

    mat: MaterializationGateOutcome | None = None
    if apply_materialization_gate and system_dsn and cdm_entity_id:
        mat = run_materialization_gate(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            source_connector=source_connector,
            source_record_id=source_record_id,
            fields=fields,
            system_dsn=system_dsn,
            cdm_entity_id=cdm_entity_id,
        )

    return ResolveOutcome(
        tenant_id=tenant_id,
        cdm_entity_type=cdm_entity_type,
        source_connector=source_connector,
        source_record_id=source_record_id,
        cdm_entity_id=cdm_entity_id,
        resolution_method=method,
        confidence=confidence,
        action=action,
        signal_b_score=signal_b_score,
        materialization=mat,
    )
