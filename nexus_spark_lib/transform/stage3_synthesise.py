"""Stage 3 — Golden Record Synthesis.

Applies survivorship rules to produce a single canonical Golden Record
from all contributing sources.

Current implementation note:
    this module still performs a simplified per-row Stage 3 pass for the Spark
    stream.  The immediate goal here is to make its provenance contract line up
    with the Iteration 2 spec even before full multi-source re-election is wired.

Survivorship rules (current implementation support):
  MOST_RECENT          — field from source with latest source_ts
  HIGHEST_CONFIDENCE   — field with highest DQ score
  SOURCE_PRIORITY      — source_system in priority_sources list wins
  MOST_COMPLETE        — source with fewest null fields wins
  LONGEST_VALUE        — longest non-null string value wins
  EXACT_MATCH          — only include if all sources agree exactly

NFR-D3-05: Synthesis is deterministic — same contributing sources, same
output, regardless of processing order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructField, StructType

from nexus_spark_lib.models.survivorship import SurvivorshipRuleSet, SurvivorshipRuleType
from nexus_spark_lib.models.survivorship import ProvenanceRow, SurvivorshipRule, SynthesisResult
from nexus_spark_lib.observability.metrics import SYNTHESIS_LATENCY, SYNTHESIS_RECORDS
from nexus_spark_lib.observability.structured_log import get_stage_logger
from nexus_spark_lib._internal.hash_utils import provenance_hash_from_winning_records, sha256_hex

logger = get_stage_logger(__name__)

_NORMALISED_OBJECT_SCHEMA = MapType(
    StringType(),
    StructType([
        StructField("value", StringType(), True),
    ]),
)
_NORMALISED_SCALAR_SCHEMA = MapType(StringType(), StringType())


@dataclass
class NormalisedRecord:
    """Stage 3 record shape used by the pure-Python section 5 implementation."""

    tenant_id: str
    cdm_entity_type: str
    connector_id: str
    source_table: str
    source_record_id: str
    source_ts: str
    canonical_fields: dict[str, Any]
    materialization_level: str = "hot"
    source_op: str = "INSERT"
    cdm_entity_id: str = ""
    er_confidence: float = 0.0


@dataclass
class ResolutionResult:
    """Minimal Stage 2 output needed by Stage 3 synthesis."""

    cdm_entity_id: str
    confidence: float = 0.0
    is_provisional: bool = False
    is_new_entity: bool = False


@dataclass
class ProvenanceChange:
    """One attribute-level provenance mutation produced by synthesis."""

    action: str
    row: ProvenanceRow
    changed: bool = True


@dataclass
class DeleteResult:
    """Result of the section 5.5 source-delete re-election flow."""

    affected_cdm_entity_id: str | None
    action: str
    re_elected_attributes: list[str] = field(default_factory=list)
    removed_attributes: list[str] = field(default_factory=list)


@dataclass
class ERIndexEntry:
    """Minimal surviving-source handle used during re-election."""

    connector_id: str
    source_table: str
    source_record_id: str
    source_ts: str
    confidence: float = 0.0
    completeness_score: float = 0.0
    cdm_entity_id: str = ""


@dataclass
class SynthesisContext:
    """Small dependency bundle for the section 5 pure-Python API."""

    provenance: Any
    survivorship_rules: Any
    gr_index: Any
    er_index: Any
    completeness_cache: Any | None = None
    delta_lake: Any | None = None
    events: Any | None = None


def synthesise(
    *args: Any,
) -> DataFrame | SynthesisResult:
    """Dispatch Stage 3 synthesis.

    Supported call shapes:
    - `synthesise(df, survivorship_broadcast)` for the current Spark DataFrame path
    - `synthesise(record, resolution, ctx)` for the section 5 pure-Python path
    """
    if len(args) == 2 and isinstance(args[0], DataFrame):
        return _synthesise_dataframe(args[0], args[1])

    if len(args) == 3 and not isinstance(args[0], DataFrame):
        return _synthesise_record(args[0], args[1], args[2])

    raise TypeError(
        "synthesise() expects either (DataFrame, Broadcast) or "
        "(NormalisedRecord, ResolutionResult, SynthesisContext)"
    )


def _synthesise_dataframe(
    df: DataFrame,
    survivorship_broadcast: Broadcast,
) -> DataFrame:
    """Apply survivorship rules and produce the Golden Record canonical values.

    Args:
        df:                   Enriched DataFrame with cdm_entity_id from Stage 2.
        survivorship_broadcast: Broadcast[SurvivorshipBroadcast].

    Returns:
        DataFrame with added columns:
        - golden_fields_json    (str, JSON: attribute_name → canonical value)
        - attribute_provenance_json (str, JSON: attribute_name → connector:record)
        - provenance_hash       (str, SHA-256/128 hex)
    """
    # The current DataFrame path is a per-row projection from normalised fields to
    # canonical values. Keep it on JVM expressions so local Windows test runs do
    # not depend on Python UDF workers.
    _ = survivorship_broadcast

    parsed_object_fields = F.from_json(F.col("normalised_json"), _NORMALISED_OBJECT_SCHEMA)
    parsed_scalar_fields = F.from_json(F.col("normalised_json"), _NORMALISED_SCALAR_SCHEMA)
    extracted_object_fields = F.when(
        parsed_object_fields.isNotNull(),
        F.transform_values(parsed_object_fields, lambda _, field: field["value"]),
    )
    raw_golden_fields = F.map_filter(
        F.coalesce(
            extracted_object_fields,
            parsed_scalar_fields,
            F.expr("cast(map() as map<string,string>)"),
        ),
        lambda _, value: value.isNotNull(),
    )
    golden_keys = F.sort_array(F.map_keys(raw_golden_fields))
    golden_fields = F.map_from_arrays(
        golden_keys,
        F.transform(golden_keys, lambda attr: F.element_at(raw_golden_fields, attr)),
    )

    record_id = F.coalesce(F.col("source_record_id").cast("string"), F.lit(""))
    source_pointer = F.concat_ws(
        ":",
        F.coalesce(F.col("source_system").cast("string"), F.lit("")),
        record_id,
    )
    attribute_provenance = F.map_from_arrays(
        golden_keys,
        F.transform(golden_keys, lambda _: source_pointer),
    )
    provenance_summary = F.aggregate(
        golden_keys,
        F.lit(""),
        lambda acc, attr: F.concat(
            acc,
            F.when(F.length(acc) > 0, F.lit("|")).otherwise(F.lit("")),
            attr,
            F.lit("="),
            record_id,
            F.lit(":"),
            F.sha2(F.element_at(golden_fields, attr).cast("string"), 256),
        ),
    )

    result = df.withColumn(
        "golden_fields_json",
        F.to_json(golden_fields),
    ).withColumn(
        "attribute_provenance_json",
        F.to_json(attribute_provenance),
    ).withColumn(
        "provenance_hash",
        F.concat(F.lit("sha256:"), F.sha2(provenance_summary, 256)),
    )

    SYNTHESIS_RECORDS.labels(tenant_id="system", cdm_entity_type="mixed", status="ok").inc()
    return result


def _synthesise_record(
    record: NormalisedRecord,
    resolution: ResolutionResult,
    ctx: SynthesisContext,
) -> SynthesisResult:
    """Pure-Python Stage 3 implementation matching library spec section 5.1.

    This path is the algorithmic implementation used by tests and future
    foreachBatch integration. It complements, but does not replace yet, the
    current DataFrame/UDF helper above.
    """
    if resolution.is_provisional:
        return SynthesisResult(
            cdm_entity_id=resolution.cdm_entity_id,
            provenance_hash="",
            hash_changed=False,
        )

    cdm_entity_id = resolution.cdm_entity_id
    record.cdm_entity_id = cdm_entity_id
    record.er_confidence = resolution.confidence

    existing_provenance = _coerce_provenance_map(ctx.provenance.get_all(cdm_entity_id))
    rules = _get_rules_map(ctx.survivorship_rules, record.tenant_id, record.cdm_entity_type)

    provenance_changes: list[ProvenanceChange] = []
    for attr_name, raw_attr_value in record.canonical_fields.items():
        attr_value = _extract_field_value(raw_attr_value)
        if attr_value is None:
            continue

        rule = rules.get(
            attr_name,
            SurvivorshipRule(
                tenant_id=record.tenant_id,
                cdm_entity_type=record.cdm_entity_type,
                attribute_name=attr_name,
                rule_type=SurvivorshipRuleType.MOST_RECENT,
            ),
        )
        existing = existing_provenance.get(attr_name)
        change = _evaluate_survivorship(
            attr_name=attr_name,
            attr_value=attr_value,
            incoming_record=record,
            existing_prov=existing,
            rule=rule,
            ctx=ctx,
        )
        if change is not None:
            provenance_changes.append(change)

    actually_changed = False
    for change in provenance_changes:
        written = _upsert_provenance_row(change.row, ctx)
        if written:
            actually_changed = True

    updated_provenance = dict(existing_provenance)
    for change in provenance_changes:
        if change.changed:
            updated_provenance[change.row.attribute_name] = change.row

    previous_hash = _get_previous_hash(ctx, cdm_entity_id)
    if actually_changed or resolution.is_new_entity:
        new_hash = _compute_provenance_hash(updated_provenance)
        hash_changed = new_hash != previous_hash
        if hash_changed:
            ctx.gr_index.update_hash(cdm_entity_id, new_hash)
    else:
        new_hash = previous_hash
        hash_changed = False

    attribute_provenance = {
        attr_name: f"{row.winning_connector_id}:{row.winning_record_id}"
        for attr_name, row in sorted(updated_provenance.items())
    }
    contributing_sources = sorted({row.winning_connector_id for row in updated_provenance.values()})

    return SynthesisResult(
        cdm_entity_id=cdm_entity_id,
        rows_to_upsert=[change.row for change in provenance_changes if change.changed],
        rows_to_delete=[],
        provenance_hash=new_hash,
        hash_changed=hash_changed,
        contributing_sources=contributing_sources,
        attribute_provenance=attribute_provenance,
    )


def _synthesise_row(survivorship_bc: Broadcast):
    """UDF closure: apply survivorship rules to a single record."""

    def _fn(
        tenant_id: str,
        cdm_entity_type: str,
        cdm_entity_id: str,
        normalised_json: str,
        source_system: str,
        source_record_id: str,
        source_ts: Any,
        dq_score: str,
    ) -> str:
        import time
        t0 = time.perf_counter()

        ruleset: SurvivorshipRuleSet = survivorship_bc.value
        fields: dict[str, Any] = json.loads(normalised_json or "{}")

        golden: dict[str, Any] = {}
        for attr, field_val in fields.items():
            rule = ruleset.get_rule(tenant_id, cdm_entity_type, attr)
            value = field_val.get("value") if isinstance(field_val, dict) else field_val
            raw_dq = float(dq_score or "1.0")

            canonical = _apply_rule(
                rule_type=rule.rule_type if rule else SurvivorshipRuleType.MOST_RECENT,
                candidate_value=value,
                candidate_ts=str(source_ts or ""),
                candidate_system=source_system,
                candidate_dq=raw_dq,
                priority_sources=rule.priority_sources if rule else [],
                existing=golden.get(attr),
            )
            if canonical is not None:
                golden[attr] = canonical

        return json.dumps(golden, default=str)

    return _fn


def _compute_provenance_hash_udf():
    """UDF: compute deterministic provenance hash from winner pointers."""

    def _fn(attribute_provenance_json: str, golden_fields_json: str) -> str:
        try:
            attribute_provenance = json.loads(attribute_provenance_json or "{}")
        except json.JSONDecodeError:
            attribute_provenance = {}

        try:
            golden_fields = json.loads(golden_fields_json or "{}")
        except json.JSONDecodeError:
            golden_fields = {}

        winners: dict[str, str] = {}
        value_hashes: dict[str, str] = {}
        for attribute_name, pointer in attribute_provenance.items():
            if isinstance(pointer, str) and ":" in pointer:
                _, winning_record_id = pointer.split(":", 1)
            else:
                winning_record_id = str(pointer)
            winners[attribute_name] = winning_record_id

            if attribute_name in golden_fields:
                value_hashes[attribute_name] = sha256_hex(str(golden_fields[attribute_name]))

        return provenance_hash_from_winning_records(winners, value_hashes)

    return _fn


def _build_attribute_provenance_json():
    """UDF: build a Stage 3 provenance summary without persisting business values."""

    def _fn(golden_fields_json: str, source_system: str, source_record_id: str) -> str:
        try:
            golden_fields = json.loads(golden_fields_json or "{}")
        except json.JSONDecodeError:
            golden_fields = {}

        attribute_provenance = {
            attribute_name: f"{source_system}:{source_record_id}"
            for attribute_name in sorted(golden_fields)
        }
        return json.dumps(attribute_provenance, sort_keys=True)

    return _fn


def _evaluate_survivorship(
    attr_name: str,
    attr_value: Any,
    incoming_record: NormalisedRecord,
    existing_prov: ProvenanceRow | None,
    rule: SurvivorshipRule,
    ctx: SynthesisContext,
) -> ProvenanceChange | None:
    """Section 5.2 — attribute-level survivorship evaluation."""
    incoming_hash = sha256_hex(str(attr_value))

    if existing_prov is None:
        return ProvenanceChange(
            action="insert",
            row=_make_provenance_row(incoming_record, attr_name, incoming_hash, _rule_kind(rule)),
            changed=True,
        )

    if _rule_kind(rule) == SurvivorshipRuleType.MANUAL.value:
        return None

    incoming_wins = _apply_survivorship_rule(rule, incoming_record, existing_prov, ctx)
    if not incoming_wins:
        return None

    if (
        existing_prov.observed_value_hash == incoming_hash
        and existing_prov.winning_connector_id == incoming_record.connector_id
        and existing_prov.winning_record_id == incoming_record.source_record_id
    ):
        return ProvenanceChange(action="update", row=existing_prov, changed=False)

    return ProvenanceChange(
        action="update",
        row=_make_provenance_row(incoming_record, attr_name, incoming_hash, _rule_kind(rule)),
        changed=True,
    )


def _upsert_provenance_row(row: ProvenanceRow, ctx: SynthesisContext) -> bool:
    """Section 5.3 — idempotent provenance upsert wrapper."""
    return bool(ctx.provenance.upsert_returning_changed(row))


def _compute_provenance_hash(provenance: dict[str, ProvenanceRow]) -> str:
    """Section 5.4 — deterministic Golden Record provenance hash."""
    winners = {attr_name: row.winning_record_id for attr_name, row in provenance.items()}
    value_hashes = {attr_name: row.observed_value_hash for attr_name, row in provenance.items()}
    return provenance_hash_from_winning_records(winners, value_hashes)


def handle_source_delete(
    source_record_id: str,
    connector_id: str,
    source_table: str,
    tenant_id: str,
    cdm_entity_type: str,
    ctx: SynthesisContext,
) -> DeleteResult:
    """Section 5.5 — remove a source and re-elect winners from survivors."""
    er_entry = ctx.er_index.lookup(
        tenant_id=tenant_id,
        connector_id=connector_id,
        source_table=source_table,
        source_record_id=source_record_id,
    )
    if er_entry is None:
        return DeleteResult(affected_cdm_entity_id=None, action="no_op")

    cdm_entity_id = er_entry.cdm_entity_id
    winning_attributes = list(
        ctx.provenance.get_attributes_won_by(
            cdm_entity_id=cdm_entity_id,
            connector_id=connector_id,
            source_record_id=source_record_id,
        )
    )

    ctx.er_index.delete(
        connector_id=connector_id,
        source_table=source_table,
        source_record_id=source_record_id,
    )

    remaining_sources = [
        _coerce_er_index_entry(source)
        for source in ctx.er_index.get_all_for_entity(cdm_entity_id)
    ]

    if not remaining_sources:
        ctx.gr_index.update(
            cdm_entity_id=cdm_entity_id,
            state="tombstoned",
            state_change_reason="all_sources_deleted",
        )
        ctx.provenance.delete_all(cdm_entity_id)
        if ctx.events is not None and hasattr(ctx.events, "emit_remove"):
            ctx.events.emit_remove(cdm_entity_id, tenant_id)
        return DeleteResult(affected_cdm_entity_id=cdm_entity_id, action="tombstoned")

    rules = _get_rules_map(ctx.survivorship_rules, tenant_id, cdm_entity_type)
    re_elected: list[str] = []
    removed: list[str] = []

    for attr_name in winning_attributes:
        ctx.provenance.delete_attribute(
            cdm_entity_id=cdm_entity_id,
            attribute_name=attr_name,
            connector_id=connector_id,
            source_record_id=source_record_id,
        )

        rule = rules.get(
            attr_name,
            SurvivorshipRule(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                attribute_name=attr_name,
                rule_type=SurvivorshipRuleType.MOST_RECENT,
            ),
        )

        new_winner = _re_elect_winner(
            cdm_entity_id=cdm_entity_id,
            attr_name=attr_name,
            remaining_sources=remaining_sources,
            rule=rule,
            ctx=ctx,
        )
        if new_winner is not None:
            ctx.provenance.upsert_returning_changed(new_winner)
            re_elected.append(attr_name)
        else:
            removed.append(attr_name)

    new_hash = _compute_provenance_hash(_coerce_provenance_map(ctx.provenance.get_all(cdm_entity_id)))
    ctx.gr_index.update_hash(cdm_entity_id, new_hash)
    if ctx.events is not None and hasattr(ctx.events, "emit_upsert"):
        ctx.events.emit_upsert(cdm_entity_id, tenant_id)

    return DeleteResult(
        affected_cdm_entity_id=cdm_entity_id,
        action="re_elected",
        re_elected_attributes=re_elected,
        removed_attributes=removed,
    )


def _re_elect_winner(
    cdm_entity_id: str,
    attr_name: str,
    remaining_sources: list[ERIndexEntry],
    rule: SurvivorshipRule,
    ctx: SynthesisContext,
) -> ProvenanceRow | None:
    """Re-run survivorship for one attribute over the full surviving source set."""
    candidates: list[tuple[ERIndexEntry, Any]] = []
    for source in remaining_sources:
        value = ctx.delta_lake.read_attribute(
            connector_id=source.connector_id,
            source_table=source.source_table,
            source_record_id=source.source_record_id,
            attribute_name=attr_name,
        )
        if value is not None:
            candidates.append((source, value))

    if not candidates:
        return None

    kind = _rule_kind(rule)
    if kind == SurvivorshipRuleType.SOURCE_PRIORITY.value:
        priority = _rule_priority_sources(rule)
        candidates.sort(key=lambda candidate: _priority_rank(candidate[0].connector_id, priority))
    elif kind == SurvivorshipRuleType.MOST_RECENT.value:
        candidates.sort(key=lambda candidate: candidate[0].source_ts, reverse=True)
    elif kind == SurvivorshipRuleType.MOST_COMPLETE.value:
        candidates.sort(key=lambda candidate: candidate[0].completeness_score, reverse=True)
    elif kind == SurvivorshipRuleType.MOST_CONFIDENT.value:
        candidates.sort(key=lambda candidate: candidate[0].confidence, reverse=True)
    elif kind == SurvivorshipRuleType.FIRST_OBSERVED.value:
        candidates.sort(key=lambda candidate: candidate[0].source_ts)
    elif kind == SurvivorshipRuleType.MANUAL.value:
        return None

    winner_source, winner_value = candidates[0]
    return ProvenanceRow(
        cdm_entity_id=cdm_entity_id,
        attribute_name=attr_name,
        winning_connector_id=winner_source.connector_id,
        winning_source_table=winner_source.source_table,
        winning_record_id=winner_source.source_record_id,
        observed_value_hash=sha256_hex(str(winner_value)),
        observed_at=winner_source.source_ts,
        rule_applied=kind,
    )


# ---------------------------------------------------------------------------
# Survivorship rule application
# ---------------------------------------------------------------------------

def _apply_rule(
    rule_type: SurvivorshipRuleType,
    candidate_value: Any,
    candidate_ts: str,
    candidate_system: str,
    candidate_dq: float,
    priority_sources: list[str],
    existing: Any,
) -> Any:
    """Apply one survivorship rule to decide between existing and candidate values.

    In streaming mode, each record is processed independently. For multi-source
    survivorship (e.g. MOST_COMPLETE), full re-evaluation happens in foreachBatch
    via Stage 3 re-synthesis using get_all_provenance().
    """
    if candidate_value is None:
        return existing

    if existing is None:
        return candidate_value

    if rule_type == SurvivorshipRuleType.MOST_RECENT:
        # Caller must pass source_ts; choose whichever is more recent
        # In UDF we can only compare within one record at a time; return candidate
        # (full multi-source comparison happens in foreachBatch re-synthesis)
        return candidate_value

    elif rule_type == SurvivorshipRuleType.SOURCE_PRIORITY:
        # If candidate comes from a priority source, it wins
        if candidate_system in priority_sources:
            return candidate_value
        return existing

    elif rule_type in (
        SurvivorshipRuleType.MOST_CONFIDENT,
        SurvivorshipRuleType.HIGHEST_CONFIDENCE,
    ):
        return candidate_value  # DQ scoring re-evaluated in foreachBatch

    elif rule_type == SurvivorshipRuleType.FIRST_OBSERVED:
        # The first chosen source remains the winner until a full re-election path runs.
        return existing

    elif rule_type == SurvivorshipRuleType.MANUAL:
        # A manual steward override must not be replaced by the streaming row-level pass.
        return existing

    elif rule_type == SurvivorshipRuleType.LONGEST_VALUE:
        return candidate_value if len(str(candidate_value)) >= len(str(existing)) else existing

    elif rule_type == SurvivorshipRuleType.EXACT_MATCH:
        return candidate_value if candidate_value == existing else None

    # Default: MOST_RECENT
    return candidate_value


def _apply_survivorship_rule(
    rule: SurvivorshipRule,
    incoming: NormalisedRecord,
    existing_prov: ProvenanceRow,
    ctx: SynthesisContext,
) -> bool:
    """Pure-Python section 5.2 rule evaluation against the current winner."""
    kind = _rule_kind(rule)

    if kind == SurvivorshipRuleType.SOURCE_PRIORITY.value:
        priority = _rule_priority_sources(rule)
        existing_rank = _priority_rank(existing_prov.winning_connector_id, priority)
        incoming_rank = _priority_rank(incoming.connector_id, priority)
        return incoming_rank < existing_rank

    if kind == SurvivorshipRuleType.MOST_RECENT.value:
        return incoming.source_ts > existing_prov.observed_at

    if kind == SurvivorshipRuleType.MOST_COMPLETE.value:
        incoming_completeness = sum(1 for value in incoming.canonical_fields.values() if _extract_field_value(value) is not None)
        existing_completeness = 0
        if ctx.completeness_cache is not None and hasattr(ctx.completeness_cache, "get"):
            existing_completeness = ctx.completeness_cache.get(
                cdm_entity_id=existing_prov.cdm_entity_id,
                connector_id=existing_prov.winning_connector_id,
            )
        if incoming_completeness != existing_completeness:
            return incoming_completeness > existing_completeness
        return incoming.source_ts > existing_prov.observed_at

    if kind == SurvivorshipRuleType.MOST_CONFIDENT.value:
        existing_confidence = 0.0
        if ctx.er_index is not None and hasattr(ctx.er_index, "get_confidence"):
            existing_confidence = ctx.er_index.get_confidence(
                cdm_entity_id=existing_prov.cdm_entity_id,
                connector_id=existing_prov.winning_connector_id,
            )
        return incoming.er_confidence > existing_confidence

    if kind in (SurvivorshipRuleType.FIRST_OBSERVED.value, SurvivorshipRuleType.MANUAL.value):
        return False

    return False


def _make_provenance_row(
    record: NormalisedRecord,
    attr_name: str,
    value_hash: str,
    rule_kind: str,
) -> ProvenanceRow:
    return ProvenanceRow(
        cdm_entity_id=record.cdm_entity_id,
        attribute_name=attr_name,
        winning_connector_id=record.connector_id,
        winning_source_table=record.source_table,
        winning_record_id=record.source_record_id,
        observed_value_hash=value_hash,
        observed_at=record.source_ts,
        rule_applied=rule_kind,
    )


def _priority_rank(connector_id: str, priority: list[str]) -> int:
    try:
        return priority.index(connector_id)
    except ValueError:
        return len(priority)


def _coerce_provenance_map(provenance_rows: Any) -> dict[str, ProvenanceRow]:
    if provenance_rows is None:
        return {}
    if isinstance(provenance_rows, dict):
        return provenance_rows
    if isinstance(provenance_rows, list):
        return {row.attribute_name: row for row in provenance_rows}
    raise TypeError(f"Unsupported provenance container: {type(provenance_rows)!r}")


def _coerce_er_index_entry(entry: Any) -> ERIndexEntry:
    if isinstance(entry, ERIndexEntry):
        return entry
    if isinstance(entry, dict):
        return ERIndexEntry(
            connector_id=entry["connector_id"],
            source_table=entry["source_table"],
            source_record_id=entry["source_record_id"],
            source_ts=str(entry.get("source_ts", "")),
            confidence=float(entry.get("confidence", 0.0) or 0.0),
            completeness_score=float(entry.get("completeness_score", 0.0) or 0.0),
            cdm_entity_id=str(entry.get("cdm_entity_id", "")),
        )
    return ERIndexEntry(
        connector_id=entry.connector_id,
        source_table=entry.source_table,
        source_record_id=entry.source_record_id,
        source_ts=str(getattr(entry, "source_ts", "")),
        confidence=float(getattr(entry, "confidence", 0.0) or 0.0),
        completeness_score=float(getattr(entry, "completeness_score", 0.0) or 0.0),
        cdm_entity_id=str(getattr(entry, "cdm_entity_id", "")),
    )


def _extract_field_value(field_value: Any) -> Any:
    if isinstance(field_value, dict) and "value" in field_value:
        return field_value["value"]
    return field_value


def _get_previous_hash(ctx: SynthesisContext, cdm_entity_id: str) -> str:
    if ctx.gr_index is not None and hasattr(ctx.gr_index, "get_hash"):
        return ctx.gr_index.get_hash(cdm_entity_id)
    return ""


def _get_rules_map(survivorship_rules: Any, tenant_id: str, cdm_entity_type: str) -> dict[str, SurvivorshipRule]:
    if survivorship_rules is None:
        return {}
    if isinstance(survivorship_rules, SurvivorshipRuleSet):
        return {
            attribute_name: rule
            for (rule_tenant_id, rule_entity_type, attribute_name), rule in survivorship_rules.rules.items()
            if rule_tenant_id == tenant_id and rule_entity_type == cdm_entity_type
        }
    if hasattr(survivorship_rules, "get_all"):
        return dict(survivorship_rules.get_all(tenant_id=tenant_id, cdm_entity_type=cdm_entity_type))
    raise TypeError(f"Unsupported survivorship rules accessor: {type(survivorship_rules)!r}")


def _rule_kind(rule: SurvivorshipRule) -> str:
    rule_type = getattr(rule, "rule_type", SurvivorshipRuleType.MOST_RECENT)
    return rule_type.value if hasattr(rule_type, "value") else str(rule_type)


def _rule_priority_sources(rule: SurvivorshipRule) -> list[str]:
    priority_sources = getattr(rule, "priority_sources", None)
    if priority_sources:
        return list(priority_sources)
    return []
