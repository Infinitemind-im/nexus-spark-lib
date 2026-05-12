# Golden Record Synthesis — Spec Extract And Implementation Notes

This file condenses the Golden Record Synthesis requirements that currently matter for implementation in `nexus-spark-lib` and later integration in `nexus-spark-transformer`.

## Authoritative Sources

- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/specs/developer-workstreams/NEXUS-Iter2-SPEC-ER-CRUD-v0.1.md`
- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/specs/pipeline/NEXUS-Iter2-REF-DataPaths-v0.3.md`
- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/specs/libraries/NEXUS-Iter2-SPEC-LIB-SparkTransform-v0.1.md`
- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/specs/developer-workstreams/NEXUS-Iter2-SVC-nexus-spark-transformer-v0.1.md`

## What Stage 3 Must Do

Stage 3 lives in `nexus_spark_lib` and is called from `nexus-spark-transformer` after Stage 2 ER has produced a resolved `cdm_entity_id`.

This extract is specifically about **section 5 of the library spec**:

- `5.1 Entry point`
- `5.2 Survivorship evaluation`
- `5.3 Idempotent provenance upsert`
- `5.4 Provenance hash computation`
- `5.5 Edge case: Source DELETE — survivorship re-election`

For each canonical attribute contributed by a record:

- Load the survivorship rule from `nexus_system.survivorship_rules`.
- Compare the incoming source against the current winner for that attribute.
- Insert, update, or delete rows in `nexus_system.golden_record_provenance`.
- Recompute `golden_records_index.provenance_hash`.
- Emit downstream `transformed_records` with provenance metadata.

Additional constraints stated explicitly in `5.1`:

- Stage 3 is called immediately after `resolve()`.
- Only `hot` records are synthesised.
- `warm` and `cold` records are not synthesised.
- `provisional` records are not synthesised.

## Rule Set Required By Spec

The supported rule types in the Iteration 2 specs are:

- `source_priority`
- `most_recent`
- `most_complete`
- `most_confident`
- `first_observed`
- `manual`

Important constraint:

- Survivorship must be deterministic for the same full contributing source set, regardless of event order.
- Full re-evaluation is mandatory for DELETE handling and survivorship-rebuild operations.

## Provenance Table Contract

`golden_record_provenance` is a pointer table, not a business-value store.

Required fields from the spec:

- `cdm_entity_id`
- `attribute_name`
- `winning_connector_id`
- `winning_source_table`
- `winning_record_id`
- `observed_value_hash`
- `observed_at`
- `rule_applied`

Hard constraint:

- No raw business field value is stored in provenance.
- Only hashes and source pointers are stored there.
- This preserves the Virtual CDM rule.

## Provenance Hash Contract

The **library spec section 5.4** says the hash input is:

- sorted `(attribute_name, winning_record_id, value_hash)` tuples

This is the contract to implement inside `nexus-spark-lib`.

Important note about spec consistency:

- Some higher-level pipeline docs abbreviate the description to sorted `(attribute_name, winning_record_id)` pairs.
- For actual Stage 3 library work, the more detailed section `5.4` is the better implementation target because it detects a winner keeping the same source record while the source value itself changes.

So the working contract is:

- `provenance_hash = sha256(sorted(attribute_name, winning_record_id, value_hash))`

Purpose:

- Downstream services can detect a Golden Record content change without reading every provenance row.
- The M3 writer can short-circuit when the hash is unchanged.

## CRUD Semantics Required

### INSERT / SNAPSHOT_READ

- Resolve to a new or existing `cdm_entity_id`.
- First contributor wins every non-null canonical attribute it supplies.
- Create one provenance row per winning attribute.

### UPDATE

- Compute attribute-level diff.
- Re-run ER only when ER-relevant attributes changed.
- Re-run survivorship for changed attributes.
- Replace provenance row only when the winner changes or the winner's value hash changes.

### 5.3 Idempotent upsert semantics

- Upsert key is `(cdm_entity_id, attribute_name)`.
- Update is conditional: only write when the new `observed_value_hash` differs.
- Replay of the same event must be a no-op.

### DELETE

- Remove provenance rows that were won by the deleted source.
- Re-elect winners for affected attributes from surviving sources.
- Read surviving values from Delta Lake, not from provenance.
- If no provenance remains, tombstone the Golden Record and emit `REMOVE`.
- Otherwise recompute `provenance_hash` and emit `UPSERT`.

Additional `5.5` details that matter for implementation:

- First look up `entity_resolution_index` to recover `cdm_entity_id`.
- Remove the deleted source from `entity_resolution_index`.
- Re-election is always over the **full surviving source set**, never incrementally.
- If surviving sources exist but none has a non-null value for an attribute, that attribute disappears from the GR.

### RELEVEL

- Skip ER lookup changes.
- Re-run synthesis because survivorship rules may have changed.

## What The Transformer Must Eventually Wire

The target pipeline is:

1. Stage 0 — materialization gate
2. Stage 1 — normalisation
3. Stage 2 — resolve
4. Stage 3 — synthesise
5. Publish `m1.int.transformed_records`

Current repository status during this work:

- `nexus-spark-lib` already contains Stage 2 and Stage 3 code.
- `nexus-spark-transformer` currently orchestrates only Stage 0 + Stage 1 in `handlers/foreachbatch.py`.
- So yes: the correct order is to fix the Stage 3 contract in `spark-lib` first, then wire it into `spark-transformer`, then run focused tests.

## Gaps Found In Current Codebase

- `nexus_spark_lib.transform.stage3_synthesise` is still a simplified per-row implementation.
- Current Stage 3 still needs to fully implement the section `5.1` pseudocode shape rather than only a row-level approximation.
- The hash logic originally drifted away from the detailed `5.4` contract and must follow `(attribute_name, winning_record_id, value_hash)`.
- `nexus_spark_lib.db.golden_records` still persists raw `source_value`, which violates Virtual CDM.
- `nexus-spark-transformer` does not yet invoke Stage 2 or Stage 3.

## Recommended Implementation Order

1. Fix `spark-lib` provenance contract:
   - pointer-only provenance rows
   - spec-aligned provenance hash
   - Stage 3 provenance summary output
2. Extend Stage 3 rule engine toward the full spec rule set.
3. Wire Stage 2 + Stage 3 into `nexus-spark-transformer`.
4. Add DELETE / re-election coverage.
5. Run focused library tests, then transformer integration tests.