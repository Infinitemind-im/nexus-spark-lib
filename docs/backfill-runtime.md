# Bulk extraction runtime

Install the `backfill` extra. Initial and transaction extraction resolve a local
registry in this published library; no shared `nexus_ports` distribution is added.
Unknown source types fail without producing demo records.

Connectors must exist, be enabled and active in the tenant-scoped registry.
Database sources require `bf__<connector_id>__dsn` or a JSON `pg_sources` list
in Airflow Variables. Each source declares its schemas. API sources require a
JSON `bf__<connector_id>__credentials` dictionary with their credential environment
names. Alternatively, global credentials require both BACKFILL_CONNECTOR_ID and
NEXUS_TENANT_ID to identify that exact registered connector. Unspecified API
credentials are cleared during extraction and restored afterwards.

Database extraction currently supports tables with a single primary key. It
rejects missing/composite keys rather than emitting ambiguous record IDs.
Initial extraction excludes timestamped tables unless `initial_tables` explicitly
selects them. A full source snapshot includes timestamped tables. Transaction
windows require persisted per-table policies in transaction_backfill_configs;
there is no implicit fallback that advances a twenty-year cursor.

Window planning does not change the cursor. Completion advances it only after
all publication callbacks and Kafka flushes confirm delivery. End boundaries
are exclusive; partial final windows are retained. Retrying a committed
checkpoint is idempotent, and a changed checkpoint is rejected.

Handover state is stored in connector_backfill_handover. Source corpus estimates
come from this persisted row; monetary estimates are unavailable without a
pricing contract. Raw-topic retention is checked before initial extraction and
must be unlimited or at least fourteen days. These functions publish raw
records to Kafka and do not write to AI stores.
