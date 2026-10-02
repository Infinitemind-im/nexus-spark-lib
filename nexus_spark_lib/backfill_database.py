"""Database bulk extraction publishes raw records; it owns no AI-store writes."""
import json
from datetime import timezone

import psycopg2
from psycopg2 import sql

from nexus_spark_lib.backfill import _base_payload, _connector_var, _csv, _iso_now, _producer


def _sources(connector):
    raw = _connector_var(connector,"pg_sources")
    if raw:
        sources = json.loads(raw)
        if not isinstance(sources,list) or not sources:
            raise ValueError("Database extractor requires a nonempty source list")
        return sources
    dsn = _connector_var(connector,"dsn")
    if not dsn:
        raise RuntimeError("Database credentials must be configured for this connector")
    return [{"dsn":dsn,"schemas":_csv(_connector_var(connector,"schemas","public"))}]


def _table_names(conn, schemas, timestamp_column=None, exclude_columns=()):
    rows = conn.cursor()
    try:
        rows.execute("""SELECT t.table_schema,t.table_name FROM information_schema.tables t
            WHERE t.table_type='BASE TABLE' AND t.table_schema=ANY(%s)
              AND t.table_schema NOT IN ('pg_catalog','information_schema')
              AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM information_schema.columns c
                WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name AND c.column_name=%s))
              AND NOT EXISTS (SELECT 1 FROM information_schema.columns c
                WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name AND lower(c.column_name)=ANY(%s))
            ORDER BY t.table_schema,t.table_name""",
            (schemas,timestamp_column,timestamp_column,list(exclude_columns)))
        return [(schema,table) for schema,table in rows.fetchall()]
    finally:
        rows.close()


def _record_keys(conn, schema, table):
    with conn.cursor() as cursor:
        cursor.execute("""SELECT a.attname FROM pg_index i
            JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
            CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,position)
            JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum
            WHERE i.indisprimary AND n.nspname=%s AND t.relname=%s ORDER BY k.position""", (schema,table))
        keys = [row[0] for row in cursor.fetchall()]
    if len(keys) != 1:
        raise ValueError("Database backfill requires one primary record key per table")
    return keys[0]


def extract_tables(connector, publish_topic, start_date=None, end_date=None,
                   *, table_name=None, timestamp_column=None, full_snapshot=False):
    from nexus_spark_lib.backfill import _control_connection
    window = {"start":start_date,"end":end_date} if start_date and end_date else None
    with _control_connection(connector.tenant_id) as control:
        if not control.execute("SELECT 1 FROM nexus_system.connectors WHERE tenant_id=%s AND connector_id::text=%s AND enabled AND active",
                               (connector.tenant_id,connector.connector_id)).fetchone():
            raise ValueError("Extractor connector does not belong to the active tenant")
    producer = _producer()
    total = 0
    for source in _sources(connector):
        schemas = source.get("schemas") or ["public"]
        if isinstance(schemas,str):
            schemas = _csv(schemas)
        with psycopg2.connect(source["dsn"],connect_timeout=10) as conn:
            exclusions = _csv(_connector_var(connector,"initial_exclude_timestamp_columns",
                "modifieddate,created_at,updated_at,write_date,sys_updated_on,lastmodifieddate")) if not window and not full_snapshot else []
            explicit = table_name or _connector_var(connector,"initial_tables" if not window else "transaction_tables")
            tables = [tuple(name.split(".",1)) if "." in name else (schemas[0],name) for name in _csv(explicit)] if explicit else _table_names(conn,schemas,timestamp_column,exclusions)
            if window and not timestamp_column:
                raise ValueError("Transaction window requires its timestamp column")
            for schema,table in tables:
                if schema not in schemas:
                    raise ValueError("Requested table is outside this connector's schemas")
                key = _record_keys(conn,schema,table)
                query = sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema),sql.Identifier(table))
                params = ()
                if window:
                    query += sql.SQL(" WHERE {} >= %s AND {} < %s ORDER BY {}").format(
                        sql.Identifier(timestamp_column),sql.Identifier(timestamp_column),sql.Identifier(timestamp_column))
                    params = (start_date,end_date)
                with conn.cursor(name="nexus_bulk_extract") as cursor:
                    cursor.itersize=1000
                    cursor.execute(query,params)
                    names = None
                    for row in cursor:
                        names = names or [column.name for column in cursor.description]
                        record = dict(zip(names,row))
                        ts = record.get(timestamp_column) if timestamp_column else None
                        if ts is not None and getattr(ts,"tzinfo",None) is None and hasattr(ts,"replace"):
                            ts=ts.replace(tzinfo=timezone.utc)
                        payload=_base_payload(connector,connector.source_type,f"{schema}.{table}",
                            str(record[key]),ts.isoformat() if hasattr(ts,"isoformat") else _iso_now(),record,window)
                        producer.produce(publish_topic,key=str(record[key]).encode(),value=json.dumps(payload,default=str).encode())
                        total+=1
                producer.flush()
    return {"records_published":total}
