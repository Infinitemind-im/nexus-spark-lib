"""Real source PostgreSQL extraction: tenant binding, PK=0 and window bounds."""
import json
import os
import subprocess
import time
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest

from nexus_spark_lib import backfill


@pytest.mark.integration
def test_database_backfill_reads_real_rows_without_other_tenant_control_state(monkeypatch):
    image="pgvector/pgvector@sha256:c3c84b85691a264aa3c5b8fc1d611e67d42b0cca8596e3d3d22dc2424c12c4e2"
    container=subprocess.check_output(["docker","run","--rm","-d","-p","127.0.0.1::5432",
        "-e","POSTGRES_PASSWORD=source-fixture",image],text=True).strip()
    try:
        port=subprocess.check_output(["docker","port",container,"5432/tcp"],text=True).strip().rsplit(":",1)[1]
        dsn=f"postgresql://postgres:source-fixture@127.0.0.1:{port}/postgres"
        for attempt in range(30):
            try:
                with psycopg.connect(dsn,connect_timeout=1):
                    break
            except psycopg.OperationalError:
                if attempt==29:
                    raise
                time.sleep(1)
        connector_id=str(uuid4())
        with psycopg.connect(dsn) as conn:
            conn.execute("""CREATE SCHEMA nexus_system;
                CREATE TABLE nexus_system.connectors (tenant_id text,connector_id uuid,
                  connector_type text,system_type text,source_system text,enabled bool,active bool);
                CREATE ROLE bulk_fixture LOGIN PASSWORD 'source-fixture';
                GRANT USAGE ON SCHEMA nexus_system TO bulk_fixture;
                GRANT SELECT ON nexus_system.connectors TO bulk_fixture;
                ALTER TABLE nexus_system.connectors ENABLE ROW LEVEL SECURITY;
                ALTER TABLE nexus_system.connectors FORCE ROW LEVEL SECURITY;
                CREATE POLICY tenant_isolation ON nexus_system.connectors
                  USING (tenant_id=current_setting('nexus.current_tenant_id',true));
                CREATE TABLE public.orders (id integer PRIMARY KEY,modifieddate timestamptz,amount integer);
                INSERT INTO public.orders VALUES (0,'2026-09-01',10),(1,'2026-09-30',20),(2,'2026-10-01',30);
                GRANT SELECT ON public.orders TO bulk_fixture;
                """)
            conn.execute("INSERT INTO nexus_system.connectors VALUES ('tenant-a',%s,'postgresql',NULL,NULL,true,true)", (connector_id,))
        app_dsn=dsn.replace("postgres:source-fixture","bulk_fixture:source-fixture")
        monkeypatch.setenv("CDM_DB_DSN",app_dsn)
        values={"dsn":app_dsn,"schemas":"public","initial_tables":"public.orders"}
        monkeypatch.setattr(backfill,"_connector_var",lambda connector,suffix,default=None:values.get(suffix,default))
        producer=SimpleNamespace(events=[])
        producer.produce=lambda topic,**kwargs:producer.events.append(json.loads(kwargs["value"]))
        producer.flush=lambda *args:0
        monkeypatch.setattr(backfill,"_producer",lambda:producer)
        from nexus_spark_lib import backfill_database
        monkeypatch.setattr(backfill_database,"_producer",lambda:producer)
        connector=SimpleNamespace(connector_id=connector_id,tenant_id="tenant-a",source_type="postgresql")
        result=backfill.run_windowed_backfill(connector,"2026-09-01","2026-10-01","transactions_only",True,"raw",
            table_name="public.orders",timestamp_column="modifieddate")
        assert result=={"records_published":2}
        assert [event["source_record_id"] for event in producer.events]==["0","1"]
        assert all(event["tenant_id"]=="tenant-a" and event["after_payload"]["amount"]>0 for event in producer.events)
        with pytest.raises(ValueError,match="belongs to another tenant"):
            backfill.run_snapshot_extract(SimpleNamespace(connector_id=connector_id,tenant_id="tenant-b",source_type="postgresql"),True,"raw")
        assert len(producer.events)==2
        result=backfill.run_snapshot_extract(connector,True,"raw")
        assert result=={"records_published":3}
    finally:
        subprocess.run(["docker","rm","-f",container],check=True,capture_output=True)
