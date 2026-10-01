"""Pre-warm 50,000 real PostgreSQL rows into an isolated real Redis C1."""
import os
import time
import uuid

import pytest

pytestmark=[pytest.mark.integration,
            pytest.mark.skipif(os.getenv('RUN_ER_PREWARM_INTEGRATION')!='1',reason='explicit cache fixture opt-in')]


def test_fifty_thousand_keys_are_ready_before_batch_and_other_tenants_are_absent():
    import psycopg
    import redis
    from testcontainers.core.container import DockerContainer
    from nexus_spark_lib.er.lookup_client import ERLookupClient
    pg_image='pgvector/pgvector@sha256:c3c84b85691a264aa3c5b8fc1d611e67d42b0cca8596e3d3d22dc2424c12c4e2'
    redis_image='redis@sha256:bb186d083732f669da90be8b0f975a37812b15e913465bb14d845db72a4e3e08'
    with (DockerContainer(pg_image).with_env('POSTGRES_PASSWORD','fixture-password').with_exposed_ports(5432) as pg,
          DockerContainer(redis_image).with_exposed_ports(6379) as cache):
        host=pg.get_container_host_ip().replace('localhost','127.0.0.1')
        dsn=f'postgresql://postgres:fixture-password@{host}:{pg.get_exposed_port(5432)}/postgres'
        deadline=time.monotonic()+60
        while True:
            try:
                conn=psycopg.connect(dsn)
                break
            except psycopg.OperationalError:
                if time.monotonic()>=deadline: raise
                time.sleep(.25)
        client=redis.Redis(host=cache.get_container_host_ip().replace('localhost','127.0.0.1'),
                           port=cache.get_exposed_port(6379),socket_timeout=3)
        while True:
            try:
                client.ping()
                break
            except redis.exceptions.ConnectionError:
                if time.monotonic()>=deadline: raise
                time.sleep(.25)
        connector=str(uuid.uuid4())
        with conn:
            conn.execute('CREATE SCHEMA nexus_system; CREATE TABLE nexus_system.entity_resolution_index (tenant_id text,connector_id uuid,source_table text,source_record_id text,cdm_entity_id text,is_active boolean)')
            conn.execute("INSERT INTO nexus_system.entity_resolution_index SELECT 'fixture',%s::uuid,'accounts',i::text,'entity-'||i,true FROM generate_series(1,50000) i",(connector,))
            conn.execute("INSERT INTO nexus_system.entity_resolution_index VALUES ('other',%s,'accounts','private','private-entity',true),('fixture',%s,'accounts','inactive','inactive-entity',false)",(connector,connector))
            conn.commit()
            count=ERLookupClient(redis=client,postgres=conn).prewarm('fixture',connector)
            assert count==50000 and client.dbsize()==50000
            assert client.get(f'er:fixture:{connector}:50000')==b'entity-50000'
            assert 3500 <= client.ttl(f'er:fixture:{connector}:1') <= 3600
            assert client.get(f'er:other:{connector}:private') is None
            assert client.get(f'er:fixture:{connector}:inactive') is None
            print(f'NEXUS_ER_PREWARM_FIXTURE_COUNT={count}')
