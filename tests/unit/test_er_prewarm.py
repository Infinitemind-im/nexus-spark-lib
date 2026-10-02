from unittest.mock import MagicMock

import pytest

from nexus_spark_lib.er.lookup_client import ERLookupClient


def test_ambiguous_connector_record_ids_fail_before_any_cache_write():
    postgres, redis = MagicMock(), MagicMock()
    postgres.execute.return_value.fetchone.return_value = ('duplicate',)
    with pytest.raises(ValueError, match='ambiguous'):
        ERLookupClient(redis=redis, postgres=postgres).prewarm('tenant','connector')
    redis.pipeline.assert_not_called()


def test_redis_failure_propagates_for_airflow_retry():
    postgres, redis = MagicMock(), MagicMock()
    postgres.execute.return_value.fetchone.return_value = None
    cursor=postgres.cursor.return_value.__enter__.return_value
    cursor.fetchmany.side_effect=[[('id','entity')], []]
    redis.pipeline.return_value.__enter__.return_value.execute.side_effect=ConnectionError('fixture')
    with pytest.raises(ConnectionError):
        ERLookupClient(redis=redis, postgres=postgres).prewarm('tenant','connector')


def test_no_spark_session_is_created_and_nx_ttl_are_applied():
    postgres, redis = MagicMock(), MagicMock()
    postgres.execute.return_value.fetchone.return_value = None
    cursor=postgres.cursor.return_value.__enter__.return_value
    cursor.fetchmany.side_effect=[[('id','entity')], []]
    assert ERLookupClient(redis=redis, postgres=postgres).prewarm('tenant','connector')==1
    redis.pipeline.return_value.__enter__.return_value.set.assert_called_once_with(
        'er:tenant:connector:id','entity',ex=3600,nx=True)
