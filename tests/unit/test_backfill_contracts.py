"""Publication errors and failed windows must never advance source progress."""
import json
import os
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from nexus_spark_lib import backfill, backfill_windows as windows


def test_source_extension_uses_registry_without_modifying_dispatch(monkeypatch):
    implementation=Mock()
    registry=backfill.ExtractRegistry()
    registry.register("fixture",implementation)
    monkeypatch.setattr(backfill,"registry",registry)
    monkeypatch.setattr(backfill,"_validate_connector",lambda connector:None)
    connector=SimpleNamespace(source_type="fixture",tenant_id="tenant-a",connector_id="connector")
    backfill.run_snapshot_extract(connector,True,"raw")
    implementation.snapshot.assert_called_once_with(connector,"raw",True)
    with pytest.raises(ValueError,match="No bulk extractor"):
        registry.get("unknown")


@pytest.mark.parametrize("failure",["delivery","timeout","missing_callback",None])
def test_delivery_confirmation_rejects_flush_zero_with_error(monkeypatch,failure):
    producer=Mock()
    callbacks=[]
    producer.produce.side_effect=lambda *args,**kwargs:callbacks.append(kwargs["on_delivery"])
    def flush(_timeout):
        if failure != "missing_callback":
            callbacks[0](RuntimeError("failed") if failure == "delivery" else None,None)
        return 1 if failure == "timeout" else 0
    producer.flush.side_effect=flush
    monkeypatch.setattr(backfill,"Producer",lambda config:producer)
    confirmed=backfill.ConfirmedProducer({})
    confirmed.produce("raw",value=b"data")
    if failure:
        with pytest.raises(RuntimeError,match="not confirmed"):
            confirmed.flush()
    else:
        assert confirmed.flush()==0


def test_credentials_do_not_inherit_other_tenant_secrets_and_are_restored(monkeypatch):
    monkeypatch.setenv("SALESFORCE_REFRESH_TOKEN","different-tenant-fixture")
    monkeypatch.setattr(backfill,"_connector_var",lambda *args:json.dumps({"SALESFORCE_CLIENT_ID":"fixture"}))
    connector=SimpleNamespace(connector_id="connector",tenant_id="tenant-a")
    with backfill.credential_environment(connector):
        assert os.getenv("SALESFORCE_REFRESH_TOKEN") is None
        assert os.getenv("SALESFORCE_CLIENT_ID")=="fixture"
    assert os.getenv("SALESFORCE_REFRESH_TOKEN")=="different-tenant-fixture"


def test_global_api_credentials_require_explicit_tenant_and_connector_binding(monkeypatch):
    monkeypatch.setattr(backfill,"_connector_var",lambda *args:None)
    monkeypatch.setenv("BACKFILL_CONNECTOR_ID","other")
    monkeypatch.setenv("NEXUS_TENANT_ID","tenant-b")
    with pytest.raises(RuntimeError,match="bound"):
        with backfill.credential_environment(SimpleNamespace(connector_id="connector",tenant_id="tenant-a")):
            pytest.fail("Cross-tenant credentials must not be used")


def test_final_partial_window_is_planned_and_no_checkpoint_is_written(monkeypatch):
    monkeypatch.setattr(windows,"_get_var",lambda key:None)
    planned=windows.plan_window("connector",{
        "start_index":"2026-09-27","time_window_length":"1 week",
        "stopping_criteria":"fixed_date","stopping_date":"2026-10-02",
        "fill_direction":"forward","table_name":"public.orders"},today=date(2026,10,2))
    assert planned["start"]=="2026-09-27" and planned["end"]=="2026-10-02"
    assert planned["expected_cursor"] is None


def test_checkpoint_only_advances_after_success_and_retry_is_idempotent(monkeypatch):
    monkeypatch.setattr(windows,"_get_var",lambda key:None)
    planned=windows.plan_window("connector",{
        "start_index":"2026-09-01","time_window_length":"1 month",
        "stopping_criteria":"fixed_date","stopping_date":"2026-10-02"},today=date(2026,10,2))
    variables=Mock()
    variables.get.return_value=None
    variables.set.assert_not_called()
    windows.complete_window(planned,variables=variables)
    variables.set.assert_called_once_with(planned["cursor_key"],"2026-10-01")
    variables.get.return_value="2026-10-01"
    windows.complete_window(planned,variables=variables)
    assert variables.set.call_count==1
    variables.get.return_value="2026-10-02"
    with pytest.raises(RuntimeError,match="changed"):
        windows.complete_window(planned,variables=variables)


@pytest.mark.parametrize("retention",[-1,14*24*60*60*1000,7*24*60*60*1000])
def test_raw_retention_preflight_rejects_short_log(retention):
    from nexus_spark_lib.backfill_preflight import MINIMUM_RETENTION_MS,require_raw_retention
    class Admin:
        def describe_configs(self,resources,**kwargs):
            future=Mock()
            future.result.return_value={"retention.ms":SimpleNamespace(value=str(retention))}
            return {resources[0]:future}
    if retention != -1 and retention < MINIMUM_RETENTION_MS:
        with pytest.raises(RuntimeError,match="fourteen days"):
            require_raw_retention("raw",admin=Admin())
    else:
        assert require_raw_retention("raw",admin=Admin())["retention_ms"] == retention


def test_sync_api_cannot_return_connectors_for_another_tenant(monkeypatch):
    from nexus_spark_lib import source_sync
    settings={"NEXUS_API_BASE":"http://fixture","NEXUS_AUTH_BEARER":"fixture-token"}
    monkeypatch.setattr(source_sync,"_setting",lambda key,default=None:settings.get(key,default))
    response=Mock()
    response.json.return_value=[{"tenant_id":"tenant-b","connector_id":"other"}]
    get=Mock(return_value=response)
    monkeypatch.setattr(source_sync.requests,"get",get)
    with pytest.raises(ValueError,match="another tenant"):
        source_sync.list_connectors({"tenant_id":"tenant-a"})
    assert get.call_args.kwargs["headers"]["X-Tenant-ID"]=="tenant-a"


def test_sync_requires_configured_authenticated_platform_endpoint(monkeypatch):
    from nexus_spark_lib import source_sync
    monkeypatch.setattr(source_sync,"_setting",lambda key,default=None:default)
    with pytest.raises(RuntimeError,match="authenticated"):
        source_sync.list_connectors({"tenant_id":"tenant-a"})
