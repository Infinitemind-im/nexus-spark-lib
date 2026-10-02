"""M1 platform sync API adapter, resolved only during task execution."""
import os
from urllib.parse import quote
import requests


def _setting(name,default=None):
    value=os.getenv(name)
    if value:
        return value
    try:
        from airflow.models import Variable
        return Variable.get(name,default_var=default)
    except ImportError:
        return default


def _tenants(conf=None):
    if conf is None:
        from airflow.operators.python import get_current_context
        run=get_current_context().get("dag_run")
        conf=(run.conf or {}) if run else {}
    tenants=conf.get("tenant_ids") or conf.get("tenants") or conf.get("tenant_id") or _setting("NEXUS_TENANT_IDS") or _setting("NEXUS_TENANT_ID")
    if isinstance(tenants,str):
        tenants=tenants.split(",")
    if not isinstance(tenants,list) or not tenants or any(not isinstance(t,str) or not t.strip() for t in tenants):
        raise ValueError("Source sync requires explicit tenant identifiers")
    return list(dict.fromkeys(t.strip() for t in tenants))


def _client(tenant):
    base=_setting("NEXUS_API_BASE")
    token=_setting("NEXUS_AUTH_BEARER") or _setting("NEXUS_JWT") or _setting("nexus_auth_bearer")
    if not base or not token:
        raise RuntimeError("M1 platform API base and authenticated service credential are required")
    return base.rstrip("/"),{"X-Tenant-ID":tenant,"Authorization":"Bearer "+token}


def list_connectors(conf=None):
    allow={t.strip().lower() for t in (_setting("TRIGGER_SYSTEM_TYPES","") or "").split(",") if t.strip()}
    timeout=int(_setting("NEXUS_API_TIMEOUT","30"))
    items=[]
    for tenant in _tenants(conf):
        base,headers=_client(tenant)
        response=requests.get(base+"/api/v1/m1/connectors",headers=headers,timeout=timeout)
        response.raise_for_status()
        data=response.json()
        rows=data if isinstance(data,list) else data.get("items",[])
        for row in rows:
            source_type=str(row.get("system_type","")).lower()
            if row.get("tenant_id") and row["tenant_id"] != tenant:
                raise ValueError("M1 returned a connector for another tenant")
            connector_id=row.get("connector_id") or row.get("id")
            if connector_id and (not allow or source_type in allow):
                items.append({"tenant_id":tenant,"connector_id":connector_id,"system_type":source_type})
    return items


def trigger_sync(item):
    tenant,connector=item["tenant_id"],str(item["connector_id"])
    base,headers=_client(tenant)
    response=requests.post(base+"/api/v1/m1/sync/"+quote(connector,safe=""),headers=headers,
        json={"sync_mode":_setting("SYNC_MODE","incremental"),
              "force":str(_setting("SYNC_FORCE","false")).lower()=="true"},
        timeout=int(_setting("NEXUS_API_TIMEOUT","30")))
    response.raise_for_status()
    return {"tenant_id":tenant,"connector_id":connector,"job":response.json()}
