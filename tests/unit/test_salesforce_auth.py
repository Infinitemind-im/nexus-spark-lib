"""Session-token precedence, tenant isolation and credential-safe errors."""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from nexus_spark_lib import backfill


def test_direct_token_does_not_refresh_or_require_oauth_client(monkeypatch):
    monkeypatch.setenv('SALESFORCE_ACCESS_TOKEN','session-fixture')
    monkeypatch.setenv('SALESFORCE_INSTANCE_URL','https://fixture.my.salesforce.com/')
    request=Mock(side_effect=AssertionError('Refresh must not run'))
    monkeypatch.setattr(backfill,'_sf_request',request)
    assert backfill._sf_token()==('session-fixture','https://fixture.my.salesforce.com')


@pytest.mark.parametrize('url',['','http://fixture.test','https://user:secret@fixture.test',
    'https://fixture.test/path','https://fixture.test?token=secret'])
def test_direct_token_rejects_unsafe_instance_urls(monkeypatch,url):
    monkeypatch.setenv('SALESFORCE_ACCESS_TOKEN','session-fixture')
    monkeypatch.setenv('SALESFORCE_INSTANCE_URL',url)
    with pytest.raises(ValueError,match='HTTPS instance'):
        backfill._sf_token()


def test_direct_token_does_not_leak_to_another_connector(monkeypatch):
    monkeypatch.setenv('SALESFORCE_ACCESS_TOKEN','other-tenant-session')
    monkeypatch.setattr(backfill,'_connector_var',lambda *args:json.dumps({
        'SALESFORCE_INSTANCE_URL':'https://fixture.my.salesforce.com',
        'SALESFORCE_ACCESS_TOKEN':'tenant-session'}))
    with backfill.credential_environment(SimpleNamespace(tenant_id='tenant-a',connector_id='connector-a')):
        assert backfill._sf_token()[0]=='tenant-session'
    assert backfill.os.environ['SALESFORCE_ACCESS_TOKEN']=='other-tenant-session'


def test_error_does_not_include_response_body_url_or_credentials(monkeypatch):
    response=Mock(ok=False,status_code=401,text='private-record bearer-token')
    response.json.return_value=[{'errorCode':'INVALID_SESSION_ID','message':'private-record'}]
    monkeypatch.setattr(backfill.requests,'request',lambda *args,**kwargs:response)
    monkeypatch.setenv('SF_HTTP_MAX_ATTEMPTS','1')
    with pytest.raises(RuntimeError) as error:
        backfill._sf_request('get','https://fixture.test/private?token=bearer-token')
    assert str(error.value)=='Salesforce API error 401 (INVALID_SESSION_ID)'
