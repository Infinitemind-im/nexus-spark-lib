"""Registry metadata parses without extraction dependencies; runtime stays lazy."""
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from nexus_spark_lib.extract_registry import ExtractRegistry, registry


def test_capability_import_loads_no_extractor_dependencies():
    subprocess.run([sys.executable,"-c", """
import sys
from nexus_spark_lib.extract_registry import registry
assert registry.get(' POSTGRESQL ')
assert not any(name in sys.modules for name in ('nexus_spark_lib.backfill','requests','confluent_kafka','xmlrpc.client'))
"""], check=True)


def test_extensions_and_registry_misses():
    local=ExtractRegistry()
    extension=Mock()
    local.register(' Partner_Plugin ',extension)
    assert local.get('partner_plugin') is extension
    with pytest.raises(ValueError,match='already registered'):
        local.register('partner_plugin',Mock())
    with pytest.raises(ValueError,match='No bulk extractor'):
        local.get('unsupported')
    with pytest.raises(ValueError,match='required'):
        local.register('',Mock())


def test_task_execution_loads_and_delegates_to_the_owner(monkeypatch):
    extractor=SimpleNamespace(snapshot=Mock(return_value='snapshot'),window=Mock(return_value='window'))
    owner=SimpleNamespace(DatabaseExtractor=Mock(return_value=extractor))
    loader=Mock(return_value=owner)
    monkeypatch.setattr('nexus_spark_lib.extract_registry.import_module',loader)
    selected=registry.get('postgresql')
    loader.assert_not_called()
    assert selected.snapshot('connector','topic') == 'snapshot'
    assert selected.window('connector','topic','start','end') == 'window'
    extractor.snapshot.assert_called_once_with('connector','topic')
    extractor.window.assert_called_once_with('connector','topic','start','end')
