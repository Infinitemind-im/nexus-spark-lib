"""Bulk extractor capabilities available without importing a vendor/runtime SDK."""
from importlib import import_module


class ExtractRegistry:
    def __init__(self):
        self._implementations = {}

    def register(self, source_type, implementation):
        key = source_type.strip().lower()
        if not key:
            raise ValueError("Extractor source type is required")
        if key in self._implementations:
            raise ValueError("Extractor already registered")
        self._implementations[key] = implementation

    def get(self, source_type):
        try:
            return self._implementations[source_type.strip().lower()]
        except KeyError as exc:
            raise ValueError("No bulk extractor is registered for this source type") from exc


class _OwnedExtractor:
    """Load the owning implementation only when a task invokes extraction."""
    def __init__(self, name, *, api=False):
        self.name = name
        self.api = api

    def _load(self):
        owner = import_module("nexus_spark_lib.backfill")
        implementation = getattr(owner, self.name)
        return owner.ApiExtractor(implementation) if self.api else implementation()

    def snapshot(self, *args, **kwargs):
        return self._load().snapshot(*args, **kwargs)

    def window(self, *args, **kwargs):
        return self._load().window(*args, **kwargs)


registry = ExtractRegistry()
registry.register("postgres", _OwnedExtractor("DatabaseExtractor"))
registry.register("postgresql", _OwnedExtractor("DatabaseExtractor"))
registry.register("salesforce", _OwnedExtractor("_extract_salesforce", api=True))
registry.register("servicenow", _OwnedExtractor("_extract_servicenow", api=True))
registry.register("odoo", _OwnedExtractor("_extract_odoo", api=True))
