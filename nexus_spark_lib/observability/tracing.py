"""OpenTelemetry tracing helpers for nexus_spark_lib pipeline stages."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

_tracer: trace.Tracer | None = None


def init_tracer(service_name: str = "nexus-spark-transformer") -> trace.Tracer:
    """Initialise OpenTelemetry tracer. Call once at Spark application startup."""
    global _tracer
    if _tracer is not None:
        return _tracer
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)
    return _tracer


def get_tracer() -> trace.Tracer:
    """Return the active tracer, initialising with defaults if not yet set up."""
    global _tracer
    if _tracer is None:
        _tracer = init_tracer()
    return _tracer


@contextmanager
def stage_span(
    stage_name: str,
    tenant_id: str,
    trace_id: str | None = None,
) -> Generator[trace.Span, None, None]:
    """Context manager that wraps a pipeline stage in an OpenTelemetry span.

    Usage:
        with stage_span("stage1_normalise", tenant_id=tenant_id) as span:
            span.set_attribute("record_count", len(batch))
            # ... do work ...
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(stage_name) as span:
        span.set_attribute("nexus.tenant_id", tenant_id)
        if trace_id:
            span.set_attribute("nexus.trace_id", trace_id)
        span.set_attribute("nexus.stage", stage_name)
        yield span
