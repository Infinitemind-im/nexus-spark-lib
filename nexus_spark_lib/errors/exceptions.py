"""Exception hierarchy for nexus_spark_lib.

All exceptions inherit from NexusSparkLibError so callers can catch a single
base class across the library. nexus_core.errors.NexusException is the root of
the platform-wide hierarchy — NexusSparkLibError inherits from it so that any
service catching NexusException will also catch these.
"""

from nexus_core.errors import NexusException


class NexusSparkLibError(NexusException):
    """Base class for all nexus_spark_lib errors."""


# ── Transform stage errors ────────────────────────────────────────────────────

class NormalisationError(NexusSparkLibError):
    """Raised when Stage 1 type coercion fails unrecoverably for a field."""


class MaterializationPolicyError(NexusSparkLibError):
    """Raised when Stage 0 policy evaluation cannot be completed."""


# ── Entity Resolution errors ──────────────────────────────────────────────────

class ERResolutionError(NexusSparkLibError):
    """Raised when entity resolution fails for a record."""


class ERIndexLookupError(ERResolutionError):
    """Raised when entity_resolution_index is unreachable."""


class ERSignalError(ERResolutionError):
    """Raised when a resolution signal (A, B, or C) fails to execute."""


class ERStateTransitionError(ERResolutionError):
    """Raised when an illegal Golden Record state transition is attempted."""


class ERIdGenerationError(ERResolutionError):
    """Raised when cdm_entity_id generation fails."""


# ── Synthesis errors ──────────────────────────────────────────────────────────

class SynthesisError(NexusSparkLibError):
    """Raised when Stage 3 Golden Record synthesis fails."""


class SurvivorshipRuleError(SynthesisError):
    """Raised when a survivorship rule cannot be evaluated."""


# ── Kafka / IO errors ─────────────────────────────────────────────────────────

class KafkaWriteError(NexusSparkLibError):
    """Raised when writing to a Kafka topic fails after all retries."""


class DeadLetterError(NexusSparkLibError):
    """Raised when writing to the dead-letter topic itself fails (circuit open)."""


# ── Broadcast errors ──────────────────────────────────────────────────────────

class BroadcastRefreshError(NexusSparkLibError):
    """Raised when a broadcast variable cannot be refreshed from the source."""


class BroadcastExpiredError(NexusSparkLibError):
    """Raised when a broadcast variable has exceeded its TTL with no refresh."""


# ── DB errors ─────────────────────────────────────────────────────────────────

class DBConnectionError(NexusSparkLibError):
    """Raised when a database connection cannot be established from an executor."""


class DBWriteError(NexusSparkLibError):
    """Raised when a database write fails after all retries."""
