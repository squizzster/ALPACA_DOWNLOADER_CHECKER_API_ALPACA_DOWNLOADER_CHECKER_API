"""Compatibility imports; shared heartbeat reporting lives in alpaca_heartbeat."""

from libs.alpaca_heartbeat import (  # noqa: F401
    AI_AGENT_READ_THIS,
    DEFAULT_HEARTBEAT_INTERVALS,
    ERROR_SCHEMA,
    PROGRESS_SCHEMA,
    SCHEMA_VERSION,
    LifecycleReporter,
    Phase,
)
