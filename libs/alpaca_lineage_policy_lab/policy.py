"""Read-only lab adapter for the production-owned policy compiler."""

from libs.alpaca_lineage_build.correlation_policy import *  # noqa: F403
from libs.alpaca_lineage_build.correlation_policy import load_policy as _load_policy

from .contract import validate_change_log


def load_policy(path):
    validate_change_log()
    return _load_policy(path)
