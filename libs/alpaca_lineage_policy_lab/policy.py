"""Read-only lab adapter for the production-owned policy compiler."""

from libs.alpaca_lineage_build.correlation_policy import *  # noqa: F403
from libs.alpaca_lineage_build.correlation_policy import load_policy as _load_policy

from .contract import validate_change_log


def load_policy(path):
    # Compilation supplies executable policy meaning, not production authority.
    # The lab may compare a candidate; only the production owner's authorization
    # path can select policy for new stored lineage publications.
    validate_change_log()
    return _load_policy(path)
