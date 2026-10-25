"""Compatibility alias for the production-owned shared policy evaluator."""

import sys

from libs.alpaca_lineage_build import policy_engine as _implementation

sys.modules[__name__] = _implementation
