"""Compatibility alias for the production-owned shared policy evaluator."""

import sys

from libs.alpaca_lineage_build import policy_engine as _implementation

# Shared evaluation preserves policy meaning. Candidate replay is still read-only;
# this alias grants no production authorization or publication capability.
sys.modules[__name__] = _implementation
