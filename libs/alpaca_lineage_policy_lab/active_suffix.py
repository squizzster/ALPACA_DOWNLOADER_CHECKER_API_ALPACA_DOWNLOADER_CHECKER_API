"""Compatibility alias for the production-owned policy fact producer."""

import sys

from libs.alpaca_lineage_build import policy_active_suffix as _implementation

# Keep one fact producer: a lab-local copy would let replay and production
# certify different evidence while claiming the same policy contract.
sys.modules[__name__] = _implementation
