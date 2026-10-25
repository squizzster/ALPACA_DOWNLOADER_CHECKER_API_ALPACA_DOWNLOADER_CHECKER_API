"""Compatibility alias for the production-owned policy fact producer."""

import sys

from libs.alpaca_lineage_build import policy_evidence_quality as _implementation

sys.modules[__name__] = _implementation
