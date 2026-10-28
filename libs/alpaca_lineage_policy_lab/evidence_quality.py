"""Compatibility alias for the production-owned policy fact producer."""

import sys

from libs.alpaca_lineage_build import policy_evidence_quality as _implementation

# Reuse the production fact boundary; the lab must not invent an alternative
# evidence-validation pass with different acceptance or storage semantics.
sys.modules[__name__] = _implementation
