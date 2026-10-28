"""Compatibility version retained in current correlation reports."""

from .pipeline_version import PIPELINE_CHANGE_LOG_PATH, PIPELINE_VERSION, validate_pipeline_change_log

# This compatibility version is report provenance, not a second storage owner.
# Pricing, lineage and serving retain their own publication/recovery contracts.
__all__ = ["PIPELINE_CHANGE_LOG_PATH", "PIPELINE_VERSION", "validate_pipeline_change_log"]
