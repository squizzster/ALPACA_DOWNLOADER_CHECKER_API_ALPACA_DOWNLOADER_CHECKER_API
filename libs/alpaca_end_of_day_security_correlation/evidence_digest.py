"""Canonical digests of supplied lineage evidence; no storage authentication."""

import json
from copy import deepcopy
from hashlib import sha256
from typing import Any


def canonical_json_sha256(document: object) -> str:
    # This evidence identity uses sorted UTF-8 JSON without a trailing newline.
    # Pricing and lineage receipts use different bytes; do not substitute their serializer.
    serialized_document = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return sha256(serialized_document).hexdigest()


def lineage_evidence_sha256(lineage_report: dict[str, Any]) -> str:
    semantic_lineage_report = deepcopy(lineage_report)
    semantic_lineage_report.pop("generated_at", None)
    lineage = semantic_lineage_report.get("lineage")
    if isinstance(lineage, dict):
        lineage.pop("api_request_count", None)
    return canonical_json_sha256(semantic_lineage_report)
