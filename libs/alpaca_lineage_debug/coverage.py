"""Describe published allocation and unfinished work without replaying decisions."""

from collections import Counter
from pathlib import Path

from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.evidence import published_assignments
from libs.alpaca_lineage_build.projection import require_complete
from libs.common.store_lock import operation_lock


def allocation(segment):
    """Classify the recorded explanation, never infer a new ownership decision."""
    reasons = segment.get("reason_codes", ())
    if not reasons:
        return "unclassified"
    if any(any(word in reason for word in ("ambiguous", "conflicting", "disputed")) for reason in reasons):
        return "ambiguous"
    if "exchange_test_symbol_excluded" in reasons:
        return "excluded"
    return "unsupported"


def coverage(store, *, details=False, previous=None):
    store = Path(store).absolute()
    carried = {}
    if previous is not None:
        from . import accept_report

        accept_report(previous, "coverage")
        if previous.get("store") != str(store):
            raise ValueError("coverage report belongs to another store")
        if not isinstance(previous.get("sources"), dict):
            raise ValueError("coverage report is missing its completed source records")
        carried = previous["sources"]
    counts = Counter(
        dict.fromkeys(
            (
                "resolved",
                "unresolved",
                "empty",
                "sources_with_ambiguous",
                "sources_with_unsupported",
                "sources_with_excluded",
                "sources_with_unclassified",
            ),
            0,
        )
    )
    sources, failures = {}, []
    reads = Counter(index=0, receipts=0, assignment_boundaries=0, reused_sources=0)

    def assignment_document(path, generation):
        reads["assignment_boundaries"] += 1
        return storage.selected_document(path, generation)

    # Metadata-only inspection needs no pricing locks or payloads. The
    # operation gate and the publication gate keep the selected JSON stable;
    # there is no per-source selector handshake or universe filesystem crawl.
    with operation_lock(store, exclusive=False):
        accepted = storage.accept_store(store)
        with storage.publication_lock(accepted.root):
            if storage.path_at(accepted.root, ".pending.json").exists():
                raise storage.LineageError("lineage publication incomplete; finish owning recovery first")
            index = storage.read_index(accepted.root)
            reads["index"] += 1
            require_complete(index)
            state = universe_state.pending_status(store)
            selected = [
                name for name, entry in index["symbols"].items() if entry["projection"]["unresolved_rows"]
            ]

            def reusable(name, entry):
                prior = carried.get(name, {})
                return (
                    prior.get("receipt_generation") == entry["sha256"]
                    and prior.get("source_generation") == entry["source_sha256"]
                    and "unresolved_class_rows" in prior
                    and "reason_rows" in prior
                    and "detail_error" not in prior
                )

            needed = sum(not reusable(name, index["symbols"][name]) for name in selected) if details else 0
            expected = {
                "selected_unresolved_sources": needed,
                "receipt_reads": needed,
                "assignment_boundary_reads_maximum": 3 * needed,
                "pricing_payload_reads": 0,
                "provider_requests": 0,
                "rule_evaluations": 0,
            }
            for name, entry in sorted(index["symbols"].items()):
                projection = entry["projection"]
                kind = (
                    "empty"
                    if not projection["source_rows"]
                    else "resolved"
                    if not projection["unresolved_rows"]
                    else "unresolved"
                )
                item = {
                    "allocation": kind,
                    "receipt_generation": entry["sha256"],
                    "source_generation": entry["source_sha256"],
                    "processed_through": projection["processed_through"],
                    "source_rows": projection["source_rows"],
                    "unresolved_rows": projection["unresolved_rows"],
                }
                counts[kind] += 1
                if kind == "unresolved":
                    if details:
                        try:
                            if reusable(name, entry):
                                classes = carried[name]["unresolved_class_rows"]
                                reasons = carried[name]["reason_rows"]
                                reads["reused_sources"] += 1
                            else:
                                reads["receipts"] += 1
                                receipt = storage.read_receipt(accepted.root, name, index)
                                boundaries = storage.read_boundaries(
                                    accepted.root, receipt, selected_loader=assignment_document
                                )
                                classes, reasons = Counter(), Counter()
                                for segment in published_assignments(receipt, boundaries):
                                    if segment["branch"] != "uncorrelated":
                                        continue
                                    classes[allocation(segment)] += segment["row_count"]
                                    for reason in segment.get("reason_codes", ()):
                                        reasons[reason] += segment["row_count"]
                            item["unresolved_class_rows"] = dict(classes)
                            item["reason_rows"] = dict(reasons)
                            for category in classes:
                                counts["sources_with_" + category] += 1
                        except (RuntimeError, OSError, ValueError) as error:
                            failures.append({"symbol": name, "error": str(error)})
                            item["detail_error"] = str(error)
                    else:
                        counts["sources_with_unclassified"] += 1
                sources[name] = item
            pending = {}
            for name, work in sorted(state["work"].items()):
                pending[name] = {
                    "status": work["status"],
                    "reasons": work.get("reasons", []),
                    "outcome": work.get("outcome"),
                    "obligation_id": work["obligation_id"],
                }
                if name in sources:
                    sources[name]["pending_work"] = pending[name]
    failed_work = sum(bool((item.get("outcome") or {}).get("error")) for item in pending.values())
    return {
        "status": "incomplete" if failures else "complete",
        "execution_complete": not failures,
        "mode": "published_decisions_metadata_only",
        "store": str(store),
        "selector": index["projection_state"],
        "workflow_revision": state["revision"],
        "summary": {
            "indexed_sources": len(sources),
            **counts,
            "pending_work": len(pending),
            "failed_work": failed_work,
            "deferred_work": len(pending) - failed_work,
        },
        "expected_io": expected,
        "reads": dict(reads),
        "sources": sources,
        "pending_work": pending,
        "failures": failures,
        "limits": [
            "Resolved means all published rows are assigned; it does not independently establish correct ownership.",
            "Ambiguous means a recorded unresolved reason names ambiguity, conflict or dispute; unsupported means another recorded reason withholds allocation.",
            "Sources may contain multiple unresolved classes. Reason counts can overlap; row counts include adjustment views.",
            "Empty histories and excluded test symbols are separate from failed work. Pending work can coexist with a published resolved history.",
            "Without unresolved details, the compact index cannot distinguish ambiguity from insufficient support.",
            "No price payloads, provider requests, ownership rules or numerical decisions are replayed.",
        ],
    }
