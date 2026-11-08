"""Selected correction evidence belongs to acquisition, not to its consumers.

The observations are completed provider responses. Their binding survives retry;
only newly acquired output is compared with them. A conflicting broad response
cannot erase a narrow observation or silently complete the correction.
"""

from decimal import Decimal, InvalidOperation

from libs.common.debug_trace import event

from .contract import ADJUSTMENTS, COLUMNS, NEW_YORK, DownloadError, timestamp
from .endpoints import same_row


class ProviderObservationConflict(DownloadError):
    def __init__(self, symbol, adjustment, expected, actual):
        self.evidence = {
            "outcome": "provider_observation_conflict",
            "http_status": None,
            "symbol": symbol,
            "adjustment": adjustment,
            "session": expected[0],
            "selected_observation": list(expected),
            "acquired_observation": list(actual) if actual is not None else None,
        }
        super().__init__(
            f"{symbol}: acquired {adjustment} history conflicts with selected observation "
            f"at {expected[0]}; correction unresolved, publication unchanged"
        )


def validate_durable_observations(contract):
    """Validate serialized evidence at ingress; consumers carry the accepted rows."""
    if contract is None:
        return
    if not isinstance(contract, dict) or contract.get("schema_version") not in (1, 2, 3):
        raise DownloadError("invalid durable correction contract; owning recovery required")
    if contract["schema_version"] == 1 and "observations" not in contract:
        return
    selected = contract.get("observations")
    if not isinstance(selected, dict) or any(key not in ADJUSTMENTS for key in selected):
        raise DownloadError("invalid durable correction adjustments; owning recovery required")
    for rows in selected.values():
        if not isinstance(rows, list):
            raise DownloadError("invalid durable correction rows; owning recovery required")
        previous = None
        for row in rows:
            if (
                not isinstance(row, list)
                or len(row) != len(COLUMNS)
                or any(not isinstance(value, str) or not value for value in row)
            ):
                raise DownloadError("invalid durable correction row; owning recovery required")
            instant = timestamp(row[0])
            if previous is not None and instant < previous:
                raise DownloadError("unordered durable correction rows; owning recovery required")
            previous = instant
            try:
                finite = all(Decimal(value).is_finite() for value in row[1:])
            except InvalidOperation as error:
                raise DownloadError("invalid durable correction number; owning recovery required") from error
            if not finite:
                raise DownloadError("nonfinite durable correction number; owning recovery required")


def observations(request):
    """Carry completed raw probes and all-view comparisons without replaying either."""
    result = dict(request.get("correction_observations", {}))
    comparison = request.get("comparison")
    if comparison is not None and not comparison.matched:
        result = merge_observations(result, {"all": comparison.live_all})
    return result


def merge_observations(*selected):
    """Union witnesses; conflicting values remain obligations, never last-write wins."""
    result = {}
    for observations in selected:
        for adjustment, rows in observations.items():
            by_instant = result.setdefault(adjustment, {})
            for row in rows:
                values = by_instant.setdefault(timestamp(row[0]), [])
                if not any(same_row(row, previous) for previous in values):
                    values.append(list(row))
    return {
        adjustment: [row for instant in sorted(instants) for row in instants[instant]]
        for adjustment, instants in result.items()
    }


def accept_observations(symbol, views, selected, *, allow_all_context_conflicts=False):
    """Validate newly constructed output once, with no provider/storage reads."""
    confirmed = {}
    context_conflicts = []
    for adjustment, expected in selected.items():

        def session(row):
            return timestamp(row[0]).astimezone(NEW_YORK).date()

        required = {session(row) for row in expected}
        first_witness, conflicting_sessions = {}, set()
        if allow_all_context_conflicts and adjustment == "all":
            for row in expected:
                day = session(row)
                prior = first_witness.setdefault(day, row)
                if not same_row(prior, row):
                    conflicting_sessions.add(day)
        actual = {}
        for row in views[adjustment]:
            day = session(row)
            if day in required:
                actual.setdefault(day, []).append(row)
        for row in expected:
            candidates = actual.get(session(row), [])
            acquired = candidates[0] if candidates else None
            if len(candidates) != 1 or not same_row(row, acquired):
                error = ProviderObservationConflict(symbol, adjustment, row, acquired)
                error.evidence["session_rows"] = len(candidates)
                event("correction_observation_conflict", **error.evidence)
                if (
                    allow_all_context_conflicts
                    and adjustment == "all"
                    and len(candidates) == 1
                    and session(row) not in conflicting_sessions
                ):
                    context_conflicts.append(error)
                    continue
                raise error
        confirmed[adjustment] = frozenset(timestamp(row[0]) for row in expected)
    if context_conflicts:
        # A present all-view request-context difference is only a candidate for
        # the publisher's exact-equal disposition. Establish EVERY witness first:
        # an earlier such difference cannot hide a missing/ambiguous session or a
        # contradictory observation in another adjustment view.
        error = context_conflicts[0]
        error.confirmed_observations = confirmed
        error.context_conflicts = tuple(context_conflicts)
        raise error
    event("correction_observations_accepted", symbol=symbol, rows=sum(map(len, selected.values())))
    return confirmed


def applicable_contract(contract, *, store, symbol, generation, start, end, asof):
    """Check only the durable decision's binding, including full replacements."""
    if contract is None:
        return False
    if contract.get("schema_version") not in (1, 2, 3) or contract.get("scope") not in (
        "latest_session",
        "full_history",
    ):
        raise DownloadError("unsupported pending pricing correction; owning recovery required")
    if contract.get("schema_version") in (2, 3):
        selected = contract.get("observations")
        needed = {"selected_all_rows_changed": "all", "provider_late_observations": "raw"}.get(
            contract.get("cause")
        )
        if not isinstance(selected, dict) or (needed and not selected.get(needed)):
            raise DownloadError("missing correction trigger observations; owning recovery required")
    # v1 is store-local work state; v2 also records its explicit store/symbol.
    # Older latest-session decisions kept the comparison mapping in their
    # completed selection; acquisition may legitimately use a later mapping.
    comparison_asof = (
        contract.get("comparison_asof")
        or (contract.get("selection") or {}).get("comparison_asof")
        or contract.get("asof")
    )
    applicable = (
        (
            contract.get("schema_version") == 1
            or (contract.get("store"), contract.get("symbol")) == (str(store), symbol)
        )
        and contract.get("source_sha256") == generation
        and contract.get("start") == start
        and contract.get("comparison_end_exclusive", contract.get("end_exclusive")) == end
        and comparison_asof == asof
        and (
            contract["scope"] != "full_history"
            or (contract.get("end_exclusive"), contract.get("asof")) == (end, asof)
        )
    )
    event(
        "correction_binding",
        symbol=symbol,
        source_generation=generation,
        selected_generation=contract.get("source_sha256"),
        scope=contract["scope"],
        applicable=applicable,
    )
    return applicable


def completed(root, correction_id, generation):
    """Selected recovery only: one atomic receipt, independent of price identity."""
    from .storage import read_document, regular_path

    if not correction_id:
        return None
    try:
        receipt = read_document(regular_path(root, "correction.complete.json", published=True), published=True)
    except DownloadError as error:
        if isinstance(error.__cause__, FileNotFoundError):
            return None
        raise
    if receipt.get("schema_version") != 1 or receipt.get("symbol") != root.name:
        raise DownloadError("invalid correction completion receipt; owning recovery required")
    return (
        receipt
        if (receipt.get("correction_id"), receipt.get("source_sha256")) == (correction_id, generation)
        else None
    )


def complete(root, correction_id, generation):
    """Publish the sole output of an equal-price correction under the ticker lock.

    There are no changed price artifacts to journal. Atomic replacement commits
    this completed producer outcome; interruption before workflow acknowledgement
    consumes it without reacquisition or a new source/lineage/serving generation.
    """
    import os

    from .contract import json_bytes
    from .storage import regular_path, write_file

    receipt = {
        "schema_version": 1,
        "symbol": root.name,
        "correction_id": correction_id,
        "source_sha256": generation,
    }
    target = regular_path(root, "correction.complete.json")
    staged = regular_path(root, ".correction.complete.writing")
    staged.unlink(missing_ok=True)  # Only this owner's uncommitted single-output staging file.
    write_file(staged, json_bytes(receipt))
    os.replace(staged, target)
    return receipt
