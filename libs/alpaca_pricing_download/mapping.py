"""Completed mapped observations retained by the selected acquisition owner.

The lineage owner selects the request and persists ``document()`` in its work
record.  This value owns only new provider observations: it cannot approve an
old generation, choose identity, or publish pricing.  Its binding is checked
against the selected source after the pricing lock is acquired.
"""

from dataclasses import asdict, replace
from datetime import date, timedelta
from pathlib import Path

from .adaptive import AdjustmentAcquisition, CompletedAdjustments
from .adjustment_policy import AdaptiveAdjustmentDecision
from .contract import ADJUSTMENTS, NEW_YORK, DownloadError, timestamp
from .endpoints import same_row, session_window
from .responses import CompletedBars, completed_bars

_BINDING_FIELDS = {"store", "symbol", "source_generation", "start", "end", "asof", "candidate_id"}
ADJUSTMENT_STATE_FIELDS = (
    "symbol",
    "start",
    "end",
    "asof",
    "observed",
    "hashes",
    "decision",
    "completed",
    "failure",
    "exact_fallback",
)


class MappingCoverageIncomplete(DownloadError):
    """A completed mapped provider response omitted selected contributions."""

    def __init__(self, missing_sessions):
        self.missing_sessions = tuple(missing_sessions)
        super().__init__(
            "mapped acquisition omits required selected sessions: " + ",".join(self.missing_sessions)
        )


def _encode_bars(value):
    return {
        "symbol": value.symbol,
        "adjustment": value.adjustment,
        "start": value.start.isoformat(),
        "end": value.end.isoformat(),
        "asof": value.asof,
        "rows": value.rows,
        "singleton": value.singleton,
    }


def _decode_bars(document):
    """Accept this persisted handoff's schema once, without replaying a transform."""
    if not isinstance(document, dict) or set(document) != {
        "symbol",
        "adjustment",
        "start",
        "end",
        "asof",
        "rows",
        "singleton",
    }:
        raise DownloadError("invalid completed mapping observation")
    if (
        not isinstance(document["symbol"], str)
        or document["adjustment"] not in ADJUSTMENTS
        or type(document["singleton"]) is not bool
        or not isinstance(document["rows"], (list, tuple))
        or date.fromisoformat(document["asof"]).isoformat() != document["asof"]
    ):
        raise DownloadError("invalid completed mapping observation fields")
    start, end = timestamp(document["start"]), timestamp(document["end"])
    if start >= end:
        raise DownloadError("invalid completed mapping observation range")
    # The completed HTTP producer established each row's numeric, ordering and
    # range guarantees.  Restoration assembles the carried strings; it must not
    # parse timestamps/numbers, normalize rows or authenticate their digests.
    rows = tuple(tuple(row) for row in document["rows"])
    return CompletedBars(
        document["symbol"],
        document["adjustment"],
        start,
        end,
        document["asof"],
        rows,
        document["singleton"],
    )


class MappingAcquisition:
    """One generation-bound, resumable mapped full acquisition.

    The exact all-view witness is already completed by the HTTP producer.  The
    first fragment is retained even when empty; empty completes a negative
    assessment and must never authorize replacement.  Only an actual changed
    store/source/mapping/range/candidate binding selects a different acquisition.
    """

    NATIVE_FIELDS = (
        "store",
        "symbol",
        "source_generation",
        "start",
        "end",
        "asof",
        "candidate_id",
        "fragments",
        "remaining_all_ranges",
        "assembled_all",
        "state",
        "confirmations",
        "confirmed",
        "required_sessions",
        "coverage_accepted",
        "coverage_missing",
        "contribution_extent",
        "publication_assembly",
        "output_error",
        "view_sessions",
        "rejected_view",
    )

    def __init__(
        self,
        store,
        symbol,
        source_generation,
        start,
        end,
        asof,
        candidate_id,
        witness,
        *,
        required_sessions=None,
    ):
        self.store = str(Path(store).absolute())
        self.symbol, self.source_generation = symbol, source_generation
        self.start, self.end, self.asof = start, end, asof
        self.candidate_id = candidate_id
        self._check_binding_fields()
        if not isinstance(witness, CompletedBars):
            raise DownloadError("mapping witness requires completed provider observations")
        self._check_fragment(witness)
        if len(witness.rows) > 1:
            raise DownloadError("mapping witness must cover one selected observation")
        self.fragments = [witness]
        self.remaining_all_ranges = [
            (lower, upper) for lower, upper in ((start, witness.start), (witness.end, end)) if lower < upper
        ]
        self.assembled_all = None
        self.state = AdjustmentAcquisition(symbol, start, end, asof)
        self.confirmations, self.confirmed = {}, None
        if not required_sessions or any(
            not isinstance(day, str) or date.fromisoformat(day).isoformat() != day for day in required_sessions
        ):
            raise DownloadError("mapping acquisition requires completed source contribution dates")
        self.required_sessions = tuple(required_sessions)
        self.contribution_extent = {"from": min(self.required_sessions), "to": max(self.required_sessions)}
        self.coverage_accepted = False
        self.coverage_missing = None
        self.publication_assembly = None
        self.output_error = None
        self.view_sessions = {}
        self.rejected_view = None

    def _check_binding_fields(self):
        if (
            not isinstance(self.symbol, str)
            or not self.symbol
            or self.source_generation is not None
            and not isinstance(self.source_generation, str)
            or not isinstance(self.candidate_id, str)
            or not self.candidate_id
            or self.start >= self.end
            or date.fromisoformat(self.asof).isoformat() != self.asof
        ):
            raise DownloadError("invalid mapping acquisition binding")

    def _check_fragment(self, value):
        if (value.symbol, value.adjustment, value.asof) != (
            self.symbol,
            "all",
            self.asof,
        ) or not self.start <= value.start < value.end <= self.end:
            raise DownloadError("completed mapping fragment has a different scope")

    @property
    def available(self):
        return bool(self.fragments[0].rows)

    def require_binding(self, store, symbol, generation, start, end, asof):
        if (self.store, self.symbol, self.source_generation, self.start, self.end, self.asof) != (
            str(Path(store).absolute()),
            symbol,
            generation,
            start,
            end,
            asof,
        ):
            raise DownloadError("mapped acquisition binding changed; reselect owning work")

    def _all(self, fetch, on_progress):
        if self.assembled_all is not None:
            return self.assembled_all
        while self.remaining_all_ranges:
            start, end = self.remaining_all_ranges[0]
            value = completed_bars(fetch("all", start, end), self.symbol, "all", start, end, self.asof)
            self.fragments.append(value)
            self.remaining_all_ranges.pop(0)
            if on_progress is not None:
                on_progress(self)
        # Each fragment's HTTP producer already established ordering and exact
        # requested scope.  Joining those disjoint ranges is assembly, not a
        # second validation or numerical acceptance of their rows.
        self.assembled_all = CompletedBars(
            self.symbol,
            "all",
            self.start,
            self.end,
            self.asof,
            tuple(
                row
                for fragment in sorted(self.fragments, key=lambda value: value.start)
                for row in fragment.rows
            ),
        )
        if on_progress is not None:
            on_progress(self)
        return self.assembled_all

    def confirm(self, fetch, *, on_progress=None):
        """Complete the existing independent first-row checks exactly once.

        The witness is now part of acquisition, so it cannot also impersonate
        independent confirmation.  Each newly acquired confirmation and match
        decision crosses the same durable producer handoff before the next one.
        """
        if self.confirmed is not None:
            return self.confirmed
        completed = self.state.completed
        if completed is None or not self.coverage_accepted:
            raise DownloadError("mapping confirmation requires completed acquisition and scope")
        endpoints = completed.endpoints
        for name in self.state.observed:
            if name in self.confirmations:
                continue
            rows = endpoints[name][:1]
            actual, matched = None, True
            if rows:
                expected = rows[0]
                start, end = session_window(expected, cutoff=self.end)
                actual = completed_bars(fetch(name, start, end), self.symbol, name, start, end, self.asof)
                matched = len(actual.rows) == 1 and same_row(actual.rows[0], expected)
            self.confirmations[name] = actual, matched
            if not matched:
                self.confirmed = False
            if on_progress is not None:
                on_progress(self)
            if not matched:
                return False
        self.confirmed = True
        if on_progress is not None:
            on_progress(self)
        return True

    def finish(self, fetch, *, on_progress=None):
        if not self.available:
            raise DownloadError("unavailable mapped prefix cannot authorize pricing replacement")
        if self.coverage_missing is not None:
            raise MappingCoverageIncomplete(self.coverage_missing)
        if self.output_error is not None:
            raise DownloadError(self.output_error)
        # This is the ordinary adaptive owner, with only its all-view fetch
        # supplied by the completed fragment assembly.  Completed stages and
        # digests survive both operation retry and persisted restoration.
        while self.state.completed is None:
            for name in self.state.decision.next_adjustments:
                if name in self.state.observed:
                    continue
                value = self._all(fetch, on_progress) if name == "all" else fetch(name, self.start, self.end)
                value = completed_bars(value, self.symbol, name, self.start, self.end, self.asof)
                # Once a required date is absent, a later adjustment cannot
                # rescue a coherent bundle.  Complete that negative outcome
                # now instead of downloading the rest of the decision tree.
                days = frozenset(timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in value.rows)
                self.view_sessions[name] = days
                lower_month = self.start.strftime("%Y-%m")
                upper_month = (self.end - timedelta(days=1)).strftime("%Y-%m")
                missing = set(self.required_sessions) - days
                reference = next(iter(self.view_sessions.values()))
                missing.update(reference ^ days)
                if any(not lower_month <= day[:7] <= upper_month for day in days):
                    self.output_error = "provider rows do not fit the requested session-month range"
                if missing or self.output_error is not None:
                    self.rejected_view = value
                    if missing:
                        self.coverage_missing = tuple(sorted(missing))
                    if on_progress is not None:
                        on_progress(self)
                    if self.coverage_missing is not None:
                        raise MappingCoverageIncomplete(self.coverage_missing)
                    raise DownloadError(self.output_error)
                self.state.accept(name, value)
                if on_progress is not None:
                    on_progress(self)
            self.state.advance()
            if on_progress is not None:
                on_progress(self)
        if not self.coverage_accepted:
            # This validates new output against the selected producer's carried
            # contribution dates, never reconstructs dates from old price files.
            # These projections are all needed by confirmation/publication.
            # Construct and persist them at their first producer boundary so a
            # later interruption cannot send a consumer back through the work.
            self.state.completed.endpoints
            self.state.completed.views
            self.state.completed.extent
            self.state.completed.proof
            self.coverage_accepted = True
            if on_progress is not None:
                on_progress(self)
        return self.state.completed

    def document(self):
        completed = self.state.completed
        return {
            "schema_version": 1,
            "binding": {
                "store": self.store,
                "symbol": self.symbol,
                "source_generation": self.source_generation,
                "start": self.start.isoformat(),
                "end": self.end.isoformat(),
                "asof": self.asof,
                "candidate_id": self.candidate_id,
            },
            "fragments": [_encode_bars(value) for value in self.fragments],
            "remaining_all_ranges": [
                [start.isoformat(), end.isoformat()] for start, end in self.remaining_all_ranges
            ],
            "assembled_all": None if self.assembled_all is None else _encode_bars(self.assembled_all),
            "observed": {name: _encode_bars(value) for name, value in self.state.observed.items()},
            "hashes": dict(self.state.hashes),
            "decision": asdict(self.state.decision),
            "completed": None
            if completed is None
            else {
                "sources": completed.sources,
                "pattern": completed.pattern,
                "fallback": completed.fallback,
                "cached": {
                    name: (
                        {"values": dict(value), "tail_all": value.tail_all}
                        if name == "endpoints"
                        else True
                        if name == "views"
                        else value
                    )
                    for name, value in completed.__dict__.items()
                    if name in {"views", "endpoints", "extent", "proof"}
                },
            },
            "confirmations": {
                name: {"bars": None if actual is None else _encode_bars(actual), "matched": matched}
                for name, (actual, matched) in self.confirmations.items()
            },
            "confirmed": self.confirmed,
            "required_sessions": self.required_sessions,
            "coverage_accepted": self.coverage_accepted,
            "coverage_missing": self.coverage_missing,
            "contribution_extent": self.contribution_extent,
            "publication_assembly": self.publication_assembly,
            "output_error": self.output_error,
            "view_sessions": {name: list(days) for name, days in self.view_sessions.items()},
            "rejected_view": None if self.rejected_view is None else _encode_bars(self.rejected_view),
        }

    @classmethod
    def from_document(cls, document):
        """Own-contract ingress; never rehash, normalize or replay a saved decision."""
        if (
            not isinstance(document, dict)
            or set(document)
            != {
                "schema_version",
                "binding",
                "fragments",
                "remaining_all_ranges",
                "assembled_all",
                "observed",
                "hashes",
                "decision",
                "completed",
                "confirmations",
                "confirmed",
                "required_sessions",
                "coverage_accepted",
                "coverage_missing",
                "contribution_extent",
                "publication_assembly",
                "output_error",
                "view_sessions",
                "rejected_view",
            }
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
        ):
            raise DownloadError("unsupported mapped acquisition handoff")
        binding = document["binding"]
        if not isinstance(binding, dict) or set(binding) != _BINDING_FIELDS:
            raise DownloadError("invalid mapped acquisition handoff binding")
        self = object.__new__(cls)
        for name in _BINDING_FIELDS:
            setattr(self, name, timestamp(binding[name]) if name in {"start", "end"} else binding[name])
        if not isinstance(self.store, str) or str(Path(self.store).absolute()) != self.store:
            raise DownloadError("invalid mapped acquisition store")
        self._check_binding_fields()
        if not isinstance(document["fragments"], (list, tuple)) or not document["fragments"]:
            raise DownloadError("missing completed mapping witness")
        self.fragments = [_decode_bars(value) for value in document["fragments"]]
        for value in self.fragments:
            self._check_fragment(value)
        ranges = document["remaining_all_ranges"]
        if not isinstance(ranges, (list, tuple)) or any(
            not isinstance(pair, (list, tuple)) or len(pair) != 2 for pair in ranges
        ):
            raise DownloadError("invalid completed mapping range plan")
        self.remaining_all_ranges = [(timestamp(start), timestamp(end)) for start, end in ranges]
        self.assembled_all = (
            None if document["assembled_all"] is None else _decode_bars(document["assembled_all"])
        )
        if self.assembled_all is not None:
            if self.remaining_all_ranges:
                raise DownloadError("assembled mapping handoff has incomplete request coverage")
            self.assembled_all = self.assembled_all.select(self.symbol, "all", self.start, self.end, self.asof)
        if not isinstance(document["observed"], dict) or set(document["observed"]) - set(ADJUSTMENTS):
            raise DownloadError("invalid mapped acquisition observations")
        observed = {name: _decode_bars(value) for name, value in document["observed"].items()}
        for name, value in observed.items():
            observed[name] = value.select(self.symbol, name, self.start, self.end, self.asof)
        hashes = document["hashes"]
        if (
            not isinstance(hashes, dict)
            or hashes.keys() != observed.keys()
            or any(
                not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value)
                for value in hashes.values()
            )
        ):
            raise DownloadError("invalid completed mapping acquisition digests")
        decision = document["decision"]
        if (
            not isinstance(decision, dict)
            or set(decision) != {"next_adjustments", "pattern", "source_adjustments", "fallback_used"}
            or type(decision["fallback_used"]) is not bool
        ):
            raise DownloadError("invalid completed mapping acquisition decision")
        following = decision["next_adjustments"]
        sources = decision["source_adjustments"]
        if (
            not isinstance(following, (list, tuple))
            or set(following) - set(ADJUSTMENTS)
            or not isinstance(sources, (list, tuple))
            or any(
                not isinstance(pair, (list, tuple))
                or len(pair) != 2
                or pair[0] not in ADJUSTMENTS
                or pair[1] not in ADJUSTMENTS
                for pair in sources
            )
            or decision["pattern"] is not None
            and not isinstance(decision["pattern"], str)
        ):
            raise DownloadError("invalid mapping adjustment decision fields")
        state = object.__new__(AdjustmentAcquisition)
        state.symbol, state.start, state.end, state.asof = self.symbol, self.start, self.end, self.asof
        state.observed, state.hashes = observed, dict(hashes)
        state.decision = AdaptiveAdjustmentDecision(
            tuple(following),
            decision["pattern"],
            tuple(tuple(pair) for pair in sources),
            decision["fallback_used"],
        )
        state.failure, state.exact_fallback, state.completed = None, False, None
        completed = document["completed"]
        if completed is not None:
            if (
                not isinstance(completed, dict)
                or set(completed) != {"sources", "pattern", "fallback", "cached"}
                or not state.decision.complete
            ):
                raise DownloadError("invalid completed mapped acquisition")
            pairs = completed["sources"]
            if (
                not isinstance(pairs, (list, tuple))
                or any(not isinstance(pair, (list, tuple)) or len(pair) != 2 for pair in pairs)
                or len(pairs) != len(ADJUSTMENTS)
                or {pair[0] for pair in pairs} != set(ADJUSTMENTS)
                or any(pair[1] not in observed for pair in pairs)
                or completed["pattern"] != state.decision.pattern
                or type(completed["fallback"]) is not bool
                or completed["fallback"] != state.decision.fallback_used
            ):
                raise DownloadError("invalid completed mapping sources")
            state.completed = CompletedAdjustments(
                self.symbol,
                self.start,
                self.end,
                self.asof,
                tuple(replace(observed[source], adjustment=name) for name, source in pairs),
                tuple(hashes.items()),
                tuple(tuple(pair) for pair in pairs),
                completed["pattern"],
                completed["fallback"],
            )
            cached = completed["cached"]
            if not isinstance(cached, dict) or set(cached) - {"views", "endpoints", "extent", "proof"}:
                raise DownloadError("invalid completed mapping cached handoffs")
            for name, value in cached.items():
                if name == "endpoints":
                    from .endpoints import Endpoints

                    if not isinstance(value, dict) or set(value) != {"values", "tail_all"}:
                        raise DownloadError("invalid completed mapping endpoints")
                    value = Endpoints(value["values"], tail_all=value["tail_all"])
                elif name == "views":
                    if value is not True:
                        raise DownloadError("invalid completed mapping view capability")
                    # Reconnect the producer's accepted aliases to its direct
                    # observations.  Re-decoding every logical view separately
                    # would lose alias identity and repeat later activity work.
                    value = {bar.adjustment: bar.rows for bar in state.completed.bars}
                state.completed.__dict__[name] = value
        elif state.decision.complete:
            raise DownloadError("missing completed mapping adjustment handoff")
        self.state = state
        confirmations = document["confirmations"]
        if not isinstance(confirmations, dict) or set(confirmations) - set(observed):
            raise DownloadError("invalid completed mapping confirmations")
        self.confirmations = {}
        for name, value in confirmations.items():
            if (
                not isinstance(value, dict)
                or set(value) != {"bars", "matched"}
                or type(value["matched"]) is not bool
            ):
                raise DownloadError("invalid completed mapping confirmation")
            actual = None if value["bars"] is None else _decode_bars(value["bars"])
            self.confirmations[name] = actual, value["matched"]
        if document["confirmed"] is not None and type(document["confirmed"]) is not bool:
            raise DownloadError("invalid mapped confirmation completion")
        self.confirmed = document["confirmed"]
        if not isinstance(document["required_sessions"], (list, tuple)) or any(
            not isinstance(day, str) for day in document["required_sessions"]
        ):
            raise DownloadError("invalid mapped contribution scope")
        if type(document["coverage_accepted"]) is not bool:
            raise DownloadError("invalid mapped scope completion")
        self.required_sessions, self.coverage_accepted = (
            tuple(document["required_sessions"]),
            document["coverage_accepted"],
        )
        extent = document["contribution_extent"]
        if (
            not isinstance(extent, dict)
            or set(extent) != {"from", "to"}
            or any(not isinstance(value, str) for value in extent.values())
        ):
            raise DownloadError("invalid completed mapping contribution extent")
        self.contribution_extent = dict(extent)
        assembly = document["publication_assembly"]
        if assembly is not None:
            if not isinstance(assembly, dict) or set(assembly) != {"changed_views", "update", "old_documents"}:
                raise DownloadError("invalid completed mapped publication assembly")
            assembly = {
                "changed_views": {
                    kind: {name: tuple(tuple(row) for row in rows) for name, rows in views.items()}
                    for kind, views in assembly["changed_views"].items()
                },
                "update": assembly["update"],
                "old_documents": assembly["old_documents"],
            }
        self.publication_assembly = assembly
        if document["output_error"] is not None and not isinstance(document["output_error"], str):
            raise DownloadError("invalid completed mapped output disposition")
        self.output_error = document["output_error"]
        sessions = document["view_sessions"]
        if (
            not isinstance(sessions, dict)
            or set(sessions) - set(ADJUSTMENTS)
            or any(
                not isinstance(days, (list, tuple)) or any(not isinstance(day, str) for day in days)
                for days in sessions.values()
            )
        ):
            raise DownloadError("invalid completed mapping session projection")
        self.view_sessions = {name: frozenset(days) for name, days in sessions.items()}
        if not observed.keys() <= self.view_sessions.keys():
            raise DownloadError("missing completed mapping session projection")
        self.rejected_view = (
            None if document["rejected_view"] is None else _decode_bars(document["rejected_view"])
        )
        missing = document["coverage_missing"]
        if missing is not None and (
            not isinstance(missing, (list, tuple)) or any(not isinstance(day, str) for day in missing)
        ):
            raise DownloadError("invalid completed mapping coverage result")
        self.coverage_missing = None if missing is None else tuple(missing)
        if (
            not self.required_sessions
            or self.coverage_accepted
            and (
                state.completed is None
                or not {"views", "endpoints", "extent", "proof"} <= state.completed.__dict__.keys()
            )
        ):
            raise DownloadError("missing completed mapping contribution capability")
        return self
