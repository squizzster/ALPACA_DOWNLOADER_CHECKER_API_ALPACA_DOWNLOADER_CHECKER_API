"""Ordered acquisition of new rows and an operation-local completed view handoff."""

from dataclasses import dataclass, replace
from datetime import datetime
from time import monotonic

from libs.alpaca_data_api_solver import VERSION as PLAN_VERSION
from libs.alpaca_data_api_solver import build_plan
from libs.common.debug_trace import event
from libs.common.provider_http import balanced_batches

from .adjustment_policy import AdaptiveAdjustmentDecision, decide_adaptive_adjustment_downloads
from .contract import ADJUSTMENTS, BarsSymbolUnavailable, DownloadError, csv_bytes, digest, timestamp
from .failures import AcquisitionFailure
from .responses import CompletedBars, completed_bars

POLICY = "alpaca_adjustment_equivalence_v1"


@dataclass(frozen=True)
class CompletedAdjustments:
    symbol: str
    start: datetime
    end: datetime
    asof: str
    bars: tuple[CompletedBars, ...]
    observed: tuple[tuple[str, str], ...]
    sources: tuple[tuple[str, str], ...]
    pattern: str
    fallback: bool

    @property
    def views(self):
        return {value.adjustment: value.rows for value in self.bars}

    @property
    def proof(self):
        return {
            "policy": POLICY,
            "start": self.start.isoformat(),
            "end_exclusive": self.end.isoformat(),
            "asof": self.asof,
            "pattern": self.pattern,
            "observed_sha256": dict(self.observed),
            "source_adjustments": dict(self.sources),
            "fallback": self.fallback,
        }


class AdjustmentAcquisition:
    """One exact scope; partial transport fallback retains completed observations."""

    def __init__(self, symbol, start, end, asof):
        self.symbol, self.start, self.end, self.asof = symbol, start, end, asof
        self.observed, self.hashes = {}, {}
        self.decision = decide_adaptive_adjustment_downloads({})
        self.completed = None
        self.failure = None
        self.exact_fallback = False

    @property
    def key(self):
        return self.symbol, self.start, self.end, self.asof

    def accept(self, adjustment, value):
        if adjustment in self.observed:
            raise DownloadError("adaptive acquisition repeated a completed observation")
        value = completed_bars(value, self.symbol, adjustment, self.start, self.end, self.asof)
        self.observed[adjustment] = value
        # Hash only the newly acquired complete range, once. No retained storage reads.
        self.hashes[adjustment] = digest(csv_bytes(value.rows))

    def advance(self):
        if len({bool(value.rows) for value in self.observed.values()}) > 1:
            # A provider omission is not an adjustment equivalence. Preserve
            # partial-view evidence rather than infer additional missing views.
            missing = tuple(name for name in ADJUSTMENTS if name not in self.observed)
            self.decision = AdaptiveAdjustmentDecision(
                missing,
                None if missing else "inconsistent_presence_full_fallback",
                tuple((name, name) for name in ADJUSTMENTS),
                True,
            )
        else:
            self.decision = decide_adaptive_adjustment_downloads(self.hashes)
        if not self.decision.complete:
            return
        sources = dict(self.decision.source_adjustments)
        # Direct observations retain their identity even when another observed view
        # is equal. Only omitted views use the policy's chosen representative.
        sources.update((name, name) for name in self.observed)
        self.completed = CompletedAdjustments(
            self.symbol,
            self.start,
            self.end,
            self.asof,
            tuple(replace(self.observed[sources[name]], adjustment=name) for name in ADJUSTMENTS),
            tuple(self.hashes.items()),
            tuple(sources.items()),
            self.decision.pattern,
            self.decision.fallback_used,
        )
        event("pricing_adjustment_selection", symbol=self.symbol, **self.completed.proof)

    def finish(self, fetch):
        if self.failure is not None and not self.exact_fallback:
            if self.failure.outcome == "invalid_symbol":
                raise BarsSymbolUnavailable(self.failure.symbol, self.failure.http_status)
            raise self.failure.exception()
        while self.completed is None:
            for name in self.decision.next_adjustments:
                if name not in self.observed:
                    self.accept(name, fetch(name))
            self.advance()
        return self.completed


def acquire_adjustments(needs, client, *, metrics=None, progress=None, on_failure=None, merge_ranges=False):
    """Batch each adaptive stage, then select the next from completed observations.

    No plan crosses a response-dependent decision. Failed actions establish no
    observations; exact transport fallback may consume successful earlier stages.
    Returned states live only as long as their bounded acquisition cohort.
    """
    states = []
    for need in needs:
        if len(need["symbols"]) != 1 or len(need["ranges"]) != 1 or set(need["adjustments"]) != set(ADJUSTMENTS):
            raise DownloadError("adaptive acquisition requires one complete ticker/range need")
        window = need["ranges"][0]
        states.append(
            AdjustmentAcquisition(
                need["symbols"][0], timestamp(window["start"]), timestamp(window["end_exclusive"]), need["asof"]
            )
        )
    metrics = {} if metrics is None else metrics
    plans = []
    while True:
        pending = [i for i, state in enumerate(states) if state.completed is None and state.failure is None]
        if not pending:
            break
        stage_needs = [{**needs[i], "adjustments": list(states[i].decision.next_adjustments)} for i in pending]
        plan = build_plan({"steps": [{"needs": stage_needs}]}, max_symbols=2000, merge_ranges=merge_ranges)
        plans.append(plan)
        stage = stage_needs[0]["why"]
        started, pages_before = monotonic(), metrics.get("page_requests", 0)
        if progress:
            actions = plan["steps"][0]["actions"]
            progress(
                stage,
                {
                    "state": "started",
                    "symbols": len(pending),
                    "actions": len(actions),
                    "minimum_pages": sum(
                        len(balanced_batches(a["request"]["params"]["symbols"].split(","))) for a in actions
                    ),
                    "adaptive_stage": len(plans),
                },
            )

        def failed(step, action, error):
            failure = AcquisitionFailure.from_error(error)
            exact = failure.http_status == 400 and len(action["request"]["params"]["symbols"].split(",")) > 1
            for target in action["deliver"]:
                state = states[pending[target["need"]]]
                if state.failure is None or state.exact_fallback and not exact:
                    state.failure, state.exact_fallback = failure, exact
            if on_failure is not None:
                on_failure(step, action, error)

        try:
            acquired = client.execute_plan(plan, metrics=metrics, on_failure=failed)
        except (RuntimeError, OSError, ValueError):
            if progress:
                progress(
                    stage,
                    {
                        "state": "failed",
                        "seconds": monotonic() - started,
                        "page_requests": metrics.get("page_requests", 0) - pages_before,
                    },
                )
            raise
        for stage_index, index in enumerate(pending):
            state = states[index]
            for name in stage_needs[stage_index]["adjustments"]:
                key = (0, stage_index, 0, state.symbol, name)
                if key in acquired:
                    state.accept(name, acquired[key])
                elif state.failure is None:
                    raise DownloadError("adaptive action did not complete its selected delivery")
            if state.failure is None:
                state.advance()
        if progress:
            progress(
                stage,
                {
                    "state": "completed",
                    "page_requests": metrics.get("page_requests", 0) - pages_before,
                    "seconds": monotonic() - started,
                    "adaptive_stage": len(plans),
                },
            )
    return states, {
        "schema_version": PLAN_VERSION,
        "steps": [step for plan in plans for step in plan["steps"]],
    }
