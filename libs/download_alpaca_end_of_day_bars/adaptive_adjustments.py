from dataclasses import dataclass
from typing import Mapping

from .settings import ALPACA_ADJUSTMENT_MODES

_PATTERN_GROUPS = {
    "all_equal": (tuple(ALPACA_ADJUSTMENT_MODES),),
    "dividend_only": (
        ("raw", "split", "spin-off", "split,spin-off"),
        ("dividend", "split,dividend", "dividend,spin-off", "all"),
    ),
    "split_only": (
        ("raw", "dividend", "spin-off", "dividend,spin-off"),
        ("split", "split,dividend", "split,spin-off", "all"),
    ),
    "split_and_dividend": (
        ("raw", "spin-off"),
        ("split", "split,spin-off"),
        ("dividend", "dividend,spin-off"),
        ("split,dividend", "all"),
    ),
    "dividend_and_spin_off": (
        ("raw", "split"),
        ("dividend", "split,dividend"),
        ("spin-off", "split,spin-off"),
        ("dividend,spin-off", "all"),
    ),
    "spin_off_only": (
        ("raw", "split", "dividend", "split,dividend"),
        ("spin-off", "split,spin-off", "dividend,spin-off", "all"),
    ),
    "all_distinct": tuple((adjustment,) for adjustment in ALPACA_ADJUSTMENT_MODES),
    "observed_pattern_8": (
        ("raw", "dividend"),
        ("split", "split,dividend"),
        ("spin-off", "dividend,spin-off"),
        ("split,spin-off", "all"),
    ),
    "observed_pattern_9": (
        ("raw", "dividend"),
        ("split", "split,dividend", "split,spin-off", "all"),
        ("spin-off", "dividend,spin-off"),
    ),
}


@dataclass(frozen=True)
class AdaptiveAdjustmentDecision:
    next_adjustments: tuple[str, ...]
    pattern: str | None = None
    source_adjustments: tuple[tuple[str, str], ...] = ()
    fallback_used: bool = False

    @property
    def complete(self) -> bool:
        return not self.next_adjustments and self.pattern is not None

    def source_adjustment_for(self, adjustment: str) -> str:
        try:
            return dict(self.source_adjustments)[adjustment]
        except KeyError as error:
            raise ValueError(f"decision has no source for adjustment {adjustment!r}") from error


def _validate_observed_sha256(observed_sha256_by_adjustment: Mapping[str, str]) -> None:
    unsupported = set(observed_sha256_by_adjustment) - set(ALPACA_ADJUSTMENT_MODES)
    if unsupported:
        raise ValueError(f"unsupported observed adjustments: {sorted(unsupported)!r}")
    for adjustment, sha256 in observed_sha256_by_adjustment.items():
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise ValueError(f"invalid SHA-256 for observed adjustment {adjustment!r}")
        try:
            int(sha256, 16)
        except ValueError as error:
            raise ValueError(f"invalid SHA-256 for observed adjustment {adjustment!r}") from error


def _pattern_matches_observations(
    pattern: str,
    observed_sha256_by_adjustment: Mapping[str, str],
) -> bool:
    group_by_adjustment = {
        adjustment: group_index
        for group_index, group in enumerate(_PATTERN_GROUPS[pattern])
        for adjustment in group
    }
    observed_adjustments = tuple(observed_sha256_by_adjustment)
    return all(
        (observed_sha256_by_adjustment[left_adjustment] == observed_sha256_by_adjustment[right_adjustment])
        == (group_by_adjustment[left_adjustment] == group_by_adjustment[right_adjustment])
        for left_index, left_adjustment in enumerate(observed_adjustments)
        for right_adjustment in observed_adjustments[left_index + 1 :]
    )


def _complete_known_pattern(
    pattern: str,
    observed_sha256_by_adjustment: Mapping[str, str],
) -> AdaptiveAdjustmentDecision | None:
    if not _pattern_matches_observations(pattern, observed_sha256_by_adjustment):
        return None
    observed_adjustments = set(observed_sha256_by_adjustment)
    source_adjustments = []
    for group in _PATTERN_GROUPS[pattern]:
        representatives = [adjustment for adjustment in group if adjustment in observed_adjustments]
        if not representatives:
            return None
        representative = representatives[0]
        source_adjustments.extend((adjustment, representative) for adjustment in group)
    return AdaptiveAdjustmentDecision(
        next_adjustments=(),
        pattern=pattern,
        source_adjustments=tuple(source_adjustments),
    )


def _fallback_decision(
    observed_sha256_by_adjustment: Mapping[str, str],
) -> AdaptiveAdjustmentDecision:
    missing_adjustments = tuple(
        adjustment for adjustment in ALPACA_ADJUSTMENT_MODES if adjustment not in observed_sha256_by_adjustment
    )
    if missing_adjustments:
        return AdaptiveAdjustmentDecision(
            next_adjustments=missing_adjustments,
            fallback_used=True,
        )

    representative_by_sha256 = {}
    source_adjustments = []
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        sha256 = observed_sha256_by_adjustment[adjustment]
        representative = representative_by_sha256.setdefault(sha256, adjustment)
        source_adjustments.append((adjustment, representative))
    return AdaptiveAdjustmentDecision(
        next_adjustments=(),
        pattern="unseen_full_fallback",
        source_adjustments=tuple(source_adjustments),
        fallback_used=True,
    )


def decide_adaptive_adjustment_downloads(
    observed_sha256_by_adjustment: Mapping[str, str],
) -> AdaptiveAdjustmentDecision:
    """Choose the next requests or identify all logical views from observed payload hashes."""
    _validate_observed_sha256(observed_sha256_by_adjustment)
    observed = dict(observed_sha256_by_adjustment)

    if len(observed) == len(ALPACA_ADJUSTMENT_MODES):
        for pattern in _PATTERN_GROUPS:
            decision = _complete_known_pattern(pattern, observed)
            if decision is not None:
                return decision
        return _fallback_decision(observed)

    if "raw" not in observed:
        return AdaptiveAdjustmentDecision(next_adjustments=("raw",))
    if "all" not in observed:
        return AdaptiveAdjustmentDecision(next_adjustments=("all",))

    if observed["all"] == observed["raw"]:
        return _complete_known_pattern("all_equal", observed) or _fallback_decision(observed)

    if "dividend" not in observed:
        return AdaptiveAdjustmentDecision(next_adjustments=("dividend",))
    if observed["dividend"] == observed["all"]:
        return _complete_known_pattern("dividend_only", observed) or _fallback_decision(observed)

    if observed["dividend"] == observed["raw"]:
        if "spin-off" not in observed:
            return AdaptiveAdjustmentDecision(next_adjustments=("spin-off",))
        if observed["spin-off"] == observed["raw"]:
            return _complete_known_pattern("split_only", observed) or _fallback_decision(observed)
        if observed["spin-off"] == observed["all"]:
            return _complete_known_pattern("spin_off_only", observed) or _fallback_decision(observed)
        if "split" not in observed:
            return AdaptiveAdjustmentDecision(next_adjustments=("split",))
        for pattern in ("observed_pattern_8", "observed_pattern_9"):
            decision = _complete_known_pattern(pattern, observed)
            if decision is not None:
                return decision
        return _fallback_decision(observed)

    missing_single_adjustments = tuple(
        adjustment for adjustment in ("split", "spin-off") if adjustment not in observed
    )
    if missing_single_adjustments:
        return AdaptiveAdjustmentDecision(next_adjustments=missing_single_adjustments)

    five_adjustments = ("raw", "all", "dividend", "split", "spin-off")
    if len({observed[adjustment] for adjustment in five_adjustments}) == 5:
        return _fallback_decision(observed)
    for pattern in ("split_and_dividend", "dividend_and_spin_off"):
        decision = _complete_known_pattern(pattern, observed)
        if decision is not None:
            return decision
    return _fallback_decision(observed)
