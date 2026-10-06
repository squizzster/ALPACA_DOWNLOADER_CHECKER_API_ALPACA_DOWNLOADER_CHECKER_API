import math
from dataclasses import dataclass
from typing import Literal, Mapping

PASS = "pass"
WARNING = "warning"
ERROR = "error"
NOT_EVALUATED = "not_evaluated"

ViolationOutcome = Literal["warning", "error"]
RuleOutcome = Literal["pass", "warning", "error", "not_evaluated"]


@dataclass(frozen=True)
class RuleEvaluation:
    """A policy-classified result from a deterministic, side-effect-free calculation."""

    outcome: RuleOutcome
    measurements: dict[str, object]

    @property
    def passed(self) -> bool:
        return self.outcome == PASS


def _classified_outcome(rule_passed: bool, violation_outcome: ViolationOutcome) -> RuleOutcome:
    if violation_outcome not in {WARNING, ERROR}:
        raise ValueError(f"violation outcome must be {WARNING!r} or {ERROR!r}")
    return PASS if rule_passed else violation_outcome


def evaluate_ordered_bounds(
    lower_bound: float,
    upper_bound: float,
    *,
    violation_outcome: ViolationOutcome,
) -> RuleEvaluation:
    """Classify whether a lower/upper pair is ordered."""
    return RuleEvaluation(
        outcome=_classified_outcome(lower_bound <= upper_bound, violation_outcome),
        measurements={"lower_bound": lower_bound, "upper_bound": upper_bound},
    )


def evaluate_value_within_bounds(
    value: float,
    lower_bound: float,
    upper_bound: float,
    *,
    absolute_allowance: float = 0.0,
    violation_outcome: ViolationOutcome,
) -> RuleEvaluation:
    """Classify one value against inclusive bounds with an explicit absolute allowance."""
    if absolute_allowance < 0 or not math.isfinite(absolute_allowance):
        raise ValueError("absolute allowance must be finite and nonnegative")
    permitted_lower_bound = lower_bound - absolute_allowance
    permitted_upper_bound = upper_bound + absolute_allowance
    return RuleEvaluation(
        outcome=_classified_outcome(
            permitted_lower_bound <= value <= permitted_upper_bound,
            violation_outcome,
        ),
        measurements={
            "value": value,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "absolute_allowance": absolute_allowance,
            "permitted_lower_bound": permitted_lower_bound,
            "permitted_upper_bound": permitted_upper_bound,
        },
    )


def evaluate_common_multiplicative_price_factor(
    raw_prices: Mapping[str, float],
    adjusted_prices: Mapping[str, float],
    *,
    raw_absolute_error: float,
    adjusted_absolute_error: float,
    violation_outcome: ViolationOutcome,
) -> RuleEvaluation:
    """Intersect feasible adjusted/raw factor intervals for every supplied price field."""
    if raw_prices.keys() != adjusted_prices.keys() or not raw_prices:
        raise ValueError("raw and adjusted prices must contain the same nonempty field set")
    if (
        raw_absolute_error < 0
        or adjusted_absolute_error < 0
        or not math.isfinite(raw_absolute_error)
        or not math.isfinite(adjusted_absolute_error)
    ):
        raise ValueError("price serialization errors must be finite and nonnegative")

    lower_bound, upper_bound = 0.0, math.inf
    field_intervals = {}
    for field_name in raw_prices:
        raw_value = float(raw_prices[field_name])
        adjusted_value = float(adjusted_prices[field_name])
        if raw_value <= raw_absolute_error or adjusted_value <= 0:
            return RuleEvaluation(
                outcome=NOT_EVALUATED,
                measurements={
                    "reason": "price resolution prevents a stable ratio interval",
                    "field": field_name,
                    "raw_value": raw_value,
                    "adjusted_value": adjusted_value,
                },
            )
        field_lower = max(0.0, adjusted_value - adjusted_absolute_error) / (raw_value + raw_absolute_error)
        field_upper = (adjusted_value + adjusted_absolute_error) / (raw_value - raw_absolute_error)
        field_intervals[field_name] = (field_lower, field_upper)
        lower_bound = max(lower_bound, field_lower)
        upper_bound = min(upper_bound, field_upper)

    interval = (lower_bound, upper_bound)
    return RuleEvaluation(
        outcome=_classified_outcome(lower_bound <= upper_bound + 1e-12, violation_outcome),
        measurements={
            "factor_interval_intersection": interval,
            "field_factor_intervals": field_intervals,
            "raw_absolute_error": raw_absolute_error,
            "adjusted_absolute_error": adjusted_absolute_error,
        },
    )
