"""Infer split-only price multipliers from jointly rounded adjustment views.

The provider's supplied prices are observations, never reconstructed here.  The
seven adjusted views constrain positive factors S, D, O, S*D, S*O, D*O and
S*D*O.  Price allowances and the reciprocal split-volume relationship supply
intervals for those products.  An exact analytical projection in log space
finds the range of S for which one *shared* D/O solution exists.

These are estimates conditional on the stated rounding model.  Numerical
feasibility does not establish corporate-action history or its adjustment
horizon.  Missing intermediate views are allowed and impose no constraints;
raw, split and all are required.

Runtime dependencies: NumPy and the Python standard library only.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np

PRICE_FIELDS = ("open", "high", "low", "close")
ADJUSTED_VIEWS = (
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)
SPLIT_VIEWS = ("split", "split,dividend", "split,spin-off", "all")
REQUIRED_VIEWS = ("raw", "split", "all")

# This slack only absorbs floating-point roundoff in the analytical solver.
# It is separate from, and vastly smaller than, the supplied price allowances.
NUMERICAL_LOG_TOLERANCE = 2e-12
RATIONAL_RELATIVE_WIDTH_LIMIT = 0.01
RATIONAL_MAX_DENOMINATOR = 1000


class FactorInferenceError(ValueError):
    """An invalid observation or an inconsistent joint factor model.

    ``row_indices`` are zero-based positions within the supplied aligned
    arrays, so a caller can add symbol/date context without parsing a message.
    """

    def __init__(
        self,
        message: str,
        *,
        row_indices: list[int] | None = None,
        view: str | None = None,
    ) -> None:
        super().__init__(message)
        self.row_indices = row_indices or []
        self.view = view


@dataclass
class _Model:
    views: dict[str, dict[str, np.ndarray]]
    lower: np.ndarray
    upper: np.ndarray
    split_lower: np.ndarray
    split_upper: np.ndarray
    volume_score: np.ndarray
    volume_anchor: np.ndarray
    price_score: np.ndarray
    price_anchor: np.ndarray
    diagnostics: dict[str, Any]


def _raise_rows(message: str, mask: np.ndarray, *, view: str | None = None) -> None:
    positions = np.flatnonzero(mask)
    if not positions.size:
        return
    indices = positions[:8].astype(int).tolist()
    suffix = ", ..." if len(positions) > len(indices) else ""
    rendered = ", ".join(str(i) for i in indices)
    raise FactorInferenceError(
        f"{message} at row indices {rendered}{suffix} ({len(positions)} affected rows)",
        row_indices=indices,
        view=view,
    )


def _normalise_views(
    views: Mapping[str, Mapping[str, np.ndarray]],
) -> tuple[dict[str, dict[str, np.ndarray]], int]:
    missing = [name for name in REQUIRED_VIEWS if name not in views]
    if missing:
        raise FactorInferenceError("Required adjustment views are missing: " + ", ".join(missing))
    if "close" not in views["raw"]:
        raise FactorInferenceError("View 'raw' is missing the close field")
    try:
        raw_close = np.asarray(views["raw"]["close"], dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise FactorInferenceError("View 'raw' close must contain numeric values") from exc
    if raw_close.ndim != 1:
        raise FactorInferenceError("All observation fields must be one-dimensional aligned arrays")
    n = len(raw_close)
    prepared: dict[str, dict[str, np.ndarray]] = {}
    for view in ("raw",) + ADJUSTED_VIEWS:
        if view not in views:
            continue
        source = views[view]
        absent = [field for field in PRICE_FIELDS + ("volume",) if field not in source]
        if absent:
            raise FactorInferenceError(f"View {view!r} is missing fields: " + ", ".join(absent))
        fields = PRICE_FIELDS + ("volume",) + (("vwap",) if "vwap" in source else ())
        prepared[view] = {}
        for field in fields:
            try:
                arr = np.asarray(source[field], dtype=np.float64)
            except (TypeError, ValueError) as exc:
                raise FactorInferenceError(f"View {view!r} field {field!r} must be numeric") from exc
            if arr.ndim != 1 or len(arr) != n:
                raise FactorInferenceError(
                    f"View {view!r} field {field!r} has shape {arr.shape}; expected ({n},)"
                )
            _raise_rows(
                f"View {view!r} contains negative or non-finite {field}",
                (~np.isfinite(arr)) | (arr < 0),
                view=view,
            )
            prepared[view][field] = arr
    return prepared, n


def _project_log_split(lower: np.ndarray, upper: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Project the seven product log-intervals onto log S, without an LP.

    Arrays have shape (7, n), ordered as ``ADJUSTED_VIEWS``.  Missing bounds
    use -inf/+inf.  The returned arrays are (log lower S, log upper S,
    feasible mask).  All log factors are unrestricted real numbers.

    Proof: at fixed x=log S, y=log D is in the intersection of [lD,uD]
    and [lSD-x,uSD-x]; z=log O has the analogous intersection.  y+z is
    in the intersection of [lDO,uDO] and [lSDO-x,uSDO-x].  There is a
    shared y,z solution exactly when all three intervals are nonempty,
    the sum of the two lower endpoints is no larger than the sum upper
    bound, and the sum of the two upper endpoints is no smaller than the
    sum lower bound.  Expanding max/min endpoints gives the inequalities
    below.  This expansion also works with unbounded endpoints, without
    an arbitrary finite replacement for infinity.
    """
    lower = np.asarray(lower, dtype=np.float64)
    upper = np.asarray(upper, dtype=np.float64)
    if lower.ndim != 2 or lower.shape[0] != 7 or upper.shape != lower.shape:
        raise ValueError("Log bounds must have matching shape (7, n)")
    ls, ld, lo, lsd, lso, ldo, lsdo = lower
    us, ud, uo, usd, uso, udo, usdo = upper
    eps = NUMERICAL_LOG_TOLERANCE

    # For ordinary bounds every expression below is finite or has an
    # unambiguous one-sided infinity.  Invalid infinities/NaNs are explicitly
    # marked infeasible before their arithmetic can affect a result.
    with np.errstate(invalid="ignore", over="ignore"):
        feasible = np.all(
            (~np.isnan(lower))
            & (~np.isnan(upper))
            & (~np.isposinf(lower))
            & (~np.isneginf(upper))
            & (lower <= upper + eps),
            axis=0,
        )
        feasible &= ld + lo <= udo + eps
        feasible &= ld + lso <= usdo + eps
        feasible &= lsd + lo <= usdo + eps
        feasible &= ldo <= ud + uo + eps
        feasible &= lsdo <= ud + uso + eps
        feasible &= lsdo <= usd + uo + eps

        projected_lower = np.maximum.reduce(
            (
                ls,
                lsd - ud,
                lso - uo,
                lsdo - udo,
                ld + lso - udo,
                lsd + lo - udo,
                (lsd + lso - udo) / 2.0,
                lsd + lso - usdo,
                lsdo - ud - uo,
            )
        )
        projected_upper = np.minimum.reduce(
            (
                us,
                usd - ld,
                uso - lo,
                usdo - ldo,
                usdo - ld - lo,
                ud + uso - ldo,
                usd + uo - ldo,
                (usd + uso - ldo) / 2.0,
                usd + uso - lsdo,
            )
        )
        feasible &= projected_lower <= projected_upper + eps
    return projected_lower, projected_upper, feasible


def _fixed_split_feasible(lower: np.ndarray, upper: np.ndarray, x: np.ndarray) -> np.ndarray:
    """Independently verify a shared D/O solution for every chosen log S."""
    ls, ld, lo, lsd, lso, ldo, lsdo = lower
    us, ud, uo, usd, uso, udo, usdo = upper
    ly = np.maximum(ld, lsd - x)
    uy = np.minimum(ud, usd - x)
    lz = np.maximum(lo, lso - x)
    uz = np.minimum(uo, uso - x)
    lq = np.maximum(ldo, lsdo - x)
    uq = np.minimum(udo, usdo - x)
    eps = NUMERICAL_LOG_TOLERANCE
    return (
        np.isfinite(x)
        & (ls <= x + eps)
        & (x <= us + eps)
        & (ly <= uy + eps)
        & (lz <= uz + eps)
        & (lq <= uq + eps)
        & (ly + lz <= uq + eps)
        & (lq <= uy + uz + eps)
    )


def _build_model(
    views: Mapping[str, Mapping[str, np.ndarray]],
    *,
    price_tolerance: float,
    raw_price_tolerance: float,
    volume_tolerance: float,
) -> _Model:
    tolerances = {
        "price_tolerance": price_tolerance,
        "raw_price_tolerance": raw_price_tolerance,
        "volume_tolerance": volume_tolerance,
    }
    for name, value in tolerances.items():
        if not isinstance(value, (int, float, np.number)) or not math.isfinite(value) or value < 0:
            raise FactorInferenceError(f"{name} must be a finite nonnegative number")
    prepared, n = _normalise_views(views)
    raw = prepared["raw"]
    lower = np.full((7, n), -np.inf, dtype=np.float64)
    upper = np.full((7, n), np.inf, dtype=np.float64)
    used_fields: dict[str, list[str]] = {}
    for index, view in enumerate(ADJUSTED_VIEWS):
        if view not in prepared:
            continue
        adjusted = prepared[view]
        fields = PRICE_FIELDS + (("vwap",) if "vwap" in raw and "vwap" in adjusted else ())
        used_fields[view] = list(fields)
        factor_lower = np.zeros(n, dtype=np.float64)
        factor_upper = np.full(n, np.inf, dtype=np.float64)
        for field in fields:
            raw_price = raw[field]
            adjusted_price = adjusted[field]
            denominator = raw_price + raw_price_tolerance
            numerator = np.maximum(0.0, adjusted_price - price_tolerance)
            field_lower = np.zeros(n, dtype=np.float64)
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                np.divide(numerator, denominator, out=field_lower, where=denominator > 0)
            field_lower[(denominator == 0) & (numerator > 0)] = np.inf
            denominator = raw_price - raw_price_tolerance
            field_upper = np.full(n, np.inf, dtype=np.float64)
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                np.divide(
                    adjusted_price + price_tolerance,
                    denominator,
                    out=field_upper,
                    where=denominator > 0,
                )
            np.maximum(factor_lower, field_lower, out=factor_lower)
            np.minimum(factor_upper, field_upper, out=factor_upper)
        with np.errstate(divide="ignore", invalid="ignore"):
            lower[index] = np.log(factor_lower)
            upper[index] = np.log(factor_upper)

    # Volume constrains S itself for every supplied split-containing view.
    # Zero raw volume carries no factor information, but cannot produce an
    # adjusted volume outside the declared absolute allowance.
    raw_volume = raw["volume"]
    positive_raw = raw_volume > 0
    volume_score = np.zeros(n, dtype=np.float64)
    volume_anchor = np.ones(n, dtype=np.float64)
    used_volume_views = []
    for view in SPLIT_VIEWS:
        if view not in prepared:
            continue
        used_volume_views.append(view)
        adjusted_volume = prepared[view]["volume"]
        _raise_rows(
            f"View {view!r} has nonzero adjusted volume while raw volume is zero "
            f"(exceeds allowance {volume_tolerance:g})",
            (~positive_raw) & (adjusted_volume > volume_tolerance),
            view=view,
        )
        volume_lower = np.zeros(n, dtype=np.float64)
        denominator = adjusted_volume + volume_tolerance
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            np.divide(raw_volume, denominator, out=volume_lower, where=positive_raw & (denominator > 0))
        volume_lower[positive_raw & (denominator == 0)] = np.inf
        volume_upper = np.full(n, np.inf, dtype=np.float64)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            np.divide(
                raw_volume,
                adjusted_volume - volume_tolerance,
                out=volume_upper,
                where=positive_raw & (adjusted_volume > volume_tolerance),
            )
        with np.errstate(divide="ignore", invalid="ignore"):
            np.maximum(lower[0], np.log(volume_lower), out=lower[0])
            np.minimum(upper[0], np.log(volume_upper), out=upper[0])
        better = positive_raw & (adjusted_volume > volume_score)
        volume_score[better] = adjusted_volume[better]
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            volume_anchor[better] = raw_volume[better] / adjusted_volume[better]

    # Use the strongest available direct split-price observation as a fallback
    # anchor; it never overrides the joint feasible interval.
    price_score = np.zeros(n, dtype=np.float64)
    price_anchor = np.ones(n, dtype=np.float64)
    split = prepared["split"]
    for field in used_fields["split"]:
        better = (raw[field] > price_score) & (split[field] > 0)
        price_score[better] = raw[field][better]
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            price_anchor[better] = split[field][better] / raw[field][better]

    projected_lower, projected_upper, feasible = _project_log_split(lower, upper)
    _raise_rows(
        "No common positive split/dividend/spin-off factors satisfy the source price and volume bounds",
        ~feasible,
    )
    diagnostics: dict[str, Any] = {
        "method": "joint_log_interval_projection",
        "method_version": 1,
        "factor_status": "inferred_from_rounded_observations",
        "corporate_actions_certified": False,
        "row_count": n,
        "available_views": [name for name in ("raw",) + ADJUSTED_VIEWS if name in prepared],
        "missing_optional_views": [name for name in ADJUSTED_VIEWS if name not in prepared],
        "price_fields_used": used_fields,
        "split_volume_views_used": used_volume_views,
        "tolerances": {key: float(value) for key, value in tolerances.items()},
        "numerical_log_tolerance": NUMERICAL_LOG_TOLERANCE,
        "horizon_assumption": "none; factors are relative to the supplied adjustment views",
        "all_rows_jointly_feasible": True,
        "zero_raw_volume_rows": int(np.count_nonzero(~positive_raw)),
    }
    return _Model(
        prepared,
        lower,
        upper,
        projected_lower,
        projected_upper,
        volume_score,
        volume_anchor,
        price_score,
        price_anchor,
        diagnostics,
    )


def _exp_bounds(values: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore", under="ignore"):
        return np.exp(values)


def feasible_split_bounds(
    views: Mapping[str, Mapping[str, np.ndarray]],
    *,
    price_tolerance: float = 0.01,
    raw_price_tolerance: float = 1e-6,
    volume_tolerance: float = 1.0,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Return each row's jointly feasible split-factor interval.

    Returns ``(lower, upper, diagnostics)``.  An endpoint of zero denotes an
    open lower bound (factors are strictly positive); infinity means no finite
    upper bound.  All supplied views must already be aligned by session and
    security.  Inconsistent rows raise ``FactorInferenceError``.
    """
    model = _build_model(
        views,
        price_tolerance=price_tolerance,
        raw_price_tolerance=raw_price_tolerance,
        volume_tolerance=volume_tolerance,
    )
    return _exp_bounds(model.split_lower), _exp_bounds(model.split_upper), model.diagnostics


def validate_split_factors(
    views: Mapping[str, Mapping[str, np.ndarray]],
    factors: np.ndarray,
    *,
    price_tolerance: float = 0.01,
    raw_price_tolerance: float = 1e-6,
    volume_tolerance: float = 1.0,
) -> dict[str, Any]:
    """Cross-check externally established factors against the joint model.

    This checks numerical compatibility only.  A caller using action records
    must establish their completeness, identity and adjustment horizon.
    ``factors`` may be a vector aligned with the observations or one scalar.
    """
    model = _build_model(
        views,
        price_tolerance=price_tolerance,
        raw_price_tolerance=raw_price_tolerance,
        volume_tolerance=volume_tolerance,
    )
    values = np.asarray(factors, dtype=np.float64)
    n = model.diagnostics["row_count"]
    if values.ndim == 0:
        values = np.full(n, values.item(), dtype=np.float64)
    if values.ndim != 1 or len(values) != n:
        raise FactorInferenceError(f"Split factors must be scalar or have shape ({n},)")
    _raise_rows("Split factors must be finite and strictly positive", (~np.isfinite(values)) | (values <= 0))
    feasible = _fixed_split_feasible(model.lower, model.upper, np.log(values))
    _raise_rows(
        "Supplied split factors are inconsistent with a shared dividend/spin-off solution under the source bounds",
        ~feasible,
    )
    diagnostics = dict(model.diagnostics)
    diagnostics["factor_status"] = "supplied_factors_numerically_cross_checked"
    diagnostics["supplied_factors_jointly_feasible"] = True
    return diagnostics


def _simple_rational(lower: float, upper: float, anchor: float) -> float | None:
    """Prefer the lowest denominator available inside a narrow interval."""
    if not (math.isfinite(lower) and math.isfinite(upper) and 0 < lower <= upper):
        return None
    if math.log(upper) - math.log(lower) > math.log1p(RATIONAL_RELATIVE_WIDTH_LIMIT):
        return None
    for denominator in range(1, RATIONAL_MAX_DENOMINATOR + 1):
        lo_scaled, hi_scaled = lower * denominator, upper * denominator
        if not (math.isfinite(lo_scaled) and math.isfinite(hi_scaled)):
            return None
        lo_numerator = max(1, math.ceil(lo_scaled))
        hi_numerator = math.floor(hi_scaled)
        if lo_numerator > hi_numerator:
            continue
        nearest = round(anchor * denominator)
        numerator = max(lo_numerator, min(hi_numerator, nearest))
        value = numerator / denominator
        # Check the actual float, too: multiplication at a boundary can round.
        if lower <= value <= upper:
            return value
    return None


def _finite_json(value: float) -> float | None:
    return float(value) if math.isfinite(value) else None


def _select_segment_factor(
    model: _Model, start: int, end: int, lower: float, upper: float
) -> tuple[float, dict[str, Any]]:
    # A roundoff-only overlap is represented by its midpoint, after all
    # analytical feasibility checks have allowed the documented numeric slack.
    if lower > upper:
        lower = upper = lower / 2.0 + upper / 2.0
    volume_scores = model.volume_score[start:end]
    strongest_volume = int(np.argmax(volume_scores))
    if volume_scores[strongest_volume] > 0:
        anchor_value = float(model.volume_anchor[start + strongest_volume])
        anchor = "volume"
    elif lower <= 0 <= upper:
        # Neutral is defensible when the observations cannot rule it out.
        # This specifically prevents an invented midpoint factor for unchanged
        # tiny-price, zero-volume histories.
        anchor_value = 1.0
        anchor = "neutral"
    else:
        scores = model.price_score[start:end]
        strongest_price = int(np.argmax(scores))
        anchor_value = float(model.price_anchor[start + strongest_price])
        anchor = "price"
    if not (math.isfinite(anchor_value) and anchor_value > 0):
        anchor_value = 1.0
    selected_log = min(max(math.log(anchor_value), lower), upper)
    try:
        selected = math.exp(selected_log)
        lower_factor = math.exp(lower)
        upper_factor = math.exp(upper)
    except OverflowError:
        lower_factor = math.inf if lower > 709 else math.exp(lower)
        upper_factor = math.inf if upper > 709 else math.exp(upper)
        selected = math.inf if selected_log > 709 else math.exp(selected_log)
    if not (math.isfinite(selected) and selected > 0):
        raise FactorInferenceError(
            "The inferred split interval has no selected finite positive float64 factor",
            row_indices=[start],
        )
    rational = _simple_rational(lower_factor, upper_factor, selected)
    rational_preferred = rational is not None
    if rational is not None:
        selected = rational
    log_width = upper - lower
    ambiguous = not math.isfinite(log_width) or log_width > math.log1p(RATIONAL_RELATIVE_WIDTH_LIMIT)
    relative_width = math.expm1(log_width) if math.isfinite(log_width) and log_width < 709 else math.inf
    segment = {
        "start_index": int(start),
        "end_index": int(end),
        "row_count": int(end - start),
        "factor": float(selected),
        "lower": _finite_json(lower_factor),
        "upper": _finite_json(upper_factor),
        "anchor": anchor,
        "anchor_value": float(anchor_value),
        "rational_preferred": rational_preferred,
        "ambiguous": bool(ambiguous),
        "relative_interval_width": _finite_json(relative_width),
        "positive_volume_rows": int(np.count_nonzero(volume_scores > 0)),
    }
    return selected, segment


def infer_split_factors(
    views: Mapping[str, Mapping[str, np.ndarray]],
    *,
    price_tolerance: float = 0.01,
    raw_price_tolerance: float = 1e-6,
    volume_tolerance: float = 1.0,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Infer one feasible split factor per consecutive compatible segment.

    Inputs must describe one security in chronological order, aligned across
    views.  Infer on the full held history *before* filtering requested dates.
    Every emitted factor admits a joint positive D/O solution for every row in
    its segment.  Segments use half-open positions: [start_index, end_index).

    The strongest positive-volume ratio supplies the preferred anchor.  When
    volume is uninformative, 1 is preferred if feasible; otherwise a direct
    split-price ratio supplies the anchor.  Anchors are restricted to the
    segment's feasible interval.  A rational with denominator <= 1,000 may be
    preferred only inside an interval whose relative width is <= 1%.

    ``ambiguous`` identifies intervals wider than that 1% policy (including
    unbounded intervals); even a narrow inferred interval is not an action
    certification.  Unbounded diagnostic endpoints are JSON ``null``.
    """
    model = _build_model(
        views,
        price_tolerance=price_tolerance,
        raw_price_tolerance=raw_price_tolerance,
        volume_tolerance=volume_tolerance,
    )
    n = model.diagnostics["row_count"]
    factors = np.empty(n, dtype=np.float64)
    segments: list[dict[str, Any]] = []
    if n:
        start = 0
        lower, upper = float(model.split_lower[0]), float(model.split_upper[0])
        for index in range(1, n):
            next_lower = max(lower, float(model.split_lower[index]))
            next_upper = min(upper, float(model.split_upper[index]))
            if next_lower > next_upper + NUMERICAL_LOG_TOLERANCE:
                factor, segment = _select_segment_factor(model, start, index, lower, upper)
                factors[start:index] = factor
                segments.append(segment)
                start = index
                lower, upper = float(model.split_lower[index]), float(model.split_upper[index])
            else:
                lower, upper = next_lower, next_upper
        factor, segment = _select_segment_factor(model, start, n, lower, upper)
        factors[start:n] = factor
        segments.append(segment)
        final_feasible = _fixed_split_feasible(model.lower, model.upper, np.log(factors))
        _raise_rows("Selected split factors failed the joint dividend/spin-off recheck", ~final_feasible)

    diagnostics = dict(model.diagnostics)
    diagnostics.update(
        {
            "segments": segments,
            "segment_count": len(segments),
            "segment_indices": "zero-based, start inclusive, end exclusive",
            "segmentation": "greedy intersection of consecutive per-row feasible split intervals",
            "rational_policy": {
                "max_relative_interval_width": RATIONAL_RELATIVE_WIDTH_LIMIT,
                "max_denominator": RATIONAL_MAX_DENOMINATOR,
                "preference": "smallest feasible denominator",
            },
            "ambiguous_segments": sum(segment["ambiguous"] for segment in segments),
            "ambiguous_rows": sum(segment["row_count"] for segment in segments if segment["ambiguous"]),
            "selected_factors_jointly_feasible": True,
        }
    )
    return factors, diagnostics
