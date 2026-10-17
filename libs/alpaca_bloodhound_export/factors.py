"""Infer a provider split factor jointly from the eight stored pricing views.

This is a constrained estimate, not a corporate-action ledger.  The declared
rounding allowances and multiplicative view model are construction assumptions;
they do not certify genuine share-unit semantics for Bloodhound admission.
Provider ``all`` prices are never reconstructed or changed by this module.
"""

import math
from decimal import Decimal
from fractions import Fraction

ADJUSTMENTS = (
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)
PRICE_FIELDS = ("open", "high", "low", "close", "vwap")
SPLIT_VIEWS = ("split", "split,dividend", "split,spin-off", "all")
PRICE_ERROR = 0.01
RAW_PRICE_ERROR = 0.000001
VOLUME_ERROR = 1.0
# Only absorb arithmetic roundoff in log-space, never provider disagreement.
LOG_EPSILON = 2e-12
RATIONAL_MAXIMUM_RELATIVE_WIDTH = 0.01
RATIONAL_MAXIMUM_DENOMINATOR = 1000


def _projection_templates():
    """Compile the fixed 28 inequalities once, retaining their operation order."""
    ld, ud, lo, uo, lsd, usd, lso, uso, ldo, udo, la, ua = range(12)
    a, b = (((ld,), 0), ((lsd,), -1)), (((ud,), 0), ((usd,), -1))
    c, d = (((lo,), 0), ((lso,), -1)), (((uo,), 0), ((uso,), -1))
    e, f = (((ldo,), 0), ((la,), -1)), (((udo,), 0), ((ua,), -1))
    pairs = [(left, right) for lefts, rights in ((a, b), (c, d), (e, f)) for left in lefts for right in rights]
    pairs.extend(((left[0] + other[0], left[1] + other[1]), right) for left in a for other in c for right in f)
    pairs.extend((left, (right[0] + other[0], right[1] + other[1])) for left in e for right in b for other in d)
    return tuple(
        (
            left[0][0],
            left[0][1] if len(left[0]) == 2 else -1,
            right[0][0],
            right[0][1] if len(right[0]) == 2 else -1,
            left[1] - right[1],
        )
        for left, right in pairs
    )


_PROJECTION_TEMPLATES = _projection_templates()


class FactorError(ValueError):
    """The stored views cannot support a finite, jointly feasible estimate."""


def _number(value, field, session):
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise FactorError(f"{session}: invalid {field}") from error
    if not math.isfinite(result) or result < 0:
        raise FactorError(f"{session}: nonfinite or negative {field}")
    return result


def _price_interval(raw, adjusted, session):
    lower, upper = 0.0, math.inf
    for field in PRICE_FIELDS:
        original = _number(raw[field], f"raw.{field}", session)
        observed = _number(adjusted[field], field, session)
        lower = max(lower, (observed - PRICE_ERROR) / (original + RAW_PRICE_ERROR))
        denominator = original - RAW_PRICE_ERROR
        if denominator > 0:
            upper = min(upper, (observed + PRICE_ERROR) / denominator)
    if upper <= 0 or lower > upper:
        raise FactorError(f"{session}: incompatible price-factor interval")
    return (math.log(lower) if lower > 0 else -math.inf, math.log(upper))


def _projection(intervals, volume, split_volume, session, *, split_volumes=None):
    """Exact projection onto x=log(S) of the seven-view linear polytope.

    For fixed x, y=log(D) lies in [a,b], z=log(O) in [c,d], and
    y+z in [e,f]. Feasibility is exactly a<=b, c<=d, e<=f,
    a+c<=f, b+d>=e. Expanding max/min yields 28 affine inequalities
    in x. Thus this is not separate marginal intersections or an iterative LP.
    """
    (ls, us), (ld, ud), (lo, uo), (lsd, usd), (lso, uso), (ldo, udo), (la, ua) = intervals
    lower, upper = ls, us
    for name, observed in split_volumes or (("split", split_volume),):
        if volume > 0:
            lower = max(lower, math.log(volume) - math.log(observed + VOLUME_ERROR))
            if observed > VOLUME_ERROR:
                upper = min(upper, math.log(volume) - math.log(observed - VOLUME_ERROR))
        elif observed > VOLUME_ERROR:
            raise FactorError(f"{session}: {name} volume incompatible with zero raw volume")
        if lower > upper + LOG_EPSILON:
            raise FactorError(f"{session}: {name} volume incompatible with joint price-factor bounds")

    values = ld, ud, lo, uo, lsd, usd, lso, uso, ldo, udo, la, ua
    for left_first, left_second, right_first, right_second, slope in _PROJECTION_TEMPLATES:
        left = values[left_first] if left_second < 0 else values[left_first] + values[left_second]
        right = values[right_first] if right_second < 0 else values[right_first] + values[right_second]
        if slope > 0:
            upper = min(upper, (right - left) / slope)
        elif slope < 0:
            lower = max(lower, (right - left) / slope)
        elif left > right + LOG_EPSILON:
            raise FactorError(f"{session}: no joint split/dividend/spin-off factor witness")
    if lower > upper + LOG_EPSILON:
        raise FactorError(f"{session}: no joint split/dividend/spin-off factor witness")
    if lower > upper:
        lower = upper = (lower + upper) / 2
    return lower, upper


def _clamp(preferred, lower, upper):
    return min(max(preferred, lower), upper)


def _witness(intervals, x, preferred_full, session):
    """Select and verify one joint witness; its full factor is not output OHLC."""
    _, (ld, ud), (lo, uo), (lsd, usd), (lso, uso), (ldo, udo), (la, ua) = intervals
    a, b = max(ld, lsd - x), min(ud, usd - x)
    c, d = max(lo, lso - x), min(uo, uso - x)
    lower, upper = max(ldo, la - x, a + c), min(udo, ua - x, b + d)
    total = _clamp(preferred_full - x, lower, upper)
    y = _clamp(0.0, max(a, total - d), min(b, total - c))
    z = total - y
    for value, (low, high) in zip((x, y, z, x + y, x + z, y + z, x + y + z), intervals):
        if not math.isfinite(value) or value < low - LOG_EPSILON or value > high + LOG_EPSILON:
            raise FactorError(f"{session}: selected joint factor witness is invalid")
    return x + y + z, lower + x, upper + x


def _exp_or_none(value):
    if not math.isfinite(value) or value > 700:
        return None
    return math.exp(value)


def _relative_error_or_none(value):
    if not math.isfinite(value) or value > 700:
        return None
    return math.expm1(max(0.0, value))


def _aggregate_log_ratio(numerators, denominators):
    """Log of a ratio of positive sums, without overflowing either sum."""
    top, bottom = max(numerators), max(denominators)
    if top <= 0 or bottom <= 0:
        return None
    return (
        math.log(top)
        + math.log(math.fsum(value / top for value in numerators))
        - math.log(bottom)
        - math.log(math.fsum(value / bottom for value in denominators))
    )


def _rational_candidate(lower, upper):
    """Prefer a uniquely simplest feasible ratio only in a narrow interval.

    Orient S or 1/S to be >=1 so forward/reverse factors are symmetric. At
    the first denominator containing rational candidates, a unique numerator
    is required. The 1% width limit and denominator cap are explicit estimation
    policy, not claims about which corporate actions a provider has applied.
    """
    if (
        not math.isfinite(lower)
        or not math.isfinite(upper)
        or upper - lower > math.log1p(RATIONAL_MAXIMUM_RELATIVE_WIDTH)
    ):
        return None
    if lower <= 0 <= upper:
        return Fraction(1)
    inverted = upper < 0
    low, high = (-upper, -lower) if inverted else (lower, upper)
    if high > 700:
        return None
    # Exact integer arithmetic avoids float ceil/floor errors for large ratios.
    # The only enlargement is the same arithmetic roundoff guard as the solver;
    # every preferred result must still pass the full joint-witness check.
    left = Fraction.from_float(math.exp(low - LOG_EPSILON))
    right = Fraction.from_float(math.exp(high + LOG_EPSILON))
    for denominator in range(1, RATIONAL_MAXIMUM_DENOMINATOR + 1):
        minimum = (left.numerator * denominator + left.denominator - 1) // left.denominator
        maximum = right.numerator * denominator // right.denominator
        if minimum > maximum:
            continue
        if minimum != maximum:
            return None
        ratio = Fraction(minimum, denominator)
        return 1 / ratio if inverted else ratio
    return None


def infer_split_factors(views):
    """Return (factors, report), in the exact ordered ``raw`` session grid.

    ``views`` is StandaloneReader.get_histories(...)["views"] for the COMPLETE
    held history; date filters must be applied afterwards. Adjacent rows with a
    shared feasible S are treated as one constant-S regime. This assumption
    prevents quote/volume rounding from fabricating daily share-unit changes,
    but cannot detect a true small split hidden by the uncertainty intervals.
    No actions or external prices enter; no full-factor/OHLC smoothing occurs.
    The factor is provider-implied, not an independently verified share ledger.
    """
    if set(views) != set(ADJUSTMENTS):
        raise FactorError("all eight adjustment views are required")
    bars = {name: views[name]["bars"] for name in ADJUSTMENTS}
    sessions = [row["session"] for row in bars["raw"]]
    if sessions != sorted(set(sessions)):
        raise FactorError("raw sessions must be unique and increasing")
    for name, rows in bars.items():
        if [row["session"] for row in rows] != sessions:
            raise FactorError(f"{name}: session grid differs from raw")

    factors, prepared, regimes = [], [], []
    counts = {"unchanged": 0, "volume_ratio": 0, "split_close_ratio": 0, "neutral_anchor": 0}
    clipped = direct_full_outside = unbounded = rational_regimes = 0
    maximum_width = 0.0
    maximum_excess = maximum_deficit = 0.0
    for index, session in enumerate(sessions):
        raw, split = bars["raw"][index], bars["split"][index]
        intervals, equal_prices = [], {}
        for name in ADJUSTMENTS[1:]:
            try:
                observed = bars[name][index]
                key = tuple(observed[field] for field in PRICE_FIELDS)
                if key not in equal_prices:
                    equal_prices[key] = _price_interval(raw, observed, session)
                intervals.append(equal_prices[key])
            except FactorError as error:
                raise FactorError(f"{name}: {error}") from error
        volume = _number(raw["volume"], "raw.volume", session)
        observed_volumes = {
            name: _number(bars[name][index]["volume"], f"{name}.volume", session) for name in ADJUSTMENTS
        }
        raw_exact = Decimal(str(raw["volume"]))
        for name in ("dividend", "spin-off", "dividend,spin-off"):
            if Decimal(str(bars[name][index]["volume"])) != raw_exact:
                raise FactorError(f"{session}: {name} volume differs from raw although no split applies")
        distinct_split_volumes = {
            Decimal(str(bars[name][index]["volume"])): observed_volumes[name] for name in SPLIT_VIEWS
        }
        # Repeated equal values are aliases, not independent rounding evidence.
        # The mean of distinct observations is only a preferred point; the
        # feasible interval below retains each view's full absolute allowance.
        split_volume = math.fsum(
            value / len(distinct_split_volumes) for value in distinct_split_volumes.values()
        )
        low, high = _projection(
            intervals,
            volume,
            split_volume,
            session,
            split_volumes=[(name, observed_volumes[name]) for name in SPLIT_VIEWS],
        )
        raw_close = _number(raw["close"], "raw.close", session)
        split_close = _number(split["close"], "split.close", session)
        all_close = _number(bars["all"][index]["close"], "all.close", session)
        if raw_close <= 0:
            raise FactorError(f"{session}: raw close must be positive")

        prepared.append(
            {
                "intervals": intervals,
                "low": low,
                "high": high,
                "volume": volume,
                "split_volume": split_volume,
                "raw_close": raw_close,
                "split_close": split_close,
                "preferred_full": math.log(all_close) - math.log(raw_close) if all_close > 0 else 0.0,
                "unchanged": all(float(raw[field]) == float(split[field]) for field in PRICE_FIELDS)
                and all(abs(volume - observed_volumes[name]) <= VOLUME_ERROR for name in SPLIT_VIEWS),
            }
        )

    # Greedy maximal chronological intersections are the declared model, not
    # discovered corporate-action dates. Never merge disjoint intervals using
    # relative tolerances or force a split-factor change from rounded prices.
    spans = []
    for index, row in enumerate(prepared):
        if not spans:
            spans.append([index, index + 1, row["low"], row["high"]])
            continue
        span = spans[-1]
        low, high = max(span[2], row["low"]), min(span[3], row["high"])
        if low > high + LOG_EPSILON:
            spans.append([index, index + 1, row["low"], row["high"]])
        else:
            if low > high:
                low = high = (low + high) / 2
            span[1:] = [index + 1, low, high]

    for first, end, low, high in spans:
        rows = prepared[first:end]
        anchor = _aggregate_log_ratio([row["volume"] for row in rows], [row["split_volume"] for row in rows])
        if all(row["unchanged"] for row in rows) and low - LOG_EPSILON <= 0 <= high + LOG_EPSILON:
            anchor, kind = 0.0, "unchanged"
        elif anchor is not None:
            kind = "volume_ratio"
        else:
            anchor = _aggregate_log_ratio(
                [row["split_close"] for row in rows], [row["raw_close"] for row in rows]
            )
            if anchor is None:
                anchor, kind = 0.0, "neutral_anchor"
            else:
                kind = "split_close_ratio"
        chosen = _clamp(anchor, low, high)
        counts[kind] += end - first
        anchor_clipped = abs(chosen - anchor) > LOG_EPSILON
        clipped += (end - first) if anchor_clipped else 0
        rational = _rational_candidate(low, high)
        witnesses = None
        if rational is not None:
            preferred = math.log(float(rational))
            try:
                checked = []
                for index in range(first, end):
                    row = prepared[index]
                    checked.append(_witness(row["intervals"], preferred, row["preferred_full"], sessions[index]))
                if not low - LOG_EPSILON <= preferred <= high + LOG_EPSILON:
                    raise FactorError("rational preference exceeds the feasible split interval")
            except FactorError:
                rational = None
            else:
                chosen = preferred
                rational_regimes += 1
                witnesses = checked
        for index in range(first, end):
            row = prepared[index]
            _, full_low, full_high = (
                witnesses[index - first]
                if witnesses is not None
                else _witness(row["intervals"], chosen, row["preferred_full"], sessions[index])
            )
            direct_full_outside += not (
                full_low - LOG_EPSILON <= row["preferred_full"] <= full_high + LOG_EPSILON
            )
        try:
            factor = float(rational) if rational is not None else math.exp(chosen)
        except OverflowError as error:
            raise FactorError(f"{sessions[first]}: split factor overflows float64") from error
        if not math.isfinite(factor) or factor <= 0:
            raise FactorError(f"{sessions[first]}: split factor is not finite and positive")
        factors.extend([factor] * (end - first))
        excess, deficit = max(0.0, chosen - low), max(0.0, high - chosen)
        maximum_excess, maximum_deficit = max(maximum_excess, excess), max(maximum_deficit, deficit)
        selected_error = _relative_error_or_none(excess)
        if selected_error is not None:
            selected_error = max(selected_error, -math.expm1(-deficit))
        regimes.append(
            {
                "from": sessions[first],
                "to": sessions[end - 1],
                "sessions": end - first,
                "split_factor": factor,
                "feasible_lower": _exp_or_none(low),
                "feasible_upper": _exp_or_none(high),
                "anchor": kind,
                "anchor_clipped": anchor_clipped,
                "price_factor_exact_rational": str(rational) if rational is not None else None,
                "implied_share_multiplier_exact_rational": str(1 / rational) if rational is not None else None,
                "rational_denotes_accumulated_factor_not_a_single_corporate_action": True,
                "maximum_selected_factor_relative_error_bound": selected_error,
            }
        )
        width = high - low
        if not math.isfinite(width) or width > 700:
            unbounded += end - first
        else:
            maximum_width = max(maximum_width, math.expm1(max(0.0, width)))
    maximum_error = _relative_error_or_none(maximum_excess)
    if maximum_error is not None:
        maximum_error = max(maximum_error, -math.expm1(-maximum_deficit))
    return factors, {
        "method": "eight_view_joint_log_factor_constant_split_regimes_volume_anchor",
        "semantics": "provider_implied_split_factor_not_independent_share_action_certification",
        "assumptions": {
            "multiplicative_split_dividend_spin_off_view_model": True,
            "adjusted_price_absolute_error": PRICE_ERROR,
            "raw_price_absolute_error": RAW_PRICE_ERROR,
            "split_volume_absolute_error": VOLUME_ERROR,
            "raw_volume_treated_as_observed_exact": True,
            "non_split_view_volumes_equal_raw_exactly": True,
            "all_four_split_containing_volume_views_constrain_same_factor": True,
            "identical_split_volume_values_do_not_count_as_independent_evidence": True,
            "consecutive_jointly_compatible_split_intervals_share_one_constant_factor": True,
            "changes_hidden_within_rounding_intervals_are_not_identifiable": True,
            "rational_preference_maximum_relative_interval_width": RATIONAL_MAXIMUM_RELATIVE_WIDTH,
            "rational_preference_maximum_denominator_after_reciprocal_orientation": RATIONAL_MAXIMUM_DENOMINATOR,
            "rational_preference_requires_unique_candidate_at_smallest_denominator": True,
        },
        "sessions": len(factors),
        "anchors": counts,
        "anchors_clipped_to_joint_feasible_interval": clipped,
        "direct_all_close_ratio_outside_selected_split_joint_interval": direct_full_outside,
        "maximum_split_interval_relative_width": None if unbounded else maximum_width,
        "unbounded_or_float64_unrepresentable_width_sessions": unbounded,
        "maximum_selected_factor_relative_error_bound": maximum_error,
        "maximum_cross_regime_share_ratio_relative_error_bound": _relative_error_or_none(
            maximum_excess + maximum_deficit
        ),
        "uncertainty_bounds_are_conditional_on_declared_model": True,
        "regime_count": len(regimes),
        "rational_preference_regime_count": rational_regimes,
        "regimes": regimes,
        "all_ohlc_preserved_without_reconstruction": True,
        "split_factor_constant_regime_pooling": True,
        "all_ohlc_temporal_pooling_or_smoothing": False,
        "full_history_required_before_date_filter": True,
    }
