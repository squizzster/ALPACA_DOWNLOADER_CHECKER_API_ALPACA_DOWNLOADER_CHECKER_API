"""Public factories for policy-certified bounded correlation rules.

The lineage owner proves a certificate from complete-group evidence. This
module owns the adapters that turn that certificate into correlation rules, so
callers never depend on the correlation package's private context or decision
types.
"""

from dataclasses import dataclass
from datetime import date

from .errors import SecurityCorrelationError
from .ownership_interval_rules import (
    AppliedOwnershipIntervalRule,
    OwnershipIntervalRule,
    TickerOwnershipInterval,
    _merge_exact_intervals,
    _OwnershipIntervalRuleResult,
)
from .row_correlation import RowCorrelationRule, _RowDecision
from .settings import CORRELATED_BRANCH


@dataclass(frozen=True)
class CertifiedActiveSuffix:
    """The correlation-owned portion of a lineage evidence certificate."""

    symbol: str
    asset_id: str
    cusip: str
    start_date_inclusive: date
    end_date_exclusive: date
    supporting_action_ids: tuple[str, ...]
    session_dates: tuple[date, ...]


def _by_symbol(
    certificates: tuple[CertifiedActiveSuffix, ...],
) -> dict[str, CertifiedActiveSuffix]:
    result = {item.symbol: item for item in certificates}
    if len(result) != len(certificates):
        raise SecurityCorrelationError("certified active suffix contains duplicate symbols")
    return result


def certified_active_suffix_ownership_rule(
    certificates: tuple[CertifiedActiveSuffix, ...],
    *,
    rule_name: str,
) -> OwnershipIntervalRule:
    """Build a bounded ownership rule from already-proved certificates."""
    by_symbol = _by_symbol(certificates)

    def apply(context, existing_intervals, rule_order, applied_rule_name):
        intervals = list(existing_intervals)
        for symbol, certificate in sorted(by_symbol.items()):
            if any(item.historical_symbol == symbol for item in existing_intervals):
                continue
            if context.asset_ids_by_symbol_and_cusip.get((symbol, certificate.cusip)) != certificate.asset_id:
                continue
            intervals.append(
                TickerOwnershipInterval(
                    historical_symbol=symbol,
                    cusip=certificate.cusip,
                    asset_id=certificate.asset_id,
                    start_date_inclusive=certificate.start_date_inclusive,
                    end_date_exclusive=certificate.end_date_exclusive,
                    applied_rules=(
                        AppliedOwnershipIntervalRule(
                            order=rule_order,
                            name=applied_rule_name,
                            supporting_action_ids=certificate.supporting_action_ids,
                        ),
                    ),
                )
            )
        return _OwnershipIntervalRuleResult(intervals=_merge_exact_intervals(intervals))

    return OwnershipIntervalRule(name=rule_name, apply=apply)


def certified_active_suffix_row_rule(
    certificates: tuple[CertifiedActiveSuffix, ...],
    *,
    rule_name: str,
    identity_confidence: str,
    warning_code: str,
) -> RowCorrelationRule:
    """Build a per-session assignment rule from already-proved certificates."""
    by_symbol = {
        symbol: (certificate, frozenset(certificate.session_dates))
        for symbol, certificate in _by_symbol(certificates).items()
    }

    def apply(context):
        selected = by_symbol.get(context.requested_symbol)
        if selected is None:
            return None
        certificate, sessions = selected
        if context.session_date not in sessions:
            return None
        return _RowDecision(
            branch=CORRELATED_BRANCH,
            asset_id=certificate.asset_id,
            cusip=certificate.cusip,
            identity_confidence=identity_confidence,
            warning_codes=(warning_code,),
            historical_symbol=certificate.symbol,
            correlation_method=rule_name,
        )

    return RowCorrelationRule(name=rule_name, apply=apply)
