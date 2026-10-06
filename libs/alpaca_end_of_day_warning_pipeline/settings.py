from dataclasses import asdict, dataclass

MANIFEST_VERSION = 1
PRICE_FIELDS = ("open", "high", "low", "close", "vwap")
ADJUSTMENT_ACTIONS = frozenset(("split", "dividend", "spin-off"))


@dataclass(frozen=True)
class WarningPipelineConfiguration:
    adjusted_price_error: float = 0.01
    raw_price_error: float = 0.000001
    split_volume_error: float = 1.0
    factor_step_min_relative: float = 0.001
    factor_spike_min_relative: float = 0.01
    activity_lookback_sessions: int = 60
    activity_minimum_history_sessions: int = 20
    activity_minimum_baseline_trades: float = 1000.0
    activity_trade_count_collapse_ratio: float = 0.02
    activity_robust_z_threshold: float = 6.0
    activity_log_mad_floor: float = 0.15
    activity_volume_collapse_ratio: float = 0.05
    activity_mean_trade_size_multiple: float = 20.0
    isolated_price_log_jump: float = 0.50
    neighboring_price_log_distance: float = 0.05
    unsupported_wick_multiple: float = 4.0
    repeated_bar_minimum_trades: int = 1000
    repeated_bar_minimum_volume: float = 100000.0
    maximum_findings: int = 2000
    maximum_evidence_examples: int = 8

    def as_report_object(self) -> dict:
        return asdict(self)


DEFAULT_CONFIGURATION = WarningPipelineConfiguration()


def adjustment_actions(adjustment: object) -> frozenset[str] | None:
    if not isinstance(adjustment, str):
        return None
    if adjustment == "raw":
        return frozenset()
    if adjustment == "all":
        return ADJUSTMENT_ACTIONS
    actions = adjustment.split(",")
    if actions and len(actions) == len(set(actions)) and all(action in ADJUSTMENT_ACTIONS for action in actions):
        return frozenset(actions)
    return None


def adjustment_label(actions: frozenset[str]) -> str:
    return ",".join(action for action in ("split", "dividend", "spin-off") if action in actions) or "raw"
