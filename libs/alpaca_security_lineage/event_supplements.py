"""Retired research-assertion ingress and inert legacy provenance helpers.

Completed receipts and journals may retain their historical serialized values.
These readers never authorize those values as assertions for new inference.
"""

from datetime import date
from decimal import Decimal, InvalidOperation

from .sourced_facts import EXTENSIONS

SCHEMA_VERSION = 1
SUCCESSION_SCHEMA_VERSION = 1
SUCCESSION_KIND = "holding_company_succession"
RENAME_KIND = "identity_preserving_rename"
RENAME_SCHEMA_VERSION = 1
RECAPITALIZATION_KIND = "issuer_capital_repayment"
MAX_DOCUMENT_BYTES = 1_048_576
MAX_SUPPLEMENTS = 1000


class _FrozenList(list):
    __slots__ = ()

    def _immutable(self, *_args, **_kwargs):
        raise TypeError("accepted event facts are immutable")

    __setitem__ = __delitem__ = __iadd__ = __imul__ = _immutable
    append = clear = extend = insert = pop = remove = reverse = sort = _immutable

    def __copy__(self):
        return self

    def __deepcopy__(self, _memo):
        return self


class _FrozenDict(dict):
    __slots__ = ()

    def _immutable(self, *_args, **_kwargs):
        raise TypeError("accepted event facts are immutable")

    __setitem__ = __delitem__ = __ior__ = _immutable
    clear = pop = popitem = setdefault = update = _immutable

    def __copy__(self):
        return self

    def __deepcopy__(self, _memo):
        return self


class _AcceptedSupplements(_FrozenList):
    __slots__ = ()


def _freeze(value):
    if isinstance(value, dict):
        return _FrozenDict((key, _freeze(item)) for key, item in value.items())
    if isinstance(value, list):
        return _FrozenList(_freeze(item) for item in value)
    return value


def accept_supplements(items):
    """Reject every nonempty fresh input, including old accepted containers."""
    if items:
        raise ValueError("research event supplements are inadmissible in runtime inference")
    return items if isinstance(items, _AcceptedSupplements) else _AcceptedSupplements()


def supplement_key(item):
    """Keep independent sourced facts separate from provider event identifiers."""
    if item["kind"] in {SUCCESSION_KIND, *EXTENSIONS}:
        return "sourced:" + item["match"]["source_id"]
    return item["match"]["id"]


def succession_items(evidence):
    """Inspect inert fact provenance from a legacy closed receipt."""
    return evidence.get("sourced_successions", {}).get("facts", ())


def _text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"event supplement requires {field}")
    return value


def _date(value, field):
    value = _text(value, field)
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError(f"invalid event supplement {field}")
    return value


def _positive(value):
    if isinstance(value, bool):
        raise ValueError("event supplement requires a positive numeric ratio/strike")
    try:
        number = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError("invalid event supplement number") from error
    if not number.is_finite() or number <= 0:
        raise ValueError("event supplement requires a positive finite ratio/strike")
    return number


def validate_supplements(items):
    """The retired input cannot gain authority through schema validation."""
    return accept_supplements(items)


def load_supplements(path):
    """Reject the retired file input before opening or inspecting its path."""
    raise ValueError("research event supplements are inadmissible in runtime inference")


def provider_actions(evidence):
    """Read the accepted raw union, not its supplemented equity projection."""
    return evidence.get("provider_corporate_actions", evidence.get("corporate_actions", ()))


def _validate_recapitalization(match, facts):
    required = {
        "schema_version",
        "issuer_cik",
        "symbol",
        "share_class",
        "same_issuer",
        "same_public_common",
        "cash_purpose",
        "old_cusip",
        "new_cusip",
        "old_units",
        "new_units",
        "capital_repayment_usd",
        "legal_effective_date",
        "exchange_effective_date",
        "predecessor_history_start",
    }
    if set(facts) != required or type(facts["schema_version"]) is not int or facts["schema_version"] != 1:
        raise ValueError("capital repayment requires its complete common-security fact schema")
    if (
        facts["share_class"] != "common_stock"
        or facts["cash_purpose"] != "capital_repayment"
        or facts["same_issuer"] is not True
        or facts["same_public_common"] is not True
    ):
        raise ValueError("capital repayment must preserve the issuer's own public common security")
    cik = _text(facts["issuer_cik"], "issuer_cik")
    if not cik.isascii() or not cik.isdecimal() or str(int(cik)) != cik or int(cik) <= 0:
        raise ValueError("capital repayment requires the exact continuing registrant CIK")
    symbol = _text(facts["symbol"], "symbol")
    for field in ("old_cusip", "new_cusip"):
        value = _text(facts[field], field)
        if len(value) != 9 or not value.isascii() or not value.isalnum():
            raise ValueError("capital repayment requires exact dated CUSIPs")
    if (
        facts["old_cusip"] == facts["new_cusip"]
        or not all(isinstance(facts[key], str) for key in ("old_units", "new_units", "capital_repayment_usd"))
        or _positive(facts["old_units"]) <= _positive(facts["new_units"])
    ):
        raise ValueError("capital repayment requires a documented common-share consolidation")
    cash = _positive(facts["capital_repayment_usd"])
    legal, market = (_date(facts[key], key) for key in ("legal_effective_date", "exchange_effective_date"))
    if (
        legal > market
        or facts["predecessor_history_start"] is not None
        and _date(facts["predecessor_history_start"], "predecessor_history_start") >= market
    ):
        raise ValueError("capital repayment chronology is inconsistent")
    kind = match.get("action_type")
    if kind == "cash_dividend":
        required_match = {"id", "action_type", "symbol", "ex_date", "rate"}
        if (
            not required_match <= match.keys()
            or not match.keys() <= required_match | {"cusip"}
            or match["symbol"] != symbol
            or match["ex_date"] != market
            or _positive(match["rate"]) != cash
        ):
            raise ValueError("capital repayment must bind its exact recorded cash observation")
    elif kind == "stock_and_cash_merger":
        required_match = {
            "id",
            "action_type",
            "acquiree_symbol",
            "acquirer_symbol",
            "acquiree_cusip",
            "acquirer_cusip",
            "acquiree_rate",
            "acquirer_rate",
            "cash_rate",
            "effective_date",
        }
        if (
            not required_match <= match.keys()
            or not match.keys() <= required_match | {"payable_date", "process_date"}
            or match["acquiree_symbol"] != match["acquirer_symbol"]
            or match["acquirer_symbol"] != symbol
            or match["acquirer_cusip"] != facts["new_cusip"]
            or match["effective_date"] != market
            or _positive(match["cash_rate"]) != cash
            or _positive(match["acquirer_rate"]) * _positive(facts["old_units"])
            != _positive(match["acquiree_rate"]) * _positive(facts["new_units"])
        ):
            raise ValueError("capital repayment must bind the recorded same-symbol stock/cash terms")
    else:
        raise ValueError("unsupported provider classification for a common-share capital repayment")


def _project_action(action, supplement):
    """A legacy projection recipe is not executable fresh evidence."""
    raise ValueError("research event supplements are inadmissible in runtime inference")


class EventProjection:
    """Keep legacy completed records inert; never apply them to new actions."""

    def __init__(self):
        self.accepted = {}

    def retain(self, action, supplement, projected):
        self.accepted[id(action), id(supplement)] = (action, supplement, projected)

    def bind(self, supplements):
        accept_supplements(supplements)

        def project(actions):
            return list(actions), []

        return project


def project_actions(actions, supplements):
    """Pass through provider actions only when no authored assertion is supplied."""
    return EventProjection().bind(supplements)(actions)
