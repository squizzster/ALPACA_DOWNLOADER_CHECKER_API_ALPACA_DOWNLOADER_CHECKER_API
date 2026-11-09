"""Sourced event facts, projected by the metadata producer before correlation.

The submitted document asserts researched facts, not a desired identity result.
Its exact event match prevents those facts from silently surviving a changed
provider action. Completed receipts retain both the original actions and facts;
ordinary reuse neither opens this input file nor revisits its external sources.
"""

import json
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA_VERSION = 1
SUCCESSION_SCHEMA_VERSION = 1
SUCCESSION_KIND = "holding_company_succession"
RENAME_KIND = "identity_preserving_rename"
RENAME_SCHEMA_VERSION = 1
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
    """Carry one completed ingress decision through CLI, pipeline and metadata.

    The accepted container owns frozen copies of every input value. Consumers
    keep this handoff; changing a raw input instead requires a new acceptance.
    Retained receipts have their separate producer-owned ingress and never use
    this validator merely because a new consumer reads the completed facts.
    """
    return items if isinstance(items, _AcceptedSupplements) else validate_supplements(list(items))


def supplement_key(item):
    """Keep independent sourced facts separate from provider event identifiers."""
    if item["kind"] == SUCCESSION_KIND:
        return "sourced:" + item["match"]["source_id"]
    return item["match"]["id"]


def succession_items(evidence):
    """Consume the metadata producer's accepted, separately versioned facts."""
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
    """Validate newly submitted facts once, before selecting or publishing work."""
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_SUPPLEMENTS:
        raise ValueError("event supplements must be a nonempty bounded array")
    seen = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {"kind", "match", "facts", "sources"}:
            raise ValueError("event supplement requires kind, match, facts and sources")
        match, facts, sources = item["match"], item["facts"], item["sources"]
        if not isinstance(match, dict) or not isinstance(facts, dict):
            raise ValueError("event supplement match/facts must be objects")
        identifier_field = "source_id" if item["kind"] == SUCCESSION_KIND else "id"
        _text(match.get(identifier_field), "match." + identifier_field)
        action_id = supplement_key(item)
        if action_id in seen:
            raise ValueError(f"duplicate event supplement: {action_id}")
        seen.add(action_id)
        if not isinstance(sources, list) or not sources:
            raise ValueError("event supplement requires source provenance")
        for source in sources:
            if not isinstance(source, dict) or set(source) != {"url", "title", "authority", "document_date"}:
                raise ValueError("event source requires url, title, authority and document_date")
            url = urlsplit(_text(source["url"], "source.url"))
            if url.scheme != "https" or not url.netloc or url.username or url.password:
                raise ValueError("event source must be an HTTPS document URL")
            _text(source["title"], "source.title")
            _date(source["document_date"], "source.document_date")
            if source["authority"] not in {"issuer", "exchange", "clearinghouse", "regulator"}:
                raise ValueError("unsupported event source authority")
        if item["kind"] == SUCCESSION_KIND:
            if set(match) != {"source_id", "symbol"}:
                raise ValueError("succession must name its independent source_id and unchanged symbol")
            _text(match["symbol"], "match.symbol")
            required = {
                "schema_version",
                "fact_kind",
                "old_cusip",
                "new_cusip",
                "legal_effective_date",
                "exchange_effective_date",
                "old_rate",
                "new_rate",
                "share_class",
                "automatic_exchange",
                "unchanged_rights",
            }
            if set(facts) != required:
                raise ValueError("succession requires complete versioned common-share substitution facts")
            if type(facts["schema_version"]) is not int or facts["schema_version"] != SUCCESSION_SCHEMA_VERSION:
                raise ValueError("unsupported sourced succession fact schema")
            if facts["fact_kind"] != SUCCESSION_KIND or facts["share_class"] != "common_stock":
                raise ValueError("succession requires a holding-company common-stock substitution")
            if facts["automatic_exchange"] is not True or facts["unchanged_rights"] is not True:
                raise ValueError("succession requires automatic exchange with unchanged rights")
            if _positive(facts["old_rate"]) != 1 or _positive(facts["new_rate"]) != 1:
                raise ValueError("succession requires an exact one-for-one share exchange")
            for key in ("old_cusip", "new_cusip"):
                value = _text(facts[key], "facts." + key)
                if len(value) != 9 or not value.isascii() or not value.isalnum():
                    raise ValueError("succession endpoint must be a nine-character CUSIP")
            if facts["old_cusip"] == facts["new_cusip"]:
                raise ValueError("succession endpoints must differ")
            legal = _date(facts["legal_effective_date"], "facts.legal_effective_date")
            exchange = _date(facts["exchange_effective_date"], "facts.exchange_effective_date")
            if legal > exchange:
                raise ValueError("succession exchange date cannot precede legal effectiveness")
        elif item["kind"] == RENAME_KIND:
            required = {
                "id",
                "action_type",
                "old_symbol",
                "new_symbol",
                "old_cusip",
                "new_cusip",
                "process_date",
            }
            if set(match) != required or match["action_type"] != "name_change":
                raise ValueError("equity rename must match the complete recorded provider event")
            if set(facts) != {
                "schema_version",
                "share_class",
                "unchanged_security",
                "unchanged_rights",
                "no_share_exchange",
                "legal_effective_date",
                "exchange_effective_date",
                "prior_labels",
            }:
                raise ValueError("equity rename requires complete security and dated-label facts")
            if type(facts["schema_version"]) is not int or facts["schema_version"] != RENAME_SCHEMA_VERSION:
                raise ValueError("unsupported sourced equity rename schema")
            if facts["share_class"] != "common_stock" or any(
                facts[key] is not True for key in ("unchanged_security", "unchanged_rights", "no_share_exchange")
            ):
                raise ValueError("equity rename must preserve the same common security without conversion")
            for key in ("old_symbol", "new_symbol"):
                _text(match[key], "match." + key)
            if match["old_symbol"] == match["new_symbol"]:
                raise ValueError("equity rename requires distinct dated symbols")
            for key in ("old_cusip", "new_cusip"):
                value = _text(match[key], "match." + key)
                if len(value) != 9 or not value.isascii() or not value.isalnum():
                    raise ValueError("equity rename endpoint must be a nine-character CUSIP")
            boundary = _date(facts["exchange_effective_date"], "facts.exchange_effective_date")
            if (
                _date(match["process_date"], "match.process_date") != boundary
                or _date(facts["legal_effective_date"], "facts.legal_effective_date") > boundary
            ):
                raise ValueError("equity rename dates disagree with the provider event")
            labels = facts["prior_labels"]
            if not isinstance(labels, list) or not labels or len(labels) > 100:
                raise ValueError("equity rename requires bounded prior-label chronology")
            previous, seen_labels = None, set()
            for label in labels:
                if not isinstance(label, dict) or set(label) != {
                    "symbol",
                    "cusip",
                    "start_inclusive",
                    "end_exclusive",
                }:
                    raise ValueError("prior label requires exact symbol, CUSIP and bounds")
                name = _text(label["symbol"], "prior_label.symbol")
                first, last = (
                    _date(label[key], "prior_label." + key) for key in ("start_inclusive", "end_exclusive")
                )
                if (
                    label["cusip"] != match["old_cusip"]
                    or first >= last
                    or previous is not None
                    and first != previous
                    or name in seen_labels
                    or name == match["new_symbol"]
                ):
                    raise ValueError("prior labels must be contiguous distinct labels of the same old CUSIP")
                seen_labels.add(name)
                previous = last
            if labels[-1]["symbol"] != match["old_symbol"] or previous != boundary:
                raise ValueError("prior labels must end at the matched predecessor departure")
        elif item["kind"] == "split_endpoints":
            required = {"id", "action_type", "symbol", "cusip", "ex_date", "old_rate", "new_rate"}
            if not required <= match.keys() or set(match) - required - {"process_date"}:
                raise ValueError("split supplement must match the complete split identity, date and ratios")
            if match["action_type"] not in {"forward_split", "reverse_split"}:
                raise ValueError("split supplement requires an ordinary forward/reverse split")
            if set(facts) != {"old_cusip", "new_cusip", "effective_date"}:
                raise ValueError("split facts require old_cusip, new_cusip and effective_date")
            for key in ("symbol", "cusip"):
                _text(match[key], "match." + key)
            for key in ("old_cusip", "new_cusip"):
                value = _text(facts[key], "facts." + key)
                if len(value) != 9 or not value.isascii() or not value.isalnum():
                    raise ValueError("split endpoint must be a nine-character CUSIP")
            if facts["old_cusip"] == facts["new_cusip"] or match["cusip"] not in (
                facts["old_cusip"],
                facts["new_cusip"],
            ):
                raise ValueError("split endpoints must differ and include the provider CUSIP")
            if _date(match["ex_date"], "match.ex_date") != _date(
                facts["effective_date"], "facts.effective_date"
            ):
                raise ValueError("split supplement cannot rewrite the provider boundary")
            if "process_date" in match:
                _date(match["process_date"], "match.process_date")
            old, new = _positive(match["old_rate"]), _positive(match["new_rate"])
            if new <= old if match["action_type"] == "forward_split" else old <= new:
                raise ValueError("split ratios disagree with the provider split type")
        elif item["kind"] == "listing_transfer_only":
            required = {
                "id",
                "action_type",
                "old_symbol",
                "new_symbol",
                "old_cusip",
                "new_cusip",
                "process_date",
            }
            if set(match) != required or match["action_type"] != "name_change":
                raise ValueError("listing scope must match the complete recorded name-change event")
            if set(facts) != {
                "instrument_type",
                "unchanged_symbol",
                "effective_date",
                "from_exchange",
                "to_exchange",
            }:
                raise ValueError("listing scope requires instrument, unchanged symbol, date and exchanges")
            for key in ("old_symbol", "new_symbol", "old_cusip", "new_cusip"):
                _text(match[key], "match." + key)
            if (
                facts["instrument_type"] != "equity"
                or facts["unchanged_symbol"] != match["new_symbol"]
                or match["old_cusip"] != match["new_cusip"]
            ):
                raise ValueError("listing transfer must preserve the equity symbol and CUSIP")
            if _text(facts["from_exchange"], "facts.from_exchange") == _text(
                facts["to_exchange"], "facts.to_exchange"
            ):
                raise ValueError("listing transfer requires distinct exchanges")
            if _date(match["process_date"], "match.process_date") != _date(
                facts["effective_date"], "facts.effective_date"
            ):
                raise ValueError("listing scope does not match the provider event date")
        elif item["kind"] == "derivative_only":
            required = {
                "id",
                "action_type",
                "old_symbol",
                "new_symbol",
                "old_cusip",
                "new_cusip",
                "process_date",
            }
            if set(match) != required or match["action_type"] != "name_change":
                raise ValueError("derivative scope must match the complete root-consolidation event")
            if set(facts) != {"instrument_type", "underlying_cusip", "effective_date", "series"}:
                raise ValueError("derivative scope requires instrument, underlying, date and affected series")
            if facts["instrument_type"] != "option" or not (
                facts["underlying_cusip"] == match["old_cusip"] == match["new_cusip"]
            ):
                raise ValueError("option-root consolidation must preserve its underlying CUSIP")
            for key in ("old_symbol", "new_symbol", "old_cusip", "new_cusip"):
                _text(match[key], "match." + key)
            if _date(match["process_date"], "match.process_date") != _date(
                facts["effective_date"], "facts.effective_date"
            ):
                raise ValueError("derivative scope does not match the provider event date")
            if not isinstance(facts["series"], list) or not facts["series"]:
                raise ValueError("derivative scope requires affected option series")
            for series in facts["series"]:
                if not isinstance(series, dict) or set(series) != {"type", "expiration", "strike"}:
                    raise ValueError("option series requires type, expiration and strike")
                if series["type"] not in {"put", "call"}:
                    raise ValueError("invalid option type")
                _date(series["expiration"], "series.expiration")
                _positive(series["strike"])
        else:
            raise ValueError("unsupported event supplement kind")
    return _AcceptedSupplements(_freeze(item) for item in items)


def load_supplements(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_DOCUMENT_BYTES + 1)
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ValueError("event supplement document exceeds its size limit")
    document = json.loads(raw)
    if not isinstance(document, dict) or set(document) != {"schema_version", "supplements"}:
        raise ValueError("invalid event supplement document")
    if type(document["schema_version"]) is not int or document["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported event supplement schema")
    return validate_supplements(document["supplements"])


def provider_actions(evidence):
    """Read the accepted raw union, not its supplemented equity projection."""
    return evidence.get("provider_corporate_actions", evidence.get("corporate_actions", ()))


def _project_action(action, supplement):
    """Validate one newly observed provider-action/fact pair at its producer."""
    if any(key not in action or action[key] != value for key, value in supplement["match"].items()):
        raise ValueError(f"provider action changed under event supplement {action.get('id')}")
    if supplement["kind"] == RENAME_KIND:
        if any(
            value != supplement["facts"]["exchange_effective_date"]
            for key, value in action.items()
            if key in {"process_date", "effective_date", "ex_date"} and value
        ):
            raise ValueError(f"provider rename dates conflict with supplement {action['id']}")
        return action
    if supplement["kind"] == "split_endpoints":
        endpoints = {key: supplement["facts"][key] for key in ("old_cusip", "new_cusip")}
        if any(key in action and action[key] != value for key, value in endpoints.items()):
            raise ValueError(f"provider split endpoints conflict with supplement {action['id']}")
        return {**action, **endpoints}
    # Derivative/listing-only facts retain a completed negative equity projection.
    return None


class EventProjection:
    """Carry completed projections across traversal, peer reports and retries.

    Bind actual immutable observations, never an event ID alone. Strong input
    references prevent object-ID reuse. Fresh provider observations or facts
    therefore select one fresh validation; accepted retained pairs select none.
    """

    def __init__(self):
        self.accepted = {}

    def retain(self, action, supplement, projected):
        self.accepted[id(action), id(supplement)] = (action, supplement, projected)

    def bind(self, supplements):
        by_id = {item["match"]["id"]: item for item in supplements if item["kind"] != SUCCESSION_KIND}

        def project(actions):
            projected, applied = [], {}
            for action in actions:
                supplement = by_id.get(action.get("id"))
                if supplement is None:
                    projected.append(action)
                    continue
                key = (id(action), id(supplement))
                accepted = self.accepted.get(key)
                if accepted is None:
                    result = _project_action(action, supplement)
                    self.retain(action, supplement, result)
                else:
                    result = accepted[2]
                applied[action["id"]] = supplement
                if result is not None:
                    projected.append(result)
            return projected, [applied[key] for key in sorted(applied)]

        return project


def project_actions(actions, supplements):
    """Standalone projection; traversal owners retain their bound projector."""
    return EventProjection().bind(supplements)(actions)
