"""Sourced event facts, projected by the metadata producer before correlation.

The submitted document asserts researched facts, not a desired identity result.
Its exact event match prevents those facts from silently surviving a changed
provider action. Completed receipts retain both the original actions and facts;
ordinary reuse neither opens this input file nor revisits its external sources.
"""

import json
from copy import deepcopy
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA_VERSION = 1
MAX_DOCUMENT_BYTES = 1_048_576
MAX_SUPPLEMENTS = 1000


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
        action_id = _text(match.get("id"), "match.id")
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
        if item["kind"] == "split_endpoints":
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
    return deepcopy(items)


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


def project_actions(actions, supplements):
    """Project newly acquired actions without altering raw evidence or event dates."""
    by_id = {item["match"]["id"]: item for item in supplements}
    projected, applied = [], {}
    for action in actions:
        supplement = by_id.get(action.get("id"))
        if supplement is None:
            projected.append(action)
            continue
        if any(key not in action or action[key] != value for key, value in supplement["match"].items()):
            raise ValueError(f"provider action changed under event supplement {action.get('id')}")
        applied[action["id"]] = supplement
        if supplement["kind"] == "split_endpoints":
            endpoints = {key: supplement["facts"][key] for key in ("old_cusip", "new_cusip")}
            if any(key in action and action[key] != value for key, value in endpoints.items()):
                raise ValueError(f"provider split endpoints conflict with supplement {action['id']}")
            projected.append({**action, **endpoints})
        # A derivative-only event or a sourced listing-only transfer remains in
        # the raw union and disposition, but cannot create equity occupancy or
        # dependencies. A transfer alone never proves an identity union.
    return projected, [applied[key] for key in sorted(applied)]
