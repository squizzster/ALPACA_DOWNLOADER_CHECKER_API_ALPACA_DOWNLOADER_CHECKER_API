"""Versioned public-holder and distinct-conversion facts at metadata ingress.

These are accepted assertions with document provenance, never a web scraper or
an instruction to join equal-looking prices. Only the public common kind can
contribute membership. External conversion endpoints may remain unresolved.
"""

PUBLIC_COMMON_KIND = "public_common_continuation"
CONVERSION_KIND = "distinct_security_conversion"
MARKET_KIND = "dated_market_occupancy"
EXTENSIONS = {
    PUBLIC_COMMON_KIND: "sourced_public_common_continuations",
    CONVERSION_KIND: "sourced_security_relations",
    MARKET_KIND: "sourced_market_occupancy",
}


def canonical_extension(value):
    """Project the known retained envelope spelling without replaying facts.

    Selected prepared publications used ``version`` for this internal envelope.
    Metadata ingress owns its schema check; consumers retain the same accepted
    fact objects and completed interpretations under the canonical spelling.
    """
    if isinstance(value, dict) and set(value) == {"version", "facts"}:
        return {"schema_version": value["version"], "facts": value["facts"]}
    return value


def items(evidence, kind=None):
    return tuple(
        item
        for name, extension in EXTENSIONS.items()
        if kind is None or name == kind
        for item in evidence.get(extension, {}).get("facts", ())
    )


def selected(items, *, symbols=(), cusips=()):
    symbols, cusips = set(symbols), set(cusips)
    return [
        item
        for item in items
        if item["kind"] in EXTENSIONS
        and any(
            endpoint["symbol"] in symbols or endpoint["cusip"] in cusips
            for endpoint in (item["facts"]["predecessor"], item["facts"]["successor"])
        )
    ]


def validate(item):
    from .event_supplements import _date, _positive, _text

    facts = item["facts"]
    if item["kind"] == MARKET_KIND:
        return _validate_market(item)
    if (
        set(item["match"]) != {"source_id"}
        or type(facts.get("schema_version")) is not int
        or facts["schema_version"] not in (1, 2)
    ):
        raise ValueError("sourced security fact requires source_id and a supported schema version")
    common = item["kind"] == PUBLIC_COMMON_KIND
    new_parent = common and facts["schema_version"] == 2
    base = {"schema_version", "predecessor", "successor", "legal_effective_date", "provider_event_match"}
    required = base | (
        {
            "transaction_kind",
            "exchange_effective_date",
            "continuing_registrant",
            "holder_scope",
            "public_common_participates_in_successor",
            "exchange_documents_common_transition",
            "public_share_treatment",
        }
        if common
        else {
            "first_successor_trading",
            "trading_boundary",
            "consideration_status",
            "consideration",
            "acquirer_security_continues",
            "ownership_effective_dates",
            "settlement_dates",
        }
    )
    if new_parent:
        required.add("listing_succession")
    if set(facts) != required:
        raise ValueError("sourced security fact requires its complete versioned fields")
    for role in ("predecessor", "successor"):
        endpoint = facts[role]
        if not isinstance(endpoint, dict) or set(endpoint) != {"symbol", "cusip", "instrument_kind"} | (
            {"registrant_cik"} if common else set()
        ):
            raise ValueError("sourced security endpoint requires exact instrument, label and CUSIP")
        _text(endpoint["symbol"], role + ".symbol")
        cusip = _text(endpoint["cusip"], role + ".cusip")
        if len(cusip) != 9 or not cusip.isascii() or not cusip.isalnum():
            raise ValueError("sourced security endpoint requires a nine-character CUSIP")
        if common:
            cik = endpoint["registrant_cik"]
            if (
                endpoint["instrument_kind"] != "public_common_stock"
                or not isinstance(cik, str)
                or not cik.isascii()
                or not cik.isdecimal()
                or str(int(cik)) != cik
                or int(cik) <= 0
            ):
                raise ValueError("public continuation requires public common and canonical registrant CIK")
        elif endpoint["instrument_kind"] not in {
            "ADS",
            "common_stock",
            "public_common_stock",
            "preferred_stock",
            "unit",
            "warrant",
            "right",
        }:
            raise ValueError("unsupported conversion endpoint instrument")
    if facts["predecessor"]["cusip"] == facts["successor"]["cusip"]:
        raise ValueError("sourced security endpoints must differ")
    legal = _date(facts["legal_effective_date"], "legal_effective_date")
    match = facts["provider_event_match"]
    if match is not None:
        if not isinstance(match, dict) or not {"id", "action_type"} <= match.keys():
            raise ValueError("provider event match requires a real identifier and type")
        for key in ("id", "action_type"):
            _text(match[key], "provider_event_match." + key)
        # Exact match keys describe provider fields; ownership-effective date
        # semantics remain independently explicit below (payable != ownership).
    if common:
        if (
            facts["transaction_kind"] != "business_combination"
            or facts["holder_scope"] != "non_redeemed_public_common"
            or type(facts["continuing_registrant"]) is not bool
            or facts["continuing_registrant"] is new_parent
            or any(
                facts[key] is not True
                for key in (
                    "public_common_participates_in_successor",
                    "exchange_documents_common_transition",
                )
            )
        ):
            raise ValueError("public continuation requires positive non-redeemed public-holder treatment")
        if not new_parent and facts["predecessor"]["registrant_cik"] != facts["successor"]["registrant_cik"]:
            raise ValueError("public continuation requires the continuing registrant")
        if new_parent and (
            facts["predecessor"]["registrant_cik"] == facts["successor"]["registrant_cik"]
            or not isinstance(facts["listing_succession"], dict)
            or type(facts["listing_succession"].get("publicly_traded_predecessor_count")) is not int
            or any(
                facts["listing_succession"].get(key) is not True
                for key in ("successor_common_is_listed", "operating_target_was_private")
            )
            or facts["listing_succession"]
            != {
                "kind": "new_public_parent",
                "publicly_traded_predecessor_count": 1,
                "successor_common_is_listed": True,
                "operating_target_was_private": True,
            }
        ):
            raise ValueError("public parent succession requires one documented public-common listing path")
        if _date(facts["exchange_effective_date"], "exchange_effective_date") < legal:
            raise ValueError("public trading transition precedes legal effectiveness")
        treatment = facts["public_share_treatment"]
        if not isinstance(treatment, dict) or set(treatment) != {
            "kind",
            "predecessor_units",
            "successor_units",
            "unit_basis",
            "express_exchange_clause",
            "rights_unchanged",
        }:
            raise ValueError("public continuation requires separately evidenced public share units")
        if treatment["kind"] not in (
            {"exchanged_one_for_one"} if new_parent else {"continued_unchanged", "continued_reclassified"}
        ) or any(
            not isinstance(treatment[key], str) or _positive(treatment[key]) != 1
            for key in ("predecessor_units", "successor_units")
        ):
            raise ValueError("public continuation v1 is inapplicable without exact one-for-one public units")
        _text(treatment["unit_basis"], "unit_basis")
        if any(
            treatment[key] is not None and type(treatment[key]) is not bool
            for key in ("express_exchange_clause", "rights_unchanged")
        ):
            raise ValueError("rights and express-clause assertions must be nullable booleans")
    else:
        if type(facts["acquirer_security_continues"]) is not bool:
            raise ValueError("conversion requires an explicit continuing-acquirer assertion")
        for key in ("ownership_effective_dates", "settlement_dates"):
            if not isinstance(facts[key], list):
                raise ValueError("conversion chronology must be a date list")
            for value in facts[key]:
                _date(value, key)
        trading = facts["first_successor_trading"]
        if trading is not None:
            if (
                not isinstance(trading, dict)
                or set(trading) != {"symbol", "date"}
                or _date(trading["date"], "first_successor_trading.date") < legal
            ):
                raise ValueError("invalid first successor trading scope")
            _text(trading["symbol"], "first_successor_trading.symbol")
        boundary = facts["trading_boundary"]
        if (
            not isinstance(boundary, dict)
            or set(boundary) != {"status", "preferred_last_session", "alternative_last_sessions"}
            or boundary["status"] not in {"known", "ambiguous_final_session", "unknown"}
        ):
            raise ValueError("conversion requires explicit final-session certainty")
        if boundary["preferred_last_session"] is not None:
            _date(boundary["preferred_last_session"], "preferred_last_session")
        if not isinstance(boundary["alternative_last_sessions"], list):
            raise ValueError("invalid alternative final sessions")
        for value in boundary["alternative_last_sessions"]:
            _date(value, "alternative_last_session")
        status, terms = facts["consideration_status"], facts["consideration"]
        if (
            status not in {"complete", "partial", "unreported"}
            or not isinstance(terms, list)
            or (status == "unreported" and terms)
            or (status == "complete" and not terms)
        ):
            raise ValueError("conversion consideration status disagrees with reported terms")
        for term in terms:
            if (
                not isinstance(term, dict)
                or set(term)
                != {"kind", "old_units", "old_unit_kind", "new_units", "new_unit_kind", "recipient_cusip"}
                | ({"predecessor_component", "rounding"} if facts["schema_version"] == 2 else set())
                or term["kind"] != "stock"
            ):
                raise ValueError("conversion v1 requires exact stock consideration terms")
            if term["recipient_cusip"] != facts["successor"]["cusip"]:
                raise ValueError("conversion recipient contradicts its successor")
            for key in ("old_units", "new_units"):
                if not isinstance(term[key], str):
                    raise ValueError("conversion units require exact decimal strings")
                _positive(term[key])
            for key in ("old_unit_kind", "new_unit_kind"):
                _text(term[key], key)
            if facts["schema_version"] == 2:
                component, rounding = term["predecessor_component"], term["rounding"]
                if component is not None:
                    if (
                        facts["predecessor"]["instrument_kind"] != "unit"
                        or not isinstance(component, dict)
                        or set(component) != {"cusip", "instrument_kind", "units_per_predecessor"}
                        or component["instrument_kind"]
                        not in {"public_common_stock", "common_stock", "right", "warrant"}
                        or not isinstance(component["cusip"], str)
                        or len(component["cusip"]) != 9
                        or not component["cusip"].isascii()
                        or not component["cusip"].isalnum()
                        or not isinstance(component["units_per_predecessor"], str)
                    ):
                        raise ValueError("unit conversion requires exact independently identified components")
                    _positive(component["units_per_predecessor"])
                if facts["predecessor"]["instrument_kind"] == "unit" and component is None:
                    raise ValueError("component conversion must identify its unit constituent")
                if rounding is not None and (
                    not isinstance(rounding, dict)
                    or set(rounding) != {"method", "aggregation_level", "cash_in_lieu"}
                    or rounding["method"] != "floor"
                    or rounding["aggregation_level"] != "holder_aggregate_entitlement"
                    or rounding["cash_in_lieu"] is not False
                ):
                    raise ValueError("unsupported conversion rounding scope")
        validate_consideration_units(facts)


def validate_consideration_units(facts):
    allowed = {
        "ADS": {"ADS"},
        "common_stock": {"common_share"},
        "public_common_stock": {"common_share"},
        "preferred_stock": {"preferred_share"},
        "unit": {"unit"},
        "warrant": {"warrant"},
        "right": {"right"},
    }
    for term in facts["consideration"]:
        if (
            term["old_unit_kind"] not in allowed[facts["predecessor"]["instrument_kind"]]
            or term["new_unit_kind"] not in allowed[facts["successor"]["instrument_kind"]]
        ):
            raise ValueError("consideration unit roles contradict endpoint instruments")


def _validate_market(item):
    from datetime import datetime

    from .event_supplements import _date, _positive, _text

    facts = item["facts"]
    version = facts.get("schema_version")
    if (
        set(item["match"]) != {"source_id"}
        or set(facts)
        != {"schema_version", "predecessor", "successor", "episodes", "ownership_support"}
        | ({"adjustment_basis"} if version == 2 else set())
        | ({"identity_treatment", "registrant_cik"} if version in (3, 4, 5) else set())
        | ({"public_share_reclassification"} if version == 4 else set())
        | ({"public_share_exchange"} if version == 5 else set())
        or type(version) is not int
        or version not in (1, 2, 3, 4, 5)
        or facts["ownership_support"]
        != ("documented_public_security" if version in (3, 4, 5) else "existing_public_identity")
        or not isinstance(facts["episodes"], list)
        or not facts["episodes"]
        or len(facts["episodes"]) > 100
    ):
        raise ValueError("dated market occupancy requires its complete versioned scope")
    kinds = set()
    for role in ("predecessor", "successor"):
        endpoint = facts[role]
        if not isinstance(endpoint, dict) or set(endpoint) != {"symbol", "cusip", "instrument_kind"}:
            raise ValueError("dated market endpoint requires label, CUSIP and instrument")
        _text(endpoint["symbol"], role + ".symbol")
        cusip = _text(endpoint["cusip"], role + ".cusip")
        if len(cusip) != 9 or not cusip.isascii() or not cusip.isalnum():
            raise ValueError("dated market endpoint requires exact CUSIP")
        kinds.add(endpoint["instrument_kind"])
    allowed_kinds = {
        "common_stock",
        "public_common_stock",
        "ADS",
        "preferred_stock",
        "right",
    } | ({"ETF"} if version == 3 else set())
    if len(kinds) != 1 or not kinds <= allowed_kinds:
        raise ValueError("dated market occupancy cannot merge unlike instruments")
    previous = None
    for episode in facts["episodes"]:
        if not isinstance(episode, dict) or set(episode) != {
            "symbol",
            "cusip",
            "start_inclusive",
            "end_exclusive",
            "market",
        } | ({"name", "exchange"} if version in (3, 4, 5) else set()):
            raise ValueError("market episode requires exact dated label and market scope")
        _text(episode["symbol"], "episode.symbol")
        if version in (3, 4, 5):
            _text(episode["name"], "episode.name")
            if episode["exchange"] is not None:
                _text(episode["exchange"], "episode.exchange")
        cusip = _text(episode["cusip"], "episode.cusip")
        if len(cusip) != 9 or not cusip.isascii() or not cusip.isalnum():
            raise ValueError("market episode requires exact CUSIP")
        first = _date(episode["start_inclusive"], "episode.start_inclusive")
        last = (
            _date(episode["end_exclusive"], "episode.end_exclusive")
            if episode["end_exclusive"] is not None
            else None
        )
        if (
            episode["market"] not in {"regular_way", "when_issued"}
            or last is not None
            and first >= last
            or previous is not None
            and first != previous
        ):
            raise ValueError("market episodes require contiguous ordered bounds and known markets")
        if episode["market"] == "when_issued" and last is None:
            raise ValueError("temporary market requires its closing boundary")
        previous = last
        if last is None and episode is not facts["episodes"][-1]:
            raise ValueError("open market episode must be last")
    if version in (3, 4, 5):
        cusips = {facts[role]["cusip"] for role in ("predecessor", "successor")} | {
            episode["cusip"] for episode in facts["episodes"]
        }
        treatment = facts["identity_treatment"]
        if (
            treatment
            not in (
                {"automatic_public_holding_company_exchange"}
                if version == 5
                else {"automatic_public_common_reclassification"}
                if version == 4
                else {"same_cusip_security", "preexisting_common_remained_outstanding"}
            )
            or (treatment == "same_cusip_security" and len(cusips) != 1)
            or (
                treatment == "preexisting_common_remained_outstanding"
                and not kinds <= {"common_stock", "public_common_stock"}
            )
        ):
            raise ValueError("documented public identity requires its actual continuing security treatment")
        cik = facts["registrant_cik"]
        if cik is not None and (not isinstance(cik, str) or not cik.isascii() or not cik.isdigit()):
            raise ValueError("documented registrant CIK must be digits or explicitly unavailable")
    if version == 4:
        treatment = facts["public_share_reclassification"]
        if (
            not kinds <= {"common_stock", "public_common_stock"}
            or facts["predecessor"]["cusip"] == facts["successor"]["cusip"]
            or not isinstance(treatment, dict)
            or set(treatment)
            != {
                "legal_effective_at",
                "predecessor_units",
                "successor_units",
                "same_legal_entity",
                "public_holders_continue",
                "rights_unchanged",
            }
            or treatment["same_legal_entity"] is not True
            or treatment["public_holders_continue"] is not True
            or any(
                not isinstance(treatment[key], str) or _positive(treatment[key]) != 1
                for key in ("predecessor_units", "successor_units")
            )
            or treatment["rights_unchanged"] is not None
            and type(treatment["rights_unchanged"]) is not bool
        ):
            raise ValueError("public reclassification requires actual one-for-one continuing public shares")
        instant = _text(treatment["legal_effective_at"], "legal_effective_at")
        try:
            effective = datetime.fromisoformat(instant)
        except ValueError as error:
            raise ValueError("public reclassification requires its actual legal instant") from error
        if effective.tzinfo is None or effective.utcoffset() is None:
            raise ValueError("public reclassification legal instant requires its timezone")
    if version == 5:
        exchange = facts["public_share_exchange"]
        if (
            not kinds <= {"common_stock", "public_common_stock"}
            or facts["predecessor"]["cusip"] == facts["successor"]["cusip"]
            or not isinstance(exchange, dict)
            or set(exchange)
            != {
                "legal_effective_date",
                "predecessor_units",
                "successor_units",
                "automatic_exchange",
                "public_holders_continue",
                "same_proportional_ownership",
                "rights_unchanged",
                "operating_predecessor_remains_subsidiary",
                "new_parent_only",
            }
            or any(
                exchange[key] is not True
                for key in (
                    "automatic_exchange",
                    "public_holders_continue",
                    "same_proportional_ownership",
                    "rights_unchanged",
                    "operating_predecessor_remains_subsidiary",
                    "new_parent_only",
                )
            )
            or any(
                not isinstance(exchange[key], str) or _positive(exchange[key]) != 1
                for key in ("predecessor_units", "successor_units")
            )
        ):
            raise ValueError(
                "public holding exchange requires automatic one-for-one public shares and unchanged proportional rights"
            )
        legal = _date(exchange["legal_effective_date"], "legal_effective_date")
        starts = [
            episode["start_inclusive"]
            for episode in facts["episodes"]
            if episode["cusip"] == facts["successor"]["cusip"]
        ]
        if not starts or any(day < legal for day in starts):
            raise ValueError("public holding exchange must retain its legal and marketplace boundaries")
    if version == 2 and (
        facts["adjustment_basis"] != "continuous_same_security_and_cusip"
        or len(
            {endpoint["cusip"] for endpoint in (facts["predecessor"], facts["successor"])}
            | {episode["cusip"] for episode in facts["episodes"]}
        )
        != 1
    ):
        raise ValueError("continuous market adjustment basis requires the same security and CUSIP")
