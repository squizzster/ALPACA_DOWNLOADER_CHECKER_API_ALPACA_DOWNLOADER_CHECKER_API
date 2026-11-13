"""Project accepted membership and row decisions into a selected continuation."""

from bisect import bisect_right
from copy import deepcopy
from datetime import date

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError, valid_sha256

HANDOFF_VERSION = 1


def _valid_rename_certificate(certificate):
    required = {
        "action_id",
        "old_symbol",
        "new_symbol",
        "old_cusip",
        "new_cusip",
        "old_id",
        "new_id",
        "boundary",
        "legal_date",
        "labels",
    }
    if not isinstance(certificate, dict) or set(certificate) != required:
        return False
    if any(not isinstance(certificate[key], str) or not certificate[key] for key in required - {"labels"}):
        return False
    labels = certificate["labels"]
    if not isinstance(labels, list) or not labels or len(labels) > 100:
        return False
    try:
        days = [certificate["boundary"], certificate["legal_date"]]
        previous, names = None, set()
        for label in labels:
            if (
                not isinstance(label, list)
                or len(label) != 3
                or any(not isinstance(item, str) or not item for item in label)
            ):
                return False
            name, first, last = label
            if (
                first >= last
                or previous is not None
                and first != previous
                or name in names
                or name == certificate["new_symbol"]
            ):
                return False
            previous = last
            names.add(name)
            days.extend((first, last))
        return (
            all(date.fromisoformat(day).isoformat() == day for day in days)
            and labels[-1][0] == certificate["old_symbol"]
            and previous == certificate["boundary"]
            and certificate["legal_date"] <= certificate["boundary"]
            and all(
                len(certificate[key]) == 9 and certificate[key].isascii() and certificate[key].isalnum()
                for key in ("old_cusip", "new_cusip")
            )
        )
    except (TypeError, ValueError):
        return False


def _valid_rename_handoffs(previous):
    """Validate each newly read envelope once, not its completed upstream proof."""
    if "sourced_rename_ownership" not in previous or "sourced_rename_membership" not in previous:
        return False
    membership, source = previous["sourced_rename_membership"], previous["sourced_rename_ownership"]
    certificates = {}
    if membership is not None:
        if (
            not isinstance(membership, dict)
            or set(membership) != {"version", "certificates", "issues"}
            or type(membership["version"]) is not int
            or membership["version"] != 1
        ):
            return False
        if not isinstance(membership["certificates"], list) or not isinstance(membership["issues"], list):
            return False
        for item in membership["certificates"]:
            if not _valid_rename_certificate(item) or item["action_id"] in certificates:
                return False
            certificates[item["action_id"]] = item
        if any(
            not isinstance(item, dict)
            or set(item) != {"reason_code", "corporate_action_id"}
            or any(not isinstance(value, str) or not value for value in item.values())
            for item in membership["issues"]
        ):
            return False
    if source is None:
        return True
    if (
        not isinstance(source, dict)
        or set(source) != {"version", "symbol", "first", "last", "action_id"}
        or type(source["version"]) is not int
        or source["version"] != 1
        or source["symbol"] != previous.get("symbol")
    ):
        return False
    if not isinstance(source["action_id"], str) or source["action_id"] not in certificates:
        return False
    certificate = certificates[source["action_id"]]
    try:
        return (
            all(date.fromisoformat(source[key]).isoformat() == source[key] for key in ("first", "last"))
            and certificate["new_symbol"] == source["symbol"]
            and certificate["boundary"] <= source["last"]
            and certificate["labels"][0][1] <= source["first"] <= source["last"]
        )
    except (TypeError, ValueError):
        return False


def validate_handoff(previous):
    """A declared completed handoff cannot fall back to legacy assembly."""
    if previous:
        from .source_equivalence import validate as validate_source_equivalence

        validate_source_equivalence(previous)
    if previous:
        documented = previous.get("documented_identity_membership")
        from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND, items

        required = any(
            fact["facts"]["schema_version"] in (3, 4, 5)
            for fact in items(previous.get("evidence", {}), MARKET_KIND)
        )
        if required or documented is not None:
            if (
                not isinstance(documented, dict)
                or set(documented) != {"version", "owners", "source_routes"}
                or type(documented["version"]) is not int
                or documented["version"] != 1
                or not isinstance(documented["owners"], list)
                or not isinstance(documented["source_routes"], dict)
            ):
                raise LineageError("invalid documented identity handoff; finish owning lineage recovery")
    if previous and ("numerical_source_revision" in previous or "numerical_source_revision_version" in previous):
        revision = previous.get("numerical_source_revision")
        if (
            type(previous.get("numerical_source_revision_version")) is not int
            or previous["numerical_source_revision_version"] not in (1, 2)
            or not isinstance(revision, dict)
            or set(revision)
            != {
                "version",
                "id",
                "previous_receipt_generations",
                "pricing_generations",
                "sessions",
                "identity_episodes",
                "selection_unchanged_outside_scope",
                "retained_session_grid",
                "source_membership",
                "selected_sources",
                "numeric_reuse_sessions",
            }
            | ({"labels_unchanged"} if previous["numerical_source_revision_version"] == 2 else set())
            or type(revision.get("version")) is not int
            or revision["version"] != previous["numerical_source_revision_version"]
            or revision["version"] == 2
            and revision.get("labels_unchanged") is not True
            or not valid_sha256(revision["id"])
            or revision["selection_unchanged_outside_scope"] is not True
            or revision["retained_session_grid"] is not True
            or not isinstance(revision["previous_receipt_generations"], dict)
            or not isinstance(revision["pricing_generations"], dict)
            or revision["previous_receipt_generations"].keys() != revision["pricing_generations"].keys()
            or previous["symbol"] not in revision["pricing_generations"]
            or not all(
                valid_sha256(value)
                for field in ("previous_receipt_generations", "pricing_generations")
                for value in revision[field].values()
            )
            or not isinstance(revision["sessions"], dict)
            or not revision["sessions"]
            or not isinstance(revision["identity_episodes"], dict)
            or not revision["sessions"].keys() <= revision["identity_episodes"].keys()
            or not isinstance(revision["selected_sources"], dict)
            or not isinstance(revision["numeric_reuse_sessions"], dict)
            or revision["numeric_reuse_sessions"].keys() != revision["sessions"].keys()
            or any(
                not isinstance(days, list) or not set(days) <= set(revision["sessions"][pid])
                for pid, days in revision["numeric_reuse_sessions"].items()
            )
            or revision["selected_sources"].keys() != revision["sessions"].keys()
            or any(
                not isinstance(values, dict)
                or set(values) != set(revision["sessions"][pid])
                or any(source not in revision["pricing_generations"] for source in values.values())
                for pid, values in revision["selected_sources"].items()
            )
            or not isinstance(revision["source_membership"], dict)
            or revision["source_membership"].keys() != revision["sessions"].keys()
            or any(
                not isinstance(views, dict)
                or set(views) != set(ADJUSTMENTS)
                or any(
                    not isinstance(names, list)
                    or not names
                    or not set(names) <= revision["pricing_generations"].keys()
                    for names in views.values()
                )
                for views in revision["source_membership"].values()
            )
            or any(
                not isinstance(days, list)
                or not days
                or days != sorted(set(days))
                or any(not isinstance(day, str) or len(day) != 10 for day in days)
                for days in revision["sessions"].values()
            )
        ):
            raise LineageError("incomplete numerical source revision; finish owning lineage recovery")
    if previous and ("dated_market_projection" in previous or "dated_market_projection_version" in previous):
        projection = previous.get("dated_market_projection")
        if (
            type(previous.get("dated_market_projection_version")) is not int
            or previous["dated_market_projection_version"] != 1
            or not isinstance(projection, dict)
            or set(projection)
            != {
                "version",
                "previous_receipt_generation",
                "pricing_generation",
                "previous_boundaries",
                "selection_unchanged",
                "identities",
            }
            or type(projection.get("version")) is not int
            or projection["version"] != 1
            or projection["selection_unchanged"] is not True
            or not valid_sha256(projection["previous_receipt_generation"])
            or projection["pricing_generation"] != previous["source_sha256"]
            or not isinstance(projection["previous_boundaries"], dict)
            or projection["previous_boundaries"].keys() != previous["boundaries"].keys()
            or not isinstance(projection["identities"], dict)
            or not projection["identities"].keys()
            <= {i["security_id"] for i in previous["identity_memberships"]}
            or any(
                not isinstance(episodes, list) or not episodes for episodes in projection["identities"].values()
            )
        ):
            raise LineageError("incomplete dated-market projection; finish owning lineage recovery")
    if previous and previous.get("schema_version", 0) >= 37:
        try:
            _validate_v37_handoffs(previous)
        except (KeyError, TypeError, ValueError) as error:
            raise LineageError("invalid format-37 handoff envelope; finish owning lineage recovery") from error
    if previous and previous.get("schema_version", 0) >= 36 and (not _valid_rename_handoffs(previous)):
        raise LineageError("incomplete sourced-rename ownership handoff; finish owning lineage recovery")
    if previous and previous.get("schema_version", 0) >= 35:
        certificate = previous.get("current_source_ownership")
        dates_valid = certificate is None
        if isinstance(certificate, dict):
            try:
                dates_valid = all(
                    date.fromisoformat(certificate[key]).isoformat() == certificate[key]
                    for key in ("first", "last", "departure_date")
                )
            except (KeyError, TypeError, ValueError):
                dates_valid = False
        if (
            "current_source_ownership" not in previous
            or certificate is not None
            and not (
                isinstance(certificate, dict)
                and set(certificate)
                == {
                    "version",
                    "symbol",
                    "asset_id",
                    "first",
                    "last",
                    "departed_asset_id",
                    "departure_action_id",
                    "departure_date",
                }
                and type(certificate.get("version")) is int
                and certificate.get("version") == 1
                and certificate.get("symbol") == previous.get("symbol")
                and all(
                    isinstance(certificate.get(key), str) and certificate[key]
                    for key in (
                        "asset_id",
                        "first",
                        "last",
                        "departed_asset_id",
                        "departure_action_id",
                        "departure_date",
                    )
                )
                and dates_valid
                and certificate["asset_id"] != certificate["departed_asset_id"]
                and certificate["departure_date"] < certificate["first"] <= certificate["last"]
            )
        ):
            raise LineageError("incomplete current-source ownership handoff; finish owning lineage recovery")
    if not previous or "continuation_handoff_version" not in previous:
        return
    if (
        type(previous["continuation_handoff_version"]) is not int
        or previous["continuation_handoff_version"] != HANDOFF_VERSION
        or not isinstance(previous.get("identity_memberships"), list)
        or not isinstance(previous.get("identity_intervals"), list)
        or not isinstance(previous.get("observation_routes"), dict)
    ):
        raise LineageError("incomplete lineage continuation handoff; finish owning lineage recovery")


def identity_intervals(previous, boundaries):
    """Accepted contribution ranges include every view and superseded sources."""
    if "identity_intervals" in previous:
        return previous["identity_intervals"]
    return [
        {
            "from": segment["from"],
            "to": segment["to"],
            "security_id": segment["security_identity"]["security_id"],
        }
        for kind, document in boundaries.items()
        for entry in document["adjustments"]
        for segment in entry["segments"]
        if segment["branch"] == "correlated"
        and (kind != "current_year" or segment["from"][:7] < previous["anchor_month"])
    ]


def identity_memberships(previous, boundaries):
    """Consume compact membership, or project already selected legacy assignments."""
    accepted = {}
    for symbol, prior in previous.items():
        identities = prior.get("identity_memberships")
        if identities is None:
            identities = [
                segment["security_identity"]
                for document in boundaries.get(symbol, {}).values()
                for entry in document["adjustments"]
                for segment in entry["segments"]
                if segment["branch"] == "correlated"
            ]
        for identity in identities:
            for member in identity["member_alpaca_asset_ids"]:
                existing = accepted.setdefault(member, identity)
                if existing["security_id"] != identity["security_id"]:
                    raise LineageError(
                        "accepted UUID membership is ambiguous; explicit lineage recovery required"
                    )
    return accepted


def scoped_identity_memberships(previous, boundaries):
    """Keep completed membership under each source's accepted request scope.

    Peers can carry different completed component extents, including a pinned
    historical source and a later continuation. They do not supply one global
    answer for a shared UUID. Only unambiguous peer entries may fill a missing
    local entry; a source's own accepted decision always keeps its authority.
    Ambiguity inside one source remains an owning-recovery condition.
    """
    local = {
        name: identity_memberships({name: prior}, boundaries) if prior else {}
        for name, prior in previous.items()
    }
    shared, ambiguous = {}, set()
    for memberships in local.values():
        for member, identity in memberships.items():
            if member in ambiguous:
                continue
            existing = shared.setdefault(member, identity)
            if existing["security_id"] != identity["security_id"]:
                ambiguous.add(member)
                shared.pop(member)
    return {name: {**shared, **memberships} for name, memberships in local.items()}


def retained_report(previous, boundaries, view, *, unchanged_before):
    """Freeze selected unchanged rows; numerical selection may reconsider them.

    These are accepted ownership decisions, not another correlation execution.
    Positions and request provenance describe the newly assembled suffix only.
    """
    from .transform import retained_decision_report

    report = retained_decision_report(previous, policy=None)
    report["correlation_policy"] = previous["correlation_policy"]
    report["source_row_count"] = sum(len(rows) for rows in view.views.values())
    report["source_downloads"] = []
    report["ticker_ownership_intervals"] = []
    report["retained_observation_routes"] = deepcopy(previous.get("observation_routes", {}))
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        descriptor = next(item for item in view.local["downloads"] if item["adjustment"] == adjustment)
        source = {
            "data_file": descriptor["file"],
            "download_manifest": None,
            "download_sha256": descriptor["sha256"],
            "adjustment": adjustment,
            "asof": descriptor["asof"],
            "request_from": descriptor["request_from"],
            "request_to": descriptor["request_to"],
            "feed": descriptor["feed"],
            "timeframe": descriptor["timeframe"],
        }
        report["source_downloads"].append(source)
        accepted = [
            segment
            for kind, document in boundaries.items()
            for entry in document["adjustments"]
            if entry["adjustment"] == adjustment
            for segment in entry["segments"]
            if kind != "current_year" or segment["from"][:7] < previous["anchor_month"]
        ]
        accepted.sort(key=lambda segment: segment["from"])
        starts = [segment["from"] for segment in accepted]
        last, run = None, None
        for number, row in enumerate(rows, 1):
            day = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
            if day >= unchanged_before[:10]:
                continue
            position = bisect_right(starts, day) - 1
            segment = accepted[position] if position >= 0 else None
            if segment is None or day > segment["to"] or segment["branch"] != "correlated":
                raise LineageError("selected continuation is missing an accepted row decision")
            if last is segment:
                run.update(row_end=number, row_count=run["row_count"] + 1, last_timestamp=row[0], to=day)
            else:
                run = {
                    **segment,
                    **source,
                    "row_start": number,
                    "row_end": number,
                    "row_count": 1,
                    "first_timestamp": row[0],
                    "last_timestamp": row[0],
                    "from": day,
                    "to": day,
                }
                report["correlated"]["segments"].append(run)
                last = segment
            report["correlated"]["row_count"] += 1
            interval = segment.get("ticker_ownership_interval")
            if interval and interval not in report["ticker_ownership_intervals"]:
                report["ticker_ownership_intervals"].append(interval)
    return report


def _valid_text(value):
    return isinstance(value, str) and bool(value)


def _valid_day(value):
    try:
        return isinstance(value, str) and date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _valid_endpoint(value, *, projected=False):
    required = {"instrument_kind", "cusip", "historical_symbol", "asset_id", "resolution", "candidate_asset_ids"}
    if projected:
        required.add("security_id")
    return (
        isinstance(value, dict)
        and set(value) == required
        and all(_valid_text(value[key]) for key in ("instrument_kind", "cusip", "historical_symbol"))
        and value["resolution"] in {"ambiguous", "resolved", "external_identifiers_only"}
        and (value["asset_id"] is None or _valid_text(value["asset_id"]))
        and isinstance(value["candidate_asset_ids"], list)
        and all(_valid_text(item) for item in value["candidate_asset_ids"])
        and (not projected or value["security_id"] is None or _valid_text(value["security_id"]))
    )


def _valid_common_certificate(value):
    required = {"source_fact_id", "predecessor", "successor", "legal_date", "boundary", "public_share_treatment"}
    if not isinstance(value, dict) or set(value) != required:
        return False
    treatment = value["public_share_treatment"]
    return (
        _valid_text(value["source_fact_id"])
        and all(
            _valid_endpoint(value[role]) and _valid_text(value[role]["asset_id"])
            for role in ("predecessor", "successor")
        )
        and all(_valid_day(value[key]) for key in ("legal_date", "boundary"))
        and value["legal_date"] <= value["boundary"]
        and isinstance(treatment, dict)
        and set(treatment)
        == {
            "kind",
            "predecessor_units",
            "successor_units",
            "unit_basis",
            "express_exchange_clause",
            "rights_unchanged",
        }
        and all(
            _valid_text(treatment[key]) for key in ("kind", "predecessor_units", "successor_units", "unit_basis")
        )
        and all(
            treatment[key] is None or type(treatment[key]) is bool
            for key in ("express_exchange_clause", "rights_unchanged")
        )
    )


def _valid_relation(value):
    required = {
        "relation_id",
        "relation_kind",
        "identity_union",
        "source_fact_id",
        "corporate_action_id",
        "effective_date",
        "first_successor_trading",
        "trading_boundary",
        "predecessor",
        "successor",
        "consideration",
        "consideration_status",
        "continuing_acquirer",
        "ownership_effective_dates",
        "settlement_dates",
        "endpoint_resolution",
        "source_provenance",
    }
    if not isinstance(value, dict):
        return False
    extended = "consideration_schema_version" in value
    if (
        set(value) != required | ({"consideration_schema_version"} if extended else set())
        or extended
        and (
            type(value["consideration_schema_version"]) is not int or value["consideration_schema_version"] != 2
        )
    ):
        return False
    trading, boundary = value["first_successor_trading"], value["trading_boundary"]
    return (
        value["identity_union"] is False
        and value["relation_kind"] == "distinct_security_conversion"
        and all(
            _valid_text(value[key])
            for key in ("relation_id", "source_fact_id", "consideration_status", "endpoint_resolution")
        )
        and (value["corporate_action_id"] is None or _valid_text(value["corporate_action_id"]))
        and _valid_day(value["effective_date"])
        and all(_valid_endpoint(value[role], projected=True) for role in ("predecessor", "successor"))
        and type(value["continuing_acquirer"]) is bool
        and all(
            isinstance(value[key], list) and all(_valid_day(day) for day in value[key])
            for key in ("ownership_effective_dates", "settlement_dates")
        )
        and (
            trading is None
            or isinstance(trading, dict)
            and set(trading) == {"symbol", "date"}
            and _valid_text(trading["symbol"])
            and _valid_day(trading["date"])
        )
        and isinstance(boundary, dict)
        and set(boundary) == {"status", "preferred_last_session", "alternative_last_sessions"}
        and _valid_text(boundary["status"])
        and (boundary["preferred_last_session"] is None or _valid_day(boundary["preferred_last_session"]))
        and isinstance(boundary["alternative_last_sessions"], list)
        and all(_valid_day(day) for day in boundary["alternative_last_sessions"])
        and isinstance(value["consideration"], list)
        and all(
            isinstance(term, dict)
            and set(term)
            == {"kind", "old_units", "old_unit_kind", "new_units", "new_unit_kind", "recipient_cusip"}
            | ({"predecessor_component", "rounding"} if extended else set())
            and all(
                _valid_text(term[key])
                for key in (
                    "kind",
                    "old_units",
                    "old_unit_kind",
                    "new_units",
                    "new_unit_kind",
                    "recipient_cusip",
                )
            )
            and (not extended or _valid_component_terms(term))
            for term in value["consideration"]
        )
        and isinstance(value["source_provenance"], list)
        and bool(value["source_provenance"])
        and all(
            isinstance(source, dict)
            and {"url", "title", "document_date", "authority"} <= source.keys()
            and all(_valid_text(source[key]) for key in ("url", "title", "authority"))
            and (source["document_date"] is None or _valid_day(source["document_date"]))
            for source in value["source_provenance"]
        )
    )


def _valid_component_terms(term):
    component, rounding = term["predecessor_component"], term["rounding"]
    return (
        component is None
        or isinstance(component, dict)
        and set(component) == {"cusip", "instrument_kind", "units_per_predecessor"}
        and all(_valid_text(value) for value in component.values())
    ) and (
        rounding is None
        or isinstance(rounding, dict)
        and set(rounding) == {"method", "aggregation_level", "cash_in_lieu"}
        and rounding["method"] == "floor"
        and rounding["aggregation_level"] == "holder_aggregate_entitlement"
        and rounding["cash_in_lieu"] is False
    )


def _validate_v37_handoffs(previous):
    """Accept complete serialized envelopes once, never their upstream proofs."""
    required = {
        "public_common_continuation_membership",
        "public_common_continuation_ownership",
        "security_relations",
        "membership_only_rebind",
    }
    if not required <= previous.keys():
        raise LineageError("missing format-37 handoff; finish owning lineage recovery")
    membership = previous["public_common_continuation_membership"]
    certificates = {}
    if membership is not None:
        if (
            not isinstance(membership, dict)
            or set(membership) != {"version", "certificates", "issues"}
            or type(membership["version"]) is not int
            or membership["version"] != 1
            or not isinstance(membership["certificates"], list)
            or not isinstance(membership["issues"], list)
        ):
            raise LineageError("invalid public-common membership handoff; finish owning lineage recovery")
        for certificate in membership["certificates"]:
            if not _valid_common_certificate(certificate) or certificate["source_fact_id"] in certificates:
                raise LineageError("invalid public-common certificate envelope; finish owning lineage recovery")
            certificates[certificate["source_fact_id"]] = certificate
        if any(
            not isinstance(issue, dict)
            or set(issue) != {"source_fact_id", "reason_code"}
            or not all(_valid_text(value) for value in issue.values())
            for issue in membership["issues"]
        ):
            raise LineageError("invalid public-common issue envelope; finish owning lineage recovery")
    source = previous["public_common_continuation_ownership"]
    if source is not None and (
        not isinstance(source, dict)
        or set(source) != {"version", "symbol", "source_fact_id", "route", "first", "last"}
        or type(source["version"]) is not int
        or source["version"] != 1
        or source["symbol"] != previous["symbol"]
        or not _valid_text(source["source_fact_id"])
        or source["source_fact_id"] not in certificates
        or not isinstance(source["route"], list)
        or not all(_valid_text(item) for item in source["route"])
        or not _valid_day(source["first"])
        or not _valid_day(source["last"])
        or source["first"] > source["last"]
    ):
        raise LineageError("invalid public-common source handoff; finish owning lineage recovery")
    relations = previous["security_relations"]
    if (
        not isinstance(relations, dict)
        or set(relations) != {"version", "relations", "issues"}
        or type(relations["version"]) is not int
        or relations["version"] != 1
        or not isinstance(relations["relations"], list)
        or not isinstance(relations["issues"], list)
        or not all(_valid_relation(item) for item in relations["relations"])
        or any(
            not isinstance(issue, dict)
            or set(issue) != {"source_fact_id", "reason_code"}
            or not all(_valid_text(value) for value in issue.values())
            for issue in relations["issues"]
        )
    ):
        raise LineageError("invalid security-relations projection; finish owning lineage recovery")
    rebind = previous["membership_only_rebind"]
    if rebind is None:
        return
    required = {
        "version",
        "source",
        "previous_receipt_generation",
        "previous_pricing_generation",
        "pricing_generation",
        "identities",
        "runs",
    }
    if isinstance(rebind, dict) and rebind.get("version") == 2:
        required.add("previous_selection")
    else:
        required.add("previous_boundaries")
    valid = isinstance(rebind, dict) and set(rebind) == required
    if valid:
        valid = (
            type(rebind["version"]) is int
            and rebind["version"] in (1, 2, 3)
            and rebind["source"] == previous["symbol"]
            and rebind["pricing_generation"] == previous["source_sha256"]
            and all(
                _valid_text(rebind[key])
                for key in ("previous_receipt_generation", "previous_pricing_generation", "pricing_generation")
            )
            and isinstance(rebind["identities"], dict)
            and isinstance(rebind["runs"], list)
            and (
                isinstance(rebind.get("previous_boundaries"), dict)
                and set(rebind["previous_boundaries"]) == {"completed_years", "current_year", "current_month"}
                if rebind["version"] in (1, 3)
                else isinstance(rebind.get("previous_selection"), dict)
                and set(rebind["previous_selection"]) == {"kind", "generation"}
                and rebind["previous_selection"]["kind"] == "accepted_serving_checkpoint"
                and _valid_text(rebind["previous_selection"]["generation"])
            )
        )
    if valid:
        valid = all(
            isinstance(reference, dict) and _valid_text(reference.get("sha256"))
            for reference in rebind.get("previous_boundaries", {}).values()
        ) and all(
            _valid_text(old)
            and isinstance(identity, dict)
            and _valid_text(identity.get("security_id"))
            and isinstance(identity.get("member_alpaca_asset_ids"), list)
            and bool(identity["member_alpaca_asset_ids"])
            and all(_valid_text(item) for item in identity["member_alpaca_asset_ids"])
            and isinstance(identity.get("member_tickers"), list)
            and all(_valid_text(item) for item in identity["member_tickers"])
            for old, identity in rebind["identities"].items()
        )
    if valid:
        for run in rebind["runs"]:
            if (
                not isinstance(run, dict)
                or set(run)
                != {
                    "source",
                    "adjustment",
                    "boundary",
                    "row_start",
                    "row_end",
                    "from",
                    "to",
                    "old_public_id",
                    "new_public_id",
                    "labels",
                }
                | ({"old_labels"} if rebind["version"] == 3 else set())
                or run["source"] != previous["symbol"]
                or run["adjustment"] not in ADJUSTMENTS
                or run["boundary"] not in {"completed_years", "current_year", "current_month"}
                or type(run["row_start"]) is not int
                or type(run["row_end"]) is not int
                or not 1 <= run["row_start"] <= run["row_end"]
                or not _valid_day(run["from"])
                or not _valid_day(run["to"])
                or run["from"] > run["to"]
                or not _valid_text(run["old_public_id"])
                or run["old_public_id"] not in rebind["identities"]
                or run["new_public_id"] != rebind["identities"][run["old_public_id"]]["security_id"]
                or not isinstance(run["labels"], list)
                or not run["labels"]
                or not all(_valid_text(item) for item in run["labels"])
                or rebind["version"] == 3
                and (
                    not isinstance(run["old_labels"], list)
                    or not run["old_labels"]
                    or not all(_valid_text(item) for item in run["old_labels"])
                )
            ):
                valid = False
                break
    if not valid:
        raise LineageError("invalid membership-only transfer; finish owning lineage recovery")
