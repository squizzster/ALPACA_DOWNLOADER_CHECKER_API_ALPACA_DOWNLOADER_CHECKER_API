"""Project dated labels while retaining completed owners and numerical choices.

This producer consumes selected assignment metadata. It does not execute the
identity cascade or choose numerical sources. Only a genuinely interior dated
cut asks the caller for the selected partition's stored timestamp grid.
"""

from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation import market_occupancy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .assignment_handoff import produce
from .contract import VERSION, LineageError, document_digest, versions


@dataclass(frozen=True)
class CompletedMarketProjection:
    """Accepted unpublished label output, independent of its superseded choice."""

    receipts: dict
    documents: dict


def _reports(documents):
    result = {}
    for name, parts in documents.items():
        for kind, document in parts.items():
            branches = {branch: {"row_count": 0, "segments": []} for branch in ("correlated", "uncorrelated")}
            for entry in document["adjustments"]:
                for segment in entry["segments"]:
                    branch = branches[segment["branch"]]
                    branch["segments"].append({**segment, "adjustment": entry["adjustment"]})
                    branch["row_count"] += segment["row_count"]
            result[name, kind] = branches
    return result


class _DateViews:
    def __init__(self, name, kind, supplier):
        self.name, self.kind, self.supplier = name, kind, supplier

    def __getitem__(self, adjustment):
        if self.supplier is None:
            raise market_occupancy.MarketDateProjectionRequired(
                f"{self.name}/{self.kind}: owning dated assignment split requires stored timestamps"
            )
        return self.supplier(self.name, self.kind)[adjustment]


def bind_group(previous, evidence):
    """Bind from compact accepted membership before selecting assignment bodies."""
    return market_occupancy.bind(
        {
            name: {
                "correlated": {
                    "segments": [{"security_identity": identity} for identity in receipt["identity_memberships"]]
                }
            }
            for name, receipt in previous.items()
        },
        evidence,
    )


def selected_sources(previous, plan):
    targets = {pid for _fact_id, pid, _episodes in plan.bound}
    return {
        name
        for name, receipt in previous.items()
        if any(item["security_id"] in targets for item in receipt["identity_memberships"])
    }


def project_group(
    previous,
    evidence,
    receipt_generations,
    documents,
    policy,
    *,
    date_rows=None,
    plan=None,
    temporary_source_provider=None,
    selection_output=None,
    completed_labels=None,
):
    """Produce a label-only group from accepted inputs, with no price checks."""
    from libs.alpaca_security_lineage.inference_evidence import reject_assertions

    reject_assertions(evidence, where="retired researched plan")
    if plan is not None or completed_labels is not None:
        raise LineageError(
            "researched market projection recipes are inadmissible; consume completed publication outputs directly"
        )
    if policy.implementation_correlation_version not in (83, 84, 85, 86, 87, 88, 89, 90, 91):
        raise LineageError("dated assignment projection requires its authorized policy stage")
    baseline_reports = _reports(documents)
    accepted_documents = documents
    if completed_labels is not None:
        if not isinstance(completed_labels, CompletedMarketProjection):
            raise LineageError("completed market labels require their producer handoff")
        accepted_documents = {
            name: {**parts, **completed_labels.documents.get(name, {})} for name, parts in documents.items()
        }
    reports = _reports(accepted_documents) if completed_labels is not None else baseline_reports
    inputs, source_names = {}, {}
    for name, parts in accepted_documents.items():
        for kind, document in parts.items():
            key = name, kind
            source_names[key] = name
            inputs[key] = SimpleNamespace(views=_DateViews(name, kind, date_rows))
    plan = bind_group(previous, evidence) if plan is None else plan
    for key, report in reports.items():
        for segment in report["correlated"]["segments"]:
            pid = segment["security_identity"]["security_id"]
            if segment.get("observation_selection") is None and any(
                owner == pid
                and episode["start_inclusive"] <= segment["to"]
                and (episode["end_exclusive"] is None or segment["from"] < episode["end_exclusive"])
                for _fact, owner, episodes in plan.bound
                for episode in episodes
            ):
                raise LineageError(
                    f"{source_names[key]}: unknown numerical choice requires owning scoped selection"
                )
    if completed_labels is None:
        projected = market_occupancy.project(
            inputs,
            reports,
            evidence,
            plan=plan,
            source_names=source_names,
            recover_unassigned=False,
            allow_adjustment_continuity=policy.implementation_correlation_version >= 84,
        )
    else:
        projected = market_occupancy.apply_adjustment_authority(reports, source_names, plan)
    scopes, bundles, equivalence = {}, {}, {}
    if temporary_source_provider is not None:
        from .market_observations import select_temporary

        projected, scopes, bundles = select_temporary(
            projected,
            inputs,
            source_names,
            plan,
            previous,
            temporary_source_provider,
            equivalence_output=equivalence,
            baseline_reports=baseline_reports if completed_labels is not None else None,
        )
    identities = {}
    for _fact_id, pid, episodes in plan.bound:
        identities.setdefault(pid, []).extend(episodes)
    revision = None
    if scopes:
        contributing = {pid: {view: set() for view in ADJUSTMENTS} for pid in scopes}
        for key, report in projected.items():
            name, kind = source_names[key], key[1]
            for segment in report["correlated"]["segments"]:
                pid = segment["security_identity"]["security_id"]
                if (
                    pid not in contributing
                    or segment.get("observation_selection") == "superseded"
                    or kind == "current_year"
                    and segment["from"][:7] >= previous[name]["anchor_month"]
                ):
                    continue
                contributing[pid][segment["adjustment"]].add(name)
        revision = {
            "version": 1,
            "previous_receipt_generations": receipt_generations,
            "pricing_generations": {name: receipt["source_sha256"] for name, receipt in previous.items()},
            "sessions": scopes,
            "identity_episodes": identities,
            "selection_unchanged_outside_scope": True,
            "retained_session_grid": True,
            "numeric_reuse_sessions": equivalence,
            "source_membership": {
                pid: {view: sorted(names) for view, names in views.items()}
                for pid, views in contributing.items()
            },
            "selected_sources": {
                pid: {day: values["raw"]["source"] for day, values in days.items()}
                for pid, days in bundles.items()
            },
        }
        revision["id"] = document_digest(revision)
    if selection_output is not None:
        selection_output.update(revision=revision, bundles=bundles)
    if scopes:
        from .source_equivalence import retire as retire_source_equivalence

        remaining_equivalence = retire_source_equivalence(previous, scopes)
    receipts, changed_documents = {}, {}
    current_versions = versions()
    for name, prior in previous.items():
        receipt = receipts[name] = {
            **prior,
            "versions": current_versions,
            "correlation_policy": policy.provenance(),
            "retained_base_policy": prior["correlation_policy"],
            "evidence": evidence[name],
            "boundaries": dict((completed_labels.receipts[name] if completed_labels else prior)["boundaries"]),
            "assignment_handoffs": dict(
                (completed_labels.receipts[name] if completed_labels else prior).get("assignment_handoffs", {})
            ),
            "dated_market_projection_version": 1,
            "dated_market_projection": {
                "version": 1,
                "previous_receipt_generation": receipt_generations[name],
                "pricing_generation": prior["source_sha256"],
                "previous_boundaries": prior["boundaries"],
                "selection_unchanged": True,
                "identities": {
                    pid: episodes
                    for pid, episodes in identities.items()
                    if pid in {item["security_id"] for item in prior["identity_memberships"]}
                },
            },
        }
        changed_documents[name] = dict(completed_labels.documents.get(name, {})) if completed_labels else {}
        if revision is not None:
            receipt["numerical_source_revision_version"] = 1
            receipt["numerical_source_revision"] = revision
            receipt.pop("numerical_source_equivalence_version", None)
            receipt.pop("numerical_source_equivalence", None)
            if remaining_equivalence is not None:
                receipt["numerical_source_equivalence_version"] = 1
                receipt["numerical_source_equivalence"] = remaining_equivalence
            if scopes.keys() & {item["security_id"] for item in prior["identity_memberships"]}:
                receipt.pop("dated_market_projection_version", None)
                receipt.pop("dated_market_projection", None)
        routes = {} if documents.get(name) else prior.get("observation_routes", {})
        for kind, document in accepted_documents.get(name, {}).items():
            report = projected[name, kind]
            changed = {**document, "adjustments": []}
            for entry in document["adjustments"]:
                segments = sorted(
                    (
                        {**{k: v for k, v in segment.items() if k != "adjustment"}, "branch": branch}
                        for branch in ("correlated", "uncorrelated")
                        for segment in report[branch]["segments"]
                        if segment["adjustment"] == entry["adjustment"]
                    ),
                    key=lambda segment: segment["row_start"],
                )
                changed["adjustments"].append({**entry, "segments": segments})
                if entry["adjustment"] == "raw":
                    for segment in segments:
                        if (
                            segment["branch"] == "correlated"
                            and segment["historical_symbol"] == name
                            and not (kind == "current_year" and segment["from"][:7] >= prior["anchor_month"])
                        ):
                            pid = segment["security_identity"]["security_id"]
                            if pid not in routes or routes[pid][0] < segment["last_timestamp"]:
                                routes[pid] = [
                                    segment["last_timestamp"],
                                    segment.get("ticker_ownership_interval") or {},
                                ]
            if changed != document:
                changed["schema_version"] = VERSION
                generation = document_digest(changed)
                changed_documents[name][kind] = changed
                receipt["boundaries"][kind] = {**prior["boundaries"][kind], "sha256": generation}
                receipt["assignment_handoffs"][kind] = produce(changed, generation)
        receipt["observation_routes"] = routes
        receipt["assignment_handoff_kinds"] = sorted(receipt["assignment_handoffs"])
        # Ownership, explicit numerical states, row counts and assignment dates
        # are inherited. The producer mutates labels and their provenance only.
        # Avoid sharing mutable explanatory output with an accepted input.
        if "dated_market_projection" in receipt:
            receipt["dated_market_projection"] = deepcopy(receipt["dated_market_projection"])
    return receipts, changed_documents
