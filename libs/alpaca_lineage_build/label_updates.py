"""Automatic selected-group completion of the configured dated-label rule."""

from pathlib import PurePosixPath
from types import SimpleNamespace

from .dated_labels import NAME, project_completed_group


def binding(previous, evidence):
    """Compact applicability inputs, not authentication of accepted payloads."""
    return {
        "producer": NAME,
        "sources": {name: item["source_sha256"] for name, item in previous.items()},
        "assignments": {name: item["boundaries"] for name, item in previous.items()},
        "requests": {name: item.get("pricing_request", {}) for name, item in previous.items()},
        "actions": {name: list(facts.get("corporate_actions", ())) for name, facts in evidence.items()},
    }


def applicable(previous):
    """Use the completed binding before asking for any inference projection."""
    if not previous or not all(previous.values()):
        return False
    held = next(iter(previous.values())).get("dated_label_completion")
    if held is None or held.get("producer") != NAME:
        return False
    if set(held["sources"]) != set(previous):
        return False
    return all(
        prior.get("dated_label_completion") == held
        and held["sources"][name] == prior["source_sha256"]
        and held["assignments"][name] == prior["boundaries"]
        and held["requests"].get(name) == prior.get("pricing_request", {})
        and held["actions"].get(name)
        == prior["evidence"].get("provider_corporate_actions", prior["evidence"].get("corporate_actions", []))
        for name, prior in previous.items()
    )


def possible(names, evidence):
    if len(names) < 2:
        return False
    if len(names) == 2:
        return any(
            action.get("symbol") in names
            and (action.get("ex_date") or action.get("effective_date") or action.get("process_date"))
            for facts in evidence.values()
            for action in facts.get("corporate_actions", ())
        )
    return any(
        action.get("action_type") == "name_change"
        and action.get("old_symbol") in names
        and action.get("new_symbol") in names
        and action.get("old_symbol") != action.get("new_symbol")
        for facts in evidence.values()
        for action in facts.get("corporate_actions", ())
    )


def pending(previous, evidence, *, selected=None):
    if not all(previous.values()) or not possible(previous, evidence):
        return False
    selected = binding(previous, evidence) if selected is None else selected
    return any(prior.get("dated_label_completion") != selected for prior in previous.values())


class _Dates:
    def __init__(self, store, source, kind, cache):
        self.store, self.source, self.kind, self.cache = store, source, kind, cache

    def __getitem__(self, adjustment):
        from libs.alpaca_pricing_download.contract import decode_csv
        from libs.alpaca_pricing_download.storage import read_artifact, read_document, regular_path

        source = self.source
        root = self.store / "end_of_day/pricing" / source["symbol"]
        reference = source["boundaries"][self.kind]
        key = source["symbol"], self.kind, reference["sha256"]
        if key not in self.cache:
            self.cache[key] = read_document(
                regular_path(root, reference["manifest"], published=True), published=True
            )
        document = self.cache[key]
        # Only manifest-declared physical aliases establish the same grid.
        # Activity consistency alone permits missing observations.
        name = adjustment
        entry = next(item for item in document["adjustments"] if item["adjustment"] == name)
        if entry["storage"] == "empty":
            return ()
        relative = str(PurePosixPath(reference["manifest"]).parent / entry["file"])
        payload_key = source["symbol"], relative, reference["sha256"]
        if payload_key not in self.cache:
            self.cache[payload_key] = decode_csv(
                read_artifact(
                    regular_path(root, relative, published=True),
                    purpose="selected_label_dates",
                    selector_sha256=reference["sha256"],
                )
            )
        return self.cache[payload_key]


def complete(store, root, previous, sources, evidence, generations, cache, *, assignment_cache):
    """Finish once under already held group/pricing locks; retain through retry."""
    from .source_work import assignment_documents

    selected = binding(previous, evidence)
    if not pending(previous, evidence, selected=selected):
        return previous, {}, None
    key = ("dated_labels", tuple(sorted(generations.items())), tuple(sorted(selected["sources"].items())))
    accepted = cache.get(key)
    if accepted is not None and accepted[0] == selected:
        return accepted[1]
    documents = {name: assignment_documents(root, prior, assignment_cache) for name, prior in previous.items()}
    dates = cache.setdefault("dated_label_dates", {})
    inputs = {
        kind: {
            name: SimpleNamespace(views=_Dates(store, sources[name], kind, dates))
            for name in previous
            if kind in documents[name]
        }
        for kind in {kind for parts in documents.values() for kind in parts}
    }
    receipts, changed, trace = project_completed_group(
        previous, documents, evidence, generations, date_inputs=inputs
    )
    receipts = stamp(receipts, evidence, trace["plan"])
    result = receipts, changed, trace
    cache[key] = selected, result
    return result


def encode_plan(decision):
    preferences, reasons = decision
    return {
        "preferences": [
            {"public_id": pid, "alias": alias, "periods": periods}
            for (pid, alias), periods in sorted(preferences.items())
        ],
        "reasons": reasons,
    }


def continue_plan(previous, evidence, reports):
    """Carry a completed archive/rename relation onto newly owned suffix rows.

    The caller has already selected accepted prefix reuse. Changed provider
    observations select fresh label inference; accepted historical labels stay
    within their completed scope. Only the continuing own-label source can lend
    its newly owned dates to the already established archive relation.
    """
    if not all(previous.values()):
        return None
    held = next(iter(previous.values()))
    completed = held.get("dated_label_plan")
    if completed is None or any(item.get("dated_label_plan") != completed for item in previous.values()):
        return None
    actions = {name: list(facts.get("corporate_actions", ())) for name, facts in evidence.items()}
    if any(item.get("dated_label_completion", {}).get("actions") != actions for item in previous.values()):
        return None
    preferences = {(item["public_id"], item["alias"]): [] for item in completed["preferences"]}
    # This is new suffix evidence, not a reconstruction of accepted old periods.
    for reason in completed["reasons"]:
        if "rename" not in reason:
            continue
        source = reason["rename"][1]
        key = reason["public_id"], reason["mapped_alias"]
        for segment in reports.get(source, {}).get("correlated", {}).get("segments", ()):
            if (
                segment["adjustment"] == "raw"
                and segment["historical_symbol"] == source
                and segment["security_identity"]["security_id"] == key[0]
            ):
                lower = segment.get("first_session_date", segment.get("from"))
                upper = segment.get("last_session_date", segment.get("to"))
                if lower and upper:
                    preferences[key].append((lower, upper, source))
    return preferences, completed["reasons"]


def stamp(receipts, evidence, decision):
    selected = binding(receipts, evidence)
    encoded = encode_plan(decision)
    return {
        name: {**receipt, "dated_label_completion": selected, "dated_label_plan": encoded}
        for name, receipt in receipts.items()
    }


def validate_completion(receipt):
    """Accept a declared compact label handoff once at receipt ingress."""
    from .contract import LineageError, valid_sha256

    if "dated_label_completion" not in receipt and "dated_label_plan" not in receipt:
        return
    completion, decision = receipt.get("dated_label_completion"), receipt.get("dated_label_plan")
    if (
        not isinstance(completion, dict)
        or completion.get("producer") != NAME
        or set(completion) != {"producer", "sources", "assignments", "actions", "requests"}
        or not isinstance(completion["sources"], dict)
        or not all(valid_sha256(value) for value in completion["sources"].values())
        or completion["sources"].get(receipt["symbol"]) != receipt["source_sha256"]
        or not isinstance(completion["assignments"], dict)
        or completion["assignments"].get(receipt["symbol"]) != receipt["boundaries"]
        or not isinstance(completion["requests"], dict)
        or completion["requests"].get(receipt["symbol"]) != receipt.get("pricing_request", {})
        or not isinstance(completion["actions"], dict)
        or not isinstance(decision, dict)
        or set(decision) != {"preferences", "reasons"}
        or not isinstance(decision["preferences"], list)
        or not isinstance(decision["reasons"], list)
    ):
        raise LineageError("incomplete dated-label completion; owning lineage recovery required")
