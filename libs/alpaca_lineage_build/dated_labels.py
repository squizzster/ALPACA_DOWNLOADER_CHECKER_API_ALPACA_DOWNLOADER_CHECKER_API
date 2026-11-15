"""Prefer observed rename periods or uniquely dated actions to a mapped alias.

This label producer consumes completed ownership and numerical choices. It never
selects an owner or a price, and never compares prices or opens storage. Provider
rename observations support the chronology; the actual source intervals supply
its dates, because a provider processing date is not an effective trading date.
"""

from bisect import bisect_left, bisect_right
from collections import defaultdict

NAME = "observed_rename_periods_over_mapped_alias_v1"


def _roundtrip(spans, edges, actions, evidence):
    preferences, reasons = defaultdict(set), []
    for broad, temporary in sorted(edges):
        if (temporary, broad) not in edges:
            continue
        if any(
            (old in (broad, temporary) or new in (broad, temporary))
            and (old, new) not in ((broad, temporary), (temporary, broad))
            for old, new in edges
        ):
            continue
        for (pid, label), (first, last) in spans.items():
            if label != temporary or (pid, broad) not in spans:
                continue
            lower, upper = spans[pid, broad]
            if not lower < first <= last < upper:
                continue
            if not any(first <= (a.get("process_date") or "") <= last for a in actions[broad, temporary]):
                continue
            if not any((a.get("process_date") or "") > last for a in actions[temporary, broad]):
                continue
            if not any(
                a.get("action_type") in ("forward_split", "reverse_split")
                and a.get("symbol") == broad
                and (a.get("ex_date") or a.get("effective_date")) == first
                for facts in evidence.values()
                for a in facts.get("corporate_actions", ())
            ):
                continue
            preferences[pid, broad].add((first, last, temporary))
            reasons.append(
                {
                    "public_id": pid,
                    "mapped_alias": broad,
                    "supported_label": temporary,
                    "observed_span": [first, last],
                    "reason": "A provider split effective at the observed temporary source start and a unique round-trip rename route support the temporary label over the spanning label.",
                    "limits": "Processing dates bound the route, not its effective start; only existing label collisions inside the observed temporary interval are resolved.",
                }
            )
    return preferences, reasons


def plan(reports, evidence, *, requests=None):
    """Return dated label preferences from held metadata, including limitations."""
    edges = set()
    actions = defaultdict(list)
    for facts in evidence.values():
        for action in facts.get("corporate_actions", ()):
            if action.get("action_type") == "name_change":
                old, new = action.get("old_symbol"), action.get("new_symbol")
                if old and new and old != new:
                    edges.add((old, new))
                    actions[old, new].append(action)
    action_pair = len(reports) == 2 and any(
        action.get("symbol") in reports
        and (action.get("ex_date") or action.get("effective_date") or action.get("process_date"))
        for facts in evidence.values()
        for action in facts.get("corporate_actions", ())
    )
    if not action_pair and (
        len(reports) < 3 or not any(old in reports and new in reports for old, new in edges)
    ):
        return {}, []
    intervals = defaultdict(list)
    cusips = defaultdict(set)
    for source, report in reports.items():
        for segment in report["correlated"]["segments"]:
            if segment["adjustment"] != "raw" or segment["historical_symbol"] != source:
                continue
            first = segment.get("first_session_date", segment.get("from"))
            last = segment.get("last_session_date", segment.get("to"))
            if first and last:
                key = segment["security_identity"]["security_id"], source
                intervals[key].append((first, last))
                cusip = segment.get("canonical_security", {}).get("cusip")
                if cusip:
                    cusips[key].add(cusip)
    spans = {key: (min(a for a, _ in parts), max(b for _, b in parts)) for key, parts in intervals.items()}
    if action_pair:
        if edges:
            preferences, reasons = _roundtrip(spans, edges, actions, evidence)
            return {key: sorted(value) for key, value in preferences.items()}, reasons
        preferences, reasons = {}, []
        left, right = sorted(reports)
        for (pid, label), span in spans.items():
            if label != left or spans.get((pid, right)) != span:
                continue
            first, last = span
            if not all(
                (requests or {}).get(source, reports[source].get("pricing_request", {})).get("start", "")[:10]
                > last
                for source in (left, right)
            ):
                continue
            # A direct rename is a competing dated route, not a dividend vote.
            if any(old in reports or new in reports for old, new in edges):
                continue
            supported = {
                action["symbol"]
                for facts in evidence.values()
                for action in facts.get("corporate_actions", ())
                if action.get("symbol") in reports
                and first
                <= (action.get("ex_date") or action.get("effective_date") or action.get("process_date") or "")
                <= last
            }
            if len(supported) != 1:
                continue
            preferred = next(iter(supported))
            alias = right if preferred == left else left
            preferences[pid, alias] = [(first, last, preferred)]
            reasons.append(
                {
                    "public_id": pid,
                    "mapped_alias": alias,
                    "supported_label": preferred,
                    "observed_span": [first, last],
                    "reason": "Only one coextensive same-owner label has contemporaneous named provider actions; it is the more likely historical label.",
                    "limits": "A later completed request supports stopped history; empty/manual request scopes remain an inference limit. Action dates do not establish a rename date or extend into future alias-only history.",
                }
            )
        return preferences, reasons
    outgoing, incoming = defaultdict(set), defaultdict(set)
    for old, new in edges:
        outgoing[old].add(new)
        incoming[new].add(old)
    preferences, reasons = _roundtrip(spans, edges, actions, evidence)
    for old, new in sorted(edges):
        if len(outgoing[old]) != 1 or len(incoming[new]) != 1:
            continue
        for pid, label in spans:
            if label != old or (pid, new) not in spans:
                continue
            old_first, old_last = spans[pid, old]
            new_first, new_last = spans[pid, new]
            if old_last >= new_first:
                continue  # Overlapping direct routes do not establish one transition.
            relevant_actions = [
                action
                for action in actions[old, new]
                if (not action.get("process_date") or old_first <= action["process_date"])
            ]
            if not relevant_actions:
                continue
            for (owner, alias), (first, last) in spans.items():
                if owner != pid or alias in (old, new) or alias in outgoing or alias in incoming:
                    continue
                if first > old_last or last < new_first:
                    continue
                # A separate contemporaneous provider market observation is
                # positive contrary evidence. Current lookup exchange/status is
                # not dated historical market evidence.
                contrary = any(
                    action.get("symbol") == alias and old_first <= (action.get("process_date") or "") <= new_last
                    for facts in evidence.values()
                    for action in facts.get("corporate_actions", ())
                )
                if contrary:
                    continue
                for preferred in (old, new):
                    for lower, upper in intervals[pid, preferred]:
                        preferences[pid, alias].add((lower, upper, preferred))
                reasons.append(
                    {
                        "public_id": pid,
                        "mapped_alias": alias,
                        "rename": [old, new],
                        "observed_transition": [old_last, new_first],
                        "reason": "Disjoint own-label rename periods are more specific dated evidence than a mapped alias spanning both periods.",
                        "limits": "Processing date is not the trading transition; uncovered dates and competing direct labels remain ambiguous.",
                        "identifier_disagreements": [
                            {"field": field, "observed": action[field], "accepted": sorted(cusips[pid, symbol])}
                            for action in relevant_actions
                            for field, symbol in (("old_cusip", old), ("new_cusip", new))
                            if action.get(field)
                            and cusips[pid, symbol]
                            and action[field] not in cusips[pid, symbol]
                        ],
                    }
                )
    return {key: sorted(value) for key, value in preferences.items()}, reasons


def select_labels(pid, day, labels, preferences):
    selected = set(labels)
    for alias in labels:
        preferred = {label for lower, upper, label in preferences.get((pid, alias), ()) if lower <= day <= upper}
        if len(preferred) == 1 and preferred <= set(labels):
            selected.discard(alias)
    return sorted(selected)


def project(inputs, reports, evidence, *, context=None, decision=None, date_cache=None):
    """Project selected reports once, carrying every numerical/ownership field.

    Context can include frozen segments to establish chronology, but only the
    selected reports are changed. No rows are accessed for a whole-segment label
    change; an interior cut consumes only the already assembled timestamp grid.
    """
    preferences, reasons = (
        plan(reports if context is None else context, evidence) if decision is None else decision
    )
    if not preferences:
        return reports, {"producer": NAME, "changes": [], "reasons": reasons}
    result = dict(reports)
    changes = []
    dates = {} if date_cache is None else date_cache
    for source, report in reports.items():
        projected = []
        for segment in report["correlated"]["segments"]:
            pid = segment["security_identity"]["security_id"]
            labels = segment.get("observation_tickers") or [segment["historical_symbol"]]
            routes = {alias: preferences[pid, alias] for alias in labels if (pid, alias) in preferences}
            first = segment.get("first_session_date", segment.get("from"))
            last = segment.get("last_session_date", segment.get("to"))
            if len(labels) < 2 or not routes or not first or not last:
                projected.append(segment)
                continue
            relevant = {
                alias: [(lo, hi, label) for lo, hi, label in parts if lo <= last and first <= hi]
                for alias, parts in routes.items()
            }
            relevant = {alias: parts for alias, parts in relevant.items() if parts}
            if not relevant:
                projected.append(segment)
                continue
            points = {segment["row_start"], segment["row_end"] + 1}
            interior = any(
                first < lo <= last or first <= hi < last for parts in relevant.values() for lo, hi, _ in parts
            )
            grid = None
            if interior:
                rows = inputs[source].views[segment["adjustment"]]
                offset = segment["row_start"] - 1
                key = id(rows), offset, segment["row_end"]
                if key not in dates:
                    # Assemble only the selected segment's timestamps, sharing
                    # accepted physical aliases across all adjustment views.
                    from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

                    dates[key] = [
                        timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                        for row in rows[offset : segment["row_end"]]
                    ]
                grid = dates[key]
                for parts in relevant.values():
                    for lo, hi, _ in parts:
                        points.add(max(segment["row_start"], offset + bisect_left(grid, lo) + 1))
                        points.add(min(segment["row_end"] + 1, offset + bisect_right(grid, hi) + 1))
            cuts = sorted(point for point in points if segment["row_start"] <= point <= segment["row_end"] + 1)
            for left, stop in zip(cuts, cuts[1:]):
                if left == stop:
                    continue
                lower, upper = (
                    (grid[left - 1 - offset], grid[stop - 2 - offset]) if grid is not None else (first, last)
                )
                selected = set(labels)
                for alias, parts in relevant.items():
                    preferred = {label for lo, hi, label in parts if lo <= lower and upper <= hi}
                    # Multiple plausible chains stay visible, together with the
                    # alias; no lexical or numerical winner decides a label.
                    if len(preferred) == 1 and preferred <= set(labels):
                        selected.discard(alias)
                if (
                    sorted(selected) == sorted(labels)
                    and left == segment["row_start"]
                    and stop == segment["row_end"] + 1
                ):
                    projected.append(segment)
                    continue
                part = {
                    **segment,
                    "row_start": left,
                    "row_end": stop - 1,
                    "observation_tickers": sorted(selected),
                }
                if grid is not None:
                    part.update(
                        {"from": lower, "to": upper, "first_session_date": lower, "last_session_date": upper}
                    )
                    for name in ("first_timestamp", "last_timestamp", "row_count"):
                        part.pop(name, None)  # Publication owns newly split boundary fields.
                projected.append(part)
                changes.append(
                    {
                        "source": source,
                        "public_id": pid,
                        "adjustment": segment["adjustment"],
                        "row_start": left,
                        "row_end": stop - 1,
                        "before": list(labels),
                        "after": sorted(selected),
                    }
                )
        if projected != report["correlated"]["segments"]:
            result[source] = {**report, "correlated": {**report["correlated"], "segments": projected}}
    return result, {"producer": NAME, "changes": changes, "reasons": reasons}


def project_completed_group(previous, documents, evidence, receipt_generations, *, date_inputs=None):
    """Build a selected historical label correction for normal journal publication.

    The caller owns complete-group selection and generation applicability. These
    are held accepted receipts/assignments and provider-only metadata, not paths.
    The returned completed artifacts must be carried through retry/publication;
    consumers must not call this producer again to confirm its result.
    ``date_inputs[kind][source]`` supplies already selected timestamps only if a
    real partition-interior cut needs them. No provider/pricing work is allowed.
    """
    from .assignment_handoff import produce
    from .contract import document_digest

    context = {}
    partitions = defaultdict(dict)
    for source, parts in documents.items():
        context[source] = {
            "correlated": {"segments": []},
            "pricing_request": previous[source].get("pricing_request", {}),
        }
        for kind, document in parts.items():
            segments = [
                {**segment, "adjustment": entry["adjustment"]}
                for entry in document["adjustments"]
                for segment in entry["segments"]
                if segment["branch"] == "correlated"
            ]
            context[source]["correlated"]["segments"].extend(segments)
            partitions[kind][source] = {"correlated": {"segments": segments}}
    receipts = dict(previous)
    changed_documents = defaultdict(dict)
    trace = {"producer": NAME, "changes": [], "reasons": []}
    planned = plan(context, evidence)
    trace["plan"] = planned
    completed_reports = {}
    date_cache = {}
    for kind, reports in partitions.items():
        projected, decision = project(
            (date_inputs or {}).get(kind, {}),
            reports,
            evidence,
            context=context,
            decision=planned,
            date_cache=date_cache,
        )
        completed_reports[kind] = projected
        trace["changes"].extend({**change, "partition": kind} for change in decision["changes"])
        trace["reasons"] = decision["reasons"]
        for source, report in projected.items():
            if report is reports[source]:
                continue
            original = documents[source][kind]
            changed = {**original, "adjustments": []}
            for entry in original["adjustments"]:
                segments = [segment for segment in entry["segments"] if segment["branch"] != "correlated"]
                for segment in report["correlated"]["segments"]:
                    if segment["adjustment"] != entry["adjustment"]:
                        continue
                    part = {key: value for key, value in segment.items() if key != "adjustment"}
                    if "first_timestamp" not in part:
                        rows = date_inputs[kind][source].views[entry["adjustment"]]
                        part["first_timestamp"] = rows[part["row_start"] - 1][0]
                        part["last_timestamp"] = rows[part["row_end"] - 1][0]
                        part["row_count"] = part["row_end"] - part["row_start"] + 1
                    segments.append(part)
                changed["adjustments"].append(
                    {**entry, "segments": sorted(segments, key=lambda item: item["row_start"])}
                )
            generation = document_digest(changed)
            if receipts[source] is previous[source]:
                prior = previous[source]
                receipts[source] = {
                    **prior,
                    "boundaries": dict(prior["boundaries"]),
                    "assignment_handoffs": dict(prior.get("assignment_handoffs", {})),
                    "dated_label_projection": {
                        "producer": NAME,
                        "previous_receipt_generation": receipt_generations[source],
                        "pricing_generation": prior["source_sha256"],
                        "previous_boundaries": prior["boundaries"],
                        "ownership_unchanged": True,
                        "selection_unchanged": True,
                    },
                }
            receipt = receipts[source]
            receipt["boundaries"][kind] = {**previous[source]["boundaries"][kind], "sha256": generation}
            receipt["assignment_handoffs"][kind] = produce(changed, generation)
            receipt["assignment_handoff_kinds"] = sorted(receipt["assignment_handoffs"])
            changed_documents[source][kind] = changed
    if changed_documents:
        from datetime import date, timedelta

        episodes = defaultdict(set)
        for reports in completed_reports.values():
            for report in reports.values():
                for segment in report["correlated"]["segments"]:
                    if segment["adjustment"] != "raw" or segment.get("observation_selection") != "selected":
                        continue
                    owner = segment["security_identity"]["security_id"]
                    end = (date.fromisoformat(segment["to"]) + timedelta(days=1)).isoformat()
                    for label in segment.get("observation_tickers", (segment["historical_symbol"],)):
                        episodes[owner].add((segment["from"], end, label))
        identities = {
            owner: [
                {"start_inclusive": first, "end_exclusive": end, "symbol": label}
                for first, end, label in sorted(parts)
            ]
            for owner, parts in episodes.items()
        }
        # This is the existing label-only serving handoff: old numerical
        # acceptance and exact dates remain valid while labels are projected.
        affected_owners = {change["public_id"] for change in trace["changes"]}
        for source, prior in previous.items():
            if not affected_owners.intersection(
                identity["security_id"] for identity in prior["identity_memberships"]
            ):
                continue
            receipts[source] = {
                **receipts[source],
                "dated_market_projection_version": 1,
                "dated_market_projection": {
                    "version": 1,
                    "previous_receipt_generation": receipt_generations[source],
                    "pricing_generation": prior["source_sha256"],
                    "previous_boundaries": prior["boundaries"],
                    "selection_unchanged": True,
                    "identities": {
                        pid: values
                        for pid, values in identities.items()
                        if pid in {identity["security_id"] for identity in prior["identity_memberships"]}
                    },
                },
            }
    return receipts, dict(changed_documents), trace


def project_observation_frame(dimensions, reports, evidence, row_indexes, *, requests=None):
    """Diagnostic adapter to the same label decision, carrying other dimensions."""
    preferences, reasons = plan(reports, evidence, requests=requests)
    if not preferences:
        return dimensions, {"producer": NAME, "changes": [], "reasons": reasons}
    observations = dict(dimensions["observation"])
    changes = []
    for source, (views, _keys) in row_indexes.items():
        for adjustment, (rows, dates) in views.items():
            for row, day in zip(rows, dates):
                key = source, adjustment, row[0]
                semantic = dimensions["semantic"][key]
                identity = semantic.get("security_identity")
                if not identity:
                    continue
                before = observations[key]
                labels = before.get("observation_tickers") or [semantic.get("historical_symbol")]
                after = select_labels(identity["security_id"], day, labels, preferences)
                if after != labels:
                    observations[key] = {**before, "observation_tickers": after}
                    changes.append(
                        {
                            "source": source,
                            "adjustment": adjustment,
                            "timestamp": row[0],
                            "before": labels,
                            "after": after,
                        }
                    )
    return {**dimensions, "observation": observations}, {
        "producer": NAME,
        "changes": changes,
        "reasons": reasons,
    }
