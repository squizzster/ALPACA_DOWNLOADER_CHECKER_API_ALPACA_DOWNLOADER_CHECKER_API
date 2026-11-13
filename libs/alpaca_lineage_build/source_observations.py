"""Bounded new observation contests with frozen accepted source ownership."""

from bisect import bisect_left, bisect_right
from copy import deepcopy
from types import SimpleNamespace

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.alpaca_pricing_download.handoff import load_published_boundary

from .contract import LineageError
from .observations import select_observations


def _dates(segment):
    return segment.get("from", segment["first_timestamp"][:10]), segment.get(
        "to", segment["last_timestamp"][:10]
    )


def _accepted_runs(ownership):
    result = {}
    for adjustment, view in ownership["views"].items():
        position, runs = 0, []
        for run in view["ownership"]:
            runs.append((position + 1, position + run["rows"], run["owner"]))
            position += run["rows"]
        result[adjustment] = runs
    return result


def _preserved_fields(source, segment, runs, changes, *, offset=0, choices=None):
    first, last = offset + segment["row_start"], offset + segment["row_end"]
    current_id = (segment.get("security_identity") or {}).get("security_id")
    targets = {identity["security_id"] for identity in changes.values()}
    matching = [(left, right, owner) for left, right, owner in runs if left <= last and first <= right]
    affected = current_id in targets or any(owner[0] in changes for _, _, owner in matching)
    if not affected:
        return None
    if choices is not None:
        accepted = [run for run in choices if run.row_start <= last and first <= run.row_end]
        if not accepted:
            return None
        if len(accepted) != 1 or first < accepted[0].row_start or last > accepted[0].row_end:
            raise LineageError(f"{source}: accepted numerical choice needs dated assignment subdivision")
        choice = accepted[0]
        refinement = segment.get("completed_membership_refinement")
        if refinement is not None:
            if (
                refinement["source"] != source
                or choice.owner != refinement["old_public_id"]
                or current_id != refinement["new_public_id"]
                or not refinement["row_start"] <= first <= last <= refinement["row_end"]
            ):
                raise LineageError(f"{source}: completed membership scope changed its accepted positions")
            return None
        if choice.state in (None, "unassigned"):
            return None
        expected = changes.get(choice.owner, {}).get("security_id", choice.owner)
        if current_id != expected or segment.get("branch", "correlated") != "correlated":
            raise LineageError(f"{source}: accepted numerical choice changed row ownership")
        labels = (
            segment.get("observation_tickers", choice.tickers)
            if segment.get("dated_identity_refinement", {}).get("numerical_selection_unchanged") is True
            else choice.tickers
        )
        return {"observation_selection": choice.state, "observation_tickers": list(labels)}
    return None


def _scope(source, adjustment, segment, *, complete_grid=False):
    first, last = _dates(segment)
    return {
        "source": source,
        "public_id": segment["security_identity"]["security_id"],
        "adjustment": adjustment,
        "from": first,
        "to": last,
        "rows": segment["row_end"] - segment["row_start"] + 1,
        "complete_grid": complete_grid,
    }


def _covered(source, adjustment, segment, scopes, target_grid):
    target = _scope(source, adjustment, segment)
    if not target_grid or not target_grid["first"] <= target["from"] <= target["to"] <= target_grid["last"]:
        return False
    donors = {}
    for scope in scopes:
        if (
            scope["source"] != source
            and scope.get("complete_grid") is True
            and scope["public_id"] == target["public_id"]
            and scope["adjustment"] == adjustment
            and target["from"] <= scope["from"] <= scope["to"] <= target["to"]
        ):
            donors.setdefault(scope["source"], []).append(scope)
    for values in donors.values():
        ordered = sorted(values, key=lambda value: (value["from"], value["to"]))
        if (
            ordered[0]["from"] == target["from"]
            and ordered[-1]["to"] == target["to"]
            and sum(item["rows"] for item in ordered) == target["rows"]
            and all(left["to"] < right["from"] for left, right in zip(ordered, ordered[1:]))
        ):
            return True
    return False


def _local_choice_fragments(segment, offset, choices):
    full = {**segment, "row_start": offset + segment["row_start"], "row_end": offset + segment["row_end"]}
    for piece in _choice_fragments(full, choices):
        yield {**piece, "row_start": piece["row_start"] - offset, "row_end": piece["row_end"] - offset}


def selected_donor_scopes(
    source,
    *,
    old_serving_ownership,
    accepted_identity_changes,
    current_receipt,
    current_documents,
    accepted_checkpoint=None,
    accepted_choices=None,
):
    """Assemble dated scopes only for already selected joined donor positions."""
    if accepted_choices is None:
        raise LineageError(f"{source}: donor selection requires explicit accepted numerical choices")
    if not any(
        run["owner"][0] in accepted_identity_changes
        for view in old_serving_ownership["views"].values()
        for run in view["ownership"]
    ):
        return []
    complete_ids = complete_donor_ids(
        source,
        old_serving_ownership,
        current_receipt["source_sha256"],
        accepted_checkpoint,
    )
    if not complete_ids:
        return []
    documents = current_documents() if callable(current_documents) else current_documents
    runs, offsets, result = _accepted_runs(old_serving_ownership), dict.fromkeys(ADJUSTMENTS, 0), []
    for kind in ("completed_years", "current_year", "current_month"):
        for entry in documents[kind]["adjustments"]:
            adjustment = entry["adjustment"]
            for segment in (
                piece
                for segment in entry["segments"]
                for piece in _local_choice_fragments(
                    segment, offsets[adjustment], accepted_choices.runs[adjustment]
                )
            ):
                if kind == "current_year" and segment["from"][:7] >= current_receipt["anchor_month"]:
                    continue
                first, last = (
                    offsets[adjustment] + segment["row_start"],
                    offsets[adjustment] + segment["row_end"],
                )
                if not any(
                    run.row_start <= first <= last <= run.row_end for run in accepted_choices.runs[adjustment]
                ):
                    raise LineageError(f"{source}: recovery assignment positions lack accepted choice bounds")
                fields = _preserved_fields(
                    source,
                    segment,
                    runs[adjustment],
                    accepted_identity_changes,
                    offset=offsets[adjustment],
                    choices=accepted_choices.runs[adjustment],
                )
                if fields and fields["observation_selection"] == "selected":
                    first = offsets[adjustment] + segment["row_start"]
                    old_id = next(
                        run.owner
                        for run in accepted_choices.runs[adjustment]
                        if run.row_start <= first <= run.row_end
                    )
                    result.append(_scope(source, adjustment, segment, complete_grid=old_id in complete_ids))
            offsets[adjustment] += current_receipt["assignment_handoffs"][kind]["ownership"][adjustment]["rows"]
    return result


def complete_donor_ids(source, ownership, generation, checkpoint):
    """Consume the exact accepted numerical input and its completed grid result."""
    history = (checkpoint or {}).get("history_validation", {})
    old_input = history.get("inputs", {}).get(source)
    if (
        not old_input
        or old_input["pricing_generation"] != generation
        or old_input["views"] != ownership["views"]
    ):
        return set()
    return {
        record["public_id"]
        for record in history["prefix_records"]
        if not record["missing_sessions"]
        and not record["price_reasons"]
        and record["coverage"].get("meaning") == "approved_stored_history"
    }


def preserve_selection(
    source,
    *,
    old_serving_ownership,
    accepted_identity_changes,
    current_receipt,
    current_documents,
    preserves_pricing,
    accepted_donor_scopes=(),
    accepted_choices=None,
):
    """Carry accepted numerical selections through a completed membership join.

    Current assignment ownership remains authoritative. Only observation choice
    changes; no row, date, label or numerical approval is newly inferred.
    """
    if not preserves_pricing:
        raise LineageError(f"{source}: accepted observation selection changed pricing generation")
    if current_receipt.get("membership_transfer_handoff_version") == 1:
        return {}  # The completed final-choice producer already carried it.
    if accepted_choices is None:
        raise LineageError(f"{source}: membership selection requires explicit accepted numerical choices")
    if not any(
        run.owner in accepted_identity_changes for runs in accepted_choices.runs.values() for run in runs
    ):
        return {}
    if any(
        run.owner in accepted_identity_changes and run.state is None
        for runs in accepted_choices.runs.values()
        for run in runs
    ):
        raise LineageError(f"{source}: unknown accepted numerical choice requires owning selection")
    runs = _accepted_runs(old_serving_ownership)
    documents = current_documents() if callable(current_documents) else current_documents
    result = deepcopy(documents)
    offsets = dict.fromkeys(ADJUSTMENTS, 0)
    for kind in ("completed_years", "current_year", "current_month"):
        for entry in result[kind]["adjustments"]:
            adjustment = entry["adjustment"]
            original_segments = entry["segments"]
            entry["segments"] = []
            for segment in (
                piece
                for segment in original_segments
                for piece in _local_choice_fragments(
                    segment, offsets[adjustment], accepted_choices.runs[adjustment]
                )
            ):
                entry["segments"].append(segment)
                if kind == "current_year" and segment["from"][:7] >= current_receipt["anchor_month"]:
                    continue
                first, last = (
                    offsets[adjustment] + segment["row_start"],
                    offsets[adjustment] + segment["row_end"],
                )
                if not any(
                    run.row_start <= first <= last <= run.row_end for run in accepted_choices.runs[adjustment]
                ):
                    raise LineageError(f"{source}: recovery assignment positions lack accepted choice bounds")
                fields = _preserved_fields(
                    source,
                    segment,
                    runs[adjustment],
                    accepted_identity_changes,
                    offset=offsets[adjustment],
                    choices=accepted_choices.runs[adjustment],
                )
                if fields is None and segment.get("branch", "correlated") == "correlated":
                    first, last = (
                        offsets[adjustment] + segment["row_start"],
                        offsets[adjustment] + segment["row_end"],
                    )
                    newly_owned = any(
                        run.state == "unassigned" and run.row_start <= first <= last <= run.row_end
                        for run in accepted_choices.runs[adjustment]
                    )
                    if newly_owned and segment.get("observation_selection") != "superseded":
                        if not _covered(
                            source,
                            adjustment,
                            segment,
                            accepted_donor_scopes,
                            current_receipt.get("public_common_continuation_ownership"),
                        ):
                            raise LineageError(
                                f"{source}: newly owned observations lack accepted donor coverage"
                            )
                        fields = {"observation_selection": "superseded"}
                if fields:
                    segment.update(fields)
            offsets[adjustment] += current_receipt["assignment_handoffs"][kind]["ownership"][adjustment]["rows"]
    return {kind: document for kind, document in result.items() if document != documents[kind]}


def _choice_fragments(segment, choices):
    """Split new ownership at accepted choice bounds using metadata alone."""
    first, last = segment["row_start"], segment["row_end"]
    relevant = [run for run in choices if run.row_start <= last and first <= run.row_end]
    cuts = sorted(
        {first, last + 1}
        | {
            position
            for run in relevant
            for position in (run.row_start, run.row_end + 1)
            if first < position <= last
        }
    )
    for left, stop in zip(cuts, cuts[1:]):
        right = stop - 1
        if left == first and right == last:
            yield segment
            continue
        piece = {**segment, "row_start": left, "row_end": right, "row_count": right - left + 1}
        if left != first:
            run = next(run for run in relevant if run.row_start == left)
            if run.first_timestamp is None:
                raise LineageError(
                    "accepted choice subdivision lacks its first timestamp; owning projection required"
                )
            piece.update(first_timestamp=run.first_timestamp, **{"from": run.first})
        if right != last:
            run = next(run for run in relevant if run.row_end == right)
            if run.last_timestamp is None:
                raise LineageError(
                    "accepted choice subdivision lacks its last timestamp; owning projection required"
                )
            piece.update(last_timestamp=run.last_timestamp, to=run.last)
        yield piece


def _protect_reports(reports, retained):
    selected, protected = {}, {}
    for name, report in reports.items():
        if name not in retained:
            selected[name] = report
            continue
        ownership, changes = retained[name][:2]
        runs = _accepted_runs(ownership)
        choices = retained[name][3] if len(retained[name]) > 3 else None
        remaining, frozen = [], []
        fragments = (
            piece
            for segment in report["correlated"]["segments"]
            for piece in (
                _choice_fragments(segment, choices.runs[segment["adjustment"]])
                if choices is not None
                else (segment,)
            )
        )
        for segment in fragments:
            # Source-work carry consumes explicit states. Legacy ownership alone
            # supplies no choice authority; those observations enter selection.
            fields = (
                _preserved_fields(
                    name,
                    segment,
                    runs[segment["adjustment"]],
                    changes,
                    choices=choices.runs[segment["adjustment"]],
                )
                if choices is not None
                else None
            )
            if fields is None:
                remaining.append(segment)
            else:
                frozen.append({**segment, **fields})
        protected[name] = frozen
        selected[name] = {**report, "correlated": {**report["correlated"], "segments": remaining}}
    return selected, protected


def _cover_newly_owned(reports, protected, retained, donor_scopes, *, exact_coverage=None):
    """Select newly allocated duplicates without reopening accepted choices."""
    result = {}
    for name, report in reports.items():
        binding = retained.get(name, ())
        choices = binding[3] if len(binding) > 3 else None
        if choices is None:
            result[name] = report
            continue
        remaining = []
        for segment in report["correlated"]["segments"]:
            first, last = segment["row_start"], segment["row_end"]
            prior_unassigned = any(
                run.state == "unassigned" and run.row_start <= first <= last <= run.row_end
                for run in choices.runs[segment["adjustment"]]
            )
            if prior_unassigned and (
                exact_coverage is not None
                and exact_coverage.covers(name, segment)
                or _covered(
                    name,
                    segment["adjustment"],
                    segment,
                    donor_scopes,
                    report.get("public_common_continuation_ownership"),
                )
            ):
                protected.setdefault(name, []).append({**segment, "observation_selection": "superseded"})
            else:
                remaining.append(segment)
        result[name] = {**report, "correlated": {**report["correlated"], "segments": remaining}}
    return result


def select_scopes(reports, closed_reports):
    """Name counterpart source/date scopes before a payload reader is called."""
    scopes = {}
    combined = {**closed_reports, **reports}
    for name, report in closed_reports.items():
        for segment in report["correlated"]["segments"]:
            if segment.get("observation_selection") == "superseded":
                continue
            first, last = _dates(segment)
            pid = segment["security_identity"]["security_id"]
            for other, peer in combined.items():
                if other == name:
                    continue
                for rival in peer["correlated"]["segments"]:
                    if (
                        rival["adjustment"] != segment["adjustment"]
                        or rival["security_identity"]["security_id"] != pid
                        or rival.get("observation_selection") == "superseded"
                    ):
                        continue
                    left, right = _dates(rival)
                    overlap = max(first, left), min(last, right)
                    if overlap[0] <= overlap[1]:
                        scopes.setdefault(name, set()).add(overlap)
    return {name: tuple(sorted(spans)) for name, spans in scopes.items()}


def counterpart(store, control, report, scopes, cache):
    """Assemble only named counterpart observations; never execute its rules."""
    source, prior = control.source, control.previous
    root = store / "end_of_day/pricing" / control.symbol
    selected, positions = {name: [] for name in ADJUSTMENTS}, {name: [] for name in ADJUSTMENTS}
    dates = {name: {} for name in ADJUSTMENTS}
    kinds = set()
    anchor = source["anchor_month"]
    for first, last in scopes:
        if first[:4] < anchor[:4]:
            kinds.add("completed_years")
        if first[:7] < anchor and last[:4] >= anchor[:4]:
            kinds.add("current_year")
        if last[:7] >= anchor:
            kinds.add("current_month")
    for kind in ("completed_years", "current_year", "current_month"):
        if kind not in kinds:
            continue
        key = str(root), kind, source["boundaries"][kind]["sha256"]
        if key not in cache:
            cache[key] = load_published_boundary(root, source, kind, selected=True)
        from .recovery_handoffs import remember_partition

        remember_partition(store, control.symbol, kind, key[-1], cache[key])
        for adjustment, rows in cache[key].items():
            offset = prior["serving_ownership"]["offsets"][adjustment][kind]
            for index, row in enumerate(rows):
                day = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                if kind == "current_year" and day[:7] >= anchor:
                    continue
                dates[adjustment][offset + index + 1] = (day, row[0])
                if any(first <= day <= last for first, last in scopes):
                    selected[adjustment].append(row)
                    positions[adjustment].append(offset + index + 1)
    projected = {
        **report,
        "correlated": {"row_count": 0, "segments": []},
        "uncorrelated": {"row_count": 0, "segments": []},
    }
    for adjustment in ADJUSTMENTS:
        for segment in report["correlated"]["segments"]:
            if segment["adjustment"] != adjustment:
                continue
            for local, original in enumerate(positions[adjustment], 1):
                if segment["row_start"] <= original <= segment["row_end"]:
                    projected["correlated"]["segments"].append(
                        {**segment, "row_start": local, "row_end": local, "row_count": 1}
                    )
                    projected["correlated"]["row_count"] += 1
    return (
        SimpleNamespace(source=source, source_sha256=control.source_sha256, views=selected),
        projected,
        positions,
        dates,
    )


def selection_changes(report, rows, positions):
    result = {name: {} for name in ADJUSTMENTS}
    for segment in report["correlated"]["segments"]:
        adjustment = segment["adjustment"]
        for index in range(segment["row_start"] - 1, segment["row_end"]):
            row = rows[adjustment][index]
            result[adjustment][positions[adjustment][index]] = {
                "observation_selection": segment["observation_selection"],
                "observation_tickers": segment["observation_tickers"],
                "timestamp": row[0],
                "day": timestamp(row[0]).astimezone(NEW_YORK).date().isoformat(),
            }
    return result


def counterpart_positions(store, control, report, intervals, cache):
    """Materialize admitted boundary-local positions without a date search.

    The completion owner has bound these positions to accepted owners and input
    generations. Exact-copy payload aliases share assembly. Only the timestamps
    adjoining an actual assignment cut may be read outside the selected rows;
    those endpoints construct retained fragments, never enter numerical choice.
    """
    from .numerical_completion import NumericalInput

    source, previous = control.source, control.previous
    root = store / "end_of_day/pricing" / control.symbol
    offsets = previous["serving_ownership"]["offsets"]
    requested = {}
    for interval in intervals:
        key = interval["boundary"], interval["adjustment"]
        requested.setdefault(key, []).append((interval["row_start"], interval["row_end"]))
    for key, ranges in requested.items():
        merged = []
        for first, last in sorted(ranges):
            if first < 1 or last < first:
                raise LineageError("invalid numerical counterpart row interval")
            if merged and first <= merged[-1][1] + 1:
                merged[-1] = merged[-1][0], max(last, merged[-1][1])
            else:
                merged.append((first, last))
        requested[key] = merged

    parts = {name: [] for name in ADJUSTMENTS}
    positions = {name: [] for name in ADJUSTMENTS}
    dates = {name: {} for name in ADJUSTMENTS}
    kinds = {name: {} for name in ADJUSTMENTS}
    instants = {}
    remembered = set()

    def partition(kind):
        from .recovery_handoffs import recall, remember, remember_partition

        key = str(root), kind, source["boundaries"][kind]["sha256"]
        document_key = "numerical_counterpart_document", *key
        if key not in cache:
            held = recall(store, "pricing_partition", (control.symbol, kind, key[-1]))
            if held is not None:
                cache[key] = held
            else:
                documents = {}
                cache[key] = load_published_boundary(root, source, kind, selected=True, documents=documents)
                document = documents.get(source["boundaries"][kind]["manifest"])
                if document is None:
                    raise LineageError("materialized numerical counterpart lacks its publication document")
                cache[document_key] = document
        if key not in remembered:
            cache[key] = remember_partition(store, control.symbol, kind, key[-1], cache[key])
            if document_key in cache:
                remember(
                    store,
                    "pricing_partition_document",
                    (control.symbol, kind, key[-1]),
                    cache[document_key],
                )
            remembered.add(key)
        return cache[key]

    def date_of(row):
        stamp = row[0]
        key = "numerical_counterpart_date", stamp
        if key not in cache:
            instant = timestamp(stamp)
            cache[key] = instant.astimezone(NEW_YORK).date().isoformat(), instant
        day, instant = cache[key]
        instants[stamp] = instant
        return day, stamp

    assembled = {}
    for kind in ("completed_years", "current_year", "current_month"):
        if not any(key[0] == kind for key in requested):
            continue
        views = partition(kind)
        for adjustment in ADJUSTMENTS:
            ranges = requested.get((kind, adjustment), ())
            if not ranges:
                continue
            rows = views[adjustment]
            if ranges[-1][1] > len(rows):
                raise LineageError("numerical counterpart positions exceed their selected boundary")
            key = id(rows), tuple(ranges)
            if key not in assembled:
                assembled[key] = tuple(row for first, last in ranges for row in rows[first - 1 : last])
            selected = assembled[key]
            original = [
                offsets[adjustment][kind] + number for first, last in ranges for number in range(first, last + 1)
            ]
            parts[adjustment].append(selected)
            positions[adjustment].extend(original)
            for number, row in zip(original, selected, strict=True):
                dates[adjustment][number] = date_of(row)
                kinds[adjustment][number] = kind

    selected, aliases = {}, {}
    for adjustment in ADJUSTMENTS:
        if any(left >= right for left, right in zip(positions[adjustment], positions[adjustment][1:])):
            raise LineageError("numerical counterpart intervals select overlapping boundary positions")
        key = tuple(id(part) for part in parts[adjustment])
        if key not in aliases:
            aliases[key] = tuple(row for part in parts[adjustment] for row in part)
        selected[adjustment] = aliases[key]

    segments = []
    for segment in report["correlated"]["segments"]:
        adjustment = segment["adjustment"]
        original = positions[adjustment]
        left = bisect_left(original, segment["row_start"])
        stop = bisect_right(original, segment["row_end"])
        if left == stop:
            continue
        cuts = [left]
        cuts.extend(index for index in range(left + 1, stop) if original[index] != original[index - 1] + 1)
        cuts.append(stop)
        for start, end in zip(cuts, cuts[1:]):
            first, last = original[start], original[end - 1]
            for adjacent, selected_position in (
                (first - 1, first),
                (last + 1, last),
            ):
                if segment["row_start"] <= adjacent <= segment["row_end"] and adjacent not in dates[adjustment]:
                    if adjacent == segment["row_start"]:
                        dates[adjustment][adjacent] = _dates(segment)[0], segment["first_timestamp"]
                    elif adjacent == segment["row_end"]:
                        dates[adjustment][adjacent] = _dates(segment)[1], segment["last_timestamp"]
                    else:
                        kind = kinds[adjustment][selected_position]
                        rows = partition(kind)[adjustment]
                        index = adjacent - offsets[adjustment][kind] - 1
                        key = "numerical_counterpart_endpoint", id(rows), index
                        if key not in cache:
                            cache[key] = date_of(rows[index])
                        dates[adjustment][adjacent] = cache[key]
            first_day, first_stamp = dates[adjustment][first]
            last_day, last_stamp = dates[adjustment][last]
            segments.append(
                {
                    **segment,
                    "row_start": start + 1,
                    "row_end": end,
                    "row_count": end - start,
                    "from": first_day,
                    "to": last_day,
                    "first_session_date": first_day,
                    "last_session_date": last_day,
                    "first_timestamp": first_stamp,
                    "last_timestamp": last_stamp,
                }
            )
    projected = {
        **report,
        "correlated": {"row_count": sum(item["row_count"] for item in segments), "segments": segments},
        "uncorrelated": {"row_count": 0, "segments": []},
    }
    return (
        NumericalInput(
            source=source,
            source_sha256=control.source_sha256,
            views=selected,
            instant_keys=instants,
        ),
        projected,
        positions,
        dates,
    )


def reconcile(
    store,
    materialized,
    reports,
    closed,
    closed_reports,
    cache,
    *,
    retained=None,
    selection_output=None,
    stable_identities=(),
):
    retained = retained or {}
    active, protected = _protect_reports(reports, retained)
    closed_active, closed_protected = _protect_reports(closed_reports, retained)
    accepted_scopes = []
    for name, segments in {**closed_protected, **protected}.items():
        binding = retained[name]
        complete_ids = binding[2] if len(binding) > 2 else set()
        runs = _accepted_runs(binding[0])
        for segment in segments:
            if segment["observation_selection"] != "selected":
                continue
            old_id = (
                next(
                    run.owner
                    for run in binding[3].runs[segment["adjustment"]]
                    if run.row_start <= segment["row_start"] <= run.row_end
                )
                if len(binding) > 3 and binding[3] is not None
                else next(
                    owner[0]
                    for left, right, owner in runs[segment["adjustment"]]
                    if left <= segment["row_start"] <= right
                )
            )
            accepted_scopes.append(
                _scope(name, segment["adjustment"], segment, complete_grid=old_id in complete_ids)
            )
    exact_coverage = None
    if stable_identities:
        from .source_choice_coverage import AcceptedBundleCoverage

        exact_coverage = AcceptedBundleCoverage(materialized, protected, identities=stable_identities)
    active = _cover_newly_owned(active, protected, retained, accepted_scopes, exact_coverage=exact_coverage)
    closed_active = _cover_newly_owned(closed_active, closed_protected, retained, accepted_scopes)
    # A join can create a genuinely new contest between two accepted donors.
    # Preserve disjoint selections; leave overlapping accepted donors to the
    # existing bounded selection owner instead of claiming they compose.
    donors, contested = {}, {}

    def contest(key, first, last):
        contested.setdefault(key, []).append((first, last))

    for name, segments in {**closed_protected, **protected}.items():
        for segment in segments:
            if segment["observation_selection"] != "selected":
                continue
            key = segment["security_identity"]["security_id"], segment["adjustment"]
            first, last = _dates(segment)
            choices = retained[name][3]
            old_owner = next(
                run.owner for run in choices.runs[key[1]] if run.row_start <= segment["row_start"] <= run.row_end
            )
            for other, left, right, other_owner in donors.get(key, ()):
                if other != name and old_owner != other_owner and max(first, left) <= min(last, right):
                    contest(key, max(first, left), min(last, right))
            donors.setdefault(key, []).append((name, first, last, old_owner))
    for name, report in {**closed_active, **active}.items():
        for segment in report["correlated"]["segments"]:
            key = segment["security_identity"]["security_id"], segment["adjustment"]
            first, last = _dates(segment)
            for other, left, right, _owner in donors.get(key, ()):
                if other != name and max(first, left) <= min(last, right):
                    contest(key, max(first, left), min(last, right))
    if contested:
        for selected_reports, frozen in ((active, protected), (closed_active, closed_protected)):
            for name, segments in frozen.items():
                opened, kept = [], []
                for segment in segments:
                    key = segment["security_identity"]["security_id"], segment["adjustment"]
                    spans = contested.get(key, ())
                    first, last = _dates(segment)
                    spans = [
                        (max(first, left), min(last, right))
                        for left, right in spans
                        if max(first, left) <= min(last, right)
                    ]
                    if not spans:
                        kept.append(segment)
                    elif name in materialized:
                        for active_piece, piece in _split_contest(
                            segment, spans, materialized[name].views[key[1]]
                        ):
                            (opened if active_piece else kept).append(piece)
                    else:
                        # Closed peers are projected to exact requested dates by
                        # counterpart; unrelated positions never enter selection.
                        opened.append(segment)
                frozen[name] = kept
                if opened:
                    report = selected_reports[name]
                    selected_reports[name] = {
                        **report,
                        "correlated": {
                            **report["correlated"],
                            "segments": report["correlated"]["segments"] + opened,
                        },
                    }
    active = {name: report for name, report in active.items() if report["correlated"]["segments"]}
    closed_active = {name: report for name, report in closed_active.items() if report["correlated"]["segments"]}
    selected, changes, scopes = _reconcile(
        store,
        {name: materialized[name] for name in active},
        active,
        closed,
        closed_active,
        cache,
        contested=contested,
        retained=retained,
        selection_output=selection_output,
        instant_keys=exact_coverage.instants if exact_coverage is not None else None,
    )
    result = {}
    for name, report in reports.items():
        chosen = selected.get(name, {**report, "correlated": {**report["correlated"], "segments": []}})
        result[name] = {
            **chosen,
            "correlated": {
                **chosen["correlated"],
                "segments": sorted(
                    chosen["correlated"]["segments"] + protected.get(name, []),
                    key=lambda segment: (segment["adjustment"], segment["row_start"]),
                ),
            },
        }
    return result, changes, scopes


def _split_contest(segment, spans, rows):
    """Use exact stored positions only to construct a newly reopened fragment."""
    first, stop = segment["row_start"] - 1, segment["row_end"]
    days = {}

    def day(row):
        value = row[0]
        if value not in days:
            days[value] = timestamp(value).astimezone(NEW_YORK).date().isoformat()
        return days[value]

    ranges = [
        (
            bisect_left(rows, left, lo=first, hi=stop, key=day),
            bisect_right(rows, right, lo=first, hi=stop, key=day),
        )
        for left, right in spans
    ]
    cuts = sorted({first, stop} | {value for left, right in ranges for value in (left, right)})
    for left, right in zip(cuts, cuts[1:]):
        if left == right:
            continue
        active = any(start <= left < end for start, end in ranges)
        if left == first and right == stop:
            yield active, segment
        else:
            yield (
                active,
                {
                    **segment,
                    "row_start": left + 1,
                    "row_end": right,
                    "row_count": right - left,
                    "from": day(rows[left]),
                    "to": day(rows[right - 1]),
                    "first_timestamp": rows[left][0],
                    "last_timestamp": rows[right - 1][0],
                },
            )


def _reconcile(
    store,
    materialized,
    reports,
    closed,
    closed_reports,
    cache,
    *,
    contested=None,
    retained=None,
    selection_output=None,
    instant_keys=None,
):
    scopes = select_scopes(reports, closed_reports)
    if contested:
        allowed = [span for spans in contested.values() for span in spans]
        scopes = {
            name: tuple(
                sorted(
                    {
                        (max(first, left), min(last, right))
                        for first, last in spans
                        for left, right in allowed
                        if max(first, left) <= min(last, right)
                    }
                )
            )
            for name, spans in scopes.items()
        }
        scopes = {name: spans for name, spans in scopes.items() if spans}
    equivalence, instants = {}, {} if instant_keys is None else instant_keys
    if not scopes:
        selected, corrections = (
            select_observations(materialized, reports, instant_keys=instants, equality_outcomes=equivalence)
            if reports
            else ({}, {})
        )
        if corrections:
            raise LineageError(f"selected observation mapping correction required for {sorted(corrections)}")
        if selection_output is not None:
            from .source_equivalence import project

            selection_output.update(
                project(
                    materialized,
                    selected,
                    {},
                    retained or {},
                    equivalence,
                    instants,
                    track_opened=selection_output.get("track_opened", True),
                )
            )
        return selected, {}, scopes
    inputs, selected_reports, positions, dates = dict(materialized), dict(reports), {}, {}
    for name, scope in scopes.items():
        inputs[name], selected_reports[name], positions[name], dates[name] = counterpart(
            store, closed[name], closed_reports[name], scope, cache
        )
    selected, corrections = select_observations(
        inputs, selected_reports, instant_keys=instants, equality_outcomes=equivalence
    )
    if corrections:
        raise LineageError(
            f"bounded observation selection requires mapping correction for {sorted(corrections)}; accepted peers retained"
        )
    if selection_output is not None:
        from .source_equivalence import project

        selection_output.update(
            project(
                inputs,
                selected,
                positions,
                retained or {},
                equivalence,
                instants,
                track_opened=selection_output.get("track_opened", True),
            )
        )
    changes = {name: selection_changes(selected[name], inputs[name].views, positions[name]) for name in scopes}
    for name in changes:
        changes[name]["dates"] = dates[name]
    return {name: selected[name] for name in reports}, changes, scopes


def apply_selection(document, changes, offsets):
    """Construct changed assignments only at the selected row positions."""
    result = deepcopy(document)
    for entry in result["adjustments"]:
        adjustment = entry["adjustment"]
        selected = changes[adjustment]
        offset = offsets[adjustment]
        segments = []
        for segment in entry["segments"]:
            indexes = sorted(
                index - offset
                for index in selected
                if segment["row_start"] <= index - offset <= segment["row_end"]
            )
            if not indexes:
                segments.append(segment)
                continue
            # A cut's adjacent timestamp comes from the selected partition
            # assembly; no numerical check or inferred market grid is involved.
            cursor, run = segment["row_start"], None
            for index in indexes:
                if cursor < index:
                    first, last = cursor, index - 1
                    first_day, first_time = (
                        (segment["from"], segment["first_timestamp"])
                        if first == segment["row_start"]
                        else changes["dates"][adjustment][first + offset]
                    )
                    last_day, last_time = changes["dates"][adjustment][last + offset]
                    segments.append(
                        {
                            **segment,
                            "row_start": first,
                            "row_end": last,
                            "row_count": last - first + 1,
                            "from": first_day,
                            "to": last_day,
                            "first_timestamp": first_time,
                            "last_timestamp": last_time,
                        }
                    )
                    run = None
                decision = selected[index + offset]
                fields = {key: decision[key] for key in ("observation_selection", "observation_tickers")}
                if (
                    run is not None
                    and run["row_end"] + 1 == index
                    and all(run[key] == value for key, value in fields.items())
                ):
                    run.update(
                        row_end=index,
                        row_count=run["row_count"] + 1,
                        to=decision["day"],
                        last_timestamp=decision["timestamp"],
                    )
                else:
                    run = {
                        **segment,
                        **fields,
                        "row_start": index,
                        "row_end": index,
                        "row_count": 1,
                        "from": decision["day"],
                        "to": decision["day"],
                        "first_timestamp": decision["timestamp"],
                        "last_timestamp": decision["timestamp"],
                    }
                    segments.append(run)
                cursor = index + 1
            if cursor <= segment["row_end"]:
                first_day, first_time = changes["dates"][adjustment][cursor + offset]
                segments.append(
                    {
                        **segment,
                        "row_start": cursor,
                        "row_count": segment["row_end"] - cursor + 1,
                        "from": first_day,
                        "first_timestamp": first_time,
                    }
                )
        entry["segments"] = segments
    return result


def retained_transfers(runs, changes):
    """Keep every exact unchanged selection, including either side of a cut."""
    result = []
    for run in runs:
        decisions = changes[run["adjustment"]]
        excluded = sorted(
            index
            for index, decision in decisions.items()
            if run["row_start"] <= index <= run["row_end"]
            and (
                decision["observation_selection"] == "superseded"
                or decision["observation_tickers"] != run["labels"]
            )
        )
        cursor = run["row_start"]
        for end in [*excluded, run["row_end"] + 1]:
            if cursor < end:
                dates = changes["dates"][run["adjustment"]]
                result.append(
                    {
                        **run,
                        "row_start": cursor,
                        "row_end": end - 1,
                        "from": run["from"] if cursor == run["row_start"] else dates[cursor][0],
                        "to": run["to"] if end - 1 == run["row_end"] else dates[end - 1][0],
                    }
                )
            cursor = end + 1
    return result
