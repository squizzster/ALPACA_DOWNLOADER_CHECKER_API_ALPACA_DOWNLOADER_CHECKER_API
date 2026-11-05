"""Live check and correction produce an explicit, generation-bound append approval."""

from libs.alpaca_pricing_download.acquire import MeasuredRequests
from libs.alpaca_pricing_download.append import select_append_sources
from libs.alpaca_pricing_download.check_summary import assert_current, describe
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.live_check import verify
from libs.alpaca_pricing_download.refresh import (
    check_retained_source,
    prepare_refresh,
    probe_late_bar_repairs,
    replace_prepared,
)
from libs.common.debug_trace import span, traced

from . import storage
from .context import accept_context, recover_and_accept_correlation_policy
from .pipeline import run_lineage


@traced("required_history")
def refresh_required_history(store, *, pricing_client, work_policy, end_date, context=None, progress=None):
    """Accepted checks/corrections stay executable after summary removal."""
    # This owner handles explicitly owed checks outside active summary membership.
    # Absence from that summary does not erase a correction or authorize checking
    # every other inactive ticker. Consume only the selected durable obligations.
    empty = {"results": [], "requests": {}, "failed": 0, "checked": []}
    if not any(item.get("required_history_check") for item in work_policy.work.values()):
        return empty
    context = accept_context(store, context, work_policy)
    recover_and_accept_correlation_policy(store, context)
    metrics = {}
    if callable(getattr(pricing_client, "execute_plan", None)):
        pricing_client = MeasuredRequests(pricing_client, metrics)
    summarized = {entry["symbol"] for entry in context.select_summary()["entries"]}
    prepared, checked = [], {}
    for symbol, item in list(work_policy.work.items()):
        if symbol in summarized or not item.get("required_history_check") or not work_policy.allowed({symbol}):
            continue
        try:
            if progress:
                progress("required_history", {"symbol": symbol, "state": "checking"})
            selected = check_retained_source(store, symbol, end_date, client=pricing_client)
        except (RuntimeError, OSError, ValueError) as error:
            work_policy.fail({symbol}, error, stage="history_check")
            continue
        if not selected["matched"] or (
            item.get("correction_id") and item["correction_id"] != selected["history_correction_id"]
        ):
            prepared.append(selected)
        else:
            checked[symbol] = selected["source_sha256"]
    prepared = [item for item in prepared if work_policy.allowed({item["symbol"]})]
    work_policy.require_correction(prepared)
    replaced = (
        replace_prepared(
            store,
            prepared,
            client=pricing_client,
            context=context.pricing(),
            progress=progress,
        )
        if prepared
        else empty
    )
    work_policy.record_results(replaced, stage="history_correction")
    checked.update(
        (item["symbol"], item["source_sha256"]) for item in replaced["results"] if item["status"] == "replaced"
    )
    work_policy.history_approved.update(checked)
    if progress:
        progress("required_history", {"state": "completed", "checked": len(checked), "requests": metrics})
    return {**replaced, "checked": sorted(checked), "requests": metrics or replaced["requests"]}


@traced("live_check_and_correction")
def refresh_existing(
    store,
    *,
    pricing_client,
    identity_client,
    progress=None,
    cohort_size=32,
    on_ready=None,
    end_date=None,
    evidence_client=None,
    register_discoveries=None,
    work_policy=None,
    symbols=None,
    late_bar_repairs=None,
    context=None,
):
    """Check newly eligible members too; approval never expands with the summary."""
    context = accept_context(store, context, work_policy)
    recover_and_accept_correlation_policy(store, context)
    pricing_context = context.pricing()
    verified, generations, checks, completed_replacements = {}, {}, [], {}
    pricing = {"results": [], "failed": 0, "requests": {}}
    lineage = {"results": [], "failed": 0}
    beyond, restricted = set(), set()
    snapshot = context.select_summary()
    with storage.publication_lock(context.accept_store().root):
        selected_index = context.select_index(context.accept_store().root)
    accepted_generations = {s: entry["source_sha256"] for s, entry in selected_index["symbols"].items()}

    def in_scope(entry):
        return (
            (symbols is None or entry["symbol"] in symbols)
            and (not end_date or entry["end_exclusive"][:10] <= end_date)
            and (work_policy is None or work_policy.allowed({entry["symbol"]}))
        )

    compared = {}
    if context.history_checks or context.history_comparisons:
        for entry in snapshot["entries"]:
            symbol = entry["symbol"]
            if not in_scope(entry):
                continue
            prior = context.history_checks.get(symbol)
            generation = accepted_generations.get(symbol)
            correction = work_policy.work.get(symbol, {}).get("correction_id") if work_policy else None
            comparison = context.history_comparisons.get(symbol)
            if prior is not None and prior == (entry, generation, correction):
                verified[symbol] = dict(entry)
                generations[symbol] = generation
            elif comparison is not None and comparison[:2] == (entry, generation):
                compared[symbol] = comparison[2]

    def replace_and_reconcile(prepared, baseline_token, *, serving_generation=None):
        def before_publish():
            assert_current(store, baseline_token)
            if serving_generation is not None:
                from libs.alpaca_serving_catalogue.storage import selector_state

                if selector_state(store)["generation"] != serving_generation:
                    raise DownloadError("selected serving catalogue changed before late-bar repair")

        replaced = replace_prepared(
            store,
            prepared,
            client=pricing_client,
            cohort_size=cohort_size,
            progress=progress,
            before_publish=before_publish,
            context=pricing_context,
        )
        pricing["results"].extend(replaced["results"])
        pricing["failed"] += replaced["failed"]
        for key, value in replaced["requests"].items():
            pricing["requests"][key] = pricing["requests"].get(key, 0) + value
        if work_policy is not None:
            work_policy.record_results(replaced, stage="history_correction")
        selected = [item["symbol"] for item in replaced["results"] if item["status"] == "replaced"]
        lineage_replacements = {}
        if selected:
            value = run_lineage(
                store,
                selected,
                identity_client=identity_client,
                pricing_client=pricing_client,
                end_date=end_date,
                evidence_client=evidence_client,
                register_discoveries=register_discoveries,
                work_policy=work_policy,
                context=context,
                progress=progress,
                on_pricing=progress,
                pricing_handoff={
                    item["symbol"]: item for item in replaced["results"] if "source_sha256" in item
                },
            )
            lineage["results"].extend(value["results"])
            lineage["failed"] += value["failed"]
            for item in value["results"]:
                lineage_replacements.update(item.get("pricing_replacements", {}))
        else:
            assert_current(store, baseline_token)
        for item in replaced["results"]:
            if item["status"] == "replaced":
                generations[item["symbol"]] = item["source_sha256"]
                completed_replacements[item["symbol"]] = item["source_sha256"]
        # Reconciliation may replace pricing again after observing a changed
        # identity. Only its completed producer handoff approves that generation.
        generations.update(lineage_replacements)
        completed_replacements.update(lineage_replacements)
        for symbol in set(selected) | lineage_replacements.keys():
            verified.pop(symbol, None)
            compared.pop(symbol, None)
            context.history_comparisons.pop(symbol, None)
        return selected

    def accept_publication_snapshot(ready, index):
        current = {entry["symbol"]: entry for entry in ready["entries"]}
        current_generations = {s: item["source_sha256"] for s, item in index["symbols"].items()}
        # Reconciliation may advance dependencies without reporting them as
        # replacements. Invalidate against the refreshed compact selectors,
        # not just the producer's list of explicitly replaced members.
        for symbol in list(compared):
            prior = context.history_comparisons.get(symbol)
            if prior is None or prior[:2] != (current.get(symbol), current_generations.get(symbol)):
                compared.pop(symbol)
        # A fresh comparison may bind a recovered pricing source ahead of the
        # lineage index. Only an index change invalidates that generation here;
        # append still checks the selected source under its reacquired lock.
        for symbol, entry in list(verified.items()):
            if (
                not in_scope(entry)
                or current.get(symbol) != entry
                or (
                    accepted_generations.get(symbol) != current_generations.get(symbol)
                    and generations.get(symbol, accepted_generations.get(symbol))
                    != current_generations.get(symbol)
                )
            ):
                verified.pop(symbol)
        accepted_generations.clear()
        accepted_generations.update(current_generations)
        # A successful producer correction survives an unrelated failure only
        # after lineage accepted that exact generation. This also replaces any
        # older comparison before late-bar repair rejoins the daily sequence.
        for entry in ready["entries"]:
            symbol = entry["symbol"]
            generation = completed_replacements.get(symbol)
            if (
                in_scope(entry)
                and generation is not None
                and index["symbols"].get(symbol, {}).get("source_sha256") == generation
            ):
                verified[symbol] = dict(entry)
                generations[symbol] = generation

    late_bar_check = {
        "catalogue_generation": None,
        "inspected_groups": 0,
        "selected_groups": 0,
        "selected_identities": 0,
        "selected_sources": 0,
        "requested_sources": 0,
        "requested_sessions": 0,
        "returned_sources": 0,
        "returned_sessions": 0,
        "stale_sources": [],
        "beyond_target": [],
        "outside_retained_span": 0,
        "requests": {},
    }
    if late_bar_repairs is not None:
        repair_sources = {}
        for symbol, item in late_bar_repairs["sources"].items():
            if symbols is not None and symbol not in symbols:
                continue
            if work_policy is not None and not work_policy.allowed({symbol}):
                restricted.add(symbol)
                continue
            repair_sources[symbol] = item
        repair_selection = {**late_bar_repairs, "sources": repair_sources}
        prepared, late_bar_check = probe_late_bar_repairs(
            store,
            repair_selection,
            client=pricing_client,
            end_date=end_date,
            progress=progress,
        )
        if work_policy is not None:
            work_policy.require_correction(prepared)
        if prepared:
            replace_and_reconcile(
                prepared,
                snapshot["token"],
                serving_generation=late_bar_repairs["catalogue_generation"],
            )
            if not pricing["failed"] and not lineage["failed"]:
                snapshot = context.select_summary()
                with storage.publication_lock(context.accept_store().root):
                    accept_publication_snapshot(snapshot, context.select_index(context.accept_store().root))
    for _ in range(100):
        if pricing["failed"] or lineage["failed"]:
            break
        candidates = []
        for entry in snapshot["entries"]:
            symbol = entry["symbol"]
            if symbols is not None and symbol not in symbols:
                continue
            if end_date and entry["end_exclusive"][:10] > end_date:
                beyond.add(symbol)
            elif work_policy is not None and not work_policy.allowed({symbol}):
                restricted.add(symbol)
            elif verified.get(symbol) != entry:
                candidates.append(entry)
        if not candidates:
            break
        if work_policy is not None:
            work_policy.require_maintenance(candidates, end_date)
        if register_discoveries is not None:
            register_discoveries(entry["symbol"] for entry in candidates)
        selected_snapshot = {**snapshot, "entries": [e for e in candidates if e["symbol"] not in compared]}
        required = {
            e["symbol"]
            for e in candidates
            if work_policy is not None and work_policy.work.get(e["symbol"], {}).get("required_history_check")
        }
        selected_sources = (
            select_append_sources(store, candidates, end_date, required_symbols=required)
            if on_ready or required
            else {}
        )
        assert_current(store, snapshot["token"])
        if progress:
            progress("checking", {"tickers": len(candidates)})
        # Live check owns remote comparison of this accepted summary snapshot.
        # DO NOT reopen ticker histories to re-prove the stored baseline here.
        with span("live_history_check", symbols=len(selected_snapshot["entries"])) as check_trace:
            handoff = verify(selected_snapshot, pricing_client._get)
            check_trace.update(complete=handoff["complete"], **handoff["stats"], requests=handoff["requests"])
        assert_current(store, snapshot["token"])
        # Live-check completion is per symbol. A peer's missing endpoint or
        # failed request cannot revoke this producer's completed comparison.
        # Retain only the accepted summary and selected generation, without
        # opening validators/payloads or granting unfinished corrections.
        matched = set(handoff["matched"])
        differing = {item["symbol"] for item in handoff["differences"]}
        completed = matched | differing
        for entry in candidates:
            symbol = entry["symbol"]
            generation = selected_sources.get(symbol, accepted_generations.get(symbol))
            if symbol in completed and generation is not None:
                context.history_comparisons[symbol] = (dict(entry), generation, symbol in differing)
                compared[symbol] = symbol in differing
                if symbol in matched:
                    generations[symbol] = generation
            if (
                symbol in matched
                and generation is not None
                and not (work_policy is not None and work_policy.work.get(symbol, {}).get("correction_id"))
            ):
                context.history_checks[symbol] = (dict(entry), generation, None)
        checks.append(handoff)
        if progress:
            progress(
                "checked",
                {**handoff["stats"], "requests": handoff["requests"], "seconds": handoff["live_seconds"]},
            )
        if not handoff["complete"]:
            if work_policy is not None:
                for failure in handoff["failures"]:
                    work_policy.fail(failure["symbols"], failure, stage="history_check")
                work_policy.fail(
                    handoff["missing"], {"reason": "missing live comparison bar"}, stage="history_check"
                )
            raise DownloadError(
                f"live check incomplete: {handoff['stats']['missing']} missing, "
                f"{handoff['stats']['failed']} failed; no replacements started"
            )
        differing = {entry["symbol"] for entry in candidates if compared.get(entry["symbol"])}
        required_corrections = {
            entry["symbol"]
            for entry in candidates
            if work_policy is not None and work_policy.work.get(entry["symbol"], {}).get("correction_id")
        }
        prepared = prepare_refresh(store, differing | required_corrections)
        if work_policy is not None:
            prepared = [
                item
                for item in prepared
                if item["symbol"] in differing
                or item["history_correction_id"] != work_policy.work[item["symbol"]]["correction_id"]
            ]
            work_policy.require_correction(prepared)
        assert_current(store, snapshot["token"])
        selected = replace_and_reconcile(prepared, snapshot["token"])
        if pricing["failed"] or lineage["failed"]:
            break
        ready = context.select_summary()
        current = {item["symbol"]: item for item in ready["entries"]}
        for entry in candidates:
            symbol = entry["symbol"]
            compared.pop(symbol, None)
            if current.get(symbol) == entry or symbol in selected:
                if symbol in current:
                    verified[symbol] = dict(current[symbol])
                    if symbol in selected_sources and symbol not in selected:
                        generations[symbol] = selected_sources[symbol]
        if prepared:
            with storage.publication_lock(context.accept_store().root):
                accept_publication_snapshot(ready, context.select_index(context.accept_store().root))
        snapshot = ready
        # Refresh membership only after publication. New members get a fresh comparison;
        # changing membership cannot promote an unchecked source into approval.
    else:
        raise DownloadError("history-check membership did not settle")
    try:
        if pricing["failed"] or lineage["failed"]:
            ready = context.select_summary()
        else:
            assert_current(store, snapshot["token"])
            ready = snapshot
        summary = describe(ready)
    except DownloadError as error:
        summary = {"error": str(error)}
    failed = pricing["failed"] + lineage["failed"] + int("error" in summary)
    if "error" not in summary:
        with storage.publication_lock(context.accept_store().root):
            index = context.select_index(context.accept_store().root)
        accept_publication_snapshot(ready, index)
        for entry in ready["entries"]:
            symbol = entry["symbol"]
            generation = index["symbols"].get(symbol, {}).get("source_sha256")
            if in_scope(entry) and verified.get(symbol) == entry and generation is not None:
                # A covered source needs no append manifest read. Its completed
                # index generation and checked summary are sufficient here.
                if generations.get(symbol, accepted_generations.get(symbol)) == generation:
                    correction = work_policy.work.get(symbol, {}).get("correction_id") if work_policy else None
                    context.history_checks[symbol] = (dict(entry), generation, correction)
    if not failed and on_ready is not None:
        on_ready(
            {
                **ready,
                "entries": [e for e in ready["entries"] if in_scope(e) and verified.get(e["symbol"]) == e],
                "approved_generations": generations,
            }
        )
    if not failed and work_policy is not None:
        # A completed pricing replacement approves its generation even when
        # lineage removes the now-inactive symbol from the active summary.
        work_policy.history_approved.update(generations)
        # Standalone refresh owns its final lineage handoff too. Daily maintenance
        # supplies on_ready and stamps the checked generation after its append.
        finish = [
            s
            for s in generations
            if s in work_policy.work
            and work_policy.work[s].get("required_history_check")
            and work_policy.allowed({s})
        ]
        if on_ready is None and finish:
            value = run_lineage(
                store,
                finish,
                identity_client=identity_client,
                pricing_client=pricing_client,
                end_date=end_date,
                evidence_client=evidence_client,
                register_discoveries=register_discoveries,
                work_policy=work_policy,
                context=context,
                progress=progress,
                on_pricing=progress,
            )
            lineage["results"].extend(value["results"])
            lineage["failed"] += value["failed"]
            failed += value["failed"]
    stats, requests = {}, {}
    for check in checks:
        for key, value in check["stats"].items():
            stats[key] = stats.get(key, 0) + value
        for key, value in check["requests"].items():
            requests[key] = requests.get(key, 0) + value
    return {
        "complete": failed == 0,
        "failed": failed,
        "check": {"stats": stats, "requests": requests, "seconds": sum(c["live_seconds"] for c in checks)},
        "late_bar_check": late_bar_check,
        "pricing": pricing,
        "lineage": lineage,
        "summary": summary,
        "already_beyond_target": sorted(beyond),
        "deferred_symbols": sorted(restricted),
    }
