"""Live check and correction produce an explicit, generation-bound append approval."""

from libs.alpaca_pricing_download.append import select_append_sources
from libs.alpaca_pricing_download.check_summary import assert_current, describe, read_snapshot
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.contract import validate_version as validate_pricing_version
from libs.alpaca_pricing_download.live_check import verify
from libs.alpaca_pricing_download.refresh import check_retained_source, prepare_refresh, replace_prepared

from .contract import validate_version
from .pipeline import run_lineage


def refresh_required_history(store, *, pricing_client, work_policy, end_date):
    """Accepted checks/corrections stay executable after summary removal."""
    empty = {"results": [], "requests": {}, "failed": 0, "checked": []}
    if not any(item.get("required_history_check") for item in work_policy.work.values()):
        return empty
    summarized = {entry["symbol"] for entry in read_snapshot(store)["entries"]}
    prepared, checked = [], {}
    for symbol, item in list(work_policy.work.items()):
        if symbol in summarized or not item.get("required_history_check") or not work_policy.allowed({symbol}):
            continue
        try:
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
    replaced = replace_prepared(store, prepared, client=pricing_client) if prepared else empty
    work_policy.record_results(replaced, stage="history_correction")
    checked.update(
        (item["symbol"], item["source_sha256"]) for item in replaced["results"] if item["status"] == "replaced"
    )
    work_policy.history_approved.update(checked)
    return {**replaced, "checked": sorted(checked)}


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
):
    """Check newly eligible members too; approval never expands with the summary."""
    validate_version()
    validate_pricing_version()
    verified, generations, checks = {}, {}, []
    pricing = {"results": [], "failed": 0, "requests": {}}
    lineage = {"results": [], "failed": 0}
    beyond, restricted = set(), set()
    for _ in range(100):
        snapshot = read_snapshot(store)
        candidates = []
        for entry in snapshot["entries"]:
            symbol = entry["symbol"]
            if symbols is not None and symbol not in symbols:
                continue
            if end_date and entry["asof"] > end_date:
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
        selected_snapshot = {**snapshot, "entries": candidates}
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
        handoff = verify(selected_snapshot, pricing_client._get)
        assert_current(store, snapshot["token"])
        checks.append(handoff)
        if progress:
            progress("checked", handoff["stats"])
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
        differing = {item["symbol"] for item in handoff["differences"]}
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
        replaced = replace_prepared(
            store,
            prepared,
            client=pricing_client,
            cohort_size=cohort_size,
            progress=progress,
            before_publish=lambda: assert_current(store, snapshot["token"]),
        )
        pricing["results"].extend(replaced["results"])
        pricing["failed"] += replaced["failed"]
        for key, value in replaced["requests"].items():
            pricing["requests"][key] = pricing["requests"].get(key, 0) + value
        if work_policy is not None:
            work_policy.record_results(replaced, stage="history_correction")
        selected = [item["symbol"] for item in replaced["results"] if item["status"] == "replaced"]
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
            )
            lineage["results"].extend(value["results"])
            lineage["failed"] += value["failed"]
        else:
            assert_current(store, snapshot["token"])
        if pricing["failed"] or lineage["failed"]:
            break
        ready = read_snapshot(store)
        current = {item["symbol"]: item for item in ready["entries"]}
        for entry in candidates:
            symbol = entry["symbol"]
            if current.get(symbol) == entry or symbol in selected:
                if symbol in current:
                    verified[symbol] = dict(current[symbol])
                    if symbol in selected_sources:
                        generations[symbol] = selected_sources[symbol]
        for item in replaced["results"]:
            if item["status"] == "replaced":
                generations[item["symbol"]] = item["source_sha256"]
        # Re-read only the compact summary. New members get a fresh comparison;
        # changing membership cannot promote an unchecked source into approval.
    else:
        raise DownloadError("history-check membership did not settle")
    try:
        ready = read_snapshot(store)
        summary = describe(ready)
    except DownloadError as error:
        summary = {"error": str(error)}
    failed = pricing["failed"] + lineage["failed"] + int("error" in summary)
    if not failed and on_ready is not None:
        on_ready(
            {
                **ready,
                "entries": [e for e in ready["entries"] if verified.get(e["symbol"]) == e],
                "approved_generations": generations,
            }
        )
    if not failed and work_policy is not None:
        work_policy.history_approved.update({s: generations[s] for s in verified if s in generations})
        # Standalone refresh owns its final lineage handoff too. Daily maintenance
        # supplies on_ready and stamps the checked generation after its append.
        finish = [
            s
            for s in verified
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
        "pricing": pricing,
        "lineage": lineage,
        "summary": summary,
        "already_beyond_target": sorted(beyond),
        "deferred_symbols": sorted(restricted),
    }
