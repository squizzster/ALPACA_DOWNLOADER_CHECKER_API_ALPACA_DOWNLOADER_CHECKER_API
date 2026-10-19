"""Live same-span refresh: trusted summary -> replacement -> lineage -> summary."""

from libs.alpaca_pricing_download.check_summary import assert_current, describe, read_snapshot
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.contract import validate_version as validate_pricing_version
from libs.alpaca_pricing_download.live_check import verify
from libs.alpaca_pricing_download.refresh import prepare_refresh, replace_prepared

from .contract import validate_version
from .pipeline import run_lineage


def refresh_existing(store, *, pricing_client, identity_client, progress=None, cohort_size=32, on_ready=None):
    """Always calculate a fresh comparison; never load a saved experimental result.

    Only differing symbols select price replacement. Lineage consumes their normal
    pending notices and affected groups, with further pricing acquisition disabled.
    """
    validate_version()
    validate_pricing_version()
    snapshot = read_snapshot(store)
    if progress:
        progress("checking", {"tickers": len(snapshot["entries"])})
    handoff = verify(snapshot, pricing_client._get)
    assert_current(store, snapshot["token"])
    if progress:
        progress("checked", handoff["stats"])
    if not handoff["complete"]:
        raise DownloadError(
            f"live check incomplete: {handoff['stats']['missing']} missing, "
            f"{handoff['stats']['failed']} failed; no replacements started"
        )
    prepared = prepare_refresh(store, [item["symbol"] for item in handoff["differences"]])
    assert_current(store, snapshot["token"])
    pricing = replace_prepared(
        store,
        prepared,
        client=pricing_client,
        cohort_size=cohort_size,
        progress=progress,
        before_publish=lambda: assert_current(store, snapshot["token"]),
    )
    selected = [item["symbol"] for item in pricing["results"] if item["status"] == "replaced"]
    lineage = {"results": [], "failed": 0}
    if selected:
        if progress:
            progress("lineage", {"selected": len(selected), "processed": 0})
        processed = set()

        def observed(**event):
            processed.update(event["receipts"])
            if progress:
                progress("lineage", {"selected": len(selected), "processed": len(processed)})

        lineage = run_lineage(
            store, selected, identity_client=identity_client, pricing_client=None, observer=observed
        )
    # Producers maintain the boundary. This reads only its final small snapshot.
    try:
        ready = read_snapshot(store)
        if not selected:
            assert_current(store, snapshot["token"])
        summary = describe(ready)
    except DownloadError as error:
        summary = {"error": str(error)}
    failed = pricing["failed"] + lineage["failed"] + int("error" in summary)
    if not failed and on_ready is not None:
        on_ready(ready)
    return {
        "complete": failed == 0,
        "failed": failed,
        "check": {
            "stats": handoff["stats"],
            "requests": handoff["requests"],
            "seconds": handoff["live_seconds"],
        },
        "pricing": pricing,
        "lineage": lineage,
        "summary": summary,
    }
