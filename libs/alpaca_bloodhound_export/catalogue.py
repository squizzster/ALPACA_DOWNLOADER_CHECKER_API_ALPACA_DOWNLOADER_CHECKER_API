"""Canonical CLI adapter for the shared online-catalogue Bloodhound converter."""

from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_heartbeat import diagnostic


def export_catalogue(args):
    from bloodhound_app.alpaca_to_bloodhound import convert

    reporter = getattr(args, "_lifecycle", None)

    def progress(message, *, warning=False):
        if warning:
            diagnostic(reporter, message, level="warning")
        elif reporter is not None:
            reporter.update("EXPORT", message=message, context={"estimated_remaining_seconds": "TBD"})

    if reporter is not None:
        reporter.update("CATALOGUE", message="Opening the published online catalogue.")
    options = SimpleNamespace(
        store=Path(args.store),
        data_in=None,
        blood_out=args.output,
        public_id=args.public_id,
        symbol=[symbol.strip().upper() for symbol in args.symbol] if args.symbol else None,
        wait=args.wait,
        cache_bytes=args.cache_mib * 1024**2,
        split_source="infer",
        split_events=None,
        action_coverage=None,
        horizon=None,
        price_basis="all",
        metadata=None,
        unknown_security_type="unknown",
        unknown_exchange="unknown",
        start_date=args.start,
        end_date=args.end,
        on_error="skip" if args.include_all_passed else "fail",
        compression="zstd",
        overwrite=args.replace,
        quiet=False,
        memory_mib=args.memory_mib,
        _progress=progress,
    )
    report = convert(options)
    return {
        "output": report["output_file"],
        "report": report["output_file"] + ".report.json",
        "rows": report["output_rows"],
        "identities": report["histories_exported"],
        "rejected": report["histories_rejected"],
        "status": report["status"],
        "catalogue_generation": report["catalogue"]["generation"],
        "diagnostic_errors": [],
    }
