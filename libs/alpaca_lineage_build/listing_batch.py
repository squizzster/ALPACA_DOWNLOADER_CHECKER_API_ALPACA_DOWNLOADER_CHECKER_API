"""Share bounded SIP requests across literal ticker/date witnesses selected by completed screens."""

from collections import Counter, defaultdict
from concurrent.futures import as_completed

from libs.alpaca_data_api_solver import instant
from libs.alpaca_data_api_solver.listing_trade_batch import opening_second
from libs.alpaca_security_lineage.listing_trade_batch import (
    MAX_BATCH_SYMBOLS,
    batch_trade_points,
    sparse_day_points,
)
from libs.common.provider_http import balanced_batches

from .listing_observations import fatal_acquisition_error


def _cluster(labels, names):
    """Find the minute containing auction timestamps from the most distinct outstanding labels."""
    points = sorted((int(instant(t).timestamp()), t, name) for name in names for t in labels[name])
    counts = Counter()
    left = 0
    best = 1
    chosen = None
    for stamp, text, name in points:
        counts[name] += 1
        while stamp - points[left][0] >= 60:
            removed = points[left][2]
            counts[removed] -= 1
            if not counts[removed]:
                del counts[removed]
            left += 1
        if len(counts) > best:
            best = len(counts)
            chosen = points[left][1], text
    return chosen


def _observe(args, cache):
    labels, day, window = args
    if window == "sparse_day":
        pending = labels
        while len(pending) >= 2:
            if cache.has("sparse_request", [day, pending]):
                break
            rows = sparse_day_points(pending, day, cache.http())
            cache.save_sparse(pending, day, rows)
            if not rows:
                break
            # A truncated page can be dominated by one active label. Remove every label
            # already observed before requesting the remainder as a new bounded subset.
            pending = [s for s in pending if s not in rows]
        return
    key = window if isinstance(window, str) else list(window)
    remaining = cache.unattempted(labels, day, key)
    if len(remaining) < (8 if window == "regular_open_minute" else 2):
        return
    if window == "regular_open_minute":
        params = {"regular_minute": True}
    elif isinstance(window, str):
        params = {"second": window}
    else:
        params = {"window_start": window[0], "window_end": window[1]}
    rows = batch_trade_points(remaining, day, cache.http(), **params)
    cache.save_window(remaining, day, key, rows)


def _jobs(by_date, cache, stage):
    for day, labels in sorted(by_date.items()):
        remaining = sorted(s for s in labels if (s, day) not in cache.retained_points)
        if len(remaining) < 2:
            continue
        if stage == "dense_windows":
            if len(remaining) < 8:
                continue
            counts = Counter(t for symbol in remaining for t in labels[symbol])
            window = min(counts, key=lambda t: (-counts[t], t)) if counts else opening_second(day)[0]
        elif stage == "opening_minutes":
            if len(remaining) < 8:
                continue
            window = "regular_open_minute"
        elif stage == "auction_windows":
            span = _cluster(labels, remaining)
            if span is None:
                continue
            window = span[0] if span[0] == span[1] else span
        else:
            window = "sparse_day"
        for batch in balanced_batches(remaining, MAX_BATCH_SYMBOLS):
            yield batch, day, window


def prepare_points(screens, cache, pool, *, progress=None, checkpoint=None):
    by_date = defaultdict(dict)
    for screen in screens:
        for day in screen.initial_sessions:
            by_date[day].setdefault(screen.symbol, set()).update(screen.auction_seconds.get(day, ()))
    cache.warm_points((s, d) for d, labels in by_date.items() for s in labels)
    if checkpoint:
        checkpoint()
    for stage in ("dense_windows", "opening_minutes", "auction_windows", "sparse_days"):
        jobs = [pool.submit(_observe, job, cache) for job in _jobs(by_date, cache, stage)]
        for i, job in enumerate(as_completed(jobs), 1):
            try:
                job.result()
            except (ValueError, RuntimeError, OSError) as error:
                if fatal_acquisition_error(error):
                    for pending in jobs:
                        pending.cancel()
                    raise
                # These windows optimize the selected point requests. A failed window
                # leaves its missing labels pending for the next route or individual lookup.
                with cache.lock:
                    cache.stats["shared_window_failures"] += 1
            if progress and (i % 32 == 0 or i == len(jobs)):
                progress(stage, i, len(jobs), cache.requests)
        if jobs and checkpoint:
            checkpoint()
