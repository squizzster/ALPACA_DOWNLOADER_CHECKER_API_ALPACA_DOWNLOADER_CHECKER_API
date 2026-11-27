"""Explicit historical listing migration. Released pricing and ownership are inputs."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing, contextmanager
from pathlib import Path

from libs.alpaca_serving_catalogue.listing_selection import select
from libs.common.store_lock import operation_lock

from .historical_listing import RangeScreen, ScreenCalendarChanged, assess_range, screen_range
from .listing_observations import Observations, fatal_acquisition_error
from .listing_ranges import _work
from .listing_staged_ranges import select_staged_ranges

RULE_VERSION = 1
BATCH_SIZE = 64
SCREEN_COHORT = 20000


@contextmanager
def _workers(count):
    pool = ThreadPoolExecutor(max_workers=count)
    try:
        yield pool
    finally:
        # Finish in-flight requests and preserve their accepted transfers on cancellation.
        pool.shutdown(wait=True, cancel_futures=True)


def _unresolved(symbol, first, last):
    return {
        "symbol": symbol,
        "first_session": first,
        "last_session": last,
        "status": "unresolved",
        "observations": [],
        "changes": [],
        "limits": ["competing_published_owners"],
        "diagnostics": {"trade_sessions_sampled": 0},
    }


def run(
    store,
    *,
    symbols=None,
    public_ids=None,
    all_published=False,
    plan=False,
    apply=False,
    stage_auctions=False,
    publish_staged=False,
    workers=4,
    limit=None,
    client_factory,
    progress=None,
):
    if sum(map(bool, (plan, apply, stage_auctions, publish_staged))) > 1:
        raise ValueError("choose a single historical listing operation")
    store = Path(store).resolve()
    with operation_lock(store, exclusive=not plan, wait=not plan):
        inventory = {}
        state, groups, owners, facts = select(store, symbols, public_ids, all_published, counts=inventory)
        recovered_owners = {}
        if apply or publish_staged:
            from libs.alpaca_serving_catalogue.historical_listings import project

            from .historical_listing_publication import recover_pending

            recovery = recover_pending(store, groups, accepted_state=state)
            recovered_owners = recovery.get("affected_public_ids", {})
            for key, handoff in recovery["retargeted"].items():
                for owner, sections in project(handoff["historical_listings"], groups[key]["ticker_ranges"]):
                    facts[key].setdefault(owner, {}).update(sections)
        selected = list(_work(groups, owners, facts, public_ids, recovered_owners_by_group=recovered_owners))
        total_outstanding = len(selected)
        if limit:
            selected = selected[:limit]
        n_ranges = sum(len(w) for _, w in selected)
        summary = {
            "mode": next(
                name
                for name, active in (
                    ("plan", plan),
                    ("apply", apply),
                    ("stage_auctions", stage_auctions),
                    ("publish_staged", publish_staged),
                    ("dry_run", True),
                )
                if active
            ),
            "catalogue_at_selection": state["generation"],
            "online_groups": sum(bool(g["ticker_ranges"]) for g in groups.values()),
            "online_identities": sum(len(g["ticker_ranges"]) for g in groups.values()),
            "groups_with_outstanding_ranges": total_outstanding,
            "groups_selected": len(selected),
            "ranges_selected": n_ranges,
            "identities_with_selected_ranges": len({s[0] for _, work in selected for s in work}),
            **inventory,
            "planned_pricing_payload_reads": 0,
        }
        if not selected:
            return {**summary, "complete": True, "provider_requests": 0, "groups_published": 0}
        with closing(Observations(store, client_factory, readonly=plan or publish_staged)) as cache:
            cache.seed(facts)
            selected = select_staged_ranges(selected, cache, RULE_VERSION)
            n_ranges = sum(len(work) for _, work in selected)
            summary["ranges_selected"] = n_ranges
            if plan:
                cached = sum(len(g["staged_listing_assessment_keys"]) for g, _ in selected)
                carried = sum(len(g.get("carried_listing_assessments", {})) for g, _ in selected)
                conflicts = sum(s[-1] for _, w in selected for s in w)
                return {
                    **summary,
                    "staged_ranges": cached,
                    "ranges_with_competing_owners": conflicts,
                    "carried_ranges": carried,
                    "missing_assessments": n_ranges - cached - carried,
                    "provider_requests": 0,
                    "groups_published": 0,
                    "complete": True,
                }
            if publish_staged:
                return _publish_staged(store, selected, cache, state, summary, progress)
            if stage_auctions:
                return _stage_screens(selected, cache, workers, progress, summary)
            return _execute(store, selected, cache, workers, apply, progress, not all_published, summary, state)


def _publish_staged(store, selected, cache, state, summary, progress):
    from .historical_listing_publication import publish_many

    published = waiting = assessed = 0
    for start in range(0, len(selected), BATCH_SIZE):
        completed = []
        for group, work in selected[start : start + BATCH_SIZE]:
            staged = group["staged_listing_assessment_keys"]
            carried = group["carried_listing_assessments"]
            if any(scope not in carried and scope not in staged for scope in work):
                waiting += 1
                continue
            rows = defaultdict(list)
            for scope in work:
                rows[scope[0]].append(carried[scope] if scope in carried else _stage_body(cache, staged[scope]))
            completed.append((group, dict(rows)))
            assessed += len(work)
        if completed:
            published += publish_many(store, completed, accepted_state=state)["published"]
        if progress:
            progress("groups", min(start + BATCH_SIZE, len(selected)), len(selected), 0)
    return {
        **summary,
        "complete": waiting == 0,
        "groups_published": published,
        "assessed_ranges": assessed,
        "groups_waiting_for_assessment": waiting,
        "provider_requests": 0,
    }


def _stage_body(cache, key):
    row = cache.get("assessment", key)
    if row is None:
        raise RuntimeError("selected historical listing assessment disappeared during the store operation")
    return row


def _screen(cache, symbol, first, last):
    key = [symbol, first, last]
    doc = cache.get("screen", key)
    if doc is not None:
        try:
            return RangeScreen.from_document(doc)
        except ScreenCalendarChanged:
            # Only the derived screen is invalid. Its validated literal pages remain applicable.
            screen = screen_range(
                symbol, first, last, lambda: cache.auction_days(symbol, first, last, selected_screen=doc)
            )
            cache.replace_screen(key, screen.to_document())
            return screen
    screen = screen_range(symbol, first, last, lambda: cache.auction_days(symbol, first, last))
    cache.put("screen", key, screen.to_document())
    return screen


def _stage_screens(selected, cache, workers, progress, summary):
    requests = []
    for group, work in selected:
        for scope in work:
            if scope in group.get("carried_listing_assessments", {}):
                continue
            _, symbol, first, last, competing = scope
            if (
                not competing
                and scope not in group["staged_listing_assessment_keys"]
                and not cache.has("screen", [symbol, first, last])
            ):
                requests.append((symbol, first, last))

    def acquire(scope):
        screen = _screen(cache, *scope)
        return len(screen.initial_sessions), len(screen.batch_sessions)

    candidates = eligible = 0
    failures = {}
    with _workers(workers) as pool:
        jobs = {pool.submit(acquire, key): key for key in dict.fromkeys(requests)}
        for i, job in enumerate(as_completed(jobs), 1):
            try:
                points, batch = job.result()
            except (RuntimeError, OSError, ValueError) as error:
                if fatal_acquisition_error(error):
                    for remaining in jobs:
                        remaining.cancel()
                    raise
                failures["/".join(jobs[job])] = str(error)
            else:
                candidates += points
                eligible += batch
            if progress and (i % 16 == 0 or i == len(jobs)):
                progress("auction_screens", i, len(jobs), cache.requests)
    return {
        **summary,
        "complete": not failures,
        "screens_staged": len(requests) - len(failures),
        "failed_ranges": len(failures),
        **({"failures": dict(list(failures.items())[:25])} if failures else {}),
        "candidate_trade_dates": candidates,
        "with_regular_open_auction": eligible,
        "groups_published": 0,
        "provider_requests": cache.requests,
    }


def _assess(group_work, cache, held=None, screens=None):
    group, work = group_work
    results = defaultdict(list)
    for scope in work:
        pid, symbol, first, last, competing = scope
        row = group.get("carried_listing_assessments", {}).get(scope)
        if row is None:
            row = (
                cache.get("assessment", cache.assessment_key(RULE_VERSION, scope))
                if held is None
                else held.get(scope)
            )
        if row is None:
            if competing:
                row = _unresolved(symbol, first, last)
            else:
                screen = _screen(cache, symbol, first, last) if screens is None else screens[symbol, first, last]
                row = assess_range(
                    symbol, first, last, cache, screen=screen, retained_points=cache.owner_points(scope)
                )
            cache.put("assessment", cache.assessment_key(RULE_VERSION, scope), row)
        results[pid].append(row)
    return group, dict(results)


def _prepare(cohort, cache, pool, progress):
    held = {}
    needed = set()
    for g, work in cohort:
        for scope in work:
            row = g.get("carried_listing_assessments", {}).get(scope)
            if row is None and scope in g["staged_listing_assessment_keys"]:
                row = _stage_body(cache, g["staged_listing_assessment_keys"][scope])
            if row is not None:
                held[scope] = row
            elif not scope[-1]:
                needed.add(scope[1:4])
    jobs = {pool.submit(_screen, cache, *key): key for key in sorted(needed)}
    screens, failures = {}, {}
    for i, job in enumerate(as_completed(jobs), 1):
        key = jobs[job]
        try:
            screens[key] = job.result()
        except (ValueError, RuntimeError, OSError) as error:
            if fatal_acquisition_error(error):
                for pending in jobs:
                    pending.cancel()
                raise
            failures[key] = str(error)
        if progress and (i % 64 == 0 or i == len(jobs)):
            progress("auction_screens", i, len(jobs), cache.requests)
    return held, screens, failures


def _execute(store, selected, cache, workers, apply, progress, detail, summary, state):
    if apply:
        from .historical_listing_publication import publish_many

    from .listing_batch import prepare_points

    counts = Counter()
    failures = {}
    records = {}
    published = processed = changes = exact = 0
    with _workers(workers) as pool:
        for start in range(0, len(selected), SCREEN_COHORT):
            cohort = selected[start : start + SCREEN_COHORT]
            held, screens, screen_failures = _prepare(cohort, cache, pool, progress)
            waiting = {}
            needs = {}
            for g, work in cohort:
                key = g["group_key"]
                failed = next((screen_failures[s[1:4]] for s in work if s[1:4] in screen_failures), None)
                if failed:
                    failures[key] = failed
                    processed += 1
                    continue
                waiting[key] = g, work
                needs[key] = {
                    (s[1], day)
                    for s in work
                    if s not in held and not s[-1]
                    for day in screens[s[1:4]].initial_sessions
                }

            def finish_ready(*, finish_remaining=False):
                nonlocal published, processed, changes, exact
                ready = [
                    key
                    for key in waiting
                    if finish_remaining or all(pair in cache.retained_points for pair in needs[key])
                ]
                for offset in range(0, len(ready), BATCH_SIZE):
                    keys = ready[offset : offset + BATCH_SIZE]
                    futures = [
                        (key, pool.submit(_assess, waiting.pop(key), cache, held, screens)) for key in keys
                    ]
                    completed = []
                    for key, job in futures:
                        try:
                            group, assessments = job.result()
                        except (OSError, RuntimeError, ValueError) as error:
                            if fatal_acquisition_error(error):
                                raise
                            failures[key] = str(error)
                            continue
                        completed.append((group, assessments))
                        if detail:
                            records.update(assessments)
                        for rows in assessments.values():
                            counts.update(row["status"] for row in rows)
                            for row in rows:
                                changes += len(row["changes"])
                                exact += sum("effective_session" in c for c in row["changes"])
                    if apply and completed:
                        published += publish_many(store, completed, accepted_state=state)["published"]
                    processed += len(keys)
                    if progress:
                        progress("groups", processed, len(selected), cache.requests)

            # Each phase can finish whole identities. Checkpoint those results before
            # moving on to the rarer windows or any missing single-symbol witnesses.
            prepare_points(screens.values(), cache, pool, progress=progress, checkpoint=finish_ready)
            finish_ready(finish_remaining=True)
    return {
        **summary,
        "complete": not failures,
        "groups_published": published,
        "assessed_ranges": sum(counts.values()),
        "outcomes": dict(counts),
        "within_ticker_changes": changes,
        "changes_with_effective_session": exact,
        "provider_requests": cache.requests,
        "acquisition": dict(cache.stats),
        "failed_groups": len(failures),
        **({"failures": dict(list(failures.items())[:25])} if failures else {}),
        **({"assessments": records} if detail else {}),
    }
