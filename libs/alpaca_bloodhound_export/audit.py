"""Offline structural and price-continuity checks for a native Bloodhound export."""

import csv
import hashlib
import json
import platform
import time
from collections import Counter
from contextlib import ExitStack, closing
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import numpy as np
import pyarrow
import pyarrow.parquet as pq

from libs.common.us_equity_market_session_calendar import US_MARKET_HOURS_FILE_PATH

from .audit_report import write_report

CALENDAR_SHA256 = "8b17aab21f9bec663e370ce6a41aa402e456849fd371ddbd9d3342f8a080bbf6"
PRICES = ["adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close", "raw_close"]
POSITIVE = PRICES + ["adjustment_ratio", "split_adjustment_ratio"]
NONNEGATIVE = ["volume", "dollar_volume"]
LABELS = ["symbol", "security_type", "exchange"]
OPTIONAL = ["cik", "composite_figi", "share_class_figi", "company_name"]
SCHEMA = {
    "date": "timestamp[ns]",
    "ohlcv_series_id": "int64",
    **dict.fromkeys(["symbol", *OPTIONAL, *LABELS[1:]], "string"),
    **dict.fromkeys(
        PRICES + ["volume", "dollar_volume", "adjustment_ratio", "split_adjustment_ratio"], "double"
    ),
}
ADJ_2X = "(adjusted_close >= 2*prev_adj OR prev_adj >= 2*adjusted_close)"
SPLIT_2X = "(split_factor >= 2*prev_split OR prev_split >= 2*split_factor)"
RAW_MODEST = "(raw_close BETWEEN 0.75*prev_raw AND 1.25*prev_raw)"
FLAGS = {
    "split_step_1pct": "abs(split_log) >= ln(1.01)",
    "split_rebase_up_2x": "split_factor >= 2*prev_split",
    "basis_up_2x_raw_within_25pct": (
        f"split_factor >= 2*prev_split AND adjusted_close >= 2*prev_adj AND {RAW_MODEST}"
    ),
    "basis_up_2x_on_ticker_change": (
        f"ticker_change AND split_factor >= 2*prev_split AND adjusted_close >= 2*prev_adj AND {RAW_MODEST}"
    ),
    "basis_shift_2x_without_large_raw_move": (
        f"{SPLIT_2X} AND {ADJ_2X} AND {RAW_MODEST} AND abs(adj_log-raw_log-split_log) <= ln(1.05)"
    ),
    "raw_step_2x": "raw_close >= 2*prev_raw OR prev_raw >= 2*raw_close",
    "adjusted_step_50pct_up_or_third_down": "abs(adj_log) >= ln(1.5)",
    "adjusted_step_2x": ADJ_2X,
    "adjusted_step_10x": "adjusted_close >= 10*prev_adj OR prev_adj >= 10*adjusted_close",
    "adjusted_step_100x": "adjusted_close >= 100*prev_adj OR prev_adj >= 100*adjusted_close",
    "large_adjusted_step_on_liquid_bar_pair": (
        f"{ADJ_2X} AND raw_close >= 5 AND prev_raw >= 5 AND dollar_volume >= 5000000 AND prev_dollar >= 5000000"
    ),
    "basis_offset_step_1pct": "abs(adj_log-raw_log-split_log) >= ln(1.01)",
    "basis_offset_step_10pct": "abs(adj_log-raw_log-split_log) >= ln(1.10)",
    "isolated_adjusted_dislocation_2x": (
        f"next_seq = seq + 1 AND {ADJ_2X} AND abs(ln(next_adj)-ln(prev_adj)) <= ln(1.20)"
    ),
    "intraday_range_2x": "adjusted_high >= 2*adjusted_low",
    "overnight_adjusted_step_2x": "adjusted_open >= 2*prev_adj OR prev_adj >= 2*adjusted_open",
    "dollar_volume_spike_20x": ("prev_dollar >= 1000000 AND dollar_volume >= 20*prev_dollar"),
    "zero_volume_price_change": "volume = 0 AND abs(raw_log) >= ln(1.01)",
    "factor_change_raw_flat": "abs(split_log) >= ln(1.1) AND raw_close = prev_raw",
}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def rows(connection, sql, params=()):
    cursor = connection.execute(sql, params)
    fields = [item[0] for item in cursor.description]
    return [dict(zip(fields, row)) for row in cursor.fetchall()]


def copy(connection, target, sql):
    connection.execute(f"COPY ({sql}) TO {literal(target)} (FORMAT CSV, HEADER)")


def csv_rows(path):
    with path.open(newline="") as stream:
        yield from csv.DictReader(stream)


def key_order(parquet):
    inversions = 0
    repeats = 0
    count = 0
    previous = None
    for batch in parquet.iter_batches():
        if not batch.num_rows:
            continue
        ids = batch.column(1).to_numpy()
        dates = batch.column(0).to_numpy().astype("int64")
        if previous is not None:
            current = (ids[0], dates[0])
            inversions += current < previous
            repeats += current == previous
        equal_ids = ids[1:] == ids[:-1]
        inversions += np.count_nonzero((ids[1:] < ids[:-1]) | (equal_ids & (dates[1:] < dates[:-1])))
        repeats += np.count_nonzero(equal_ids & (dates[1:] == dates[:-1]))
        previous = (ids[-1], dates[-1])
        count += len(ids)
    return {
        "decoded_keys": count,
        "key_order_inversions": int(inversions),
        "adjacent_key_repeats_in_file": int(repeats),
    }


def audit(parquet, output, *, threads=4, series_id=None, progress=lambda message: None):
    payload = Path(parquet).resolve(strict=True)
    output = Path(output).absolute()
    report_file = payload.with_name(payload.name + ".report.json")
    try:
        export = json.loads(report_file.read_text())
        if export["input_format"] != "alpaca_online_catalogue":
            raise ValueError("expected a Bloodhound online catalogue conversion report")
        histories = [item for item in export["histories"] if item["status"] == "exported"]
        if not histories:
            raise ValueError("conversion report contains no exported histories")
        output.mkdir(parents=True, exist_ok=False)
        with ExitStack() as stack:
            return _scan(payload, output, report_file, export, histories, threads, series_id, progress, stack)
    except (KeyError, TypeError, duckdb.Error) as error:
        raise ValueError(f"cannot audit Bloodhound export: {error}") from error


def _scan(payload, output, report_file, export, histories, threads, series_id, progress, stack):
    started = time.monotonic()
    record_count = export["output_rows"]
    date_filter = export["date_filter"]
    first = min(date.fromisoformat(h["first_session"]) for h in histories)
    last = max(date.fromisoformat(h["last_session"]) for h in histories)
    if date_filter["start"]:
        first = max(first, date.fromisoformat(date_filter["start"]))
    if date_filter["end"]:
        last = min(last, date.fromisoformat(date_filter["end"]))
    checks = []
    facts = {}

    def check(name, actual, expected):
        checks.append(
            {
                "check": name,
                "status": "PASS" if actual == expected else "FAIL",
                "actual": actual,
                "expected": expected,
            }
        )

    progress("Checking the exported payload and pinned calendar against their digests")
    payload_before = payload.stat()
    payload_sha256 = digest(payload)
    check("Payload SHA-256 matches export report", payload_sha256, export["output_sha256"])
    check("Payload byte count matches export report", payload_before.st_size, export["output_bytes"])
    calendar_bytes = US_MARKET_HOURS_FILE_PATH.read_bytes()
    check("Pinned calendar SHA-256", hashlib.sha256(calendar_bytes).hexdigest(), CALENDAR_SHA256)
    check("Conversion report published output", export["output_written"], True)
    check(
        "Conversion report requested identity count",
        export["histories_discovered"],
        len(export["histories"]) + len(export["omitted_histories"]),
    )
    checks.append(
        {
            "check": "Registered import manifest and canonical sidecar",
            "status": "SKIP",
            "actual": "fresh export audited against its conversion report",
            "expected": "registered import required for these checks",
        }
    )

    arrow = stack.enter_context(closing(pq.ParquetFile(payload, page_checksum_verification=True)))
    schema = {field.name: str(field.type) for field in arrow.schema_arrow}
    check("Exact schema columns and order", arrow.schema_arrow.names, list(SCHEMA))
    check("Exact logical types", schema, SCHEMA)
    check("Footer row count", arrow.metadata.num_rows, record_count)
    if not arrow.metadata.num_rows:
        check("Published Parquet contains rows", False, True)
        return _failed_structure(output, payload, checks)
    facts["parquet_row_groups"] = arrow.metadata.num_row_groups
    if schema != SCHEMA or arrow.schema_arrow.names != list(SCHEMA):
        return _failed_structure(output, payload, checks)
    facts["physical_order"] = key_order(arrow)
    check("Independent full Arrow decoded row count", facts["physical_order"]["decoded_keys"], record_count)
    compression = Counter()
    for i in range(arrow.metadata.num_row_groups):
        group = arrow.metadata.row_group(i)
        for j in range(group.num_columns):
            compression[group.column(j).compression] += 1
    facts["parquet"] = {
        "created_by": arrow.metadata.created_by,
        "compression_by_column_chunk": dict(compression),
        "declared_metadata": {
            k.decode(): v.decode(errors="replace")
            for k, v in (arrow.metadata.metadata or {}).items()
            if k != b"ARROW:schema"
        },
    }

    spill = stack.enter_context(TemporaryDirectory(dir=output, prefix=".spill-"))
    connection = stack.enter_context(
        closing(
            duckdb.connect(
                config={
                    "threads": threads,
                    "memory_limit": "4GB",
                    "temp_directory": str(spill),
                    "max_temp_directory_size": "7GB",
                }
            )
        )
    )
    if any(char in str(payload) for char in "*?["):
        import pyarrow.dataset as ds

        connection.register("bars", ds.dataset([str(payload)], format="parquet"))
    else:
        connection.execute(f"CREATE VIEW bars AS SELECT * FROM read_parquet({literal(payload)})")
    calendar_records = json.loads(calendar_bytes)
    calendar = [date.fromisoformat(row["date"]) for row in calendar_records]
    check("Calendar dates strictly increasing and unique", calendar == sorted(set(calendar)), True)
    session_dates = [d for d in calendar if first <= d <= last]
    session_index = {d: i for i, d in enumerate(session_dates)}
    connection.execute("CREATE TEMP TABLE sessions(day DATE PRIMARY KEY, seq INTEGER)")
    if not session_dates:
        check("Calendar sessions in report date range", 0, "at least one")
        return _failed_structure(output, payload, checks)
    connection.executemany("INSERT INTO sessions VALUES (?, ?)", session_index.items())

    progress("Scanning all rows for schema violations, arithmetic mismatches, and coverage")
    integrity_rules = {
        "null_or_nonpositive_id": "ohlcv_series_id IS NULL OR ohlcv_series_id <= 0",
        "null_or_nonmidnight_date": "date IS NULL OR date <> date_trunc('day', date)",
        "off_calendar_or_outside_declared_dates": "sessions.day IS NULL",
        **{
            f"invalid_{field}": f"{field} IS NULL OR NOT isfinite({field}) OR {field} <= 0" for field in POSITIVE
        },
        **{
            f"invalid_{field}": f"{field} IS NULL OR NOT isfinite({field}) OR {field} < 0"
            for field in NONNEGATIVE
        },
        **{f"blank_{field}": f"{field} IS NULL OR trim({field}) = ''" for field in LABELS},
        "invalid_ohlc_geometry": (
            "adjusted_low > least(adjusted_open, adjusted_close) OR "
            "greatest(adjusted_open, adjusted_close) > adjusted_high"
        ),
        "dollar_volume_arithmetic": (
            "abs(dollar_volume - raw_close*volume) > 1e-9*greatest(1,abs(raw_close*volume))"
        ),
        "adjustment_ratio_arithmetic": (
            "abs(adjustment_ratio - adjusted_close/nullif(raw_close,0)) "
            "> 1e-12*greatest(1,abs(adjusted_close/nullif(raw_close,0)))"
        ),
    }
    diagnostic_rules = {
        **{f"{col}_absent": f"{col} IS NULL OR trim({col}) = ''" for col in OPTIONAL},
        **{f"{col}_unknown": f"lower(trim({col})) = 'unknown'" for col in LABELS},
        "days_with_zero_volume_counted_as_rows": "volume = 0",
        "flat_ohlc_zero_volume": "volume = 0 AND adjusted_low = adjusted_high",
        "flat_ohlc_positive_volume": "volume > 0 AND adjusted_low = adjusted_high",
        "fractional_volume": "volume <> trunc(volume)",
        "raw_close_under_1": "raw_close < 1",
        "raw_close_under_0_01": "raw_close < 0.01",
        "adjusted_close_over_1_million": "adjusted_close > 1000000",
        "adjusted_close_over_1_billion": "adjusted_close > 1000000000",
        "adjusted_close_over_1_trillion": "adjusted_close > 1000000000000",
        "volume_over_1_billion": "volume > 1000000000",
        "adjusted_raw_basis_differs_from_split_by_1pct": (
            "abs(ln(CASE WHEN adjustment_ratio > 0 AND split_adjustment_ratio > 0 "
            "THEN adjustment_ratio/split_adjustment_ratio END)) >= ln(1.01)"
        ),
        "split_factor_over_1_million_or_reciprocal": (
            "split_adjustment_ratio > 1e6 OR split_adjustment_ratio < 1e-6"
        ),
        "ids_above_binary64_exact_integer_range_counted_as_rows": "ohlcv_series_id > 9007199254740992",
        "symbol_has_edge_whitespace": "trim(symbol) <> symbol",
    }
    counts_sql = ", ".join(
        f"count(*) FILTER (WHERE {rule}) AS {name}"
        for name, rule in (integrity_rules | diagnostic_rules).items()
    )
    population = rows(
        connection,
        f"""
        SELECT count(*) AS observations, count(DISTINCT ohlcv_series_id) AS series,
               count(DISTINCT date) AS days, count(DISTINCT symbol) AS tickers,
               count(DISTINCT ohlcv_series_id) FILTER
                   (WHERE ohlcv_series_id > 9007199254740992) AS large_ids,
               min(date)::DATE AS first_day, max(date)::DATE AS last_day, {counts_sql}
        FROM bars LEFT JOIN sessions ON bars.date::DATE = sessions.day
        """,
    )[0]
    for rule in integrity_rules:
        check(rule, population[rule], 0)
    for key, expected in {
        "observations": record_count,
        "series": export["histories_exported"],
        "days": len(session_dates),
        "first_day": first,
        "last_day": last,
    }.items():
        if key in ("first_day", "last_day") and (date_filter["start"] or date_filter["end"]):
            check(
                f"Observed {key} within filtered range",
                population[key] is not None and first <= population[key] <= last,
                True,
            )
        else:
            check(f"Observed {key}", population[key], expected)
    facts["population"] = population

    failed_row_rules = [name for name in integrity_rules if population[name]]
    if failed_row_rules:
        predicate = " OR ".join(f"({integrity_rules[name]})" for name in failed_row_rules)
        copy(
            connection,
            output / "invalid_rows.csv",
            f"SELECT bars.* FROM bars LEFT JOIN sessions ON bars.date::DATE=sessions.day "
            f"WHERE {predicate} ORDER BY ohlcv_series_id, date",
        )

        return _failed_structure(output, payload, checks)

    connection.execute("""
        CREATE TEMP TABLE series AS
        SELECT ohlcv_series_id,
               string_agg(DISTINCT symbol, '|' ORDER BY symbol) AS observed_symbols,
               min(date)::DATE AS first_date, max(date)::DATE AS last_date,
               count(*) AS row_count, count(DISTINCT date) AS distinct_dates,
               count(DISTINCT symbol) AS symbol_count,
               count(*) FILTER (WHERE volume=0) AS zero_volume_rows,
               min(raw_close) AS min_raw, max(raw_close) AS max_raw,
               min(adjusted_close) AS min_adjusted, max(adjusted_close) AS max_adjusted
        FROM bars GROUP BY ohlcv_series_id
        """)
    bounds = {}
    duplicate_report_ids = 0
    for row in histories:
        sid = int(row["ohlcv_series_id"])
        duplicate_report_ids += sid in bounds
        bounds[sid] = (
            "|".join(sorted(set(row["dated_symbols"]))),
            date.fromisoformat(row["first_session"]),
            date.fromisoformat(row["last_session"]),
            row["output_rows"],
        )

    observed = {}
    duplicate_rows = 0
    for row in rows(connection, "SELECT * FROM series"):
        sid = row["ohlcv_series_id"]
        start, end = row["first_date"], row["last_date"]
        internal = (
            session_index[end] - session_index[start] + 1 - row["distinct_dates"]
            if start in session_index and end in session_index
            else None
        )
        observed[sid] = (row["observed_symbols"], start, end, row["row_count"], internal)
        duplicate_rows += row["row_count"] - row["distinct_dates"]
    check("Duplicate (series_id, date) surplus rows", duplicate_rows, 0)
    check("Conversion report duplicate series IDs", duplicate_report_ids, 0)
    check("Observed IDs missing from conversion report", len(observed.keys() - bounds.keys()), 0)
    check("Conversion report IDs absent from payload", len(bounds.keys() - observed.keys()), 0)

    def matches_report(sid):
        labels, lo, hi, count = bounds[sid]
        actual_labels, start, end, actual_count, _ = observed[sid]
        if lo < first or hi > last:
            # The conversion report describes the source before its date filter.
            return (
                count == actual_count
                and set(actual_labels.split("|")) <= set(labels.split("|"))
                and lo <= start <= end <= hi
                and (lo < first or start == lo)
                and (hi > last or end == hi)
            )
        return observed[sid][:4] == bounds[sid]

    differing_bounds = [sid for sid in observed.keys() & bounds.keys() if not matches_report(sid)]
    check("Per-ID aliases, dates or counts differ from conversion report", len(differing_bounds), 0)
    if differing_bounds:
        (output / "conversion_report_differences.json").write_text(
            json.dumps(
                [
                    {"id": sid, "parquet": observed[sid][:4], "conversion_report": bounds[sid]}
                    for sid in sorted(differing_bounds)
                ],
                default=str,
                indent=2,
            )
        )

    facts["internal_gap_sessions"] = sum(v[4] or 0 for v in observed.values())
    facts["series_with_gaps"] = sum(bool(v[4]) for v in observed.values())
    facts["series_with_aliases"] = sum("|" in v[0] for v in observed.values())
    facts["series_present_on_last_session"] = sum(v[2] == last for v in observed.values())

    copy(
        connection,
        output / "series.csv",
        """
        SELECT series.*, end_session.seq-start_session.seq+1-distinct_dates AS internal_missing_sessions
        FROM series
        LEFT JOIN sessions start_session ON start_session.day=first_date
        LEFT JOIN sessions end_session ON end_session.day=last_date
        ORDER BY ohlcv_series_id
        """,
    )

    connection.execute("""
        CREATE TEMP TABLE coverage AS
        WITH daily AS (
            SELECT date::DATE AS day, count(*) AS bars,
                   count(*) FILTER (WHERE volume=0) AS zero_volume,
                   count(*) FILTER (WHERE raw_close>=5 AND dollar_volume>=5000000) AS liquid_bars,
                   median(raw_close) AS median_raw,
                   sum(dollar_volume) AS dollar_volume
            FROM bars GROUP BY 1
        ), births AS (
            SELECT first_date AS day, count(*) AS first_bars FROM series GROUP BY 1
        ), ends AS (
            SELECT last_date AS day, count(*) AS last_bars FROM series GROUP BY 1
        )
        SELECT sessions.day, daily.* EXCLUDE (day), coalesce(first_bars,0) AS first_bars,
               coalesce(last_bars,0) AS last_bars,
               bars-lag(bars) OVER (ORDER BY sessions.day) AS change_in_bars
        FROM sessions LEFT JOIN daily USING (day) LEFT JOIN births USING (day) LEFT JOIN ends USING (day)
        """)
    check(
        "Pinned sessions with no observations",
        rows(connection, "SELECT count(*) AS n FROM coverage WHERE bars IS NULL")[0]["n"],
        0,
    )
    copy(connection, output / "daily_coverage.csv", "SELECT * FROM coverage ORDER BY day")
    facts["daily_coverage"] = rows(
        connection,
        """
        SELECT min_by(day,bars) AS min_day, min(bars) AS min_bars,
               max_by(day,bars) AS max_day, max(bars) AS max_bars, median(bars) AS median_bars,
               sum(bars) AS row_sum FROM coverage
        """,
    )[0]
    check("Daily coverage row sum", facts["daily_coverage"]["row_sum"], population["observations"])
    facts["type_exchange"] = rows(
        connection,
        """
        SELECT security_type, exchange, count(*) AS rows, count(DISTINCT ohlcv_series_id) AS series
        FROM bars GROUP BY ALL ORDER BY rows DESC
        """,
    )
    facts["distribution"] = rows(
        connection,
        f"""
        SELECT {
            ", ".join(
                f"min({k}) AS {k}_min, approx_quantile({k}, 0.5) AS {k}_median, max({k}) AS {k}_max"
                for k in POSITIVE + NONNEGATIVE
            )
        }
        FROM bars
        """,
    )[0]

    progress("Checking ticker reuse, concurrent collisions, and alias continuity")
    connection.execute("""
        CREATE TEMP TABLE alias_ranges AS
        SELECT symbol, ohlcv_series_id, min(date)::DATE AS first_date,
               max(date)::DATE AS last_date, count(*) AS bars
        FROM bars GROUP BY ALL
        """)
    copy(
        connection,
        output / "reused_tickers.csv",
        """
        WITH reused AS (SELECT symbol FROM alias_ranges GROUP BY symbol HAVING count(*)>1)
        SELECT r.* FROM alias_ranges r JOIN reused USING (symbol) ORDER BY symbol, first_date, ohlcv_series_id
        """,
    )
    connection.execute("""
        CREATE TEMP TABLE concurrent_tickers AS
        WITH overlapping_symbols AS (
            SELECT DISTINCT a.symbol FROM alias_ranges a JOIN alias_ranges b
            ON a.symbol=b.symbol AND a.ohlcv_series_id<b.ohlcv_series_id
            AND a.first_date<=b.last_date AND b.first_date<=a.last_date
        )
        SELECT date::DATE AS day, symbol, count(DISTINCT ohlcv_series_id) AS series,
               string_agg(DISTINCT CAST(ohlcv_series_id AS VARCHAR), '|' ORDER BY
                          CAST(ohlcv_series_id AS VARCHAR)) AS ids
        FROM bars SEMI JOIN overlapping_symbols USING (symbol)
        GROUP BY 1, 2 HAVING count(DISTINCT ohlcv_series_id)>1
        """)
    facts["reused_ticker_labels"] = rows(
        connection,
        """
        SELECT count(*) AS n FROM (SELECT symbol FROM alias_ranges GROUP BY symbol HAVING count(*)>1)
        """,
    )[0]["n"]
    facts["concurrent_tickers"] = rows(
        connection,
        """
        SELECT count(*) AS ticker_sessions, count(DISTINCT symbol) AS ticker_labels,
               count(DISTINCT day) AS sessions FROM concurrent_tickers
        """,
    )[0]
    copy(
        connection,
        output / "concurrent_tickers.csv",
        "SELECT * FROM concurrent_tickers ORDER BY series DESC, symbol, day",
    )

    progress("Checking every in-series transition for split discontinuities, gaps, and extreme changes")
    flag_sql = ",\n".join(f"coalesce({rule}, false) AS {name}" for name, rule in FLAGS.items())
    include = " OR ".join(FLAGS)
    connection.execute(f"""
        CREATE TEMP TABLE transition_flags AS
        WITH lagged AS (
            SELECT ohlcv_series_id, symbol, day, seq, raw_close, adjusted_close,
                   adjusted_low, adjusted_high, adjusted_open, volume, dollar_volume,
                   split_adjustment_ratio AS split_factor, adjustment_ratio,
                   lag(day) OVER w AS prev_day, lag(seq) OVER w AS prev_seq,
                   lag(symbol) OVER w AS prev_symbol, lag(raw_close) OVER w AS prev_raw,
                   lag(adjusted_close) OVER w AS prev_adj,
                   lag(split_adjustment_ratio) OVER w AS prev_split,
                   lag(adjustment_ratio) OVER w AS prev_adjustment,
                   lag(volume) OVER w AS prev_volume, lag(dollar_volume) OVER w AS prev_dollar,
                   lead(adjusted_close) OVER w AS next_adj, lead(seq) OVER w AS next_seq
            FROM bars JOIN sessions ON date::DATE = day
            WINDOW w AS (PARTITION BY ohlcv_series_id ORDER BY date)
        ), measured AS (
            SELECT *, prev_symbol IS DISTINCT FROM symbol AND prev_day IS NOT NULL AS ticker_change,
                   coalesce(seq-prev_seq-1,0) AS missing_sessions,
                   ln(CASE WHEN adjusted_close>0 THEN adjusted_close END)
                     - ln(CASE WHEN prev_adj>0 THEN prev_adj END) AS adj_log,
                   ln(CASE WHEN raw_close>0 THEN raw_close END)
                     - ln(CASE WHEN prev_raw>0 THEN prev_raw END) AS raw_log,
                   ln(CASE WHEN split_factor>0 THEN split_factor END)
                     - ln(CASE WHEN prev_split>0 THEN prev_split END) AS split_log,
                   ln(CASE WHEN adjusted_open>0 THEN adjusted_open END)
                     - ln(CASE WHEN prev_adj>0 THEN prev_adj END) AS overnight_log
            FROM lagged
        ), flagged AS (
            SELECT *, {flag_sql} FROM measured
        )
        SELECT * FROM flagged WHERE missing_sessions>0 OR ticker_change OR {include}
        """)
    facts["signals"] = rows(
        connection,
        f"""
        SELECT count(*) AS unique_flagged_rows,
               count(DISTINCT ohlcv_series_id) AS flagged_series,
               count(*) FILTER (WHERE ticker_change) AS ticker_changes,
               count(*) FILTER (WHERE missing_sessions>0) AS gap_intervals,
               coalesce(sum(missing_sessions),0) AS total_slots_in_gap_intervals,
               {
            ", ".join(
                f"count(*) FILTER (WHERE {f} AND missing_sessions=0 AND prev_day IS NOT NULL) AS {f}"
                for f in FLAGS
            )
        }
        FROM transition_flags
        """,
    )[0]
    check(
        "Transition gap intervals reconcile with series totals",
        facts["signals"]["total_slots_in_gap_intervals"],
        facts["internal_gap_sessions"],
    )
    copy(
        connection,
        output / "transitions.csv",
        """
        SELECT * EXCLUDE (adj_log,raw_log,split_log,overnight_log),
               exp(adj_log) AS adjusted_close_multiple, exp(raw_log) AS raw_close_multiple,
               exp(split_log) AS split_factor_multiple, exp(overnight_log) AS overnight_multiple
        FROM transition_flags ORDER BY ohlcv_series_id, day
        """,
    )
    copy(
        connection,
        output / "gaps.csv",
        """
        SELECT t.ohlcv_series_id, t.prev_symbol, t.symbol, t.prev_day, t.day AS next_day,
               sessions.day AS missing_day, t.missing_sessions AS slots_in_gap
        FROM transition_flags t JOIN sessions ON sessions.seq>prev_seq AND sessions.seq<t.seq
        WHERE t.missing_sessions>0 ORDER BY t.ohlcv_series_id, sessions.day
        """,
    )
    facts["largest_transitions"] = rows(
        connection,
        """
        SELECT day, ohlcv_series_id, prev_symbol, symbol, prev_adj, adjusted_close,
               prev_raw, raw_close, prev_split, split_factor, exp(adj_log) AS adjusted_multiple,
               exp(raw_log)-1 AS raw_return, missing_sessions, dollar_volume
        FROM transition_flags WHERE adjusted_step_10x AND missing_sessions=0
        ORDER BY abs(adj_log) DESC, ohlcv_series_id, day LIMIT 12
        """,
    )
    facts["selected_series_ticker_changes"] = (
        rows(
            connection,
            """
        SELECT prev_day, day, prev_symbol, symbol, prev_raw, raw_close, prev_adj, adjusted_close,
               prev_split, split_factor, exp(raw_log)-1 AS raw_return, dollar_volume
        FROM transition_flags WHERE ohlcv_series_id=? AND ticker_change ORDER BY day
        """,
            [series_id],
        )
        if series_id is not None
        else []
    )
    copy(
        connection,
        output / "daily_flags.csv",
        f"""
        SELECT s.day, count(t.day) AS flagged_rows,
               {
            ", ".join(
                f"count(*) FILTER (WHERE {field} AND missing_sessions=0 AND prev_day IS NOT NULL) AS {field}"
                for field in FLAGS
            )
        }
        FROM sessions s LEFT JOIN transition_flags t USING (day) GROUP BY s.day ORDER BY s.day
        """,
    )

    progress("Checking zero-volume runs and early/late coverage")
    connection.execute("""
        CREATE TEMP TABLE zero_runs AS
        WITH zeros AS (
            SELECT ohlcv_series_id, symbol, day, adjusted_low, adjusted_high, raw_close,
                   seq-row_number() OVER (PARTITION BY ohlcv_series_id ORDER BY seq) AS run
            FROM bars JOIN sessions ON date::DATE=day WHERE volume=0
        )
        SELECT ohlcv_series_id, min_by(symbol, day) AS start_symbol, max_by(symbol, day) AS end_symbol,
               min(day) AS first_day, max(day) AS last_day, count(*) AS sessions,
               count(*) FILTER (WHERE adjusted_low=adjusted_high) AS flat_ohlc_bars,
               count(DISTINCT raw_close) AS distinct_raw_closes
        FROM zeros GROUP BY ohlcv_series_id, run HAVING count(*)>=5
        """)
    facts["zero_runs"] = rows(
        connection,
        """
        SELECT count(*) AS runs, count(DISTINCT ohlcv_series_id) AS affected_series,
               max(sessions) AS longest, count(*) FILTER (WHERE distinct_raw_closes=1) AS fixed_raw
        FROM zero_runs
        """,
    )[0]
    copy(
        connection,
        output / "zero_volume_runs.csv",
        "SELECT * FROM zero_runs ORDER BY sessions DESC, ohlcv_series_id, first_day",
    )
    copy(
        connection,
        output / "metadata_coverage.csv",
        """
        SELECT year(date) AS calendar_year, security_type, exchange, count(*) AS bars,
               count(DISTINCT ohlcv_series_id) AS series, count(cik) AS nonnull_cik,
               count(composite_figi) AS nonnull_composite_figi,
               count(share_class_figi) AS nonnull_share_class_figi,
               count(company_name) AS nonnull_company_name
        FROM bars GROUP BY ALL ORDER BY 1, 2, 3
        """,
    )
    copy(
        connection,
        output / "annual_coverage.csv",
        """
        SELECT year(day) AS calendar_year, count(*) AS sessions, sum(bars) AS bars,
               min(bars) AS min_daily_bars, median(bars) AS median_daily_bars, max(bars) AS max_daily_bars,
               first(bars ORDER BY day) AS first_daily_bars, last(bars ORDER BY day) AS last_daily_bars,
               sum(first_bars) AS observed_series_starts, sum(last_bars) AS observed_series_ends,
               sum(zero_volume) AS zero_volume_rows
        FROM coverage GROUP BY 1 ORDER BY 1
        """,
    )
    facts["largest_daily_changes"] = rows(
        connection,
        """
        SELECT day, bars, change_in_bars, first_bars, last_bars
        FROM coverage WHERE change_in_bars IS NOT NULL ORDER BY abs(change_in_bars) DESC, day LIMIT 10
        """,
    )
    facts["last_session"] = rows(connection, "SELECT * FROM coverage ORDER BY day DESC LIMIT 1")[0]
    prior = rows(connection, "SELECT * FROM coverage ORDER BY day DESC LIMIT 1 OFFSET 1")
    facts["prior_session"] = prior[0] if prior else None

    payload_after = payload.stat()
    check(
        "Payload device/inode/size/mtime/ctime stable during scan",
        [
            payload_after.st_dev,
            payload_after.st_ino,
            payload_after.st_size,
            payload_after.st_mtime_ns,
            payload_after.st_ctime_ns,
        ],
        [
            payload_before.st_dev,
            payload_before.st_ino,
            payload_before.st_size,
            payload_before.st_mtime_ns,
            payload_before.st_ctime_ns,
        ],
    )
    try:
        facts["triage"] = verify_exports(output, facts, len(session_dates))
    except ValueError as error:
        check("Evidence CSV reconciliation", str(error), "all counts reconcile")
        return _failed_structure(output, payload, checks)
    counts = Counter(check["status"] for check in checks)
    result = {
        "dataset": "Fresh online Bloodhound export",
        "payload": str(payload),
        "payload_sha256": payload_sha256,
        "conversion_report": str(report_file),
        "series_id": series_id,
        "export_status": export["status"],
        "export_omissions": len(export["omitted_histories"]),
        "counts": dict(counts),
        "checks": checks,
        "facts": facts,
        "transition_thresholds_sql": FLAGS,
        "runtime": {
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "pyarrow": pyarrow.__version__,
            "numpy": np.__version__,
            "threads": threads,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        },
    }
    (output / "audit.json").write_text(json.dumps(result, indent=2, default=str, allow_nan=False) + "\n")
    write_report(output, result)
    progress(f"Audit complete. Results in {output}")
    return {
        "complete": not counts["FAIL"],
        "counts": dict(counts),
        "basis_candidates": facts["signals"]["basis_shift_2x_without_large_raw_move"],
        "output": str(output),
    }


def verify_exports(output, facts, session_count):
    """Reconcile the published rows and create a smaller review queue."""
    population = facts["population"]
    signals = facts["signals"]
    observed = Counter()
    selected = []
    for row in csv_rows(output / "transitions.csv"):
        observed["unique_flagged_rows"] += 1
        if not row["prev_day"] or row["missing_sessions"] != "0":
            continue
        for flag in FLAGS:
            observed[flag] += row[flag] == "true"
        if row["basis_shift_2x_without_large_raw_move"] == "true":
            row["both_bars_raw_at_least_5_and_dollar_volume_at_least_5m"] = (
                min(float(row["raw_close"]), float(row["prev_raw"])) >= 5
                and min(float(row["dollar_volume"]), float(row["prev_dollar"])) >= 5e6
            )
            selected.append(row)
    for key in ("unique_flagged_rows", *FLAGS):
        if observed[key] != signals[key]:
            raise ValueError(f"Transition export does not reconcile for {key}")
    daily = list(csv_rows(output / "daily_coverage.csv"))
    if len(daily) != session_count or sum(int(r["bars"] or 0) for r in daily) != population["observations"]:
        raise ValueError("Daily coverage export does not reconcile")
    daily_flags = Counter()
    for row in csv_rows(output / "daily_flags.csv"):
        for flag in FLAGS:
            daily_flags[flag] += int(row[flag])
    if any(daily_flags[flag] != signals[flag] for flag in FLAGS):
        raise ValueError("Daily flag counts do not reconcile")
    if sum(1 for _ in csv_rows(output / "series.csv")) != population["series"]:
        raise ValueError("Series export does not reconcile")
    if sum(1 for _ in csv_rows(output / "gaps.csv")) != facts["internal_gap_sessions"]:
        raise ValueError("Gap export does not reconcile")
    collisions = Counter(r["symbol"] for r in csv_rows(output / "concurrent_tickers.csv"))
    if sum(collisions.values()) != facts["concurrent_tickers"]["ticker_sessions"]:
        raise ValueError("Ticker collision export does not reconcile")
    zero_runs = list(csv_rows(output / "zero_volume_runs.csv"))
    if len(zero_runs) != facts["zero_runs"]["runs"]:
        raise ValueError("Zero-volume run export does not reconcile")

    def rank(row):
        multiple = float(row["adjusted_close_multiple"])
        return (-max(multiple, 1 / multiple), row["day"], int(row["ohlcv_series_id"]))

    fields = [
        "day",
        "prev_day",
        "ohlcv_series_id",
        "prev_symbol",
        "symbol",
        "ticker_change",
        "prev_raw",
        "raw_close",
        "prev_adj",
        "adjusted_close",
        "prev_split",
        "split_factor",
        "adjusted_close_multiple",
        "raw_close_multiple",
        "split_factor_multiple",
        "prev_volume",
        "volume",
        "prev_dollar",
        "dollar_volume",
        "both_bars_raw_at_least_5_and_dollar_volume_at_least_5m",
    ]
    with (output / "basis_shift_candidates.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted(selected, key=rank))

    return {
        "basis_candidate_distinct_ids": len({r["ohlcv_series_id"] for r in selected}),
        "basis_candidate_ticker_changes": sum(r["ticker_change"] == "true" for r in selected),
        "basis_candidate_liquid_bar_pairs": sum(
            r["both_bars_raw_at_least_5_and_dollar_volume_at_least_5m"] for r in selected
        ),
        "same_day_ticker_collisions_by_ticker": dict(collisions.most_common()),
        "longest_zero_volume_run": zero_runs[0] if zero_runs else None,
        "all_screen_flag_counts_reconciled": True,
    }


def _failed_structure(output, payload, checks):
    counts = dict(Counter(c["status"] for c in checks))
    (output / "audit.json").write_text(
        json.dumps({"payload": str(payload), "counts": counts, "checks": checks}, default=str, indent=2) + "\n"
    )
    failures = [
        f"- {c['check']}: {c['actual']} (expected {c['expected']})" for c in checks if c["status"] == "FAIL"
    ]
    (output / "report.md").write_text("# Bloodhound integrity failure\n\n" + "\n".join(failures) + "\n")
    return {"complete": False, "counts": counts, "output": str(output)}
