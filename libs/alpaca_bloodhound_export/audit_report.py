"""Markdown report for a Bloodhound export integrity check."""


def write_report(output, result):
    facts = result["facts"]
    p = facts["population"]
    s = facts["signals"]
    checks = result["counts"]
    triage = facts["triage"]
    longest = triage["longest_zero_volume_run"]
    collision_names = ", ".join(triage["same_day_ticker_collisions_by_ticker"])
    text = [
        "# Alpaca Parquet credibility audit",
        "",
        f"**{p['observations']:,} bars across {p['series']:,} series and {p['days']:,} pinned sessions** "
        f"from {p['first_day']} to {p['last_day']}.",
        "",
        f"**{checks.get('PASS', 0)} structural and arithmetic checks passed**. "
        f"{checks.get('FAIL', 0)} failed and {checks.get('SKIP', 0)} were skipped. "
        "The audit checks the exported file against the conversion report and the pinned trading calendar.",
        "",
        "## Main findings",
        "",
        f"- **Metadata.** CIK absent on {p['cik_absent']:,} bars, composite FIGI absent on "
        f"{p['composite_figi_absent']:,}, share-class FIGI absent on {p['share_class_figi_absent']:,}, "
        f"and company name absent on {p['company_name_absent']:,}. "
        f"Type is unknown on {p['security_type_unknown']:,} bars and exchange on "
        f"{p['exchange_unknown']:,}. Validating historical exchange membership, common-stock membership, "
        "issuer identity, and coverage of the source universe requires external evidence.",
        f"- **Continuity.** {facts['internal_gap_sessions']:,} missing sessions within the observed "
        f"lifetimes of {facts['series_with_gaps']:,} series. "
        f"{facts['reused_ticker_labels']:,} ticker labels appear under multiple IDs, including "
        f"{facts['concurrent_tickers']['ticker_labels']:,} labels observed under multiple IDs "
        f"on the same date ({collision_names}). Ticker-only joins can cross lineages.",
        f"- **Price scale.** {p['adjusted_close_over_1_million']:,} bars exceed $1 million adjusted, "
        f"and {p['adjusted_close_over_1_trillion']:,} exceed $1 trillion. "
        "Adjusted levels depend on their basis. The transition tests below look for artificial "
        "changes across adjacent bars.",
        f"- **Large transitions.** {s['adjusted_step_2x']:,} adjacent adjusted-close changes "
        f"are at least a doubling or halving, {s['adjusted_step_10x']:,} are at least tenfold, "
        f"and {s['adjusted_step_100x']:,} are at least a hundredfold. "
        f"{s['basis_shift_2x_without_large_raw_move']:,} pairs show a large split-driven basis change "
        f"with a modest raw move across {triage['basis_candidate_distinct_ids']:,} IDs. "
        f"{triage['basis_candidate_ticker_changes']:,} coincide with ticker changes. "
        f"Both bars in {triage['basis_candidate_liquid_bar_pairs']:,} pairs have a raw close "
        "of at least $5 and dollar volume of at least $5 million.",
        f"- **Volume.** {p['days_with_zero_volume_counted_as_rows']:,} zero-volume bars, including "
        f"{p['flat_ohlc_zero_volume']:,} with flat adjusted OHLC. "
        f"{facts['zero_runs']['runs']:,} stretches of at least five consecutive zero-volume sessions "
        f"across {facts['zero_runs']['affected_series']:,} IDs. "
        f"{facts['zero_runs']['fixed_raw']:,} runs had a constant raw close."
        + (f" The longest was {longest['start_symbol']} ({longest['sessions']} sessions)." if longest else ""),
        f"- **Precision.** {p['large_ids']:,} IDs exceed 2^53. Preserve IDs as integers or strings "
        "through SQL, CSV, and downstream joins.",
    ]
    if result["series_id"] is not None:
        text.extend(
            [
                "",
                f"## Ticker changes for series {result['series_id']}",
                "",
                "| Prior session | Session | Tickers | Prior raw | Raw | Prior adjusted | Adjusted | Split factor change |",
                "|---|---|---|---:|---:|---:|---:|---:|",
            ]
        )
    for r in facts["selected_series_ticker_changes"]:
        text.append(
            f"| {r['prev_day']} | {r['day']} | {r['prev_symbol']} → {r['symbol']} | "
            f"{r['prev_raw']:,.8g} | {r['raw_close']:,.8g} | {r['prev_adj']:,.8g} | "
            f"{r['adjusted_close']:,.14g} | {r['prev_split']:,g} → {r['split_factor']:,.14g} |"
        )
    text += [
        "",
        "## Largest adjacent adjusted-close discontinuities",
        "",
        "| Session | Series ID | Tickers | Prior adjusted | Adjusted | Raw return | Absolute adjusted-close multiple |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for r in facts["largest_transitions"]:
        multiple = r["adjusted_multiple"]
        text.append(
            f"| {r['day']} | `{r['ohlcv_series_id']}` | {r['prev_symbol']} → {r['symbol']} | "
            f"{r['prev_adj']:,.8g} | {r['adjusted_close']:,.8g} | {r['raw_return']:+.2%} | "
            f"{max(multiple, 1 / multiple):,.4g}× {'up' if multiple >= 1 else 'down'} |"
        )
    text += [
        "",
        "## Coverage and physical layout",
        "",
        f"- The file is {facts['parquet']['created_by']}. Compression by column chunk is "
        f"`{facts['parquet']['compression_by_column_chunk']}`. A separate full Arrow decode counted "
        f"{facts['physical_order']['key_order_inversions']:,} adjacent physical-order inversions.",
        f"- Daily bars range from {facts['daily_coverage']['min_bars']:,} to "
        f"{facts['daily_coverage']['max_bars']:,}. "
        f"{facts['series_present_on_last_session']:,} IDs appear on the final session. "
        "Final observed dates describe coverage and do not establish delisting or zero-bar status.",
        "- The exported payload and pinned calendar were independently hashed. "
        "The file was read only. The full Arrow decode requested page-checksum verification "
        "for pages carrying checksums.",
        "- The detailed transition exports were independently reconciled back to all rule totals. "
        "Daily coverage, per-series coverage, gaps, same-day collisions, and zero-volume run "
        "counts also reconciled.",
        "",
        "## Rule definitions and limits",
        "",
        "Transition totals use adjacent observed bars on consecutive pinned sessions. "
        "The detailed CSV also retains flagged gap-spanning bars with a gap count. "
        "Thresholds are explicit screening choices with direction and denominators given in "
        "[audit.json](audit.json). Individual event flags overlap.",
        "",
        "A basis-shift candidate requires an adjusted and split-factor move of at least 2×, "
        "a raw move within ±25%, and agreement within a 1.05× residual between the adjusted/raw move "
        "and the split-factor move. Raw and adjusted price series can legitimately diverge on "
        "corporate actions. The reciprocal checks screen for changes requiring event-level review.",
        "",
        "The conversion report describes split factors as inferred from rounded provider views and supplies "
        "no common adjustment horizon, independent action tape, reviewed lineage, dated membership, "
        "or complete attempted and zero-bar population. This run therefore measures internal "
        "consistency, identifies suspect records, and tests reproducibility of the stored bytes. "
        "It cannot establish exchange truth, point-in-time availability, genuine corporate actions "
        "for every flagged row, or trading-system impact.",
        "",
        "## Evidence files",
        "",
        "- [Full check results, counts, thresholds and runtime](audit.json)",
        "- [Prioritized split-basis candidates](basis_shift_candidates.csv), "
        "[all flagged rows and transitions](transitions.csv), and [flags per session](daily_flags.csv)",
        "- [Every canonical ID](series.csv), [missing session detail](gaps.csv), "
        "[ticker reuse](reused_tickers.csv), and [same-day collisions](concurrent_tickers.csv)",
        "- [Daily coverage](daily_coverage.csv), [annual coverage](annual_coverage.csv), "
        "[metadata coverage](metadata_coverage.csv), and [zero-volume stretches](zero_volume_runs.csv)",
    ]
    (output / "report.md").write_text("\n".join(text) + "\n")
