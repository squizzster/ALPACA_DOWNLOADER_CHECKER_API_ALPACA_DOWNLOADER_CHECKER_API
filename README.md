# Alpaca EOD Data Engine

Alpaca EOD Data Engine turns ticker-labelled US-equity REST data into exact local pricing histories and deterministic
security lineage. It preserves all eight adjustment views, follows supported pricing continuity across ticker changes,
and separates unrelated securities that reuse the same symbol.

The standalone downloader and lineage builder are independent siblings using `data_pricing/`. Compact completed-years,
current-year and current-month boundaries reduce file churn. Up to three whole-ticker download workers share a
199-request/minute budget; one lineage worker consumes locked update notices. Completed boundaries are trusted and
unchanged work is reused, without repeated upstream audits or duplicate pricing copies.

`alpaca_end_of_day.StandaloneReader` and [`read_alpaca_pricing.py`](read_alpaca_pricing.py) read resolved histories as
exact values or CSV without downloads or store writes. Unrelated lineage work does not block completed groups.
The separate `DataLibrary` reads only the combined pipeline's online catalogue in `data_vtest/`; standalone lineage
does not publish that catalogue or approve availability.

The offline [Bloodhound Parquet converter](docs/operations.md#bloodhound-parquet-export) preserves Alpaca all-adjusted
prices and raw volume, derives a separate split factor from all eight views, and embeds the full public-ID mapping in
one file. It uses the latest observed ticker as a display label; schema compatibility is not Bloodhound admission.

**A `public_id` represents resolved security pricing continuity, never a ticker spelling.** A security can change
ticker while an unrelated security later reuses its old label. IDs are deterministic for fixed provider-UUID
membership; accepted membership changes can change the ID. See the [identity contract](docs/technical-contracts.md#identity-model).

Requires Python **3.11** (configured target; verified with 3.11.13), IANA `America/New_York` time-zone data and Unix
filesystem locking. Producers require Alpaca credentials and SIP entitlement. Reads need compatible published data,
not credentials or network; runtime stores are not included in Git. Parquet export additionally requires
[`requirements-bloodhound.txt`](requirements-bloodhound.txt).

Lineage development improves generic deterministic rules using the
[research-and-rule-improvement loop](docs/development.md#lineage-research-and-rule-improvement-loop). Both incorrect
attribution and withholding attributable history are errors. Research informs development, not runtime decisions.
The project's responsibility ends at the data handoff; downstream research and trading decisions belong to consumers.

## Documentation

### Start here

- [Current state](docs/VERSION_TRACKER.md#current-state) — accepted baseline, completed groups, next work and publication limits.
- [Agent instructions](AGENTS.md) — required reading order, task scope, safe changes and verification.
- [Architecture](docs/ARCHITECTURE.md) — system boundaries, supported interfaces, code ownership and store separation.
- [Pipeline](PIPELINE.md) — the implemented execution paths, ordered identity-rule cascade and reuse decisions.

### Use and operate

- [Public API](docs/public-api.md) — Python quick start, dated ticker resolution, exact histories, CSV export and errors.
- [Operations](docs/operations.md) — command selection, credentials, restart and catch-up, evidence inspection and reader blocking.
- [Technical contracts](docs/technical-contracts.md) — manifests, compact storage, identity, exact reuse gates, locks and publication.

### Develop and assess

- [Development](docs/development.md) — the research-agent loop, reusable brief, rule placement, tests and documentation ownership.
- [Version tracker](docs/VERSION_TRACKER.md) — implemented versions, dated verification evidence, milestones and open gaps.
- [Retained-store decision](docs/storage-rework-notes.md) — historical inventory, reuse restrictions and deferred cleanup;
  not permission to copy, migrate or delete stores.

This index covers every maintained project documentation file. Code, schemas, runtime validators and CLI definitions
own implementation details; dated runtime reports are evidence, not current process status.
