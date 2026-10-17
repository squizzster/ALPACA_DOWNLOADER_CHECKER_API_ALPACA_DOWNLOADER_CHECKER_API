# Alpaca EOD Data Engine

Alpaca EOD Data Engine turns ticker-labelled US-equity REST pricing into exact local histories and deterministic
security lineage. It preserves all eight adjustment views, follows supported pricing continuity across ticker changes,
separates unrelated ticker reuse, and provides an online-only catalogue, Python reader and CSV export through its
combined publication pipeline.

The standalone downloader and lineage builder are independent siblings: compact completed-years, current-year and
current-month boundaries reduce file churn; up to three whole-ticker download workers share a 199-request/minute
budget; one lineage worker consumes locked pending-date notices. Their development store is `data_pricing/`.
[`read_alpaca_pricing.py`](read_alpaca_pricing.py) and `alpaca_end_of_day.StandaloneReader` read that pair's resolved
pricing directly, without downloads or datastore writes; completed groups stay readable during unrelated lineage work.
The separate online-only `DataLibrary` still selects the
combined catalogue in `data_vtest/`; resolved lineage does not claim that catalogue's availability approval.

The offline [Bloodhound Parquet converter](docs/operations.md#bloodhound-parquet-export) preserves Alpaca all-adjusted
prices and raw volume, derives a separate split factor from all eight views, and embeds the full public-ID mapping in
one file. It uses the latest observed ticker as a display label; schema compatibility is not Bloodhound admission.

**A `public_id` represents resolved security pricing continuity, never a ticker spelling.** A security can change
ticker while an unrelated security later reuses its old label. IDs are deterministic for fixed provider-UUID
membership; accepted membership changes can change the ID. See the [identity contract](docs/technical-contracts.md#identity-model).

Each boundary owns its calculation and publishes a selecting receipt. Downstream stages trust completed boundaries
instead of re-auditing them. Restartable lineage skips unchanged completed groups, while bounded updates preserve
unaffected history. Exact-copy manifest aliases avoid duplicate pricing payloads. This is a data handoff system;
downstream research and trading decisions belong to its consumers.

Requires Python **3.11** (configured target; verified with 3.11.13) and IANA `America/New_York` time-zone data.
Producer runs require Alpaca credentials and SIP entitlement. Public reads need neither credentials nor network,
but do require compatible published boundaries (a catalogue for `DataLibrary`): runtime data is not included in Git.

Lineage development improves generic deterministic rules using the
[research-and-rule-improvement loop](docs/development.md#lineage-research-and-rule-improvement-loop). Both incorrect
attribution and withholding attributable history are errors. Research informs development, not runtime decisions.

## Documentation

### Start here

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
