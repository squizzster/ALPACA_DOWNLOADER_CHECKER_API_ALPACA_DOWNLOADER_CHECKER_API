# Alpaca EOD Data Engine

Alpaca EOD Data Engine turns ticker-labelled US-equity REST pricing into exact local histories and deterministic
security lineage. It preserves all eight adjustment views, follows supported pricing continuity across ticker changes,
separates unrelated ticker reuse, and provides an online-only catalogue, Python reader and CSV export through its
combined publication pipeline.

The standalone downloader and lineage builder are independent siblings: compact completed-years, current-year and
current-month boundaries reduce file churn; up to three whole-ticker download workers share a 199-request/minute
budget; one lineage worker consumes locked pending-date notices. Their development store is `data_pricing/`.
**The public reader is not yet connected to this standalone pair**; it selects the combined catalogue in `data_vtest/`.

**A `public_id` represents resolved security pricing continuity, never a ticker spelling.** A security can change
ticker while an unrelated security later reuses its old label. IDs are deterministic for fixed provider-UUID
membership; accepted membership changes can change the ID. See the [identity contract](docs/technical-contracts.md#identity-model).

Each boundary owns its calculation and publishes a selecting receipt. Downstream stages trust completed boundaries
instead of re-auditing them. Exact-copy manifest aliases avoid duplicate pricing payloads; bounded updates preserve
unaffected data. This is a data handoff system, not a trading engine or a logging product.

**Lineage development objective:** improve the deterministic production cascade with simple, common-sense,
non-ticker-specific rules. Both incorrect attribution and withholding attributable history are errors. Cases supply
general concepts, not symbol-specific recipes or permission to restore every older result.

Follow the [repeatable research-and-rule-improvement loop](docs/development.md#lineage-research-and-rule-improvement-loop):
one case at a time, a fresh research agent per case, main-agent ownership of the complete cascade, focused checks
during implementation, and one consolidated evaluation at the end of the agreed three-case batch. Public research
informs development; production decisions use deterministic inputs, not a model or researched ticker overrides.

Requires Python **3.11** (configured target; verified with 3.11.13) and IANA `America/New_York` time-zone data.
Producer runs require Alpaca credentials and SIP entitlement. Public reads need neither credentials nor network,
but do require a compatible published catalogue: runtime data is not included in Git.

## Documentation

### Start here

- [Agent instructions](AGENTS.md) — required reading order, task scope, safe changes and verification.
- [Architecture](docs/ARCHITECTURE.md) — system boundaries, supported interfaces, code ownership and store separation.
- [Pipeline](PIPELINE.md) — the implemented execution paths, ordered identity-rule cascade and reuse decisions.

### Use and operate

- [Public API](docs/public-api.md) — Python quick start, dated ticker resolution, exact histories, CSV export and errors.
- [Operations](docs/operations.md) — command map, credentials, date semantics, evidence inspection, locks and recovery.
- [Technical contracts](docs/technical-contracts.md) — manifests, compact storage, identity, compatibility and publication.

### Develop and assess

- [Development](docs/development.md) — the research-agent loop, reusable brief, rule placement, tests and documentation ownership.
- [Version tracker](docs/VERSION_TRACKER.md) — implemented versions, dated verification evidence, milestones and open gaps.
- [Retained-store decision](docs/storage-rework-notes.md) — historical inventory, reuse restrictions and deferred cleanup;
  not permission to copy, migrate or delete stores.

This index covers every maintained project documentation file. Code, schemas, runtime validators and CLI definitions
own implementation details; dated runtime reports are evidence, not current process status.
