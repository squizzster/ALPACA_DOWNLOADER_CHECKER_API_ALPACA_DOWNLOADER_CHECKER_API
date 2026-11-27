# Alpaca EOD Data Engine

Alpaca EOD stores US equity histories in eight adjustment views, with dated identities across ticker changes and reuse. Join prices and metadata by public security ID. Read the online catalogue locally or export CSV, Bloodhound Parquet, daily events or metadata JSON Lines.

**Each producer owns a trust boundary.** Pricing approves a generation and date range, lineage assigns ownership, and serving publishes eligible histories. Consumers reuse that handoff within scope. Changed generations return to their owner. Journals resume interrupted publication without repeating accepted checks or downloads. [Producer handoffs](docs/ARCHITECTURE.md#producer-handoffs)

Requires Unix, Python 3.11, `flock` and `America/New_York` timezone data. Acquisition needs Alpaca credentials and SIP access. Published reads and exports run offline. [Prerequisites](docs/operations.md#prerequisites)

## Set up and operate

- [Fresh installation](NEW_SETUP_VIRGIN_INSTALL_GUIDE.md) — acquisition, lineage, catalogue bootstrap, explicit exchange backfill and reproducibility.
- [Operations](docs/operations.md) — daily updates, cutoffs, registry Git behavior, recovery and focused maintenance.
- [CLI reporting](docs/cli-reporting.md) — exit codes, warnings, lock waits, heartbeats and debug traces.

## Read and export data

- [Public API](docs/public-api.md) — ticker ambiguity, public IDs, online availability and snapshot reads with `DataLibrary` and `StandaloneReader`.
- [Export formats](docs/exports.md) — metadata JSON Lines, CSV, Parquet, daily events and the optional offline Parquet audit.
- [Standalone Bloodhound converter](bloodhound_app/README.md) — inputs, adjustment arithmetic, Parquet schema and partial publication.
- [Business-equity baseline](IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/README.md) — resolved IDs and group evidence for subsequent eligibility assessment.

## Understand and change the system

- [Agent instructions](AGENTS.md) — required before changes. Covers inference, storage access, handoffs and verification.
- [Architecture](docs/ARCHITECTURE.md) — end-to-end update and package ownership.
- [Technical contracts](docs/technical-contracts.md) — selectors, scope, invalidation, locking, publication and recovery.
- [Trust-boundary implementation review](docs/trust-boundary-handoff.html) — required before handoff changes. Verified repairs, open findings, regressions and production evidence gaps.
- [Lineage contracts](docs/lineage.md) — admissible evidence, dated ownership, competing owners, exchange changes and unresolved outcomes.
- [Development and verification](docs/development.md) — generic-rule research, required checks, three pipeline stages and JSON adapters.
- [Version authorities](VERSION_TRACKER.md) — runtime and publication constants and changelogs.

## Investigate a lineage decision

- [Lineage rule tool](docs/lineage-debug.md) — explain published decisions or replay selected groups with a research candidate.
- [Correlation policy lab](docs/policy-lab.md) — offline policy comparison, candidate effects and stored/control drift.
