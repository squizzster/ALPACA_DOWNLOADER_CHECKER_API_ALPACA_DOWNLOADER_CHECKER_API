# Development

## Find the change owner

The [architecture ownership table](ARCHITECTURE.md#code-ownership) is the authority for component boundaries. Start at
the public package or root command named by the request, then follow its imports to the owning package under `libs/`.
The focused producer enters the monthly boundary package; the clean-room constructor enters its scheduling/audit
package and delegates every domain calculation to the same monthly cascade; the public reader enters
[`alpaca_end_of_day`](../alpaca_end_of_day/__init__.py). Root commands remain thin executable adapters.

Tests mirror the boundary names under [`tests/`](../tests/). Add focused coverage in the owning test module, and check
adjacent integration tests when a change crosses acquisition, correlation, publication, or read boundaries. Tests for
trusted loading must assert that consumers do not call the corresponding authenticator or recalculate a published
digest. Tests for publishers must assert that output validation precedes receipt publication.

The standalone pricing command enters only `libs/alpaca_pricing_download/`. Its focused tests are:

- `tests/test_alpaca_pricing_download.py`: exact values, pagination, overlap, rollover, first/last-session checks,
  automatic full replacement, unchanged cold/year payloads, recovery, and absence of identity/lineage imports.
- `tests/test_alpaca_pricing_handoff.py`: actual changed-date ranges, pending accumulation, full-range replacement,
  acknowledgement/failure/recovery, and separate-process locking in both directions, including process death.
- `tests/test_alpaca_pricing_validator.py`: no cold-payload reads during ordinary checks, legacy bootstrap, source/validator
  crash consistency, bounded provider-change restart, removed-session notices, force repair, and history-start safety.
- `tests/test_alpaca_pricing_cli.py`: symbol/file/universe selection, date semantics, deduplication, and ticker-specific
  failure isolation, bounded worker counts, whole-ticker worker ownership, and ordered results. Universe tests inspect
  selection only; they never download it.
- `tests/test_alpaca_pricing_rate_limit.py`: virtual-clock and threaded checks of the shared rolling-minute budget,
  pacing, overlapping I/O, retry headers, cooldown extensions, and exhaustion without clearing another worker's pause.

These use synthetic fixtures and HTTP doubles. Do not run production downloads or copy retained runtime stores to
test this path. The handoff is an internal producer interface, not a supported application import.

`tests/test_alpaca_lineage_build.py` covers the standalone lineage twin: chronological reuse/rollover, trusted
consumption without pricing authenticators or cold reads, FB → META versus reused FB, missing-price delegation,
reverse dependencies, bounded acquisition, empty inputs, metadata refresh, deletion, partial publication, acknowledgement
recovery, generation changes, one-worker locking and CLI selection. It uses synthetic temporary pricing and identity
doubles; no retained runtime data or live API calls are needed.

`libs/common/` is dependency-neutral shared infrastructure. Consumer code imports only `alpaca_end_of_day`, never a
module under `libs/`.

## Versioned boundaries

Every behavioral or serialized-contract change bumps its owning version and appends exactly one JSON changelog entry.
The tracked runtime changelogs live in [`resources/`](../resources/). They are arrays whose one-based position is the
version; entries contain `modifications` and no explicit `version` key. Runtime validators require array length to
equal the current constant.

Documentation-only work does not bump a runtime version or modify these JSON arrays. Record pending documentation and
repository-layout changes under `Unreleased` in [VERSION_TRACKER.md](VERSION_TRACKER.md#unreleased).

| Boundary | Version constant | Runtime changelog |
|---|---|---|
| Standalone pricing source | [`VERSION`](../libs/alpaca_pricing_download/contract.py) | [`alpaca_pricing_download_change_log.json`](../resources/alpaca_pricing_download_change_log.json) |
| Standalone lineage | [`VERSION`](../libs/alpaca_lineage_build/contract.py) | [`alpaca_lineage_build_change_log.json`](../resources/alpaca_lineage_build_change_log.json) |
| Clean-room run ledger | [`CLEAN_ROOM_RUN_VERSION`](../libs/alpaca_end_of_day_clean_room/generation.py) | [`alpaca_clean_room_run_change_log.json`](../resources/alpaca_clean_room_run_change_log.json) |
| Public consumer API | [`API_VERSION`](../alpaca_end_of_day/__init__.py) | [`alpaca_end_of_day_api_change_log.json`](../resources/alpaca_end_of_day_api_change_log.json) |
| Monthly source | [`SOURCE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Source-boundary changelog](../resources/alpaca_v6_source_change_log.json) |
| Monthly validation | [`VALIDATION_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Validation-boundary changelog](../resources/alpaca_v6_validation_change_log.json) |
| Lineage source | [`LINEAGE_SOURCE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Lineage-boundary changelog](../resources/alpaca_v6_lineage_source_change_log.json) |
| Correlation envelope | [`CORRELATION_ENVELOPE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Correlation-envelope changelog](../resources/alpaca_v6_correlation_envelope_change_log.json) |
| Warning envelope | [`WARNING_ENVELOPE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Warning-envelope changelog](../resources/alpaca_v6_warning_envelope_change_log.json) |
| Identity candidate | [`IDENTITY_CANDIDATE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Identity-candidate changelog](../resources/alpaca_v6_identity_candidate_change_log.json) |
| Availability | [`AVAILABILITY_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Availability changelog](../resources/alpaca_v6_availability_change_log.json) |
| Catalogue publication | [`CATALOGUE_PUBLICATION_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Catalogue-publication changelog](../resources/alpaca_v6_catalogue_publication_change_log.json) |
| Validation transform | [`PIPELINE_VERSION`](../libs/alpaca_end_of_day_pipeline/pipeline_version.py) | [`alpaca_end_of_day_pipeline_change_log.json`](../resources/alpaca_end_of_day_pipeline_change_log.json) |
| Correlation transform | [`CORRELATION_VERSION`](../libs/alpaca_end_of_day_security_correlation/correlation_version.py) | [`alpaca_end_of_day_security_correlation_change_log.json`](../resources/alpaca_end_of_day_security_correlation_change_log.json) |
| Catalogue engine | [`SECURITY_MASTER_VERSION`](../libs/alpaca_end_of_day_security_correlation/security_master_version.py) | [`alpaca_security_master_change_log.json`](../resources/alpaca_security_master_change_log.json) |
| Warning transform | [`WARNING_PIPELINE_VERSION`](../libs/alpaca_end_of_day_warning_pipeline/warning_pipeline_version.py) | [`alpaca_end_of_day_warning_pipeline_change_log.json`](../resources/alpaca_end_of_day_warning_pipeline_change_log.json) |
| Detailed history transform | [`READER_VERSION`](../libs/alpaca_end_of_day_reader/reader_version.py) | [`alpaca_end_of_day_reader_change_log.json`](../resources/alpaca_end_of_day_reader_change_log.json) |

The normalized lineage report also uses `REPORT_VERSION` in
[`alpaca_security_lineage/settings.py`](../libs/alpaca_security_lineage/settings.py). Supporting manifest and ID-scheme
versions live beside their schemas. [VERSION_TRACKER.md](VERSION_TRACKER.md) records the current matrix and gaps; the
JSON arrays remain the exact machine-validated histories.

When correlation changes, review `SUPPORTED_CORRELATION_VERSIONS` as a migration decision. Supporting an older derived
generation is deliberate compatibility, not permission to bypass authentication.

## Rule changes

Correlation and availability rules must match a generic evidence shape and apply uniformly across the universe. Add a
rule only when its inputs, positive gate, vetoes, and placement in the cascade are simple enough to state precisely.
Ambiguity remains offline.

For each rule change:

1. add focused unit coverage for success, threshold boundaries, and each material veto;
2. place it deliberately in the ordered registry and update the explicit rule-order test;
3. run it against the current derived universe without changing frozen bars;
4. inspect every identity whose result changed, not only the motivating ticker;
5. record how many identities moved online or offline; and
6. bump the owning version and append its changelog entry.

Never add a ticker-specific condition. Primary-source evidence can be symbol-specific data; executable policy cannot be.

## Verification

The repository's defined local checks are:

```bash
ruff check .
ruff format --check .
python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v
```

Tests use temporary repositories and HTTP doubles. They do not require production data or live credentials. `-B`
prevents ignored bytecode caches. Use `python3.11` explicitly in the current environment: unqualified `python3` is
Python 3.9.25 and cannot import the repository's Python 3.11 syntax.

There is no `.github/workflows/` directory and no other repository CI configuration. Local commands are therefore the
only automated gate. Ruff is configured in [`pyproject.toml`](../pyproject.toml), but its executable version is not
pinned. Ruff also scans untracked Python files, so inspect `git status` before interpreting its output. Current tool
versions, results, and unresolved gaps belong in [VERSION_TRACKER.md](VERSION_TRACKER.md#verification-status).

## Documentation ownership

| Topic | Home |
|---|---|
| Product summary and complete document index | [`README.md`](../README.md) |
| Agent workflow and repository safety rules | [`AGENTS.md`](../AGENTS.md) |
| Responsibility, flow, ownership, and invariants | [`ARCHITECTURE.md`](ARCHITECTURE.md) |
| Single execution path, ordered rule cascade, and non-divergence controls | [`PIPELINE.md`](../PIPELINE.md) |
| Consumer-facing Python and CLI contract | [`public-api.md`](public-api.md) |
| Operator commands, mutation, backup, and recovery | [`operations.md`](operations.md) |
| Schemas, evidence gates, identity rules, runtime artifacts, and tracked resources | [`technical-contracts.md`](technical-contracts.md) |
| Current versions, pending changes, history, and verification gaps | [`VERSION_TRACKER.md`](VERSION_TRACKER.md) |

Keep one explanation authoritative and link to it from other documents. The repository has no configured Markdown
linter or link checker; documentation changes require an explicit local relative-link, anchor, and referenced-path
check.
