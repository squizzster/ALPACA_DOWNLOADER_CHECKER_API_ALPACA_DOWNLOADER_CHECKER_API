# Development

## Find the change owner

The [architecture ownership table](ARCHITECTURE.md#code-ownership) is the authority for component boundaries. Start at
the public package or root command named by the request, then follow its imports to the owning package under `libs/`.
Root commands stay thin, and every tracked root Python file is executable. The
[`alpaca_end_of_day`](../alpaca_end_of_day/__init__.py) package is the supported consumer boundary.

Tests mirror the boundary names under [`tests/`](../tests/). Add focused coverage in the owning test module, and check
adjacent integration tests when a change crosses acquisition, correlation, publication, or read boundaries.
`libs/common/` is dependency-neutral shared infrastructure: it imports no application package. Consumer code imports
only `alpaca_end_of_day`, never a module under `libs/`.

## Versioned boundaries

Every behavioral or serialized-contract change bumps its owning version and appends exactly one JSON changelog entry.
The tracked runtime changelogs live in [`resources/`](../resources/). They are arrays whose one-based position is the
version; entries contain `modifications` and no explicit `version` key. Runtime validators require array length to
equal the current constant.

Documentation-only work does not bump a runtime version or modify these JSON arrays. Record pending documentation and
repository-layout changes under `Unreleased` in [VERSION_TRACKER.md](VERSION_TRACKER.md#unreleased).

| Boundary | Version constant | Runtime changelog |
|---|---|---|
| Public consumer API | [`API_VERSION`](../alpaca_end_of_day/__init__.py) | [`alpaca_end_of_day_api_change_log.json`](../resources/alpaca_end_of_day_api_change_log.json) |
| Frozen validation and processing | [`PIPELINE_VERSION`](../libs/alpaca_end_of_day_pipeline/pipeline_version.py) | [`alpaca_end_of_day_pipeline_change_log.json`](../resources/alpaca_end_of_day_pipeline_change_log.json) |
| Security correlation | [`CORRELATION_VERSION`](../libs/alpaca_end_of_day_security_correlation/correlation_version.py) | [`alpaca_end_of_day_security_correlation_change_log.json`](../resources/alpaca_end_of_day_security_correlation_change_log.json) |
| Sharded master catalogue | [`SECURITY_MASTER_VERSION`](../libs/alpaca_end_of_day_security_correlation/security_master_version.py) | [`alpaca_security_master_change_log.json`](../resources/alpaca_security_master_change_log.json) |
| Warning overlays | [`WARNING_PIPELINE_VERSION`](../libs/alpaca_end_of_day_warning_pipeline/warning_pipeline_version.py) | [`alpaca_end_of_day_warning_pipeline_change_log.json`](../resources/alpaca_end_of_day_warning_pipeline_change_log.json) |
| Detailed internal reader | [`READER_VERSION`](../libs/alpaca_end_of_day_reader/reader_version.py) | [`alpaca_end_of_day_reader_change_log.json`](../resources/alpaca_end_of_day_reader_change_log.json) |

The lineage report uses `REPORT_VERSION` in
[`alpaca_security_lineage/settings.py`](../libs/alpaca_security_lineage/settings.py). Supporting manifest and ID-scheme
versions live beside their schemas. [VERSION_TRACKER.md](VERSION_TRACKER.md) records the current matrix, pending
changes, and concise milestones; the JSON arrays remain the exact machine-validated histories.

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
python3 -W error::ResourceWarning -m unittest discover -s tests -v
```

Tests use temporary repositories and HTTP doubles. They do not require production data or live credentials. Set
`PYTHONDONTWRITEBYTECODE=1` when a strictly non-mutating verification pass must avoid ignored bytecode caches.

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
