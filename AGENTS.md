# Agent instructions

Applies throughout the repository. The user's instructions take precedence.
You **MUST** always seek user confirmed permission to edit this file.

Read [README.md](README.md), then [Architecture](docs/ARCHITECTURE.md) and the guide relevant to the task.
Check `git status` before editing. Preserve existing work and verify claims against current code and tests.

## Boundaries

- Consumer applications import only `alpaca_end_of_day`. Modules under `libs/` are internal.
- `DataLibrary` exposes only online identities; an unavailable ID reveals no internal status.
- Tickers are dated labels. Preserve multiple matches and explicit ambiguity.
- Root Python files are executable adapters. Domain behavior belongs in its
  [owning package](docs/ARCHITECTURE.md#code-ownership).
- Trust completed handoffs. Consumers must not repeat upstream authentication or calculations.
- Keep research and trading decisions outside this data boundary.

## Trust before touching storage

A completed selector, receipt or digest is authoritative from its first successful read. Its producer has
already validated the inputs and outputs and published the selector last. Check the selector's own schema,
version and snapshot token where its contract requires that; then use what it says. Do not authenticate its
upstream files again, rediscover its membership or recalculate its decision.

This is a correctness rule and an I/O rule. The prohibited failure pattern is a compact handoff expanding
into a universe-wide metadata crawl: opening every receipt, checking every ticker path, retaining every
manifest, or touching every notice and lock before useful work begins. Thousands of individually small
`open`, `stat`, hash and lock operations can saturate a busy filesystem while CPU and network remain idle.
More workers amplify that contention. Forced synchronization adds write stalls without strengthening the
repository's process-interruption contract. Re-proving facts already guaranteed by a completed boundary
therefore makes the application slower and less reliable, not safer.

Use the narrowest owning boundary:

| Need | Authority | Read only when |
|---|---|---|
| Eligible stored symbols and last `all` rows | `pricing_check_summary.sqlite3` | Planning or running the live boundary check. |
| Lineage membership and selected generations | `end_of_day/lineage/index.sqlite3` | Selecting groups; open a lineage receipt only when its body is required for selected work. |
| Published pricing generation and payload map | A selected ticker's `source.manifest.json` | Reading or replacing that ticker. |
| Interrupted publication | The owning journal or pending marker | The owner is recovering that publication. |
| Scheduling acceleration | Disposable completion/hint databases | Selecting work only; loss permits the documented bounded reconstruction. |

For every storage path:

1. Identify the producer, selecting artifact and exact guarantee before reading payloads.
2. Read the completed boundary once and keep its coherent view in memory for the operation.
3. Derive the selected work and API plan from that view.
4. Open only payloads, receipts and locks required by the selected work.
5. Validate newly produced artifacts at their producer boundary, publish the new selector last, and refresh
   or invalidate the in-memory view.
6. Recover interrupted work through the owner's bounded journal. A missing, incomplete or incompatible
   boundary fails closed into its explicit rebuild or audit command; it does not silently trigger a crawl.

Broad discovery is reserved for an explicitly requested bootstrap, rebuild, audit, migration or universe
operation whose scope is stated before it runs. In steady state, control-plane I/O must scale with the
boundary artifacts plus selected work, not with every object that happens to exist in the store. Actual
data-plane reads and writes may scale with a genuinely selected universe update.

Before a broad production run, derive its expected shape from the accepted boundary: selected symbols,
API actions/pages, metadata reads and unavoidable payload publications. If observed work instead expands
into unplanned store-wide metadata access, stop cleanly and inspect the code path before another run.
Repeated restarts are not diagnosis, and a warm filesystem cache is not an architectural fix.

Do not add `fsync`, `fdatasync` or stronger SQLite synchronization. Atomic replacement, stable locks,
publication journals and SQLite transactions cover process interruption; machine and power failure are
outside this repository's contract. A lock file's existence is not evidence that its lock is held.

See [Technical contracts](docs/technical-contracts.md#trust-and-ownership) for the exact artifact and
recovery contracts and [Architecture](docs/ARCHITECTURE.md#daily-update) for the approved execution path.

## Changes

- Preserve frozen bars and retained stores. Do not redownload, rewrite, copy, repair, delete or run a
  universe unless the task requires it. Publish derived artifacts only for authorized pipeline work.
- Use generic evidence rules, never ticker exceptions. Follow the
  [lineage research loop](docs/development.md#lineage-research-and-rule-improvement-loop);
  evaluate the complete affected identity groups.
- Behavioral or serialized boundary changes require an owning version bump and exactly one entry in its
  runtime JSON changelog. Array position is the version; do not add a `version` field.
- Documentation-only changes leave runtime versions and changelogs alone.
- Keep secrets, runtime stores, logs and experiments out of Git. `resources/ticker.csv` is the tracked
  recovery-universe exception.

## Documentation and verification

Keep each explanation in one guide, linked from the README. Describe current behavior only.
Do not add status journals, dated test histories, duplicated version tables or speculative plans.

Run focused tests during implementation and the [full checks](docs/development.md#verification) before a
code handoff. For documentation-only changes, check links, commands and consistency with code.
Report failures plainly in the handoff; do not claim checks that were not run.
