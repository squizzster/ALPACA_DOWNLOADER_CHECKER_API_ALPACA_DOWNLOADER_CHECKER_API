# Agent instructions

Applies throughout the repository. The user's instructions take precedence.

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
