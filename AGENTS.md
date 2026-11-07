# Agent instructions

Applies throughout the repository. The user's instructions take precedence.
You **MUST** always seek user confirmed permission to edit this file.

Read [README.md](README.md), then [Architecture](docs/ARCHITECTURE.md) and the guide relevant to the task.
Check `git status` before editing. Preserve existing work and verify claims against current code and tests.

## Non-negotiable: never re-prove accepted work

**THIS IS AN UNCONDITIONAL CORRECTNESS REQUIREMENT, NOT A PERFORMANCE SUGGESTION.**

**NEVER CHECK AGAIN WHAT A COMPLETED, APPLICABLE PRODUCER HANDOFF ALREADY ESTABLISHES.**
Trust it from its first successful acceptance. Carry its values, selected scope and completed decision to
every downstream consumer. Losing that handoff is a defect to fix, never permission to repeat its work.
Existing code, documentation, passing tests, defensive programming, a new phase, a retry, a missing workflow
acknowledgement or a desire for extra confidence cannot override this rule. Do not defend redundant checks
because the implementation already performs them. Remove conflicting documentation and fix the implementation.

For retained pricing, **the first fully adjusted (`all`) session plus the latest up to ten stored `all`
sessions are the acceptance boundary**. Compare overlapping dates once. Matching rows approve reuse of the
selected stored generation and acquisition of missing sessions only. A difference confined to the latest
stored session selects correction of that session and acquisition of missing sessions. Any earlier selected
row difference, including the first session, selects full retained-span replacement. An incomplete comparison
does not establish a difference. This applies to ordinary acquisition and retained dependencies as well as
daily-summary members. It is forbidden to:

- Check the other seven historical adjustment views after that approval.
- Request unselected intervening history or repeat selected observations to verify approved history again.
- Open, decode, hash or scan retained gzip payloads to authenticate approved prices. A recognized legacy
  publication without a comparison tail selects one bounded `readall_rsplit` assembly from its manifest-selected
  `all` payloads, followed by publication through the existing journal. A missing tail in the new format selects
  owning recovery, never repeated reconstruction. This transition assembles accepted data, not a new audit.
- Reread accepted validators, manifests or receipts, replay their calculations, or repeat a completed comparison
  merely because execution moved to another consumer, lock scope, cohort or phase.
- Rename repeated historical verification as an acquisition check, producer check, safety check or fallback.

Keep the approval bound to its actual store, source generation, identity mapping and permitted request scope.
After lock reacquisition, the required selector/generation check establishes whether that binding still applies;
it must not expand into upstream verification. An actual changed generation or mapping invalidates the affected
approval. A completed comparison selects the owning unchanged, latest-session correction or replacement path.
Carry its scope and live observations into acquisition, durable recovery, lineage and serving. Missing required evidence fails closed into
the explicit owning recovery/rebuild path; it never authorizes a hidden history audit or store crawl.

New rows and newly constructed artifacts are validated at their producer boundary. Reading an existing hot
partition to construct an append or rollover is data assembly, not permission to authenticate its accepted
contents again. Neither operation may repeat the historical acceptance decision. Consumers trust the resulting
completed handoff. Preserve accepted comparisons across peer failures within their valid operation scope.

**Enforce this in tests:** assert the correct result AND fail on forbidden provider requests, historical payload
reads, repeated boundary reads and repeated calculations. Cover the nearest actual invalidation and interruption.
Tests that require redundant verification must be corrected; their previous expectations are not authority.
Do not claim compliance from successful results, absence of scope alerts or a warm filesystem cache alone.

The primary acceptance rule is **zero unnecessary checks, reads, calculations or downloads**, including
lineage and metadata work. Runtime is a consequence, not a substitute: redundant work is a failure even when
the update finishes quickly. The operational target for a normal one-session update is **60 minutes, with
90 minutes the maximum**.
Exceeding 90 minutes is a failed performance acceptance, even if pricing, lineage and serving eventually finish.
Attribute the excess from debug evidence and fix its cause; do not normalize it as expected work or repeatedly
restart without diagnosis. This target never authorizes dropping required output, hiding failures or weakening
the accepted publication contract.

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

For every modification, including documentation-only changes, you **MUST** read and check the
[trust-boundary implementation review](docs/trust-boundary-handoff.html), run all owning regression test
modules linked from it, and verify its links and the accuracy of affected guarantees, evidence and finding
statuses. Update the review when the change alters them. These checks are mandatory in addition to the
checks below; report their results before handing off the change. Passing tests alone does not close the
review's remaining findings.

Run focused tests during implementation and the [full checks](docs/development.md#verification) before a
code handoff. For documentation-only changes, check links, commands and consistency with code.
Report failures plainly in the handoff; do not claim checks that were not run.
