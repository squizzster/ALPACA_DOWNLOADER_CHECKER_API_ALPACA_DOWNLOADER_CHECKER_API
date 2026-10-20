# Operations

Run from repository root with Python 3.11. Option authority: `./alpaca-eod --help`, then command `--help`.
Producers require `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY`, SIP access. Local readers/inspection require
compatible publications, no credentials/network. Standalone store defaults to `data_pricing/`; override
with `--store PATH`. Progress: stderr; final report: JSON stdout; failed/unfinished work: nonzero exit.

## Daily update

```bash
./alpaca-eod update --plan
./alpaca-eod update --through 2026-10-16
./alpaca-eod status
```

Each update fetches one complete all-status US-equity catalogue, preserving duplicate labels. Discovery:
active OTC/NYSE/NASDAQ/AMEX/TXSE, regardless of tradability. Retain other-exchange histories/dependencies.
Known active histories remain maintained when omitted, including empty/partial views outside the summary.
Explicit inactivity is separate evidence. Empty range ≠ unavailable/failed acquisition. Pricing always
uses SIP; OTC-labelled registration does not promise separate OTC-feed access.

[Flow](ARCHITECTURE.md#daily-update): register → acquire missing history → reconcile changed identity/status
→ check/correct/append → lineage. Ordinary summary advancement without pending inputs appends before
publishing new lineage; it does not republish the old span first. Metadata prefetch applies accepted
complete-group refresh requirements, including full history. Published unresolved identity is a valid
processing outcome, distinct from failure.

`--through`: inclusive cutoff, rounded back to the latest exchange session on/before it. October 16–18,
2026 all select Friday October 16, exclusive boundary `2026-10-17T00:00:00Z`. Shared calendar handles
weekends, holidays, exceptional closures. Resolve before selection/registration/requests; closed days alone
cause no coverage advancement/publication. Progress and JSON `pricing_cutoff` report requested/effective dates.
Reader/export filters remain inclusive calendar bounds.

Default: last exchange session strictly before today's New York date, frozen once. Friday becomes eligible
after Saturday midnight New York; UTC midnight has no effect. Explicit cutoffs must also precede today's
New York date; unsupported calendar ranges fail. Recovery retains accepted work/journals' original exclusive
bounds. Initial/dependency acquisition shares the target. Newer sources stay unchanged, separately reported,
without fresh checking/rewinding; reconciliation needing a later target remains incomplete.
This is coverage through a target, not a historical knowledge snapshot. Catalogue observation time,
coverage, last observed session and provider mapping `asof` differ; final-session bars need not exist.

Preview may fetch metadata; no registration, initialization, recovery or publication. It reports selected
work/reasons, additions, metadata reads, actions, page estimates and conditional dependencies. Sparse
histories, underfilled pages and producer checks alter actual requests. Migration gaps/pending publications
are explicit blockers. Execution observes/reselects independently; preview is not an execution token.

## Inspect local progress

`./alpaca-eod status --store data_pricing`: offline, read-only orchestration/registration work, scheduling,
selected pricing journals, lineage publication and summary pending inputs. Missing/incomplete boundaries
name blockers and reconstruction commands; inspection never reconstructs them.

`ready_for_update`: required boundaries available. `processing_complete`: also no outstanding local
work/publications. Either false → nonzero exit. Neither claims a fresh provider check or future-target
coverage. Counts distinguish nonempty/empty histories, unresolved rows and complete assignment.
Unresolved identity may be completed processing, not public availability. Coverage ends are exclusive,
independent of last observed bars. [Observation contract](technical-contracts.md#universe-observation-and-work-state).

## Adopt or reconstruct an existing store

Before recovery-registry expansion, pin an existing interrupted clean-room build's original input:

```bash
./alpaca-eod catalogue pin-universe --store data_clean_room --symbols-file resources/ticker.csv
```

Original labels/contracts must match; pinning cannot make incompatible versions resumable. Then adopt an
existing standalone store:

```bash
./alpaca-eod lineage migrate-index --store data_pricing
./alpaca-eod universe rebuild --store data_pricing
./alpaca-eod status --store data_pricing
./alpaca-eod update --store data_pricing --through 2026-10-16 --plan
```

`migrate-index`: bounded, resumable indexed-receipt reads; no pricing payloads/recalculation. Projects used
lookup evidence; unknown historical times stay unknown. Schema upgrade alone does not establish coverage.
`universe rebuild`: initialize durable registration/work state or retain a compatible queue; accepts an
empty store before lineage/summary construction. Does not finish work or establish those boundaries.
Reject incompatible state for explicit examination; never erase it silently.

After pending publications finish, explicitly rebuild missing/incomplete summary:

```bash
./alpaca-eod summary rebuild --store data_pricing
```

Offline indexed-status/validator reads, no pricing history/provider requests. Normal publications maintain
membership/baselines thereafter. Missing scheduling coverage requires explicit `lineage update`, which may
perform a bounded source-generation census; daily updating refuses silent reconstruction.

## Recovery registry and retries

`resources/ticker.csv`: sorted unique single CSV record; atomic union under stable lock. Membership only
grows through existing labels, accepted lineage dependencies and qualifying discoveries. Absence, inactivity,
exchange change, empty history or failure never removes labels. Registry retention preserves opportunities,
not guaranteed provider history; the current catalogue cannot reconstruct all historical priceable labels.
Tickers are dated labels, not unique identities.

`universe reconcile`: fresh observation + registration, no price acquisition. Daily update registers first.
Standalone `pricing acquire/refresh` and `lineage update/refresh` share durable registration for selections
and dependencies, including other exchanges. Require initialized work state; recover registration before
requests. Record dependency targets/complete groups before acquisition. Disabling acquisition preserves
unfinished work despite later catalogue omission. Source-file selection is captured once; registration,
execution and batch memo use it; later file/registry edits affect later commands.

Accepted targets/explicit `pricing acquire --from` starts survive interruption, inactivity, omission and
narrower retries. Preserve earliest unfinished start and required end across commands. Completion needs
both endpoints, even for empty history; later-than-invocation extensions remain deferred. Identical empty
requests reuse coverage; later targets acquire only the increment. Relevant identity changes or explicit
history refresh may invalidate coverage.

Transport failures: pending, five-minute retry delay. Access denial/exact symbol rejection: blocked until
`update --retry-blocked`; this also bypasses transport delays, never an older cutoff. Restrictions span
complete groups and every daily/standalone phase; switching commands grants no retry. Independent groups
may finish. Standalone lineage reports unsatisfied accepted coverage/check obligations; daily update
finishes them. Correction approval remains bound to its pricing generation even after lineage removes the
symbol from the active summary.

An auxiliary whose bars request is exactly rejected may retain valid metadata. The owning group can publish
that rejection with independently supported histories and complete dependency work; no invented empty
source. Preserve the auxiliary's group on reselection, including auxiliary-only work. Compare full API and
compact projection lookups by identity facts.

After interruption, rerun the owning producer. Stable locks/staging/journals finish original inputs;
an older target cannot restart larger unfinished acquisition. Never delete locks, journals or selected
artifacts manually. Lock-file existence does not establish lock ownership. Readers reject incomplete
publications and never repair.

`lineage_update_check.sqlite3` and `download_completion.json` are disposable hints. Only after authorized
out-of-band restore/import with producers stopped may they be discarded and explicitly reconstructed.
Retain receipts, locks, journals and `universe_updates.sqlite3`; rebuild summary separately. Never test
restoration over a live store.

## Focused standalone work

```bash
./alpaca-eod universe rebuild --store data_pricing
./alpaca-eod pricing acquire --store data_pricing --symbol DELL --symbol HPE --through 2026-10-16
./alpaca-eod lineage update --store data_pricing --through 2026-10-16
./alpaca-eod lineage inspect --store data_pricing --symbol DELL
```

Initialize work state once. New stores need lineage then `summary rebuild` before daily update;
`status` identifies missing boundaries. Acquisition requires explicit compatible/empty store and publishes
pricing only; follow with lineage. Initial start: 2016 unless later `--from`; reject starts excluding held
observations, before 2016 or after the resolved final session, before registration/work/requests.

`--symbols-file`: headerless CSV or one label/line. `--recovery-universe`: explicit additive recovery list,
potentially full acquisition.

| Command | Scope |
|---|---|
| `pricing check` | Live summary last-session comparison; no publication. |
| `pricing refresh` | Check eligible histories, replace differences over retained spans, finish lineage. |
| `pricing refresh --symbol TICKER` | Explicit retained-span replacement; follow with `lineage update`. |
| `lineage update --symbol TICKER` | Complete selected group; may acquire dependency prices. |
| `lineage update` | Pending inputs/incompatible rules; explicit hint reconstruction if needed. |
| `lineage refresh` | Full selected-group evidence, including older corrections. |
| `lineage inspect --symbol TICKER` | Offline evidence; no rule replay. |
| `lineage update/refresh --no-pricing-acquisition` | No price acquisition; metadata requests still occur. |

Pricing: up to three workers, one shared 199-request/minute budget per invocation, not account-wide.
Lineage: one writer/store; others wait. Daily/standalone producers share the store update lock.
Independent successful publications survive failed batches. Approved append is internal; no public
append shortcut or redundant aliases.

## Read and export

```bash
./alpaca-eod resolve --store data_pricing --symbol DELL
./alpaca-eod read --store data_pricing --symbol DELL --adjustment raw all
./alpaca-eod export bloodhound --output ./alpaca.parquet --symbol DELL
```

[Public API](public-api.md): identity selection, dates, output types. Exit statuses: 0 success;
1 runtime failure/incomplete work; 2 invalid arguments; standalone reader 3 busy (`--wait` waits).
No argument abbreviation.

Parquet requires [requirements-bloodhound.txt](../requirements-bloodhound.txt). Output parent must exist
outside source store. Default selection: all indexed lineage groups. `--include-all-passed` permits rejected
identities; diagnostics only stderr. `--replace` permits output replacement after successful conversion.
Snapshot changes, ID collisions, memory exhaustion or writer failures abort. Only persistent output:
Parquet; no Bloodhound registration.

## Separate catalogue workflow

Combined monthly store, separate from standalone:

```bash
./alpaca-eod catalogue update --store data_vtest --symbol DELL --through 2026-10-16
./alpaca-eod catalogue export --repository-root . --output ./online.csv
```

Resolve canonical symbols → full catalogue cascade. New symbols acquire from 2016; existing ones replace
selected months. `--from` must start a month. Standalone cutoff/completion rules apply; reject cutoffs before
retained coverage/observations. Interrupted publication retains canonical/submitted symbols and exact bounds.

Catalogue read/export use `--repository-root` → `data_vtest/` via `DataLibrary`; no standalone/unactivated
fallback. One producer holds the combined publication lock for the entire transaction.

Explicit fresh-universe workflow:

```bash
./alpaca-eod catalogue build --recovery-universe --store data_clean_room --through 2026-10-16
./alpaca-eod catalogue resume --store data_clean_room
./alpaca-eod catalogue audit --store data_clean_room
./alpaca-eod catalogue activate --store data_clean_room
```

Build starts empty, pins immutable input, consumes its own responses and mandates final audit. Resume:
pinned labels, original target, compatible contracts despite registry growth. Separate activation retains
previous `data_vtest/`. `--symbols-file` may select another sorted, unique, single-record universe.

`legacy probe/correlate/audit`: retained `data/end_of_day/` operations, `--repository-root`; probe/correlate
also require `--symbol`. Never fallback stores.
