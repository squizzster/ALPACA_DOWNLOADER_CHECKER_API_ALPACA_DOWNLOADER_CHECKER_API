# Operations

Run from the repository root using Python 3.11. `./alpaca-eod --help` and each command's `--help`
are the option reference. Producers need `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` and SIP access.
Local readers and inspection need compatible published data, without credentials or network.

## Daily update

```bash
./alpaca-eod update --plan
./alpaca-eod update --through 2026-10-16
./alpaca-eod status
```

The standalone store defaults to `data_pricing/`; `--store PATH` overrides it.
Every update fetches one complete US-equity catalogue, preserving all statuses and duplicate labels.
New discovery selects **active OTC, NYSE, NASDAQ, AMEX and TXSE** records, without a tradability filter.
Existing histories on other exchanges and required lineage dependencies remain retained.
Known active histories continue maintenance when absent from the catalogue, including empty or partial
adjustment histories that do not qualify for the live-check summary. Explicit inactive evidence is handled
separately. An empty requested range remains distinct from unavailable or failed acquisition.
Pricing always uses SIP; registering an OTC-labelled asset does not promise separate OTC-feed access.

The [ordered flow](ARCHITECTURE.md#daily-update) registers accepted labels, acquires missing histories,
reconciles changed identity/status evidence, then checks, corrects and appends eligible stored histories.
Stage progress goes to stderr; the final report is JSON on stdout. Failures or unfinished required work
return nonzero. Published unresolved identities are valid outcomes, distinct from failed processing.

`--through` is an inclusive cutoff date, resolved back to the latest exchange session on or before it.
For example, Friday `2026-10-16`, Saturday `2026-10-17` and Sunday `2026-10-18` all select Friday's
session and the exclusive UTC boundary `2026-10-17T00:00:00Z`. Weekends, exchange holidays and
exceptional closures use the shared market calendar. The cutoff is resolved before work selection,
registration or requests; closed days alone do not advance pricing coverage or require republication.
Progress output identifies the requested date and effective session; update/preview JSON includes both
in `pricing_cutoff`. Reader and export date filters remain inclusive calendar filters.
The default is the last exchange session **strictly before today's New York date**, frozen once per run.
Friday becomes eligible after Saturday midnight in New York. Crossing UTC midnight changes nothing.
Explicit cutoff dates must also precede today's New York date; unsupported calendar ranges are rejected.
Already accepted work and publication journals retain their original exclusive boundaries during recovery.
The target reaches initial and dependent acquisitions. Sources already covered beyond it stay unchanged
and are reported separately; they are not freshly checked or rewound. Reconciliation requiring a larger
range remains incomplete. This is an update through a target, not a historical knowledge snapshot.
Catalogue observation time, acquired coverage, last observed session and provider symbol-mapping
`asof` remain distinct. A checked range need not contain a bar on its final session.

Preview may fetch metadata but does not register labels, initialize databases, recover publications or
publish data. It reports selected work/reasons, additions, metadata reads, API actions, estimated HTTP
pages and conditional dependency work. Sparse histories, underfilled pages and producer checks affect
actual requests. Migration gaps and pending publications are explicit blockers. Execution obtains its
own observation and reselects current boundaries; a previous preview is not an execution token.

## Inspect local progress

`./alpaca-eod status --store data_pricing` is offline and read-only. It reports orchestration work,
registration intentions, lineage scheduling, selected pricing journals, lineage publication and summary
pending inputs. Missing or incomplete boundaries appear as blockers with their owning reconstruction
command; inspection never performs that reconstruction.

`ready_for_update` means the required local boundaries are available. `processing_complete` additionally
requires no outstanding local work or publications. Status returns nonzero while either condition fails.
Neither field claims that a fresh provider check ran or that pricing reached a requested future cutoff.
Published-history counts distinguish nonempty and successfully empty histories, unresolved rows and
complete row assignment. Unresolved identity can be a completed processing outcome; it is not a claim of
public availability. Coverage timestamps are exclusive boundaries, separate from the last observed bar.

## Adopt or reconstruct an existing store

Before the first recovery-registry expansion, pin any interrupted clean-room build's original input:

```bash
./alpaca-eod catalogue pin-universe --store data_clean_room --symbols-file resources/ticker.csv
```

Use this only for an existing build. Its original labels and contracts must match; pinning does not make
incompatible producer versions resumable. Then adopt an existing standalone store explicitly:

```bash
./alpaca-eod lineage migrate-index --store data_pricing
./alpaca-eod universe rebuild --store data_pricing
./alpaca-eod status --store data_pricing
./alpaca-eod update --store data_pricing --through 2026-10-16 --plan
```

`lineage migrate-index` reads indexed receipts in bounded, resumable batches, without pricing payloads
or recalculation. It projects the lookup evidence actually used by published decisions; unknown historical
observation times remain unknown. A schema upgrade alone does not mark projection coverage complete.
`universe rebuild` explicitly initializes durable registration and orchestration state; an existing compatible
queue is retained. It also accepts a new empty store, without requiring lineage or summary construction
first. It does not finish producer work or declare those boundaries complete. Incompatible durable state
is rejected for explicit examination, never silently erased.

If the pricing summary is missing or incomplete, rebuild it explicitly after pending publications finish:

```bash
./alpaca-eod summary rebuild --store data_pricing
```

This offline operation reads indexed stored status and endpoint validators, without price-history reads
or provider requests. Normal publication subsequently maintains eligibility and baselines automatically.
Missing lineage scheduling coverage requires an explicit `lineage update`, which may perform its bounded
source-generation census. Daily updating refuses to silently expand into that reconstruction.

## Recovery registry and retries

`resources/ticker.csv` contains one sorted, unique CSV record. Normal automation publishes an atomic union
under a stable lock. Membership only grows: existing labels, accepted lineage labels/dependencies and
qualifying catalogue discoveries. A ticker is a dated label, not a unique security. Catalogue absence,
inactivity, exchange changes, empty history and acquisition failure never remove it.

The provider catalogue cannot reconstruct every historical priceable label. Retaining the registry keeps
those recovery opportunities; it cannot guarantee that the provider will retain their history.
`universe reconcile` accepts a fresh observation and registers selected work without acquiring prices.
Daily `update` also performs this registration before acquisition.
Standalone `pricing acquire`, `lineage update`, `lineage refresh` and `pricing refresh` use the same durable registration path
for their selected labels and discovered dependencies, including dependencies outside the discovery
exchanges. They require initialized work state and finish pending registration intentions before acquiring
prices. Dependency acquisition targets and required group members are recorded separately from registry
membership, before acquisition. Disabling pricing acquisition retains that unfinished work. Status and later
updates expose it even if the provider no longer returns the relationship.

Durable work survives registration and publication interruptions. Accepted maintenance targets and explicit
`pricing acquire --from` starts cannot shrink after inactivity, catalogue omission or a narrower retry
request. Daily update and standalone pricing/lineage commands retain the earliest unfinished start and the
required end. Completion requires published coverage of that whole range, including successful empty
coverage. A history extension remains deferred when completing it would exceed the invocation's cutoff.
A successful identical empty request is
reused; a later target acquires the additional interval. Changed relevant identity evidence or an explicit
history refresh can invalidate earlier coverage. Transport failures remain pending with a five-minute
retry delay. Access denial and exact symbol rejection remain blocked until `update --retry-blocked`
explicitly retries them. This option also bypasses the transport retry delay, but cannot override an older
requested cutoff. Restrictions cover affected groups and every update phase, including maintenance and
standalone lineage/pricing commands. Changing commands does not authorize a retry. Unrelated selected
groups can still proceed. Standalone lineage reports pending work when its publications cannot satisfy an
accepted coverage or history-check requirement; use the daily updater to finish those requirements.
Completed correction approvals stay bound to their published pricing generation even if the subsequent
lineage decision removes that symbol from the active summary.

A missing corporate-action auxiliary may have valid identity metadata while the bars endpoint rejects its
label. The lineage owner can publish that exact rejection with the supported source histories and complete
the dependency work. It retains the label and metadata without inventing an empty pricing publication.

Rerun the owning producer after interruption. Stable locks, staging and journals finish publications with
their original inputs; an older target cannot restart a larger unfinished acquisition. Do not delete lock
files, journals or selected artifacts by hand. A lock file's existence does not mean its lock is held.
Readers reject incomplete publications and never repair them.

`lineage_update_check.sqlite3` and `download_completion.json` are disposable scheduling hints.
After an authorized out-of-band restore/import with producers stopped, those caches may be discarded
and explicitly reconstructed. Retain receipts, locks, journals and `universe_updates.sqlite3` intentions;
rebuild the summary separately. Never test restoration over a live store.

## Focused standalone work

```bash
./alpaca-eod universe rebuild --store data_pricing
./alpaca-eod pricing acquire --store data_pricing --symbol DELL --symbol HPE --through 2026-10-16
./alpaca-eod lineage update --store data_pricing --through 2026-10-16
./alpaca-eod lineage inspect --store data_pricing --symbol DELL
```

Initialize work state once before standalone pricing or lineage commands. For a new store, finish lineage
and run `summary rebuild` before daily updating; `status` identifies any remaining missing coverage.
Ordinary acquisition requires an explicit compatible or empty store. It publishes pricing only; run
lineage afterward. Initial history starts in 2016 unless `--from` specifies a later start. A start that
excludes held observations is rejected. Explicit starts before 2016 or after the resolved final trading
session are rejected before registration or durable work is accepted.
`--symbols-file` accepts headerless CSV or one label per line;
`--recovery-universe` explicitly selects the additive recovery list and can mean a full acquisition.
Acquisition reads either selection file once. Registration, durable work, downloads and the completed-batch
record use that same selection; subsequent file edits or registry additions apply to a later command.

| Command | Scope |
|---|---|
| `pricing check` | Live last-session comparison from the trusted summary; no publication. |
| `pricing refresh` | Check existing eligible histories, replace differences over their retained spans and finish affected lineage. |
| `pricing refresh --symbol TICKER` | Explicit retained-span replacement, followed separately by `lineage update`. |
| `lineage update --symbol TICKER` | Selected complete dependency group; may acquire related pricing. |
| `lineage update` | Pending inputs and incompatible rules, with explicit hint reconstruction if needed. |
| `lineage refresh` | Reacquire selected groups' full evidence, including older corrections. |
| `lineage inspect --symbol TICKER` | Offline published evidence; does not replay rules. |
| `lineage update/refresh --no-pricing-acquisition` | Disable pricing acquisition; metadata requests still occur. |

Pricing uses up to three workers sharing one 199-request/minute budget per invocation, not account-wide.
Lineage has one writer per store; a second waits. Daily updates and standalone producers
also serialize on the same stable store update lock.
Independent successful publications remain usable after a failed batch. Public append shortcuts and
redundant aliases are removed; approved append is internal to orchestration.

## Read and export

```bash
./alpaca-eod resolve --store data_pricing --symbol DELL
./alpaca-eod read --store data_pricing --symbol DELL --adjustment raw all
./alpaca-eod export bloodhound --output ./alpaca.parquet --symbol DELL
```

See [Public API](public-api.md) for identity selection, date ranges and output types.
The standalone reader returns status 3 when busy; `--wait` waits. Otherwise 0 means success,
1 runtime failure/incomplete work and 2 invalid CLI arguments. Argument abbreviation is disabled.

Parquet export additionally needs [requirements-bloodhound.txt](../requirements-bloodhound.txt).
The output parent must exist outside the source store. Default selection is all indexed lineage groups.
`--include-all-passed` permits rejected identities; diagnostics go only to stderr.
`--replace` permits output replacement after successful conversion. Snapshot changes, ID collisions,
memory exhaustion and writer failures abort. The only persistent output is the Parquet file.
Conversion does not register data with Bloodhound.

## Separate catalogue workflow

These commands operate on the combined monthly store, separate from standalone pricing:

```bash
./alpaca-eod catalogue update --store data_vtest --symbol DELL --through 2026-10-16
./alpaca-eod catalogue export --repository-root . --output ./online.csv
```

The combined producer resolves canonical symbols and runs the complete catalogue cascade. New symbols
acquire from 2016; existing symbols replace selected months. `--from` must be a month start.
The inclusive `--through` and default completion rule match standalone updating. A cutoff preceding
retained coverage/observations is rejected before publication. After interruption, the publication marker
enforces the original canonical and submitted symbols and exact request boundaries.

Public catalogue read/export use `--repository-root`, selecting its `data_vtest/` through `DataLibrary`.
They cannot silently select a standalone or unactivated store. One producer holds the combined store's
publication lock for its whole transaction.

For an explicitly requested fresh universe:

```bash
./alpaca-eod catalogue build --recovery-universe --store data_clean_room --through 2026-10-16
./alpaca-eod catalogue resume --store data_clean_room
./alpaca-eod catalogue audit --store data_clean_room
./alpaca-eod catalogue activate --store data_clean_room
```

Construction starts empty, pins an immutable universe snapshot, consumes its own provider responses and
performs a mandatory final audit. Resume uses the pinned labels, original target and compatible contracts,
regardless of later registry growth. Activation is separate and retains the previous `data_vtest/` store.
`--symbols-file` can select a different sorted, unique, single-record universe for construction.

`legacy probe`, `legacy correlate` and `legacy audit` retain the operations on `data/end_of_day/`.
They use `--repository-root`; probe/correlate also require `--symbol`. These stores are never fallbacks.
