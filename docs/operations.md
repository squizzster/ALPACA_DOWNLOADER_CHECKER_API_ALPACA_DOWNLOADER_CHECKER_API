# Operations

Run commands from the repository root with Python 3.11. Each command's current `--help` output is the option
authority. The combined producer and `DataLibrary` reader select `data_vtest/`; they do not fall back to
`data/end_of_day/`.

## Command map

| Command | Current status | Side effects and access |
|---|---|---|
| [`download_alpaca_pricing.py`](../download_alpaca_pricing.py) | Independent pricing-only downloader. | Contacts only Alpaca stock bars, writes an explicit compact-format destination, and never invokes lineage or the catalogue. |
| [`build_alpaca_lineage.py`](../build_alpaca_lineage.py) | Standalone lineage twin; exactly one worker. | Consumes the compact store, contacts identity/action APIs, delegates missing prices to the downloader, and acknowledges durably processed notices. |
| [`read_alpaca_pricing.py`](../read_alpaca_pricing.py) | Standalone resolved-pricing reader. | Explicit compact store; read-only, no credentials/network, JSON or CSV stdout, nonblocking by default. |
| [`convert_latest_alpaca_pricing_to_blood_hound_parquet`](../convert_latest_alpaca_pricing_to_blood_hound_parquet) | Offline Bloodhound-shaped export. | All eight standalone views; one Parquet with embedded full-ID mapping; no source writes or automatic Bloodhound registration. |
| [`output_current_lineage_evidence.py`](../output_current_lineage_evidence.py) | Evidence from that same lineage workflow, not a second resolver. | Default mode can acquire and publish; `--offline` only inspects existing publications under locks. Emits JSON to stdout. |
| [`run_alpaca_end_of_day_ticker_pipeline.py`](../run_alpaca_end_of_day_ticker_pipeline.py) | Canonical monthly acquisition-to-catalogue producer. | Contacts Alpaca, writes the current application store, and requires credentials. |
| [`download_alpaca_end_of_day_bars.py`](../download_alpaca_end_of_day_bars.py) | Compatibility executable for the same complete producer. | Same arguments and runtime effects. |
| [`build_alpaca_end_of_day_clean_room.py`](../build_alpaca_end_of_day_clean_room.py) | Restartable provider-only universe constructor and complete auditor. | Contacts Alpaca, writes only its explicit isolated store, requires credentials, and optionally activates only after audit. |
| [`read_alpaca_end_of_day_ticker.py`](../read_alpaca_end_of_day_ticker.py) | Public catalogue/history client. | Current store, strictly read-only, no credentials or network. |
| [`inspect_alpaca_security_lineage.py`](../inspect_alpaca_security_lineage.py) | Legacy-layout operator tool. | Reads `data/end_of_day/`, contacts Alpaca, and is not part of the current monthly cascade. |
| [`correlate_alpaca_end_of_day_security_segments.py`](../correlate_alpaca_end_of_day_security_segments.py) | Legacy-layout operator tool. | Writes derived `data/end_of_day/` artifacts and is not the current catalogue publisher. |
| [`audit_alpaca_end_of_day_catalog.py`](../audit_alpaca_end_of_day_catalog.py) | Legacy-layout catalogue audit. | Audits `data/end_of_day/`; it does **not** audit the catalogue opened by `DataLibrary`. |

The last three commands remain useful only when explicitly inspecting the legacy runtime tree. Invoke them through
`python3.11` in the current environment because their `python3` shebang resolves to Python 3.9.25 here.

## Requirements and safety

The 2026-10-14 [storage rework decision](storage-rework-notes.md) retains the inventoried project and temporary stores
in place for development reuse. Cleanup and further copies remain deferred. Standalone development uses
`data_pricing/`; a focused task does not authorize conversion, cleanup, or universe-wide acquisition of retained stores.

- Python 3.11 and IANA `America/New_York` time-zone data.
- A Unix environment and filesystem supporting `fcntl.flock`, atomic replacement and directory `fsync`.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` plus SIP entitlement for producer runs.
- No credentials or network for public reads and exports.
- No `pigz` requirement for the current monthly source; deterministic gzip is produced by Python.

The configured target in [`pyproject.toml`](../pyproject.toml) is Python 3.11. This environment has Python 3.11.13,
but unqualified `python3` is 3.9.25; use `python3.11 -B` explicitly for scripts whose shebang is not already 3.11.
Core producers and readers use the standard library. Optional Parquet export requires the pinned PyArrow dependency
in [`requirements-bloodhound.txt`](../requirements-bloodhound.txt); there is no project-wide lock or package-install
metadata. Ruff is a separate development tool. See [verification](development.md#verification) for local checks and CI status.

For the combined catalogue workflow, use only one producer process against a store. Its global lock serializes writers and readers, but it is not a durable
batch scheduler and does not make independently submitted focused transactions one coherent request. The focused
producer is sequential and has no `--workers` option. The clean-room command supplies the durable universe scheduler;
do not run another producer or activator while it is activating a completed store.

Do not hand-edit a gzip, month receipt, source inventory, validation artifact, lineage report, correlation branch,
warning output, candidate, availability report, catalogue object, or publication marker. Boundary receipts are the
selection authority; changing a selected file outside its owner violates the trust contract even when JSON remains
syntactically valid.

## Standalone pricing download

Use `download_alpaca_pricing.py`, not the older `download_alpaca_end_of_day_bars.py` compatibility command, when only
pricing is wanted. The older command still invokes the complete catalogue pipeline.

```bash
./download_alpaca_pricing.py --symbol GOOG --store data_pricing --to 2026-10-13
./download_alpaca_pricing.py --file /path/to/tickers.csv --store data_pricing --to 2026-10-13
```

The command requires `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` and SIP access. It requests all eight adjustment
views per selected ticker label. It does not resolve an asset UUID, canonicalize ticker punctuation through an
asset lookup, create public IDs, or run lineage. A response under another ticker label is rejected.

`--store` is required. Only an empty directory or a matching v3 standalone store is accepted; incompatible stores are
not automatically migrated, repaired, or copied. Prices go under `end_of_day/pricing/<TICKER>` and pending notices
under `end_of_day/manifests/<TICKER>`. `StandaloneReader` reads this pair; `DataLibrary` retains its catalogue format.

| Option | Meaning |
|---|---|
| `-s TICKER` / `--symbol TICKER` | Repeat for multiple tickers. |
| `--file PATH` | Headerless CSV, one ticker per line, or a mixture; blank cells are ignored. |
| `--all` | The tracked `resources/ticker.csv` universe; never an asset/identity query. |
| `--workers {1,2,3}` | Concurrent whole-ticker workers; default and maximum 3, sharing one 199-RPM budget. |
| `--from YYYY-MM-DD` | Optional history start, on/after 2016-01-01; rejects any ticker with earlier stored observations. |
| `--to YYYY-MM-DD` | Inclusive completed date; cannot be combined with `--request-end`. |
| `--request-end YYYY-MM-DD` | Existing exclusive UTC-midnight option; default is today UTC. |
| `--force-download` / `--refresh-all` | Reacquire the entire retained history range and replace it, including after recovery. |

The three ticker-selection modes are mutually exclusive. Labels are trimmed, uppercased, and deduplicated in input
order. A batch freezes one end date and shares one rate limiter. One worker owns a ticker's complete update, including
its lock, all adjustment views, and publication; only distinct tickers run concurrently. Ticker-specific failures—including `FROM` conflicts,
invalid labels, bad ticker receipts, and request failures—do not stop remaining tickers. Invalid global arguments,
unreadable/empty input files, or missing credentials fail before acquisition. Exit status is nonzero if any ticker fails.

All workers together admit at most 199 HTTP attempts per rolling minute, paced evenly; probes, pages, and retries all
count. HTTP 429 pauses new requests across every worker, honouring numeric or HTTP-date `Retry-After` (60 seconds for
missing/invalid values). Retryable 5xx responses use the same shared pause. Already in-flight requests may finish.
There are at most six attempts per request; an exhausted ticker fails without clearing the shared cooldown.
The budget is per CLI invocation, not account-wide across separate processes: do not run multiple download commands
with the same credentials expecting a combined 199-RPM limit. No persistent limiter files are created.

`FROM` defaults to 2016 on initial acquisition; omission retains the stored floor. An explicit start may move forward
only when no stored observation precedes it. An earlier start
backfills by full acquisition. Force cannot bypass the no-truncation rule. Backwards/future ends and non-ISO dates are
refused. The active month contains the day before the exclusive UTC end: November 1 still completes October.
Historical end/`asof` dates do not select historical adjustment vintage.

An identical completed request end and retained start returns unchanged without provider requests or payload reads.
Repeating the same entirely successful batch can also skip individual ticker receipts using an optional exit memo.
Advancing the requested end first compares the first and last stored sessions in every view against live bars. A match permits
active-month acquisition and rollover consolidation; a mismatch selects full replacement from the stored start.
The comparison uses one source-bound validator JSON, bootstrapped once for older sources. After acquisition only the
first session per nonempty view is re-requested; inconsistent results permit one full restart, then fail closed.
Automatic refresh and force both discard absent old sessions; their pending notice includes removed dates. No clipped
edges or old-price archive are retained. Force can replace damaged owned gzip data but requires intact ownership metadata.

The downloader waits for the ticker's exclusive lock before provider requests. The lineage consumer uses the
[locked handoff](technical-contracts.md#locking-publication-and-consumption), commits its work durably, then acknowledges
the notice. Never unlink `.download.lock`: its presence does not mean it is held.

An interrupted publication leaves `.pending.json` and staged changed outputs in the pricing ticker directory. Re-run
the same command to finish recovery; if it already reaches the requested end, a normal retry makes no new request.
Explicit force still reacquires after recovery. Do not
remove journal/staging files manually. Unexpected files, checksum changes, and symlinks fail closed.

Standard output is one JSON batch summary with `requested`, `succeeded`, `failed`, and per-ticker `results`. Each
result reports `updated`, `unchanged`, `empty`, `recovered`, or `failed`; successful acquisition also reports mode,
consistency, boundaries, and writes. Final results retain input order; progress/errors go to standard error in completion
order. At most the selected worker count of ticker jobs is scheduled at once. The optional batch memo retains only
the latest successful selection, without a growing run history. The precise storage and pending-range
rules live in [Technical contracts](technical-contracts.md#standalone-pricing-source-contract).

## Standalone lineage

```bash
./build_alpaca_lineage.py --store data_pricing --symbol GOOG
./build_alpaca_lineage.py --store data_pricing --pending
```

This sibling command consumes an existing standalone store; it does not copy legacy data or publish `DataLibrary`'s
catalogue. It requires the same credentials. One lineage worker is enforced across processes; `--workers` accepts
only `1`. A missing selected ticker goes through the same downloader handoff as a missing related ticker. With no
published range, the downloader uses its normal defaults: history from 2016-01-01 to today's exclusive UTC midnight.
Existing pricing supplies the range for related inputs; selecting lineage does not itself advance existing prices.

| Option | Meaning |
|---|---|
| `-s` / `--symbol` | Repeat for selected tickers; missing initial/related pricing is acquired as needed. |
| `--file PATH` | Headerless CSV or one ticker per line. |
| `--pending` | Default: changed-input hints, including provenance-only updates, and stale rule/calendar versions. Missing/incomplete hints trigger a local receipt scan. |
| `--all` | The same local catch-up scope and changed-input shortcut, **not** the downloader's tracked universe. |
| `--workers 1` | Exactly one lineage worker; another invocation against the same store waits for it. |
| `--refresh-metadata` | Reacquire full action/asset evidence for selected work, even without changed prices. |
| `--no-download` | Disable pricing acquisition only; identity/action requests still occur for work needing calculation. Missing/short pricing coverage leaves notices pending. |

Selection modes are mutually exclusive. Missing related bars normally go through the existing downloader, with all
pricing locks released first; its new notices are consumed on resumption. `--no-download` also applies to a missing
initial ticker; null range endpoints in its waiting result mean downloader defaults. Identity requests retain the established
identity client's pacing and bounded 429 retries. Downloader requests retain its own 199-RPM limiter. These limits
are not account-wide across independently running commands.

An exact rejected auxiliary may remain metadata-only while the supported source group publishes. This is distinct
from a successful empty response or a failed seed: see [unavailable auxiliary pricing](technical-contracts.md#unavailable-auxiliary-pricing).
Use `--refresh-metadata` to reconsider a recorded rejection; an unchanged group otherwise reuses it. Do not remove
the auxiliary's `.download.lock` or manufacture an empty source to make the group proceed.

The command returns one JSON summary (`workers`, `recovered`, `results`, `failed`). Results distinguish `processed`,
`unchanged`, `waiting_for_pricing`, and `failed`. Failure or waiting returns a nonzero status. Independent inputs
continue after a failure, unless an interrupted lineage publication must first be recovered. Unresolved identity is
a successful durable result, not a transport or processing failure.

### Restart and catch-up

Re-run the command after interruption. Recovery finishes an interrupted publication before selecting new work;
completed receipts remain reusable, while an unfinished calculation may repeat. Never delete
`end_of_day/lineage/.pending.json` manually. No archive of superseded pricing or lineage generations is kept.

Choose the selection for the intended work:

| Intent | Selection | Effect on completed work |
|---|---|---|
| Consume ordinary pricing updates | `--pending` (also the default) | Uses changed-input hints and indexed versions, then checks affected groups and ticker links. |
| Resume or catch up local sources | `--all` | Uses the same shortcut; an absent/incomplete work list falls back to scanning existing receipts. Provenance-only changes are included in both modes. |
| Revisit a bounded family | `--symbol TICKER` | Checks the complete dependency group, using the same reuse and publication path. |
| Deliberately reacquire older identity/action evidence | Add `--refresh-metadata` | Disables the unchanged-group shortcut and requests full metadata for the selected work. |

For an authorized all-local-source pass with no pricing acquisition:

```bash
python3.11 -B build_alpaca_lineage.py --store data_pricing --all --no-download
```

This command still needs credentials for unfinished identity work. Omit `--refresh-metadata` for an ordinary restart.
The exact [reuse gate](technical-contracts.md#resume-and-invalidation) checks input generations, pending notices and
owning versions before price loading or calculation. It also retains completed empty or unresolved outcomes.
When only correlation rules changed, unchanged group inputs reuse their approved metadata and recalculate lineage
locally. Explicit metadata refresh, pending pricing work and changed input groups still use the existing acquisition
path; a rule-only update does not itself request fresher provider facts.

Default and `--all` snapshot selected work once, after acquiring the worker lock. They can discover related labels
while running; unrelated downloads arriving later retain hints for the next invocation. Each group reads its current
published inputs under ticker locks; the invocation does not freeze every pricing generation at launch. If the
downloader completes after selection, a subsequent pass catches newly available sources. A second simultaneous
lineage command waits for the existing worker; it does not restart or accelerate it.

With a completed work list, an unchanged repeat reads shared store-level files and examines the existing index in
memory. It does not enumerate thousands of source directories or open their individual receipts. Missing coverage
requires a fallback scan once; only a successful scan exit establishes complete coverage. Changed rules/calendar can
still select substantial real work. There is no measured full-store wall-time guarantee.

`lineage_update_check.sqlite3` and `download_completion.json` at the store root are disposable scheduling caches.
Deleting the first restores lineage's receipt scan; deleting either disables the downloader's batch shortcut until
the next successful batch. Existing receipts and dated notices remain authoritative and are never changed merely
to reconstruct these caches. After an out-of-band restore/import or use of older publishing code, discard both
caches so the next run rediscovers the store. Keep the actual receipts, locks and recovery journals intact.

Use scoped metadata refresh when older provider corrections need reacquisition. Ordinary updates do not claim to
detect arbitrary old metadata revisions. Selection and shortcut behavior are owned by `run_lineage`, `local_symbols`
and `process_group` in [`pipeline.py`](../libs/alpaca_lineage_build/pipeline.py); CLI flags are owned by `build_parser`
in [`command_line.py`](../libs/alpaca_lineage_build/command_line.py).

### Lineage reader availability and progress

Each completed group is readable while the single worker handles unrelated groups. The worker-only guard spans the
invocation; the shared/exclusive publication gate covers only selection and installation. `read_lineage` and offline
evidence wait on conflicting pricing or publication work; the public reader defaults to a prompt busy response.
After a crash, a pending transaction must be recovered before readers can serve results. Keep these
[publication and lock guarantees](technical-contracts.md#lock-delegate-publish-acknowledge) when inspecting results;
unlocked file access is not a coherent reader handoff.

The build CLI prints its JSON summary only when the invocation finishes; it has no per-group progress stream. An
empty redirected stdout log is therefore not proof that work stopped. Evidence inspection is also not a live progress
poll: it observes the same locks and may read complete session grids. Neither standalone publication nor its internal
reader makes an ID available through `DataLibrary`; that library still selects the combined catalogue.

## Standalone reader

Use `read_alpaca_pricing.py` for resolved histories from `data_pricing`, not the combined-catalogue CLI:

```bash
./read_alpaca_pricing.py --store data_pricing --symbol GOOG --resolve
./read_alpaca_pricing.py --store data_pricing --symbol GOOG --type raw --from 2026-10-01 --to 2026-10-14
./read_alpaca_pricing.py --store data_pricing --symbol GOOG --type raw all --from 2026-01-01
./read_alpaca_pricing.py --store data_pricing --symbol GOOG --all-types --format csv
```

The command makes no API calls or datastore writes and creates no lock files. It requires published lineage for the
source group. If a producer holds a required group lock or the brief publication gate, it returns **status 3** promptly
with a busy message; `--wait` instead waits. A busy unrelated ticker does not block the read. Same-group acquisition
can still take time. Do not bypass locks to read partially installed files. After upgrading from lineage v2, start the
new lineage command once to activate the v3 lock protocol; compatible completed receipts are reused, not rebuilt.

Status **0** means success, **1** means an unavailable/invalid publication or query/runtime error, and **2** means a
CLI syntax/option error. No history is printed before its read succeeds. Use `--resolve`, then `--public-id` alongside
the same symbol when multiple IDs match. `--format csv` writes stdout only; an output file requires an explicit shell
redirection. The [consumer guide](public-api.md#standalone-resolved-pricing-reader) owns output types, date semantics,
lookup scope and overlap selection. This reader does not activate or populate the online catalogue.

## Bloodhound Parquet export

Install the optional writer with Python 3.11 (`python3.11 -m pip install -r requirements-bloodhound.txt`).
The tested dependency is pinned in [requirements-bloodhound.txt](../requirements-bloodhound.txt); core readers remain
standard-library-only. The output parent must exist and must be outside the source datastore.

```bash
./convert_latest_alpaca_pricing_to_blood_hound_parquet --output ./alpaca.parquet
./convert_latest_alpaca_pricing_to_blood_hound_parquet --output /tmp/alpaca_blood_hound.parquet --include-all-passed
./convert_latest_alpaca_pricing_to_blood_hound_parquet --output ./sample.parquet -s NVDA -s AAPL --from 2020-01-01
```

Default store is `data_pricing`; default selection is all existing indexed lineage groups, not every downloaded ticker.
`-s/--symbol` is a repeatable group locator; every resolved public ID in that selected group is considered. Repeat
`--public-id` to restrict identities explicitly. Reused labels never collapse IDs. The latest observed ticker in each
full history labels all output rows; GOOG and GOOGL are not excluded or combined. `--from/--to` filter inclusively
**after** full-history factor calculation and label selection. Gaps remain gaps.

The default aborts without publishing if any selected source/ID fails. `--include-all-passed` instead attempts every
selected public ID and publishes only histories passing the same conversion checks. Rejected identities contribute
no rows; valid dates are not salvaged from a rejected history. Rejections identify the source, public ID when known,
and reason on **stderr only**, alongside progress and the final rejection count. This replaces `--allow-partial`.
Stdout contains only the output path, published row count and published identity count. No diagnostic sidecar is made.

`--memory-mib` sets the RAM allowance, default/maximum **8192 MiB** (minimum 256). Accepted output is retained as
Arrow tables, not Python row dictionaries or compressed Parquet bytes. The allowance reserves a quarter, up to 2 GiB,
for active histories, metadata and writer workspace. Output buffers and conservative per-table overhead are charged
against the remainder; the read-cache limit shrinks as output grows. This is retained-memory accounting, not a hard
OS RSS cap. Insufficient output capacity aborts the entire export without omitting IDs or spilling to disk.

`--cache-mib` remains the reader's upper bound, default **2048 MiB**, maximum **6144 MiB**; zero disables retained
reuse. It is not an additional independent allowance. Cache decoded rows rather than only gzip bytes; aliases and IDs
within a group share those rows. Eviction or changed inputs can require another decode. The cache creates no files,
changes no numeric checks, and does not bypass locks or current-generation selection. Groups release their working
cache on close. Progress counts source locators processed, accepted identities and rows **held in RAM**, not written
rows. No output file is created during acquisition. At the end, one serialization packs row groups across IDs, then
the existing freshness checks precede atomic publication. Physical row order is deterministic source
group order, then series ID within each group, then date; it is not a global series-ID sort.
After successful publication, the cumulative RAM-only cache/read ledger is printed once to stderr. It is not written
to the Parquet, stdout or a sidecar. Peak process RSS is also reported once to stderr. The only persistent export
artifact is the final Parquet; staging exists only during final serialization and is removed on failure.

`--wait` waits for producer locks. Changed selected generations, ID collisions and writer errors always abort under
either policy. No passing observations means no output. `--replace` replaces an existing output only after successful
conversion; otherwise existing files are never overwritten. Failed temporary output is removed. Exit 0 means the
requested policy succeeded (with rejections permitted only under `--include-all-passed`), 1 means failure, 2 means
argument error. Passing these checks is conversion acceptance, not independent certification of market truth.

One persistent file contains the exact 18-column schema plus JSON metadata under footer key `alpaca_bloodhound`.
Read it with `json.loads(pyarrow.parquet.read_metadata(path).metadata[b"alpaca_bloodhound"])`; the JSON includes
full public-ID mappings, supporting group/source generations, inference assumptions and uncertainty for published
identities only. There are no omission records, rejection counts, partial flags or zero-window identity entries in
the file. Identity mappings and provenance remain footer metadata, not extra data columns or a source-data/audit tree.

The output is **schema-compatible**, not automatically registered or independently action-certified. Unknown dated
type/exchange are `UNKNOWN`; CIK/FIGI/name remain null. Use an all-observed universe if importing after separate
Bloodhound admission; do not pretend these are historical listing classifications or filing identities. The exact
formula, limits and evidence rationale live in the [conversion contract](technical-contracts.md#bloodhound-conversion-contract).

## Current lineage evidence

Use the actual standalone workflow to obtain the information available to deterministic decisions:

```bash
./output_current_lineage_evidence.py TICKER --store data_pricing
```

Replace `TICKER` with the requested label. The default store is the repository's `data_pricing/`. Default mode requires
credentials and calls `run_lineage` with an evidence observer under its existing locks. It can acquire identity
evidence, delegate missing prices, publish lineage and consume notices. It does not disable pricing acquisition,
advance existing pricing merely for inspection, or use a second rule engine.

For read-only inspection of an existing publication:

```bash
./output_current_lineage_evidence.py TICKER --store data_pricing --offline
```

`--offline` needs no credentials and makes no downloads, writes or rule replay. It fails if no lineage publication
exists; it does not repair one. `--refresh-metadata` selects the normal pipeline's full metadata refresh and is
mutually exclusive with `--offline`. Do not use refresh merely to obtain another copy of the same explanation.

Stdout is one JSON evidence document; a failed workflow returns status 1. Reused or offline decisions retain their
published versions, while `installed_versions` describes the code now present. Inspect freshness and decision origin
before calling a result current. Rule paths explain recorded selection and fall-through order, not unrecorded failed
predicate reasons. A full diagnostic may read closed session grids even when normal processing reuses their assignments;
it is not a cheap progress poll. Pricing values are not duplicated into the evidence document.

Both modes follow the [lineage reader/worker locking limits](#lineage-reader-availability-and-progress). The evidence
CLI's parser is [`evidence_command_line.py`](../libs/alpaca_lineage_build/evidence_command_line.py); it shares
`run_lineage` with the build CLI rather than owning another execution pipeline.

The [operator evidence contract](technical-contracts.md#operator-lineage-evidence) owns its fields and limits.
The [development loop](development.md#lineage-research-and-rule-improvement-loop) owns research-agent selection,
case-by-case evaluation and the consolidated review cadence. Neither command is an automated research scheduler.

## Run the complete producer

Process explicit submitted labels:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol AAPL \
  --symbol BFA \
  --request-end 2026-10-14
```

Or provide one label per nonblank line:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file /path/to/symbols.txt \
  --request-end 2026-10-14
```

The parser trims whitespace, uppercases labels, preserves first-seen order, and removes duplicates. Labels must match
`[A-Z][A-Z0-9.-]*`. `--symbol`, `--file`, and `--evidence-cohort` are mutually exclusive.

`--evidence-cohort` selects the locked ten-label admission cohort. If its request end is omitted, the command pins the
cohort's recorded parity boundary. It is a focused verification input, not a full-universe mode.

The tracked [`resources/ticker.csv`](../resources/ticker.csv) is a one-record comma-separated recovery snapshot. It is
not accepted directly by the line-oriented `--file` parser.

### Authoritative symbols

Every submitted label is resolved before the write transaction begins. A successful exact US-equity lookup makes the
returned Alpaca symbol authoritative, including punctuation such as `BFA -> BF.A`. Multiple submitted labels resolving
to one canonical symbol are acquired once. Submitted-to-canonical provenance is retained in the lineage report.

Do not create a second directory under the submitted spelling. Source, validation, lineage, correlation, warnings,
and catalogue contribution all use the canonical symbol.

### Request boundaries

`--request-end YYYY-MM-DD` is an exclusive UTC midnight. For example, `--request-end 2026-10-14` requests bars before
`2026-10-14T00:00:00Z`; it does not mean that October 14 is included.

If omitted, the producer uses the current UTC date at midnight. Pin it whenever reproducibility or recovery matters.

For a new canonical symbol, the producer requests every adjustment from `2016-01-01T00:00:00Z` regardless of
`--request-start`. For an existing symbol, it requests a month-partition replacement range:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol AAPL \
  --request-start 2026-09-01 \
  --request-end 2026-10-14
```

`--request-start` must be the first day of a month and strictly precede the end. When omitted for an existing source,
it defaults to the first day of the request-end month. Each nonempty selected partition is replaced as a whole; the
final partition contains only observations before the exclusive request end. Rows outside the selected months are
rejected. A selected month with no returned rows is reported as skipped; it does not delete a previously published
month.

The producer does not require a new request end to be later than the previously published one. An earlier end can
therefore replace the selected terminal partition with a shorter history. Treat the request end as an explicit
replacement boundary: pin it, compare it with the intended publication, and do not move it backwards accidentally.

Every run requests all eight adjustment views. The current command has no partial-adjustment, adaptive-acquisition,
force, repair, skip-disposition, or concurrency option.

### Boundary cascade and reuse

The producer always owns the complete cascade:

```text
source -> validation -> lineage -> correlation -> warnings
       -> identity candidate -> availability -> catalogue
```

Normal downstream consumption trusts completed receipts rather than re-running upstream work. Unchanged source months
and validation months are retained. If the complete source/validation/lineage/correlation/warning/catalogue generation
chain still matches, correlation through catalogue publication is reused as a unit.

Selected lineage evidence is still reacquired before semantic equality can be established. If an input generation
changes, correlation and warnings are recalculated for the complete processing-symbol closure. Catalogue publication
then writes only affected content-addressed objects and inventory paths. See the exact
[reuse matrix](technical-contracts.md#reuse-and-recomputation-matrix).

### Locking and reader impact

After all submitted labels resolve, the producer acquires the exclusive `.publication.lock`, writes
`.publication.pending.json`, and retains the lock through bar requests, lineage requests, correlation, availability,
and final publication. Public catalogue and history reads wait on a shared lock for the entire run.

This lock scope makes focused updates coherent but makes one very large invocation unsuitable today. It also means a
long network run causes equivalent reader downtime.

### Output

Progress lines are written to standard error. On success, standard output receives one compact JSON result containing
request boundaries, submitted and canonical symbols, processing closure, source changes, boundary receipts, reuse
status, and whether `last_run.json` was recorded.

`data_vtest/last_run.json` is operational bookkeeping only. It is not digest-bound and is never an input to another
boundary. Failure to write it does not invalidate an otherwise completed publication.

## Build a clean-room generation

Use this path for the tracked recovery universe or any deliberate new provider-only generation:

```bash
./build_alpaca_end_of_day_clean_room.py \
  --universe resources/ticker.csv \
  --store data_clean_room \
  --request-end 2026-10-14
```

The universe must be exactly one sorted, duplicate-free CSV record. `--request-end` is required and immutable for the
run. The default universe and store are the two paths shown above. A new store must be virgin: an existing monthly
store without this run's matching clean-room manifest is refused even if its store format is valid.

Construction admits exactly three input classes:

- the ordered universe and its raw file digest;
- responses obtained from Alpaca during this run; and
- the repository's runtime-validated contracts, transforms, calendar, and changelogs.

It never probes `data/`, `data_vtest/`, a backup, or another source directory. It does not migrate, compare with, or
repair from an earlier generation. Supplying `--store data` or `--store data_vtest` is refused.

The run manifest binds the ordered-label digest, request range, every relevant boundary/transform version, and that
input policy. Every label gets a durable resolution document before bar work and exactly one terminal outcome. A
canonical symbol is calculated by its first owning label only; later aliases bind to that outcome without requesting
bars or lineage again. Bar and identity rate limiters remain shared for the life of the scheduler.

Each new canonical symbol uses one complete transaction, so memory and recovery are bounded by its affected identity
closure rather than the whole universe. Completed outcome files are skipped on resume. If an interruption occurs
after source, validation, or lineage publication, recovery authenticates the completed boundary and continues after
it; it does not request that source again. Pricing-validation failure is an explicit rejected terminal outcome and is
kept for audit but never admitted to lineage or the catalogue. Other failures receive four bounded attempts and then
stop the run without fabricating an outcome.

Rerun the exact same command to resume. A different universe digest, request end, or owning contract version is
rejected instead of mixing generations.

### Mandatory final audit

Acquisition completion does not equal handoff. After all labels have outcomes, the same command automatically:

- proves exact label, resolution, canonical-owner, and directory accounting;
- authenticates every source and validation artifact;
- authenticates lineage, correlation, and warnings for every admitted canonical source;
- authenticates the candidate, availability output, catalogue engine, and final catalogue bindings;
- requires current catalogue membership to equal the admitted canonical set; and
- opens an isolated public-library facade and reads all eight adjustments for every online public ID.

Only a passing audit writes `clean_room_run/audit.json` and the digest-bound
`clean_room_run/completion.json`. A completed rerun trusts that receipt and performs no provider or domain calculation.

### Activate after audit

Add `--activate` to the same command to select the audited generation:

```bash
./build_alpaca_end_of_day_clean_room.py \
  --request-end 2026-10-14 \
  --activate
```

Activation waits for the existing public-store lock, retains the prior store as
`data_previous_public_<UTC>_<run-id-prefix>/`, and renames the audited store to `data_vtest/` on the same filesystem.
It then opens `DataLibrary`, requires the audited generation and online count, and records a receipt under
`alpaca_run_logs/`. A failed public smoke check restores both directory names. The retained store is recovery evidence
only and is never an input to the new generation.

## Interrupted publication and recovery

An error or signal after the transaction begins leaves `.publication.pending.json`. Readers then reject the store as
incomplete. A later run sees the marker and disables complete downstream reuse for that recovery transaction.

Rerun the **same canonical symbol set and exact request boundaries** to recover:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file /path/to/original-symbols.txt \
  --request-start YYYY-MM-01 \
  --request-end YYYY-MM-DD
```

The current code does not enforce that a recovery invocation matches the marker's recorded request. Supplying a
different cohort or boundary can clear the marker after publishing a different transaction and must not be used as a
recovery shortcut.

`SIGHUP` exits with `128 + signal`; `Ctrl-C` exits with 130. They are not durable batch checkpoints. If received inside
the transaction, the recovery marker remains.

## Focused updates

A rule or evidence change uses the same producer, not a direct publisher or alternate rule engine. Submit the smallest
complete set of source symbols whose source or identity evidence must change. When a catalogue exists, the producer
automatically expands those seeds through their current source/identity closure before recalculating correlation and
availability.

An ordinary focused update still contacts Alpaca for all eight selected-month views and current lineage evidence. The
current producer does not provide an offline rule-only replay switch. Do not claim a cache-only replay when provider
requests occurred.

## Consume online data

Use the [public CLI examples](public-api.md#command-line-client) for single-ID JSON history and CSV export, or
the [Python quick start](public-api.md#open-a-snapshot). Reads select a completed combined catalogue, not the
standalone store, and require no credentials or network. Export writes the requested destination; it does not mutate
the data store. [Flat export semantics](public-api.md#flat-online-csv) cover its disk spool, atomic path replacement,
ordering and failure conditions.

## Audit status

The clean-room command composes the explicit source, validation, lineage, correlation, warnings, identity-candidate,
availability, catalogue-engine, and public-reader checks into one mandatory final audit of its selected isolated
store. The audit is read-only with respect to domain boundaries; only its operational report and completion receipt
are added afterward.

The older `audit_alpaca_end_of_day_catalog.py` command still selects `data/end_of_day/` and must not be presented as
evidence for the monthly application store. The composed audit is currently an inseparable final phase of the
clean-room constructor rather than a general audit-only CLI for arbitrary stores.

## Backup and recovery

### Restore the same application state

The following is a procedure for an explicitly authorized backup/restore, not a request to copy retained development
stores. The [retention decision](storage-rework-notes.md) continues to prohibit unrequested copies or cleanup.

Back up the complete `data_vtest/` directory, not only its final catalogue receipt. The catalogue depends on monthly
source, validation, lineage, correlation, warning, candidate, availability, and content-addressed engine artifacts.

```bash
tar --format=posix --pax-option=delete=atime,delete=ctime \
  -czf /path/to/alpaca-end-of-day-backup.tar.gz \
  data_vtest
```

Restore into a staging copy of the same source revision. `DataLibrary` opening is a trusted consumer check, not by
itself a full authentication pass. The clean-room composed auditor is bound to its own run ledger and is not a generic
restored-store audit command. Never test restoration over the live store.

### Acquire a new application state

The tracked recovery snapshot contains 18,092 historical source labels, not just currently online tickers. Provider
data, identity evidence, and an intentional new request end would create a different generation.

Use the clean-room command above. Do not convert the CSV to lines and pass it as one focused `--file` run: that would
hold one transaction across the cohort, retain a large processing set, and provide no per-label terminal ledger.

## Exit status

This table describes the **combined producer**, not every entrypoint. Standalone batch and evidence statuses are
specified with their commands above; unresolved lineage can be a successful durable result.

| Status | Meaning |
|---|---|
| `0` | The selected complete transaction finished and the recovery marker was removed. |
| `1` | A runtime, provider, boundary, validation, publication, or I/O error aborted the run. |
| `2` | Command-line parsing failed. |
| `130` | Interrupted by `Ctrl-C`. |
| `128 + signal` | Interrupted by `SIGHUP`. |

The current multi-symbol producer is sequential and aborts on the first exception. It does not aggregate independent
ticker failures after continuing unrelated work.
