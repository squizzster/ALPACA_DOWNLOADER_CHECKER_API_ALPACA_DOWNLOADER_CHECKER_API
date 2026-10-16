# Storage rework and retained development data

Decision recorded on 2026-10-14 UTC: retain all existing stores and scratch directories for now. They will supply
"effective REST API source data" during development, allowing work to proceed from already downloaded observations
instead of making fresh provider requests. The earlier cleanup inventory is deferred; it is not a deletion plan.

Do not delete, move, rewrite, or make further copies of these stores. Use the existing data in place. No replay
adapter, cleanup, or pipeline redesign was implemented by this original note. The subsequently authorized standalone
downloader is recorded below; the retained-store restrictions still apply.

## First boundary and update intent

`data_v4` and `month.manifest.json` established the useful first boundary: transformed, value-preserving REST pricing,
addressed by ticker. Keeping every historical month as a separate durable directory was subsequently superseded.
The stable [standalone contract](technical-contracts.md#standalone-pricing-source-contract) now owns consolidation,
first/last-session refresh checks, and the implemented lineage handoff. This note owns only the retained-store inventory
and reuse restrictions, not a competing layout specification.

The original problem was excessive persistent artifacts, mirrored validation trees, repeated stores, and disk
activity. A logical calculation boundary does not automatically require another durable copy.

## Local source reuse

Retained stores are potential development inputs even when their former pipeline or catalogue is obsolete. Preserve
their payloads, manifests, request records, and identity evidence while deciding how to consume them. Different runs
can contain different request endpoints or provider snapshots; an obsolete implementation does not establish that its
data is redundant.

These artifacts contain transformed observations and, in some cases, normalized identity evidence. They are not
necessarily raw HTTP response bodies. A future local input adapter must retain the distinction and the available
provenance. This decision does not make an old derived catalogue authoritative for the new design.

The current clean-room command requires provider-only inputs. It is not the local replay mechanism requested here.
Its existing restriction is an implementation detail to revisit when the user specifies the development workflow;
do not start another clean-room acquisition to carry out this note.

## Project directory inventory

All paths in this table are relative to `/data2/alpaca/`. Sizes are rounded allocated disk usage observed on
2026-10-14, not guarantees of reclaimable space. Every entry is retained under the current decision.

| Directory | Observed size | Role and later cleanup consideration |
|---|---:|---|
| `data/` | Not measured in this inventory | Existing frozen historical dataset; preserve as source evidence. |
| `data_v4/` | 2.8 MiB | GOOG transformed-source reference; retained, not the new layout. |
| `data_v2/` | 7.0 GiB | Earlier conversion and storage experiment; potential local input. |
| `data_v3/` | 207 MiB | Alternative date-based acquisition experiment; potential local input. |
| `data_v3_1000/` | 821 MiB | Larger run of that acquisition experiment. |
| `data_v3_sqlite/` | 333 MiB | SQLite storage trial. |
| `data_v3_1000_sqlite/` | 3.5 GiB | Larger SQLite storage trial. |
| `data_v6/` | 15 MiB | Superseded monthly pipeline trial. |
| `data_vtest/` | 29 MiB | Store currently selected by producer and public-reader code. |
| `data_clean_room/` | 70 MiB | Partial 52-source run; shares its run identity with the larger temporary store below. |
| `alpaca_run_logs/` | 11 MiB | Experimental logs, traces, and before/after inventories. |
| `experimental_temp/` | 127 MiB | Captured provider responses and identity evidence. |

`alpaca_run_logs/vtest/public_repository/data_v6` is a symlink to `/data2/alpaca/data_vtest`, not another independent
data copy. The existing source code, tests, resources, documentation, and uncommitted changes are also preserved.

## Temporary smoke and conversion stores

These earlier cleanup candidates are also retained for development:

| Absolute directory | Observed size |
|---|---:|
| `/tmp/alpaca_clean_room_smoke` | 3.3 MiB |
| `/tmp/alpaca-v6-goog-update-fqz0ngtj` | 11 MiB |
| `/tmp/alpaca-v6-isolated-imue_n5l` | 11 MiB |
| `/tmp/alpaca-v6-goog.PRHI9U` | 3.3 MiB |
| `/tmp/alpaca-v2.XagTI6` | 330 MiB |
| `/tmp/alpaca-v2-components.y6YhKE` | 1.1 MiB |
| `/tmp/alpaca-v2-analysis.VamCA5` | 816 KiB |
| `/tmp/alpaca-v2-ranges.tpWmk3` | 1.9 MiB |
| `/tmp/alpaca-v2-final.aQ2V0v` | 8.2 MiB |
| `/tmp/alpaca-v2-direct.tL46M2` | 3.3 MiB |
| `/tmp/alpaca-v2-lineage.BmLrXf` | 4.8 MiB |
| `/tmp/alpaca-v2-publish.FsAPu1` | 270 MiB |
| `/tmp/alpaca-v2-projection.hXYg1O` | 878 MiB |
| `/tmp/alpaca-universe-audit` | 256 KiB |

The AAPL smoke store alone contained 981 filesystem objects: 644 under its source-symbol directory, 274 under its
mirrored validation-symbol directory, and 63 elsewhere.

`/tmp/alpaca-v20-rollback.wKlSTj` and `/tmp/alpaca-figi-check.spnGSy` remain unclassified because their permissions
prevented inspection. They are retained as well. Other projects' temporary directories are outside this inventory.

## Large temporary clean room store

`/tmp/alpaca-clean-room-fast-d943ac` shares run ID
`d943acb35db1bcc1e7867d29a8ddab817c0a8ff3831f62dd0e4411344a33b224` with
`/data2/alpaca/data_clean_room`. Its retained checkpoint evidence showed 11,353 completed label outcomes and 11,354
source directories. A pending transaction named NIXT; no matching producer process was found during inspection.

The inventory counted 2,454,886 files and 1,423,562 directories: 3,878,448 filesystem objects, occupying
13,610,008,576 allocated bytes, approximately 12.7 GiB. Its largest subtrees are:

| Directory inside the store | Observed size | Retention reason |
|---|---:|---|
| `end_of_day/` | 6.4 GiB | Already-downloaded monthly source payloads and manifests. |
| `validation/` | 5.2 GiB | Mirrored monthly validation artifacts; a principal target for simplifying the future design. |
| `lineage/` | 151 MiB | Normalized provider identity evidence. |
| `correlation/` | 502 MiB | Generated row-allocation and identity outputs. |
| `warnings/` | 305 MiB | Generated advisory outputs. |
| `catalogue_engine/` | 87 MiB | Generated catalogue objects and inventories. |
| `clean_room_run/` | 91 MiB | Request identity, resolution records, outcomes, and diagnostics. |
| `availability/` | 8 KiB | Generated availability output. |
| `catalogue/` | 8 KiB | Catalogue selection and retention records. |

The root filesystem containing `/tmp` was 97% full with approximately 19 GiB available at inspection. This recorded
capacity concern does not authorize deletion or another copy. The downloaded source and provenance remain in place
for the planned development reuse, along with the currently deferred derived outputs.

## Subsequent decision: standalone pricing downloader

The independent CLI and focused live GOOG experiments were subsequently authorized in the separate `data_pricing/`
store. That development included an authorized GOOG reset and later an in-place move into the split pricing/manifests
layout, not copies or conversion of this retained inventory. Current commands are in
[Operations](operations.md#standalone-pricing-download); current evidence is in
[Version tracker](VERSION_TRACKER.md#verification-status).

## Retained-store next step

Use retained downloads in place when a local replay workflow is authorized. No replay adapter has been implemented.
Cleanup, copying, conversion, and another clean-room acquisition remain outside this decision.
