# Architecture

## Responsibility boundary

This repository acquires provider data, publishes isolated evidence boundaries, validates price and session integrity,
resolves security identity, evaluates availability, and serves one online-only catalogue. Its responsibility ends at
that data handoff. Research, screening, modelling, trading policy, and investment decisions belong to consumers.

The combined catalogue workflow uses `data_vtest/`; the standalone downloader uses an explicit destination, currently
`data_pricing/` for focused development. The older `data/end_of_day/` tree is retained evidence, not a fallback for either
workflow or `alpaca_end_of_day.DataLibrary`.

## Producer and consumer boundaries

The standalone pricing producer is independent of the existing combined catalogue workflow:

| Role | Supported entrypoint | Contract |
|---|---|---|
| Standalone pricing downloader | [`download_alpaca_pricing.py`](../download_alpaca_pricing.py) | Acquire exact ticker-labelled pricing, publish compact source boundaries and pending dates, and stop; no identity or catalogue dependency. |
| Standalone lineage | [`build_alpaca_lineage.py`](../build_alpaca_lineage.py) | One worker consumes pending pricing, acquires identity evidence, applies the established rules, and publishes chronological assignments before acknowledgement. |
| Standalone resolved reader | [`read_alpaca_pricing.py`](../read_alpaca_pricing.py), `alpaca_end_of_day.StandaloneReader` | Read completed pricing/lineage groups and select published public-ID histories; no availability decision, acquisition, or datastore writes. |
| Operator lineage evidence | [`output_current_lineage_evidence.py`](../output_current_lineage_evidence.py) | Observe the same lineage workflow, or inspect published decisions offline; no alternate resolver and no public consumer API. |
| Producer | [`run_alpaca_end_of_day_ticker_pipeline.py`](../run_alpaca_end_of_day_ticker_pipeline.py) | Own the complete acquisition-to-catalogue cascade and publish receipts last. |
| Clean-room constructor | [`build_alpaca_end_of_day_clean_room.py`](../build_alpaca_end_of_day_clean_room.py) | Schedule the same cascade as bounded canonical-symbol transactions using only the fixed universe, provider responses, and repository contracts; checkpoint exact accounting; audit before activation. |
| Consumer | [`alpaca_end_of_day.DataLibrary`](../alpaca_end_of_day/__init__.py) | Trust one completed catalogue generation and expose only online public identities and exact histories. |
| Explicit auditor | Boundary-specific internal `authenticate_*` functions, composed by the clean-room constructor | Re-hash every owned boundary, verify global membership and generation closure, and exercise every online adjustment through the public library without repairing it. |

Consumer code imports only `alpaca_end_of_day`. Modules under `libs/` are implementation interfaces. An unavailable
public ID always receives the same response; the public boundary never reveals whether an offline or internal identity
exists.

## Boundary graph

The standalone path is:

```text
ticker REST bars -> exact transformed pricing -> pending from/to dates -> standalone lineage
                   end_of_day/pricing/<TICKER>   end_of_day/manifests/<TICKER>
```

Downloader and lineage share per-ticker locks, not calculation ownership. The downloader owns prices; lineage owns
normalized identity evidence and dated assignments using the existing rule registries. Missing prices are delegated
to the downloader outside pricing locks. Neither standalone command publishes the public catalogue. The downloader
imports no identity, lineage, validation, or combined-pipeline package. Consolidation changes
storage ownership, not price values or security ownership; see the
[standalone contract](technical-contracts.md#standalone-pricing-source-contract).

The standalone **downloader** execution boundary is **one worker, one ticker transaction**: a worker owns its ticker
from lock acquisition through all probes, adjustment downloads, and publication. At most three workers run concurrently;
only the in-memory request budget/cooldown is shared across tickers. Workers never split one ticker's adjustments.

The lineage twin has **one worker per store across processes**. Its unit of work is a complete dependency group,
which can contain several ticker labels; the downloader's worker count does not apply to lineage. Its three
chronological boundaries include the month-over-year overlay. The dependency index identifies connected sources,
not shared security ownership. Completed groups are restart checkpoints, and approved upstream values are consumed
without re-audit. Exact reuse, continuation and lock gates belong to the
[standalone lineage contract](technical-contracts.md#standalone-lineage-contract).

The existing combined catalogue workflow remains:

```text
Alpaca bars                         Alpaca identity evidence
    |                                        |
    v                                        v
monthly source -> validation -----------> lineage
       \              \                     /
        \              +----> correlation <-+
         \                       |
          +--------------------> warnings
                                  |
correlation + lineage -> identity candidate
                                  |
candidate + histories + warnings + lineage -> availability
                                  |
candidate + availability -> content-addressed catalogue
                                  |
                                  v
                          online public library
```

Each arrow crosses a receipt-defined handoff. The receiving stage consumes the result already calculated by the owner;
it does not re-run the owner's rules. The complete cascade and its reuse decisions are specified in
[Pipeline](../PIPELINE.md). Exact generation bindings and artifact schemas are specified in
[Technical contracts](technical-contracts.md).

## Trust model

In the existing combined cascade, trust is established at publication and consumed by identity, not repeatedly reconstructed:

1. The owner calculates a boundary output.
2. The owner validates the output it is about to publish.
3. The owner writes the selecting receipt last.
4. A normal downstream stage uses a trusted loader, checks that the receipt names the expected immediate inputs, and
   consumes the published result without re-hashing it or repeating its calculation.
5. An explicit authenticator may later re-hash the complete boundary for audit.

This split is deliberate. If validation, correlation, or the public reader independently recalculated source digests
and upstream decisions, those stages would no longer be isolated and identical work would have multiple owners.

Identical immediate-input generations select the existing downstream publication. A changed generation invalidates
only the dependent boundary and its required closure. Current exceptions and recalculation granularity are recorded in
the [reuse matrix](technical-contracts.md#reuse-and-recomputation-matrix); notably, selected lineage evidence is
reacquired before semantic equality can be known.

The clean-room scheduler adds durable trust between transactions. It records a submitted-label resolution before
price acquisition, records one terminal outcome for every universe position, and never runs a second acquisition for
an already-owned canonical symbol. After an interruption it explicitly authenticates a completed source, validation,
or lineage boundary and resumes at the first missing owner. A later source may invalidate correlation for an identity
closure; recalculating that affected closure is required work, while unrelated sources remain unopened by the
catalogue delta.

## Silo ownership

The following silos belong to the existing combined cascade:

| Silo | Owns | Does not own |
|---|---|---|
| Monthly source | Month placement, deterministic gzip bytes, exact-copy aliases, and structural inventories. | Price/session validation or identity. |
| Validation | Row rules, observed sessions, and adjacent-month seams. | Lineage, allocation, warnings, or availability. |
| Lineage | Normalized provider identity and corporate-action evidence. | Row ownership or security membership decisions. |
| Correlation | Dated ownership, exhaustive row allocation, UUID edges, identity partitions, and evidence issues. | Online status or source mutation. |
| Warnings | Advisory cross-view and segment findings. | Data repair or publication eligibility. |
| Identity candidate | Snapshot or affected-closure aggregation with provisional offline state. | Acquisition, correlation, or availability. |
| Availability | Whether every advertised published read succeeds and continuity is explained. | Membership or value changes. |
| Catalogue | Content-addressed persistence and atomic selection of already-made decisions. | Domain-rule evaluation. |
| Public library | Online filtering, typed values, search, resolution, and snapshot consistency. | Offline discovery, diagnosis, repair, or network access. |

No stage writes another stage's directory. A later stage may read an upstream published value when that value is one of
its declared inputs, but it cannot change or reinterpret the upstream output.

## Publication isolation

Standalone pricing uses the [per-ticker publication and acknowledgement protocol](technical-contracts.md#locking-publication-and-consumption).
Normal handoff readers trust published pricing receipts and aliases; explicit pricing authentication remains separate.
Downloader endpoint probes trust a small source-bound validator; they do not reopen cold gzip data on ordinary updates.

Standalone lineage keeps one writer-only guard for the invocation and a brief publication gate for each group commit.
Readers pin only their selected pricing group after a selection handshake, allowing unrelated work to continue.
`StandaloneReader` reports conflicting work as busy by default or waits explicitly. Incomplete transactions and stale
inputs still prevent reads. The [lineage lock contract](technical-contracts.md#lock-delegate-publish-acknowledge)
owns ordering, recovery and upgrade compatibility. This is separate from the combined catalogue lock below.

The combined store has one stable `.publication.lock`. The pipeline holds it exclusively from the start of the mutation
transaction through final catalogue publication. Public catalogue and history reads hold it shared. The writer also
publishes `.publication.pending.json`; an interrupted transaction retains that marker, and readers fail closed until a
complete rerun succeeds.

Source months have an additional per-symbol lock and are installed through staged directory replacement. Regular files
use same-directory temporary files, `fsync`, and `os.replace`. Boundary receipts and the reader-visible catalogue
receipt are written only after their owned outputs validate.

The focused producer's lock scope includes provider requests and therefore blocks readers for its complete cohort.
The clean-room constructor instead targets an isolated store and opens one complete transaction per canonical symbol;
no transaction retains the full universe in memory. Failed pricing validation is a terminal, authenticated source and
validation outcome that never enters lineage or the catalogue and does not leave a recovery marker. Any other
unresolved failure stops the scheduler after bounded retries, retaining its exact checkpoints for a later resume.

Activation occurs only after the isolated store passes complete authentication and every online history reads through
the public API facade. The prior `data_vtest/` directory is renamed to an ignored recovery path, the audited directory
is renamed into place on the same filesystem, and a public-library smoke check must select the audited generation. A
failed smoke check rolls the rename back. Operations permit only one producer or activator at a time.

## Update flow

The combined orchestrator is [`pipeline.py`](../libs/alpaca_end_of_day_v6/pipeline.py). Its exact execution sequence is
owned by [Pipeline](../PIPELINE.md#one-ordered-execution-path); standalone updates have their
[own entry path](../PIPELINE.md#standalone-lineage-execution) into the shared identity transforms.

For an existing catalogue, processing expands submitted canonical symbols to their current source/identity closure.
Only submitted canonical symbols reacquire bars and lineage; unchanged supporting symbols are loaded from their
published boundaries. Catalogue delta publication preserves unrelated objects and identities.

## Consumer flow

`DataLibrary` selects one completed online snapshot and trusts its published dependencies; it does not perform an
explicit digest audit or switch generations during a history read. The [public snapshot contract](public-api.md#snapshot-consistency)
owns lock, generation-change and error behavior. The standalone evidence CLI is an operator interface, not this reader.

`StandaloneReader` is the separate source-scoped consumer of the twins' existing published groups. It selects
resolved assignments, applies the compact month overlay, and reuses the established history-source policy. It does
not publish a catalogue, make online-availability decisions, or expose unresolved rows as resolved identities. Its
[consumer contract](public-api.md#standalone-resolved-pricing-reader) is distinct from `DataLibrary`.

## Code ownership

| Boundary | Owner | Responsibility |
|---|---|---|
| Standalone pricing | [`libs/alpaca_pricing_download/`](../libs/alpaca_pricing_download/) | Batch CLI, ticker REST requests, exact conversion, source-bound endpoint validator, three pricing boundaries, recoverable publication, and locked pending-date handoff; no security identity work. |
| Standalone lineage | [`libs/alpaca_lineage_build/`](../libs/alpaca_lineage_build/) | One-worker CLI, chronological evidence resumption, trusted pricing adapter, affected dependency closure, existing identity transforms, twin assignment boundaries, and publish-before-acknowledge recovery. |
| Consumer library | [`alpaca_end_of_day/`](../alpaca_end_of_day/__init__.py) | Online-only catalogue, search, ticker resolution, typed history, and flat export. |
| Standalone resolved consumption | [`standalone.py`](../alpaca_end_of_day/standalone.py), [`standalone_cli.py`](../alpaca_end_of_day/standalone_cli.py) | Source-scoped public-ID lookup, coherent shared-lock reads, exact history assembly and JSON/CSV stdout; no new datastore artifact. |
| Complete monthly orchestration | [`pipeline.py`](../libs/alpaca_end_of_day_v6/pipeline.py) | Stage order, transaction, reuse selection, fixed-point execution, and final publication. |
| Clean-room scheduling and audit | [`libs/alpaca_end_of_day_clean_room/`](../libs/alpaca_end_of_day_clean_room/) | Immutable run identity, resolution/outcome checkpoints, canonical deduplication, bounded retries, exhaustive audit, and rollback-safe activation. |
| Monthly source | [`source.py`](../libs/alpaca_end_of_day_v6/source.py) | Month partitioning, deterministic gzip payloads, exact-copy aliases, inventory, and explicit authentication. |
| Validation | [`validation.py`](../libs/alpaca_end_of_day_v6/validation.py) | Incremental month validation, predecessor seams, and ticker summary. |
| Lineage source | [`lineage.py`](../libs/alpaca_end_of_day_v6/lineage.py) | Current normalized asset and corporate-action evidence. |
| Correlation envelope | [`correlation.py`](../libs/alpaca_end_of_day_v6/correlation.py) | Adapt trusted monthly inputs to the sole correlation transform and publish its exhaustive partition. |
| Warning envelope | [`warnings.py`](../libs/alpaca_end_of_day_v6/warnings.py) | Adapt trusted inputs to advisory warning transforms and publish compact outputs. |
| Candidate, availability, catalogue | [`catalogue.py`](../libs/alpaca_end_of_day_v6/catalogue.py) | Closed candidate construction, read-based availability, delta catalogue publication, and retention. |
| Published history adapter | [`reader.py`](../libs/alpaca_end_of_day_v6/reader.py) | Assemble trusted monthly rows through established history semantics. |
| Ordered identity transforms | [`libs/alpaca_end_of_day_security_correlation/`](../libs/alpaca_end_of_day_security_correlation/) | Ownership intervals, row rules, UUID membership, master IDs, and content-addressed catalogue engine. |
| Established validation transform | [`libs/alpaca_end_of_day_pipeline/`](../libs/alpaca_end_of_day_pipeline/) | Bar and dataset rule implementation used inside the monthly validation owner. |
| Established warning transforms | [`libs/alpaca_end_of_day_warning_pipeline/`](../libs/alpaca_end_of_day_warning_pipeline/) | Cross-adjustment and security-history advisory algorithms. |
| Shared primitives | [`libs/common/`](../libs/common/) | Market calendar and dependency-neutral helpers. |

Tracked root Python files are executable adapters. The supported consumer implementation lives in
`alpaca_end_of_day/`; other application behavior belongs in the owning internal package.

## Repository and runtime layout

| Path | Role |
|---|---|
| [`alpaca_end_of_day/`](../alpaca_end_of_day/__init__.py) | Only supported Python import for consumer applications. |
| [Root command files](../run_alpaca_end_of_day_ticker_pipeline.py) | Executable adapters; exact current status is in [Operations](operations.md#command-map). |
| [`libs/`](../libs/) | Boundary implementations and transforms; not supported consumer imports. |
| [`tests/`](../tests/) | Unit and contract tests using temporary stores and HTTP doubles. |
| `data_pricing/` | Ignored standalone pricing development store; independent of the catalogue. |
| `data_vtest/` | Ignored combined-pipeline store selected by the public library. |
| `data_clean_room/` | Ignored provider-only staging store used by the clean-room constructor before activation. |
| `data_previous_public_*/` | Ignored retained pre-activation public store; recovery evidence, never an input to construction. |
| `data/` | Ignored legacy runtime tree; not selected by the current public library. |
| [`resources/`](../resources/) | Runtime-validated changelogs and the tracked recovery-symbol snapshot. |
| [`docs/`](.) | Maintained architecture, contract, public, operational, development, and status guidance. |

## Core invariants

- A fact is calculated by one owning boundary and consumed downstream through its published generation.
- Ordinary catalogue consumption does not repeat publication authentication or upstream domain calculations.
- Identical complete input generations retain the existing downstream publication.
- Combined-source updates touch selected months; standalone updates preserve cold/year payloads unless rollover or full refresh requires replacement.
- Every source row is represented exactly once in the correlated or uncorrelated branch.
- A ticker is a dated label; ambiguity is explicit and no rule hard-codes a ticker exception.
- Public IDs are SHA-256 digests of sorted unique Alpaca asset-ID membership.
- Availability can reject a resolved identity but cannot change its membership or values.
- Warning findings are advisory and cannot change returned values or online state.
- The public library exposes only the selected online snapshot and never discovers offline identities.
- An interrupted publication blocks reads until a complete recovery run publishes the final receipt.
- A clean-room run accounts for every ordered input label and calculates each resolved canonical source at most once.
- Clean-room construction never reads another runtime store; activation only locks and renames the prior public store.
- Public readers never update, repair, acquire, or contact Alpaca; the internal handoff only acknowledges pending work.

The exact artifact and trust contracts live in [Technical contracts](technical-contracts.md), consumer semantics in
[Public API](public-api.md), and operational mutation rules in [Operations](operations.md).
