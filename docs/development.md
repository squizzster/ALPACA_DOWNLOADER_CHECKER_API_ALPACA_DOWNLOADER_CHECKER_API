# Development

## Find the owner

Read [AGENTS.md](../AGENTS.md), the [package ownership map](ARCHITECTURE.md#code-ownership) and the owning contract. Develop with synthetic stores and HTTP doubles. Version and changelog rules are indexed in [VERSION_TRACKER.md](../VERSION_TRACKER.md).

Before adding a check, identify the fact still missing. Document the handoff's owner, guarantee, scope, lifetime, invalidation and prohibited repeats. A retry, new caller or missing acknowledgement grants no authority to repeat an accepted check. [Trust contract](../AGENTS.md#trust-before-touching-storage)

Test the result and its nearest failure, including interruption and acknowledgement order. Unchanged inputs must forbid redundant reads, replays and publication. Changed inputs must reach the owning update path. Reading selected payloads to serve a new history or export is legitimate. Preserve these assertions when changing completion mechanics. [Approval regressions](../tests/test_alpaca_approval_handoffs.py) · [Serving regressions](../tests/test_alpaca_serving_handoffs.py)

## Lineage research and rule-improvement loop

The main agent owns generic-rule implementation and acceptance. Research supplies an external reference only. [Ownership criterion](lineage.md#ownership-allocation-criterion) · [Generic inference requirements](../AGENTS.md#generic-lineage-inference)

1. Select one case. Record disputed dates, complete dependency group, ownership question and bindings for store, selector, receipt/pricing, implementation and policy. Retain applicable completed work even if an older queue requests it.
2. Assign one independent research agent per case (`gpt-6.1-sol`, high reasoning, `fork_turns="none"`). Request primary-source chronology, conflicts, uncertainty and counterexamples without suggesting a verdict. Researchers neither edit nor publish. A supported connection and evidence insufficient to connect are both valid findings. No report or scoped factual application enters inference.
3. Explain the recorded result from retained API observations and completed decisions. Locate the cause in evidence availability, ownership inference, membership propagation, numerical selection or consumer interpretation. Preserve the distinction between security identity, dated ticker and numerical source. Never invent bars, UUIDs, endpoint results or predicate explanations.
4. Change an existing generic rule when admissible inputs reasonably support correction. A new rule needs ordered predicates and counterexamples. Compare plausible owners with nonallocation, without requiring a special certificate or certainty. Unresolved history stays unpublished and counts as processed. Research results, ticker lists and source documents cannot become assertions, overlays or exceptions.
5. Compare candidate and unchanged control on identical held inputs. Isolate stored/control drift. For Python changes, use an opt-in candidate or preserved control. Cover equivalent evidence under other tickers, owners supported without a specific certificate, competitors, ties, invalidation and interruption. Fail on research-file access, injected facts or repeated provider calls, boundary reads, comparisons and calculations.
6. Measure impact using completed selectors and actual rule conditions. Excluding a group by metadata needs proof of a necessary condition. Keep unknowns selected. Report affected IDs, dates, decisions, reasons, failures, unevaluated scope and limits. Unaffected classification leaves stored correctness unverified. Retain completed peers.
7. Before opening another case, record verdict, external reference, counterexamples, measured impact and limits. Run owning regressions, mandatory trust review and full checks before handoff. Production publication requires separate authorization. Continuation and recovery use completed decisions without reopening research or repeating accepted work.

The [lineage regression symbols](../tests/fixtures/lineage_symbols.txt) select a bounded development
comparison. The file contains one seed per selected group, including reused tickers, delistings,
cash acquisitions, changing share terms and unresolved histories. To compare two executable policies
on those complete groups, run

```bash
xargs -a tests/fixtures/lineage_symbols.txt ./alpaca-lineage-debug run --store data_pricing \
  --control-policy resources/alpaca_security_correlation_v106_legacy_policy.yaml \
  --policy resources/alpaca_security_correlation_policy.yaml --tickers
```

The diagnostic reports stored/control drift separately from changes caused by the candidate.
The file supplies work selection only. Fresh builds discover related labels through provider
observations and use the same rules for symbols outside the regression set.

For a possible rename, check which provider observation first names the other ticker, which dates
independently establish its spelling, and whether the adjusted prices remain on a common basis
across the proposed boundary. A mapped response may reproduce both prices under either requested label.

Standalone ownership resolution and online serving eligibility remain separate facts. The
[implementation review](trust-boundary-handoff.html#f67) records the generic-only inference boundary,
its verified guards and remaining adoption limits. Former factual-repair success expectations are not
authority to reintroduce authored assertions; downstream regressions use completed-output fixtures.

## Verification

Run focused tests during implementation and these checks before code handoff:

```bash
ruff check .
ruff format --check .
python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v
git diff --check
```

[pyproject.toml](../pyproject.toml) owns Ruff configuration. No repository CI or dedicated documentation checker.
Parquet tests skip without [PyArrow](../requirements-bloodhound.txt); report actual results and gaps.
Tests do not establish retained-store correctness or live-universe throughput.

The independent [Bloodhound app](../bloodhound_app/README.md#run-the-regression-suite) has a separate test
suite and dependency pins. Run it from `bloodhound_app/` when changing that application; root `tests/`
discovery does not include it.

High-value contract tests:

- [Completed handoffs](../tests/test_alpaca_completed_handoffs.py),
  [lineage I/O](../tests/test_alpaca_lineage_io.py),
  [daily update](../tests/test_alpaca_update_everything.py).
- [Publication recovery](../tests/test_alpaca_publication_recovery.py),
  [durable maintenance](../tests/test_alpaca_durable_maintenance.py),
  [store locks](../tests/test_store_operation_lock.py).
- [Production policy](../tests/test_alpaca_lineage_production_policy.py),
  [serving reuse](../tests/test_alpaca_serving_incremental.py),
  [CSV export](../tests/test_csv_export.py), [CLI lifecycle](../tests/test_stderr_lifecycle.py).

For documentation-only changes, compare substantive old/new meaning; check links, anchors, paths and complete
README coverage. Validate examples against parsers and `./alpaca-cli-eod <command> --help`, then inspect the diff.
Never execute mutating examples for documentation verification. The mandatory boundary-review checks in
[AGENTS.md](../AGENTS.md#documentation-and-verification) also apply to documentation-only changes; prose-only
edits need no additional full runtime-suite rerun.

Keep explanations in one README-linked home. Preserve rationale, constraints and gotchas; omit status journals,
dated test histories, duplicate version tables and speculative plans.

## Pipeline Phase 1: Prepare

Prepare → Histories → Serve is the executable pipeline. Its six responsibilities are baseline restoration, initial selection, complete pricing/lineage production, serving component selection, serving preparation/assessment, then publication.

Prepare registers daily work and returns a native `Prepared` by reference. Its independent JSON adapter can continue in a fresh process without predecessor objects or repeated preparation. It owns operation identity, frozen inclusive cutoff, journal and selected pending-summary recovery, catalogue observation, work selection, registration, completion acknowledgements and initial maintenance membership. Ordinary live pricing maintenance follows Prepare. Recovery can itself finish pricing/lineage work, whose accepted results must cross the handoff.

Implementation: [prepare.py](../libs/alpaca_pipeline/prepare.py). Public `alpaca_end_of_day.prepare_pipeline(request_json: bytes) -> bytes` and [stdin/stdout adapter](../alpaca_pipeline_prepare.py) share the implementation and existing provider credential environment. Supply absolute paths for the store and registry.

```json
{
  "schema": "alpaca.pipeline.request",
  "schema_version": 1,
  "operation": "daily_update",
  "store": "/absolute/path/to/store",
  "registry": "/absolute/path/to/ticker.csv",
  "through": null,
  "options": {"workers": 3, "retry_blocked": false, "coalesce_late_bars": true},
  "debug": {"enabled": false}
}
```

```bash
python3.11 alpaca_pipeline_prepare.py < request.json
```

`through` is inclusive and resolves once using the existing session-cutoff owner;
null selects its existing default. Prepare performs production bookkeeping and
recovery, unlike `update --plan`. It returns `alpaca.pipeline.prepared`, schema 1.
A successful Prepare return means preparation completed, not that the update did.
Blocked independent groups and completed negative/failed observations remain explicit
in selection and evidence records.

The output carries operation/request and implementation-version bindings, effective obligations, selected
scope and maintenance membership, accepted selectors, provider observations,
producer decisions, recovery values and request counts. Its handoff-local object
table preserves shared values, tuple keys and producer acceptance records. Prices
remain strings. Named record kinds are allowlisted; decoding does not import
classes from document-supplied paths, replay validators or compile policy again.
The protocol is transient and does not change pricing, lineage or serving formats.

Normal execution writes no handoff archive, spool, replay cache or extra derived
store. DEBUG requires `{"enabled": true, "directory": "/absolute/debug/path"}`
and captures exact input/output, available structured errors and producer effect records
under the operation ID. Captures are diagnostic
and never recovery authority. If execution or delivery loses a result, dependent
execution fails. There is no automatic retry, reconstruction or handoff recovery.
Existing publication journals retain their separate interrupted-write responsibility.

Input parsing precedes the store operation lock and universe lock. Prepare freezes
its output under those locks, releases them, then delivers bytes. The internal
`consume_prepared` feasibility adapter composes the history JSON stage and its
actual serving consumer. They check only the required selector applicability
across lock gaps; changed implementation versions or bindings fail rather than
selecting preparation again. The ordinary `update` command uses the [native three-stage controller](#pipeline-phase-3-complete-pipeline).

Acceptance requires actual-consumer fresh-process tests, not just parsing a schema.
The [Prepare regressions](../tests/test_alpaca_pipeline_prepare.py),
[evidence regressions](../tests/test_alpaca_pipeline_evidence.py) and
[value transport regressions](../tests/test_alpaca_pipeline_codec.py) and
[CLI transport regressions](../tests/test_alpaca_pipeline_transport.py) cover the
transport and forbid repeated upstream work. Run the existing daily-update and
trust-review owning modules as well. Production readiness additionally requires
representative memory/serialization measurements, remaining recovery-path coverage
and operational acceptance; synthetic transport success does not establish lineage
quality, 20,000-security throughput or the 60-minute/90-minute target.

## Pipeline Phase 2: History production

The Phase 2 goal is an independently executable history producer: completed
Prepare JSON in, completed pricing/lineage JSON out. It owns ordinary maintenance,
acquisition, permitted dependency closure and registration, corrections, ownership,
numerical source selection, pricing/lineage publication and history-work
acknowledgements. Serving selection, assessment and catalogue publication follow
this boundary. A completed attempt carries independent group failures explicitly;
it does not turn incomplete histories into a successful daily update.

The public API is
`alpaca_end_of_day.produce_histories_pipeline(prepared_json: bytes) -> bytes`.
[alpaca_pipeline_histories.py](../alpaca_pipeline_histories.py) calls that same API:

```bash
set -o pipefail
python3.11 alpaca_pipeline_prepare.py < request.json | python3.11 alpaca_pipeline_histories.py
```

These commands emit the handoff to stdout without saving intermediate files.
Progress/errors belong on stderr. The root adapters contain no domain decisions.
The [history transport owner](../libs/alpaca_pipeline/histories.py) invokes the
[history domain owner](../libs/alpaca_lineage_build/universe_update.py),
`histories_locked`, independently of `serve_locked`. Ordinary update sequences
the native stages through the [controller](#pipeline-phase-3-complete-pipeline).

Input is the complete `alpaca.pipeline.prepared`, schema 1. The stage consumes
carried observations, graph, approvals and recovery results; it does not repeat
catalogue observation, selection or acceptance. Its internal discovery can expand
and register dependencies within the carried work policy. It finishes all such
iteration before returning `alpaca.pipeline.histories`, schema 1. Protocol runtime
compatibility is separate from these schema numbers.

The histories document contains:

| Field | Authority and remaining consumer |
|---|---|
| `operation` | Original ID, request, resolved cutoff, destinations and implementation binding; consumed unchanged. |
| `histories.result` | Completed attempt, accumulated producer stage outcomes, failures, pending obligations and request counts. `execution_complete` is true; overall `complete` remains false until serving. |
| `histories.history_complete` | Whether required history work completed without failed/pending/deferred obligations; ambiguity remains a distinct completed lineage disposition in the producer results. |
| `histories.storage_symbols` | Actual active scope after dependency expansion, required for downstream selected storage work. |
| `histories.workflow_revision` | Latest revision returned by committed workflow mutations, without a bookkeeping reread. |
| `histories.summary_token` | Accepted pricing-summary epoch/revision advanced by committed producer transactions. Rolled-back revisions do not advance the handoff. |
| `histories.context` | Accepted store, current complete lineage index and prior serving controls needed by the serving owner. |
| `histories.recovery` | Generation-bound held receipt bodies, assignments, choices, decoded partitions and other selected values required by remaining consumers. |
| `objects` | Allowlisted value records and aliases, shared across the document. |

Encoded field values use the same explicit reference codec as Prepare. Provider
clients, metadata request caches, pricing acquisition plans and source-work
calculation caches have completed their jobs and do not cross into serving.
Already materialized serving inputs still cross; dropping those would force
forbidden rereads. Unread authoritative artifacts retain selected references.

`history_complete: false` can accompany a valid completed attempt: failed groups
remain explicit while independent published groups can proceed. An unavailable
completion boundary, malformed input or missing acceptance is an execution error
and emits no successful handoff. Lost or undeliverable JSON is not reconstructed;
no automatic replay or backwards call occurs. Existing journals retain their
own interrupted-publication responsibility. A committed publication survives
an output delivery failure without turning that failed call into success.

Both executable stages receive complete input before locking, freeze output
under the existing operation/universe locks, then release locks before delivery.
The public Serve stage runs the actual serving owner in a fresh process without provider clients.
`consume_histories` remains a compatibility adapter to that same implementation. On lock reacquisition it checks
workflow, pricing-summary, lineage-index and serving generations. A real change
stops consumption. Partial-history applicability checks do not require unrelated
pending peers to be ready, and do not authorize summary membership selection.
The selected group reader retains its required current-source binding checks.

Normal execution adds no handoff files, spooling or replay store. DEBUG captures
exact stage input/output or its available structured error and producer-generated
effects. It changes no decisions or required reads/calculations. Production
pricing, lineage and catalogue schemas and layout remain unchanged.

The [history regressions](../tests/test_alpaca_pipeline_histories.py) cover real
append, corrections, dependency expansion/registration, peer failures, sourced
equality, missing inputs and actual binding invalidation. The
[history transport regressions](../tests/test_alpaca_pipeline_history_transport.py)
cover API/CLI parity, full input before locking, output exceeding pipe capacity,
broken delivery, DEBUG equivalence and committed/rolled-back summary-generation
carry. They reject repeated upstream work and physical reads of carried values.
The [Serve stage and ordinary runner](#pipeline-phase-3-complete-pipeline) consume
that same contract. Synthetic transport tests establish neither full production-scale
memory nor the live runtime target.


## Pipeline Phase 3: Complete pipeline

Ordinary execution sequences Prepare → Histories → Serve using explicit native
records under one continuous operation lock. The shared stage implementations
serve both this path and the standalone JSON APIs. The ordinary controller never
constructs an object table, encodes bulk state, decodes it or reconstructs its
accepted domain objects.

[handoffs.py](../libs/alpaca_pipeline/handoffs.py) defines the frozen outer records.
`Prepared` carries the selected work, accepted context/policy/evidence, request
counts, client references and selected recovery values. Histories consumes those
same objects and completes the coupled pricing/lineage work. Its `Histories`
record carries only the completed attempt, actual expanded scope, committed
workflow/summary bindings, a narrowed serving context and generation-bound held
values required by remaining consumers. Serve receives no provider client,
pricing acquisition plans or spent source-work calculation caches. Shared value
and row aliases remain references; no deepcopy or serializability test occurs
at a stage transition.

[runner.py](../libs/alpaca_pipeline/runner.py) sequences these records.
[execution.py](../libs/alpaca_pipeline/execution.py) acquires the gate or adopts the
CLI's actual exclusive ownership without another descriptor. Ownership binds the
store, process, thread and active lock lifetime; copied contexts cannot lend an
expired gate. Each stage retains its owner locks and explicit recovery-value
scope. Coordinated stages omit lock-gap applicability checks because exclusion
was continuous. Missing required handoffs stop dependent execution; there is no
fallback to predecessor globals, replay, reconstruction or handoff archive.
Existing journals retain their interrupted-publication responsibility.

`alpaca_end_of_day.run_pipeline(request_json: bytes) -> bytes` remains the public
JSON invocation/result adapter. Individual `prepare_pipeline`,
`produce_histories_pipeline` and `serve_pipeline` APIs retain their standalone
JSON input/output contracts around these same implementations. A real lock gap
requires the named selector/generation applicability checks; it never authorizes
repeating preparation, receipt acceptance, pricing comparisons or calculations.
The explicit JSON adapters remain suitable for independent process diagnosis:

```bash
set -o pipefail
python3.11 alpaca_pipeline_prepare.py < request.json |
  python3.11 alpaca_pipeline_histories.py |
  python3.11 alpaca_pipeline_serve.py
```

Everyday `update` passes the native invocation and final report directly; only its
CLI output is JSON. `update --plan` retains its read-only path. Progress, request
accounting, warnings, registry bookkeeping and existing stored formats are
unchanged. A selected exclusive cutoff is carried without another calendar
resolution. Producer validation of genuinely new artifacts remains with its owner.

Normal execution saves no handoff files. Ordinary `update --debug FILE.jsonl`
records compact native summaries and scalar effects in
`FILE.jsonl.handoffs/<operation-id>/`. These snapshots identify operation/runtime,
scope counts and completion outcomes. They neither traverse nor encode held
prices, receipts, assignments or caches and cannot restore execution. The compact
execution trace retains its format. Standalone JSON stages keep their explicit
full argument/result captures, with memory cost proportional to their document
size. DEBUG adds no provider work, storage reads or completed calculations.

The [runner regressions](../tests/test_alpaca_pipeline_runner.py) cover real CLI
execution, direct value/alias preservation, native-only values that forbid copying
or serialization, continuous exclusion, copied contexts, independent failures,
interruption, missing inputs and DEBUG equivalence without a bulk codec. The
[Serve regressions](../tests/test_alpaca_pipeline_serve.py) and the preceding
standalone transport suites retain actual fresh-process execution, named
invalidation, slow consumers and broken delivery after committed publication.
These checks do not establish every semantic lineage outcome or live operational
performance; the normal-update target remains 60 minutes, with 90 the maximum.
