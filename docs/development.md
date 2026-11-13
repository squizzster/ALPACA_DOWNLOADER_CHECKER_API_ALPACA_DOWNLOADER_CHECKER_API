# Development

## Find the owner

Start with [AGENTS.md](../AGENTS.md), [architecture](ARCHITECTURE.md#code-ownership) and the relevant contract.
Use synthetic stores/HTTP doubles, not production copies or ordinary live acquisition. Test changed behavior
and its nearest failure: interruption, acknowledgement order and repeated upstream authentication matter.
Version/changelog rules and constants live in [VERSION_TRACKER.md](../VERSION_TRACKER.md).

Before adding a boundary check, name the new fact it establishes and why the accepted producer handoff does not
already guarantee it. Follow the [trust contract](../AGENTS.md#trust-before-touching-storage): a new caller, retry
or workflow acknowledgement alone is not a new data fact. Keep the rationale beside the handoff: its owner,
guarantee, scope/lifetime, actual invalidation condition and the upstream work consumers must not repeat.
Do not justify an existing redundant check as necessary merely because it is already present.

For behavioral changes, regression tests must reject forbidden work as well as incorrect results: unchanged
maintenance inputs must not reopen upstream artifacts merely to repeat completed checks, replay decisions or
republish data, while changed inputs must still take the owning update path. Selected payload reads needed to
serve a new history or export request remain legitimate work. [Approval handoffs](../tests/test_alpaca_approval_handoffs.py) and
[serving handoffs](../tests/test_alpaca_serving_handoffs.py) exercise this distinction, including generation changes,
expired handoffs and interruption. Preserve those assertions when changing the mechanism of completion.

## Lineage research and rule-improvement loop

Research is an external reference; the main agent owns generic-rule implementation and acceptance.
No fact-injection candidate or mechanism is admissible. Apply the
[ownership allocation criterion](lineage.md#ownership-allocation-criterion) and the
[generic inference instructions](../AGENTS.md#generic-lineage-inference).

1. Define one active case: disputed dates, the complete dependency group and the ownership question.
   Record store, selector, receipt/pricing, executable implementation and policy bindings before an
   experiment. Carry applicable completed work forward. A queue predating an accepted repair does not
   authorize repeating it.
2. Use a fresh independent research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`.
   Request primary-source chronology, conflicts, uncertainty and counterexamples without a preferred
   verdict. Researchers never edit or publish. Their report is an external expected-result reference;
   no part of it becomes inference evidence or a scoped runtime application. Research may confirm a
   connection or establish that the API cannot reasonably connect the history; both are useful results.
3. Explain the recorded result from retained provider observations and completed decisions. Classify
   the cause before changing a rule: evidence availability, ownership inference, membership propagation,
   numerical selection or consumer interpretation. Do not invent bars, provider UUIDs, endpoint outcomes
   or unrecorded predicate explanations. Keep source identity, dated labels and numerical authority distinct.
4. Propose one generic correction when the admissible retained data reasonably supports the researched
   result. Prefer an existing-rule correction; new rules require ordered conditions and counterexamples.
   Choose the most likely reasonably supported owner without demanding certainty or a special certificate.
   A result that cannot reasonably support a connection is a valid completed, nonblocking processed
   lineage outcome, with ownerless history unpublished to consumers. Both connecting and leaving
   ownerless must be judged against the best interpretation of the available source evidence. Research conclusions, ticker lists and source documents
   must never be supplied to the inference engine as assertions, overlays or exceptions.
5. Test the candidate and nearest failures on identical held inputs with an explicit unchanged control.
   Separate stored/control drift from candidate-caused effects. A Python change needs an opt-in candidate
   or preserved control implementation. Test equivalent evidence under different ticker spellings, a
   reasonably supported allocation without a particular certificate, competing owners, genuine ties,
   scoped invalidation and interruption. Fail on research-file access, fact-injection ingress, repeated
   provider requests, boundary reads or completed calculations. Do not repeat accepted comparisons.
6. Measure wider impact from completed selectors and the actual generic rule conditions. A metadata
   exclusion needs a necessary-condition proof; unknowns remain selected. Report affected identities,
   dates, decisions, explanations, failed/unevaluated scope and limits. Unaffected classification does
   not establish stored correctness. Preserve independent completed peer results.
7. Close the case with the generic-rule verdict, external reference, counterexamples, measured impact
   and limits before starting another. Complete owning regressions, mandatory trust review and full
   checks before a code handoff. Production publication requires separate authorization; research and
   diagnostic results never authorize it. Continuation and recovery consume completed results without
   reopening research assertions or replaying accepted work.

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

Prepare turns a daily-update request into a complete registered work package.
The ordinary controller passes a native `Prepared` record by reference. The
standalone JSON adapter can continue in a fresh process without repeating preparation
or relying on predecessor Python objects. This is the first executable part of the three-part
split: Prepare, history production, then serving. The six logical responsibilities
are baseline restoration, initial selection, complete pricing/lineage production,
serving component selection, serving preparation/assessment, and publication.

Prepare owns the operation identity and frozen inclusive cutoff, existing journal
recovery, selected pending-summary recovery, catalogue observation, initial work
selection, registration, completed-work acknowledgements and initial maintenance
membership. Ordinary live pricing maintenance starts after it returns. Recovery
may itself finish pricing and lineage work; its accepted results must cross too.

The implementation is in [alpaca_pipeline](../libs/alpaca_pipeline/prepare.py).
The public entrypoint is `alpaca_end_of_day.prepare_pipeline(request_json: bytes) -> bytes`;
[alpaca_pipeline_prepare.py](../alpaca_pipeline_prepare.py) calls that same entrypoint
with stdin and stdout. It requires the existing provider credential environment.
For example, supply this document with absolute paths appropriate to your store:

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
