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

Use generic evidence rules; the main agent owns implementation/acceptance.

1. Define one active case: disputed identity/continuity interval, complete affected group, question and
   expected result. Record store, selector, receipt/pricing, policy and executable implementation bindings
   before the experiment. Preserve exact requests and completed results as explicit research artifacts
   outside the production store. Inspect [published evidence](operations.md#focused-standalone-work);
   inspection reads, refresh may acquire/publish.
   Check whether the queue artifact predates an accepted repair. A stale export is not authority to repeat
   that repair. Capture selected diagnostic metadata when it must cross to the next research operation.
2. Use a fresh research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`. Supply the question,
   evidence/current rules and no preferred verdict. Request primary-source facts, uncertainty, counterexamples
   and a generic rule concept. Researchers never edit/publish; report unavailable configurations.
3. Compare that reference with completed internal decisions using the
   [lineage rule tool](lineage-debug.md). Classify the discrepancy: evidence, ownership rule, membership
   propagation, numerical choice or consumer interpretation. Bind research to retained observations;
   do not invent unobserved bars, provider UUIDs or failed predicate explanations. Separate security identity,
   dated market labels and numerical authority. A conflicting event date need not negate agreement on identity.
4. Propose one generic candidate in the responsible owner when the retained evidence reasonably supports
   it. Prefer existing-rule corrections; new predicates need ordered gates and counterexamples. Preserve
   accepted owner, price and scope decisions outside the actual change. Compare plausible owners with
   leaving the history unallocated under the
   [allocation criterion](lineage.md#ownership-allocation-criterion). A missing certificate is one weakness
   to weigh, not automatic rejection of converging evidence. Test the positive case, nearest counterexample,
   dates and adjacent rules across complete affected groups. Include credible allocation without the
   particular certificate, a case where nonallocation is most likely, and genuinely tied alternatives.
   Reuse confirmed facts; do not fix a technical handoff by repeating its producer's work.
5. Test the case and nearest counterexamples locally. Use an explicit unchanged control and candidate on
   identical held inputs; report published/control drift separately from candidate-only effects. An unchanged
   cascade lends its completed control calculations to a later research refinement. Python changes need an
   explicit preserved control implementation or an opt-in candidate path. Assert outputs and forbidden work,
   including actual invalidation and partial failure. Research replay alone does not prove incremental
   production continuation, interruption or append preservation.
   New primary facts can use an explicit accepted-publication control without repeating a completed full
   replay. Choose unresolved-only projection when accepted historical dates and owners already agree.
6. Measure wider impact after focused success. Derive the expected work from completed selectors first.
   A metadata-only exclusion requires a proven necessary condition for this exact candidate; unknown inputs
   remain selected, and arbitrary YAML changes cannot borrow a specialized screen. Carry accepted index,
   receipt and comparison handoffs without rereads or recalculation. Report changed stocks/dates/decisions,
   diagnostic-only differences, covered and failed/unknown scope, and bounded examples separately from exact
   counts. A candidate-unaffected proof is not a claim that the stored lineage is correct.
7. The main agent reviews and closes the case with its verdict, evidence, counterexamples, measured impact
   and remaining limits before starting another case. Run full checks and the mandatory trust review before
   a code handoff. Distinguish evidence changes from rule changes. An accepted read-only candidate is separate
   from ordinary producer integration and production publication; publish only within authorization and after
   the owning continuation/recovery checks establish that scope.

Misattribution and withholding supported history are both errors. Preserve supported intervals and explicit
ambiguity. Standalone resolution does not establish online availability.

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

The Phase 1 goal is to turn a daily-update request into a complete, registered
work package through a real UTF-8 JSON argument/result boundary. Its consumer
must continue in a fresh process without repeating preparation or relying on
predecessor Python objects. This is the first executable part of the three-part
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
selecting preparation again. The ordinary `update` command uses the [three-stage JSON controller](#pipeline-phase-3-complete-pipeline).

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
the JSON stages through the [controller](#pipeline-phase-3-complete-pipeline).

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

The Phase 3 goal is ordinary execution of Prepare → Histories → Serve through
real UTF-8 JSON arguments and results under one continuous operation lock. Each
owner restores its domain context from its argument; the controller carries no
domain decisions or caches outside those bytes. All three owners are also
independently executable for development and focused diagnosis.

[runner.py](../libs/alpaca_pipeline/runner.py) sequences the stages.
[execution.py](../libs/alpaca_pipeline/execution.py) acquires the gate for a direct
pipeline call or adopts the CLI's actual exclusive ownership without opening a
second descriptor. Ownership is bound to the store, process, thread and active
lock lifetime; it cannot be supplied in JSON. Exceptions and interruption release
the gate; its persistent pathname is never deleted. Each stage keeps its owner
locks and a fresh recovery-handoff scope. Normal stages omit lock-gap applicability
checks because there was no gap. Standalone stages receive/decode complete input,
acquire their gate, check only applicable generations and release locks before
output delivery. Producer checks for genuinely new data remain in their owners.

`alpaca_end_of_day.run_pipeline(request_json: bytes) -> bytes` runs the complete
pipeline. Individual APIs are `prepare_pipeline`, `produce_histories_pipeline`
and `serve_pipeline`, each consuming and returning bytes. Completed correction contracts travel in the
explicit work-policy state alongside outstanding requirements; new workflow selection must not discard their
producer disposition or reacquire the same unchanged history. The final Serve result
is the existing compact update report, including explicit failures and overall
completion. Serve selects components, prepares and assesses required inputs,
publishes the existing catalogue and acknowledges completed work. It has no
provider client and cannot invoke upstream producers. It consumes the carried
accepted index, controls, receipts, assignments and partitions without rereading
them solely because execution entered another stage.

```bash
set -o pipefail
python3.11 alpaca_pipeline_prepare.py < request.json |
  python3.11 alpaca_pipeline_histories.py |
  python3.11 alpaca_pipeline_serve.py
```

The adapters call the same public APIs; the shell form has actual lock gaps and
therefore performs their applicability checks. Everyday `update` uses the
continuous-lock controller. `update --plan` retains its read-only selection path.
The CLI retains progress, heartbeat, ETA, request counts, reporting, exit codes
and separate registry Git bookkeeping.

Prepare resolves an inclusive `through` once. A request may instead carry
`end_exclusive` with `through: null` for an already selected exclusive date.
This preserves existing legacy/non-session intentions without calendar rounding
again. Both forms are validated before provider or store work; downstream stages
consume the frozen operation cutoff unchanged.

Normal execution saves no handoff files, spool or replay store. With ordinary
`update --debug FILE.jsonl`, exact stage inputs, outputs and producer effect records
are captured in `FILE.jsonl.handoffs/<operation-id>/`, outside the production store.
The compact trace retains its own format. Stage APIs retain their explicit DEBUG
destination contract. DEBUG observes existing values and adds no provider requests,
historical reads or completed calculations. A missing, malformed or lost return
value errors and stops dependent stages; it is never reconstructed or automatically
replayed. Existing journals recover only interrupted production writes.

The [runner regressions](../tests/test_alpaca_pipeline_runner.py) exercise actual
CLI execution, fresh contexts, competing processes between stages, copied lock
contexts, exceptions, interruption and missing handoffs. The
[Serve regressions](../tests/test_alpaca_pipeline_serve.py) exercise public API/CLI
parity, fresh processes, forbidden upstream work, DEBUG equivalence, slow output
and broken delivery after publication. Existing append, correction, replacement,
dependency and peer-failure regressions run through ordinary JSON orchestration.
Stored pricing, lineage and serving schemas/layout remain unchanged. Synthetic
transport measurements do not establish live throughput, full lineage-policy
correctness or the 60-minute target/90-minute maximum.
