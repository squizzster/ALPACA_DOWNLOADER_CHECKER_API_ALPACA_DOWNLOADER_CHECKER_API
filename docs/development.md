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

1. Agree the disputed ownership/continuity interval and batch scope. Inspect
   [published evidence](operations.md#focused-standalone-work); inspection reads, refresh may acquire/publish.
2. Use a fresh research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`. Supply the question,
   evidence/current rules and no preferred verdict. Request primary-source facts, uncertainty, counterexamples
   and a generic rule concept. Researchers never edit/publish; report unavailable configurations.
3. Map facts to deterministic internal evidence. Bind researched ownership evidence to retained source
   observations; do not invent unobserved bars or opaque provider UUIDs. Prefer existing-rule corrections;
   new rules need ordered gates/vetoes.
4. Compare plausible owners with leaving the history unallocated under the
   [allocation criterion](lineage.md#ownership-allocation-criterion). A missing certificate is one weakness
   to weigh, not automatic rejection of converging evidence. Test the positive case, nearest counterexample,
   dates and adjacent rules across complete affected groups. Include credible allocation without the
   particular certificate, a case where nonallocation is most likely, and genuinely tied alternatives.
   Reuse confirmed facts; close each case before the next.
5. Compare the final cascade on identical approved inputs for every affected identity: gains/losses,
   reassignment and membership. Run full checks; publish only within authorization. Distinguish evidence
   changes from rule changes.

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
`consume_prepared` adapter consumes the actual downstream history and serving
owners for feasibility testing. It checks only workflow, summary, lineage and
serving selector applicability across the lock gap; changed implementation versions or a
changed binding fail rather
than selecting preparation again. This adapter is not a separately exposed history
stage, and the ordinary `update` command still uses the extracted owners directly.

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
