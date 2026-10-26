# Development

## Find the owner

Start with [AGENTS.md](../AGENTS.md), [architecture](ARCHITECTURE.md#code-ownership) and the relevant contract.
Use synthetic stores/HTTP doubles, not production copies or ordinary live acquisition. Test changed behavior
and its nearest failure: interruption, acknowledgement order and repeated upstream authentication matter.
Version/changelog rules and constants live in [VERSION_TRACKER.md](../VERSION_TRACKER.md).

## Lineage research and rule-improvement loop

Use generic evidence rules; the main agent owns implementation/acceptance.

1. Agree the disputed ownership/continuity interval and batch scope. Inspect
   [published evidence](operations.md#focused-standalone-work); inspection reads, refresh may acquire/publish.
2. Use a fresh research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`. Supply the question,
   evidence/current rules and no preferred verdict. Request primary-source facts, uncertainty, counterexamples
   and a generic rule concept. Researchers never edit/publish; report unavailable configurations.
3. Map facts to deterministic internal evidence. External research cannot establish unseen row ownership or
   opaque provider UUIDs. Prefer existing-rule corrections; new rules need ordered gates/vetoes.
4. Test the positive case, nearest counterexample, dates and adjacent rules across complete affected groups.
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
Never execute mutating examples for documentation verification; prose-only edits need no runtime-suite rerun.

Keep explanations in one README-linked home. Preserve rationale, constraints and gotchas; omit status journals,
dated test histories, duplicate version tables and speculative plans.
