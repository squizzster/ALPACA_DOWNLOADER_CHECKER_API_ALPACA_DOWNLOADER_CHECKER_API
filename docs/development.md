# Development

## Make the change at its owner

Follow [AGENTS.md](../AGENTS.md) and the [ownership map](ARCHITECTURE.md#code-ownership).
Keep adapters thin; preserve existing work. Use temporary stores/HTTP doubles, never production copies or
ordinary live acquisition. Test changed behavior and its nearest failure, including publication before
acknowledgement and no repeated upstream authentication.

## Lineage research and rule-improvement loop

Use generic rules, never ticker exceptions. The main agent owns implementation and acceptance.

1. Agree the disputed ownership/continuity interval and batch scope; inspect
   [published evidence](operations.md#focused-standalone-work). Inspection reads; refresh may acquire/publish.
2. Use a fresh research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`.
   Supply a self-contained question, evidence/current rules and no preferred verdict. Request primary-source
   facts, uncertainty, counterexamples and a generic rule concept. Researchers never edit/publish;
   report unavailable configurations.
3. Map facts to deterministic internal evidence. External research cannot establish unseen bar ownership
   or opaque provider UUIDs. Prefer correcting an existing rule; new rules need ordered gates/vetoes.
4. Test the positive case, nearest counterexample, dates and adjacent rules across complete affected groups.
   Reuse confirmed facts; close each case before the next.
5. At the batch boundary, compare the final cascade on identical approved inputs for every affected identity:
   gains/losses, reassignment and membership. Run full checks; publish only within authorization.
   Distinguish evidence changes from rule changes.

Misattribution and withholding supported history are both errors. Preserve supported intervals while leaving
ambiguity unresolved. Standalone resolution does not establish online availability.

## Versioned boundaries

Behavior/serialized changes require an owning constant bump and exactly one JSON changelog entry with
`modifications`, no `version` field. One-based array position is the runtime-checked version; readable schemas
need not be reusable. Documentation-only changes leave versions/changelogs unchanged.

Constants, compatibility and changelog paths live with their owners: [public API](../alpaca_end_of_day/__init__.py),
[pricing](../libs/alpaca_pricing_download/contract.py), [lineage](../libs/alpaca_lineage_build/contract.py),
[serving](../libs/alpaca_serving_catalogue/contract.py) and the packages in the
[ownership map](ARCHITECTURE.md#code-ownership).

## Verification

Focused checks during implementation; before code handoff:

```bash
ruff check .
ruff format --check .
python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v
git diff --check
```

Optional Parquet tests skip without [PyArrow](../requirements-bloodhound.txt). No repository CI;
report actual local results and gaps. Tests do not establish retained-store correctness or live-universe throughput.

Documentation-only: compare substantive old/new meaning, check links/anchors and complete README coverage,
validate examples against the parser and `python3.11 -B alpaca-eod <command> --help`, and check the diff.
Never execute mutating examples; prose changes need no runtime-suite rerun.

Keep each explanation in one guide linked from README. Describe current behavior; no run diaries, status
trackers, duplicated version tables or speculative plans.
