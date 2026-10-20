# Development

## Make the change at its owner

Follow the [ownership map](ARCHITECTURE.md#code-ownership), then command imports. Keep root adapters thin;
preserve existing work. Tests mirror boundaries using temporary stores/HTTP doubles, never production
copies or ordinary live acquisition. Cover changed behavior and nearest failure; handoff tests verify
publication before acknowledgement and no repeated upstream authentication.

## Lineage research and rule-improvement loop

Generic rules, never ticker exceptions. Main agent owns implementation/acceptance.

1. Define disputed ownership/continuity interval and agreed batch scope.
2. Inspect [published evidence](operations.md#focused-standalone-work), trusting approved inputs.
   `lineage inspect` reads; explicit `lineage refresh` may acquire/publish.
3. Fresh research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`. Supply self-contained
   question, evidence/current rules, no preferred verdict. Request primary-source facts, uncertainty,
   counterexamples and generic rule concept. Research agents never edit/publish; report unavailable configurations.
4. Map external facts to deterministic internal evidence. Public research cannot establish unseen bar
   ownership or opaque provider UUIDs. Prefer correcting an existing rule; otherwise use the proper
   ordered registry with explicit gates/vetoes.
5. Test positive case, nearest counterexample, dates and adjacent-rule interactions across complete
   affected groups. Reuse confirmed facts; close each case before the next.
6. At the agreed batch boundary, compare the final cascade on identical approved inputs across every
   affected identity: gains/losses, reassignment, membership. Run full verification. Publish only within
   authorization; distinguish evidence changes from rule changes.

Misattribution and withholding supported history are both errors. Leave ambiguity unresolved while
preserving independently supported intervals. Standalone resolution ≠ online availability.

## Versioned boundaries

Behavior/serialized changes: bump owner constant; append exactly one `resources/*change_log.json` entry
containing `modifications`, no `version`. One-based array position is runtime-checked version.
Owner constants/validators are authoritative; review compatibility. Documentation-only changes: no bump/entry.

## Verification

Focused checks during implementation; before code handoff:

```bash
ruff check .
ruff format --check .
python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v
git diff --check
```

Optional Parquet integration tests require [requirements-bloodhound.txt](../requirements-bloodhound.txt); skip without
PyArrow. No repository CI: report local results/gaps accurately.

Documentation-only: check local links/anchors, complete README index, commands against
`python3.11 -B <entrypoint> --help`, code consistency and diff. Never execute mutating examples;
no runtime-test rerun needed for prose.

One home per explanation, linked from README; current behavior only. Git holds history.
No run diaries, status trackers, duplicated version tables or speculative plans.
