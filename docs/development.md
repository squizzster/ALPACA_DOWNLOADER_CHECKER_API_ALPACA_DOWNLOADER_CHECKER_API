# Development

## Make the change at its owner

Use the [architecture map](ARCHITECTURE.md#code-ownership), then follow the command's imports.
Root commands stay thin. Tests under `tests/` mirror the boundary names and use temporary stores
and HTTP doubles. Do not copy production data or run live acquisition for ordinary tests.

Cover the changed behavior and its nearest failure case. For handoff changes, verify publication
before acknowledgement and that trusted consumers do not repeat upstream authentication.
Preserve existing worktree changes.

## Lineage research and rule-improvement loop

Cases supply generic concepts, never ticker exceptions. The main agent owns implementation and acceptance.

1. Define the disputed ownership/continuity interval and the agreed batch scope.
2. Inspect actual [pipeline evidence](operations.md#focused-standalone-work), trusting approved inputs.
   `lineage inspect` reads published evidence; explicit `lineage refresh` can acquire and publish.
3. Use a fresh research agent per case: `gpt-6.1-sol`, high reasoning, `fork_turns="none"`.
   Give a self-contained question, evidence and current rules without a preferred verdict.
   Request primary-source facts, uncertainty, counterexamples and a generic rule concept.
   Research agents do not edit or publish; report unavailable configurations honestly.
4. Map external facts to deterministic internal evidence. Public research cannot establish unseen
   bar ownership or opaque provider UUIDs. Correct an existing rule when possible; otherwise place
   the concept in the proper ordered registry, with clear gates and vetoes.
5. Test the positive case, nearest counterexample, date boundaries and adjacent-rule interactions.
   Screen for the complete affected groups. Reuse confirmed facts; close the case before the next.
6. At the agreed batch boundary, compare the final cascade using the same approved inputs across
   every affected identity. Inspect gains, losses, reassignment and membership changes, then run full
   verification. Publish only within authorized scope and distinguish changed evidence from changed rules.

Both misattribution and withholding supported history are errors. Fail closed where evidence remains
ambiguous; do not withhold independently supported intervals. Standalone resolution is not online availability.

## Versioned boundaries

Behavioral or serialized changes bump the owning constant and append exactly one entry to its
`resources/*change_log.json`. Entries contain `modifications`, never an explicit `version`;
one-based array position is the version, checked at runtime.
Constants and validators beside each owner are authoritative. Review compatibility deliberately.
Documentation-only changes require neither bumps nor changelog entries.

## Verification

Use the smallest meaningful tests while implementing. Before a code handoff:

```bash
ruff check .
ruff format --check .
python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v
git diff --check
```

Optional Parquet integration tests require [requirements-bloodhound.txt](../requirements-bloodhound.txt);
without PyArrow they skip. There is no repository CI, so report local results and any gaps accurately.

For documentation-only changes, verify local links/anchors, the complete README index and documented
commands against `python3.11 -B <entrypoint> --help`. Do not execute mutating examples.
Check the diff; runtime tests need not be repeated for prose changes.

Keep documentation current and compact, with one home per explanation. Use Git for history.
Do not add run diaries, status trackers, copied version matrices or speculative plans.
