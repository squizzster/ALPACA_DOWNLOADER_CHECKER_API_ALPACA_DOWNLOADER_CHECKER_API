# Architecture

## Self-describing code

Let things reveal themselves through the context they carry.

Names may reveal identity, origin, purpose, state, time, scope, provenance, or certainty across code, scripts,
directories, datasets, schemas, manifests, logs, hashes, and generated artefacts.

On-disk names travel through logs, exports, caches, archives, and future systems. Let useful information about
source, purpose, state, time, version, and provenance travel with them.

Use nearby types, schemas, metadata, documentation, tests, and traces to convey what a name cannot. Keep material
differences in certainty visible through terms such as `suspected`, `observed`, `inferred`, `verified`, and
`unresolved`.

Names such as `date` and `result` illustrate why isolated names can be ambiguous. Use the smallest name and context
that remove material uncertainty. For example, does `date` refer to a romantic date or a calendar date? Is `result`
an exam result, the result of a calculation, or something else?

## Architectural code ownership

The architecture establishes who is allowed to act and in what order; local guards then enforce the boundary against
malformed input, deadlines, stale generations, and unavoidable external races. We need both—but no local guard should
compensate for ambiguous ownership.
