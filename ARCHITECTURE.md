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

## Library ownership

Dependencies point inward and do not cycle:

```text
root entry points / compatibility facades
    └── libs/download_alpaca_end_of_day_bars
            ├── Alpaca HTTP and rate limiting
            └── libs/alpaca_end_of_day_pipeline
                    └── libs/common
```

`libs/common/` owns reusable mechanisms and the authoritative US-session calendar. It knows nothing about Alpaca
requests, download pairs, or pipeline manifests.

`libs/alpaca_end_of_day_pipeline/` owns validation and every data mutation. `download_publication.py` alone publishes
the frozen gzip/sidecar boundary. `symbol_manifests.py` alone publishes pipeline receipts and integrity failures.
`cached_downloads.py` authenticates and classifies existing state; it delegates derived writes to the manifest owner.

`libs/download_alpaca_end_of_day_bars/` owns command interpretation, credential timing, HTTP requests, rate limits, and
the order in which pipeline operations are invoked. It cannot write data artifacts directly.

Root Python files remain stable entry points or compatibility facades. They contain no domain implementation.
