# Alpaca end-of-day data

This repository implements a provenance-preserving pipeline for Alpaca US-equity daily bars. It keeps each
download as an authenticated frozen source, records deterministic integrity failures separately, assigns row
segments to Alpaca asset UUIDs when the evidence permits, and publishes advisory warning overlays. Downstream
layers authenticate and describe upstream data; they do not silently rewrite it.

## Ethos

The system is building toward `READ TICKER`: a reader that can authenticate the original provider bytes and every
applicable decision layered above them before assembling a usable security history in memory.

Preserve observed reality first. Give each interpretation, segmentation, repair decision, and warning its own
versioned, digest-bound owner. Never make a downstream convenience silently rewrite an upstream fact, and never turn
uncertainty into an identity assignment. A successful branch may continue while unresolved work remains explicit in
another branch.

The reader and pricing-repair decision layers are not implemented. The current boundaries and their authority are
defined in [the architecture](ARCHITECTURE.md).

## Documentation

### Architecture

- [Pipeline boundaries and code ownership](ARCHITECTURE.md) — execution order, dependency direction, mutation
  authority, and the distinction between integrity, identity, and advisory findings.

### Technical

- [Data and artifact contracts](docs/technical-contracts.md) — authoritative source locations, validation
  invariants, on-disk artifacts, authentication, correlation semantics, and versioned generations.

### CLI and operations

- [Commands and operational recovery](docs/operations.md) — prerequisites, full and standalone commands, cache
  behavior, `--force`, `--repair`, exit behavior, and common hazards.

### Development

- [Development guide](docs/development.md) — implementation map, compatibility surfaces, version-bump rules, and
  verification commands.
