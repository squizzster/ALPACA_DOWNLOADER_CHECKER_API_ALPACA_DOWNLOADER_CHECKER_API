# Alpaca EOD Data Engine

Maintains exact US-equity prices in eight adjustment views, dated security identities and a published
serving catalogue. Incremental checks reuse completed work; journals recover interrupted publications.
Local applications can search approved identities or inspect resolved histories and their evidence.
Research and trading decisions are outside this data boundary.

Requires Python 3.11, Unix `flock` and `America/New_York` timezone data. Provider acquisition requires
`ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` and Alpaca SIP access; local readers and offline catalogue
maintenance need compatible publications, no credentials/network. The production correlation policy compiler
requires [PyYAML](requirements.txt); Parquet export additionally requires
[PyArrow](requirements-bloodhound.txt). Runtime stores stay outside Git.
See the [executable](alpaca-eod), [CLI](libs/alpaca_cli/__init__.py) and
[pricing contract](libs/alpaca_pricing_download/contract.py) for these requirements.

## Run and consume

- [Operations](docs/operations.md): setup, daily update, cutoffs, recovery and export commands.
- [Public API](docs/public-api.md): identity selection, histories, snapshots and errors.

## Understand and change

- [Architecture](docs/ARCHITECTURE.md): workflows and code ownership.
- [Technical contracts](docs/technical-contracts.md): trusted artifacts, reuse and publication guarantees.
- [Correlation policy lab](docs/policy-lab.md): read-only YAML replay and differential checks against published groups.
- [Agent instructions](AGENTS.md): repository-wide boundaries and change rules.
- [Development](docs/development.md): research, versioning and verification.

This is the complete project documentation index. Start with [store setup](docs/operations.md#adopt-or-reconstruct-an-existing-store)
before running `./alpaca-eod update`; `update --plan` previews work and may fetch metadata.
