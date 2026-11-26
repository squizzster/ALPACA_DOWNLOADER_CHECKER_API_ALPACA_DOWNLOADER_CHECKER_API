"""Prepare one canonical retained group for review, without publishing to the store."""

import hmac
import json
from contextlib import contextmanager
from copy import copy
from pathlib import Path

from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pipeline.evidence import export_evidence, import_evidence
from libs.alpaca_pricing_download.contract import json_bytes, symbol_label
from libs.alpaca_pricing_download.storage import read_update
from libs.alpaca_security_lineage.inference_evidence import provider_evidence
from libs.common.store_lock import operation_lock

from . import assignment_comparison as comparison
from . import group_engine, storage
from .acquisition import check_unavailable_pricing, unavailable_pricing
from .contract import COMPUTATION_REVISION, LineageError, document_digest, reusable_metadata_versions
from .former_ticker_bars import complete_literal_edges
from .membership_updates import update_reports
from .metadata import EvidenceClient, RetainedReportClient, WindowClient
from .pipeline import _current_sources, _group_services
from .prepared_recovery import CANDIDATE_BOUNDARY, CANDIDATE_VERSION, require_assertion_free_outputs
from .pricing import load_pricing
from .projection import require_complete
from .retained_recovery import RECOVERY_BOUNDARY, accept_selection
from .ruleset import load_ruleset
from .trade_tape import _record_following_otc_trade, record_mapped_samples, sample_session


class _MarketCheckpoint:
    """Durably transport completed native responses immediately after their physical requests."""

    def __init__(self, path, binding, client):
        self.path, self.binding = path, binding
        self.cache = client if isinstance(client, EvidenceClient) else EvidenceClient(client)
        self.seal = getattr(self.cache.client, "market_observation_handoff_token", None)
        if not callable(self.seal):
            raise LineageError("durable market observations require the authenticated native transport")
        if path.exists():
            saved = json.loads(path.read_bytes())
            token = saved.pop("token", None)
            if (
                saved.get("preparation") != binding
                or not isinstance(token, str)
                or not hmac.compare_digest(self.seal(document_digest(saved)), token)
            ):
                raise LineageError("market observation checkpoint selects different inputs")
            self.cache = import_evidence(saved["evidence"], self.cache.client, Decoder(saved["objects"]).decode)

    def _save(self):
        codec = Encoder()
        held = export_evidence(self.cache, codec.encode)
        record = {"preparation": self.binding, "evidence": held, "objects": codec.objects}
        record["token"] = self.seal(document_digest(record))
        storage.atomic_json(self.path, record, compare=False)

    def get_trade_tape_sample(self, *args, **kwargs):
        size = len(self.cache.trade_tape_samples)
        result = self.cache.get_trade_tape_sample(*args, **kwargs)
        if len(self.cache.trade_tape_samples) != size:
            self._save()
        return result

    def get_literal_bar_observation(self, *args, **kwargs):
        size = len(self.cache.literal_bar_observations)
        result = self.cache.get_literal_bar_observation(*args, **kwargs)
        if len(self.cache.literal_bar_observations) != size:
            self._save()
        return result


def _scope(store, symbol, ruleset, root):
    index = storage.read_index(root, symbol, group=True)
    require_complete(index)
    entry = index["symbols"].get(symbol)
    if entry is None:
        raise LineageError(f"{symbol}: no published group to prepare")
    scope = {
        "members": entry["group_members"],
        "pricing_generations": {n: r["source_sha256"] for n, r in index["symbols"].items()},
        "receipt_generations": {n: r["sha256"] for n, r in index["symbols"].items()},
    }
    selection = accept_selection(
        {
            "boundary": RECOVERY_BOUNDARY,
            "schema_version": 1,
            "store": str(store),
            "correlation_policy_sha256": ruleset.policy.semantic_sha256,
            "groups": [scope],
        },
        store,
    )
    group = selection.groups[0]
    group.select(index)
    return index, scope, group


@contextmanager
def _inputs(store, authorized, index, group, *, materialize, before_materialize=None):
    def select(root):
        current = index
        if storage.index_state(root) != index["projection_state"]:
            # A publication in another group cannot invalidate the retained
            # selection. Rebind only these members before opening a payload.
            current = storage.read_index(root, group.seed, group=True)
            require_complete(current)
            group.select(current)
            if any(current["symbols"][n]["sha256"] != token for n, token in group.receipts.items()):
                raise LineageError("selected lineage changed during preparation")
        return storage.SelectedIndex(
            {n: current["symbols"].get(n) for n in group.members}, root, current["projection_state"]
        )

    with storage.reader_snapshot(
        store, select, accepted_store=authorized, include_receipts=False, include_boundaries=False
    ) as snapshot:
        sources = _current_sources(store, snapshot["locked"], known=group.pricing)
        if any(read_update(store / "end_of_day/pricing" / n, published=True) is not None for n in sources):
            raise LineageError("selected group has pending pricing work")
        group.accept_sources(group.members, {n: document_digest(s) for n, s in sources.items()})
        if not materialize:
            yield None
            return
        if before_materialize is not None:
            before_materialize()
        previous = {n: storage.read_receipt(authorized.root, n, index) for n in sources}
        if any(
            not reusable_metadata_versions(r.get("versions"))
            or r.get("input_sources") != group.pricing
            or r.get("row_partition_complete") is not True
            for r in previous.values()
        ):
            raise LineageError("incomplete selected lineage input")
        absent = unavailable_pricing(previous[group.seed])
        if set(previous) | set(absent) != set(group.members):
            raise LineageError("incomplete selected component membership")
        check_unavailable_pricing(store, absent, snapshot["locked"], sources=sources)
        inputs = {
            n: load_pricing(store, n, source=s, source_sha256=group.pricing[n]) for n, s in sources.items()
        }
        yield {
            "receipts": previous,
            "inputs": inputs,
            "boundaries": {n: storage.read_boundaries(authorized.root, r) for n, r in previous.items()},
            "inference_evidence": {
                n: provider_evidence(r["evidence"], local_report=inputs[n].local) for n, r in previous.items()
            },
            "unavailable_pricing": absent,
        }


def _observe(store, selected, group, pricing, provider):
    evidence, inputs, previous = selected["inference_evidence"], selected["inputs"], selected["receipts"]
    for name, view in inputs.items():
        facts = copy(evidence[name])
        held = RetainedReportClient(provider, facts)
        window = WindowClient(held, name, previous[name], False, status_view=view)
        if name in held.symbols:
            window.consumed_symbols[name] = held.symbols[name]
        if "historical_trade_tape" not in facts:
            day = sample_session(view, facts)
            if day:
                observed = provider.get_trade_tape_sample(name, day)
                if observed is not None:
                    facts["historical_trade_tape"] = observed
        # Explicit assessment may acquire an absent mapped/OTC observation. A
        # prior completed denial or empty search retains its original scope.
        if record_mapped_samples(view, facts, window, refresh="mapped_historical_trade_tape" not in facts):
            _record_following_otc_trade(view, facts, window, refresh=False)
        evidence[name] = facts
    if not selected["unavailable_pricing"]:
        evidence.update(
            complete_literal_edges(store, inputs, evidence, previous, pricing, provider, {}, recovery=group)
        )


def _candidate(store, index, group, selected, ruleset):
    inputs, previous, evidence = selected["inputs"], selected["receipts"], selected["inference_evidence"]
    unavailable = previous[group.seed].get("unavailable_pricing", {})
    sources = {n: view.source for n, view in inputs.items()}
    deps = {n: receipt["dependencies"] for n, receipt in previous.items()}
    services = _group_services()
    components, reports = group_engine.correlate_components(
        store,
        inputs,
        evidence,
        previous,
        deps,
        unavailable,
        policy=ruleset.policy,
        retain=False,
        services=services,
    )
    if any(set(members) != set(group.members) for members in components.values()):
        raise LineageError("canonical assessment changed the selected complete group")
    reports, refinement = update_reports(
        reports,
        evidence,
        {
            "store": str(store),
            "selected_group": list(group.members),
            "components": components,
            "receipts": previous,
            "pricing_generations": group.pricing,
            "receipt_generations": group.receipts,
            "inputs": inputs,
        },
        ruleset,
    )
    required = {}
    completion = {"membership_refinement": refinement, "equal_bundles": {}}
    publication = group_engine.finish_correlated_group(
        services=services,
        reports=reports,
        acquisition_waves=0,
        affected={},
        assignment_boundaries={},
        checks={},
        choice_prefix_sources={},
        components=components,
        deps=deps,
        evidence=evidence,
        graph=None,
        index=index,
        inputs=inputs,
        no_new_rows=False,
        notices={},
        observer=None,
        old_boundaries=selected["boundaries"],
        original_choice_inputs=inputs,
        previous=previous,
        pricing_replacements=[],
        progress=None,
        read_boundaries=None,
        receipt_cache=None,
        required=required,
        resume=False,
        retained_recovery=group,
        root=store / "end_of_day/lineage",
        seed=group.seed,
        sources=sources,
        store=store,
        unavailable=unavailable,
        work_policy=None,
        completion=completion,
        dated_labels=ruleset.dated_labels,
        prepare_only=True,
    )
    if required or publication is None:
        raise LineageError(
            "canonical recovery requires additional numerical mapping: " + ", ".join(sorted(required))
        )
    selected["equal_bundles"] = completion["equal_bundles"]
    selected["numerical_instants"] = completion["instant_keys"]
    return publication


def _review(selected, publication, group):
    inputs, original, previous = selected["inputs"], selected["boundaries"], selected["receipts"]
    full = {n: {**original[n], **changed} for n, changed in publication.boundaries.items()}
    row_indexes = {}

    def frame(receipts, boundaries):
        fields = comparison._group_dimensions(boundaries, inputs, receipts, row_indexes=row_indexes)
        fields["group_diagnostics"] = comparison._group_diagnostics(receipts)
        return {
            "dimensions": fields,
            "digests": {k: comparison._digest_mapping(v) for k, v in fields.items()},
            "memberships": comparison._memberships(fields["semantic"]),
            "counts": comparison._row_counts(fields["semantic"]),
            "partition_complete": all(r["row_partition_complete"] for r in receipts.values()),
            "mapping_corrections": {},
        }

    before, after = frame(previous, original), frame(publication.receipts, full)
    result = {
        "mapping_corrections": after["mapping_corrections"],
        "candidate_partition_complete": after["partition_complete"],
        "memberships": {"baseline": before["memberships"], "candidate": after["memberships"]},
        "row_counts": {"baseline": before["counts"], "candidate": after["counts"]},
        "differences": {
            key: comparison._difference(
                values,
                after["dimensions"][key],
                12,
                baseline_sha256=before["digests"][key],
                candidate_sha256=after["digests"][key],
            )
            for key, values in before["dimensions"].items()
        },
    }
    # Cover every changed public identity/label/selected source, including the
    # intervals not represented by the bounded examples of detailed rule fields.
    ranges = []
    last = None
    for key in sorted(before["dimensions"]["semantic"]):

        def public(side):
            value = side["dimensions"]["semantic"][key]
            market = side["dimensions"]["observation"][key]
            return [
                value.get("branch"),
                (value.get("security_identity") or {}).get("security_id"),
                value.get("historical_symbol"),
                market.get("observation_selection"),
                market.get("observation_tickers"),
            ]

        left, right = public(before), public(after)
        if left == right:
            last = None
            continue
        symbol, adjustment, timestamp = key
        signature = symbol, adjustment, left, right
        if signature != last:
            ranges.append(
                {
                    "symbol": symbol,
                    "adjustment": adjustment,
                    "before": left,
                    "after": right,
                    "first": timestamp,
                    "last": timestamp,
                    "sessions": 0,
                }
            )
        ranges[-1]["sessions"] += 1
        ranges[-1]["last"] = timestamp
        last = signature
    from .candidate_numerical import capture

    label_projections = {}
    revision, bundles = capture(
        before["dimensions"],
        after["dimensions"],
        selected["equal_bundles"],
        row_indexes,
        group,
        numerical_instants=selected["numerical_instants"],
        label_output=label_projections,
    )
    if revision is not None:
        for receipt in publication.receipts.values():
            receipt["numerical_source_revision_version"] = revision["version"]
            receipt["numerical_source_revision"] = revision
    for name, identities in label_projections.items():
        receipt = publication.receipts[name]
        receipt["dated_market_projection_version"] = 1
        receipt["dated_market_projection"] = {
            "version": 1,
            "previous_receipt_generation": group.receipts[name],
            "pricing_generation": group.pricing[name],
            "previous_boundaries": previous[name]["boundaries"],
            "selection_unchanged": True,
            "identities": identities,
        }
    return {"comparison": result, "public_change_intervals": ranges}, bundles


def prepare(store, symbol, output, *, market_clients=None):
    """A completed output can be reused only for its store, exact group and acquisition mode."""
    store, output = Path(store).absolute(), Path(output).absolute()
    if output.resolve().is_relative_to(store.resolve()):
        raise LineageError("the review packet must be outside the production store")
    output.parent.mkdir(parents=True, exist_ok=True)
    symbol = symbol_label(symbol)
    authorized = storage.accept_store(store)
    ruleset = load_ruleset(authorize=True)
    with operation_lock(output, exclusive=True, wait=True):
        with storage.publication_lock(authorized.root):
            if storage.path_at(authorized.root, ".pending.json").exists():
                raise LineageError("lineage publication pending")
            index, scope, group = _scope(store, symbol, ruleset, authorized.root)
        binding = {
            "operation_id": group.operation_id,
            "execution_sha256": ruleset.execution_sha256,
            "market_evidence": market_clients is not None,
        }
        header = {
            "boundary": CANDIDATE_BOUNDARY,
            "version": CANDIDATE_VERSION,
            "store": str(store),
            **scope,
            "preparation": binding,
        }

        if output.exists():
            # The owning atomic writer emits the small scope header first. A
            # retry needs neither the new comparison nor its held receipts.
            prefix = json_bytes(header).removesuffix(b"}\n") + b","
            with output.open("rb") as stream:
                if stream.read(len(prefix)) != prefix:
                    raise LineageError("output selects different preparation inputs")
            with _inputs(store, authorized, index, group, materialize=False):
                pass
            return {"output": str(output), "members": list(group.members), "reused": True}

        checkpoint = output.with_name("." + output.name + ".market.json")
        if market_clients is None and checkpoint.exists():
            raise LineageError("market observation checkpoint selects a different acquisition mode")
        pricing, provider = None, None

        def accept_market_checkpoint():
            nonlocal pricing, provider
            if market_clients is not None:
                pricing, identity = market_clients()
                provider = _MarketCheckpoint(checkpoint, binding, identity)

        with _inputs(
            store, authorized, index, group, materialize=True, before_materialize=accept_market_checkpoint
        ) as selected:
            if provider is not None:
                _observe(store, selected, group, pricing, provider)
            publication = _candidate(store, index, group, selected, ruleset)
            review, bundles = _review(selected, publication, group)
            policy = ruleset.policy.as_report_object()
            require_assertion_free_outputs(publication.receipts, selected["receipts"], policy)
            changed = {
                n: {
                    kind: body
                    for kind, body in docs.items()
                    if publication.receipts[n]["boundaries"][kind]
                    != selected["receipts"][n]["boundaries"].get(kind)
                }
                for n, docs in publication.boundaries.items()
            }
            packet = {
                **header,
                "output_generation": document_digest(publication.receipts),
                "policy": policy,
                "computation_revision": COMPUTATION_REVISION,
                "baseline_receipts": selected["receipts"],
                "candidate_receipts": publication.receipts,
                "candidate_boundaries": changed,
                "baseline_boundaries": {
                    n: {k: v for k, v in docs.items() if k not in changed[n]}
                    for n, docs in selected["boundaries"].items()
                },
                "comparison": review,
                **({"selected_observation_bundles": bundles} if bundles is not None else {}),
                "review": {"version": 1, "status": "pending"},
            }
            storage.atomic_json(output, packet, compare=False)
            if market_clients is not None:
                checkpoint.unlink(missing_ok=True)
        return {"output": str(output), "members": list(group.members), "reused": False}
