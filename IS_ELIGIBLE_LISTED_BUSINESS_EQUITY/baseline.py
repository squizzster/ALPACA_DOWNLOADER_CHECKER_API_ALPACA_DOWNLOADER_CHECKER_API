"""Consume public group evidence once; export metadata without decoding prices."""

import csv
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from . import ROOT, SCHEMA_VERSION, VERSION, validate_version

CSV_COLUMNS = (
    "public_id",
    "names",
    "tickers",
    "latest_tickers",
    "first_session",
    "last_session",
    "exchanges",
    "asset_classes",
    "cusips",
    "group_id",
)


class BaselineError(RuntimeError):
    """A baseline could not be completed; no final manifest was published."""


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def plan_baseline(reader, roster):
    """Plan from the completed source roster without opening individual groups."""
    count = len(roster["sources"])
    if not count:
        raise BaselineError("no published source entries; publish lineage before exporting a baseline")
    return {
        "operation": "metadata_lineage_baseline",
        "scope": "all_resolved_identities_in_indexed_lineage_groups",
        "store": str(reader.store),
        "lineage_roster_generation": roster["generation"],
        "selected_source_count": count,
        "group_reads_upper_bound": count,
        "selected_group_count": roster["group_count"],
        "source_receipt_reads_upper_bound": count,
        "source_manifest_reads": 0,
        "pricing_locks": 0,
        "assignment_documents_upper_bound": 3 * count,
        "price_payload_reads": 0,
        "provider_requests": 0,
        "source_store_writes": 0,
        "identity_rule_executions": 0,
        "eligibility_assessments": 0,
    }


def _records(group):
    """Project published membership; never assign actions or infer effective dates."""
    assignments = {}
    for symbol, source in group["sources"].items():
        for assignment in source["assignments"]:
            public_id = assignment["security_identity"]["security_id"]
            assignments.setdefault(public_id, []).append({"source_symbol": symbol, **assignment})

    for security in group["securities"]:
        public_id = security["public_id"]
        segments = assignments.get(public_id, [])
        members = {
            member for segment in segments for member in segment["security_identity"]["member_alpaca_asset_ids"]
        }
        observations = []
        for symbol, source in group["sources"].items():
            evidence = source["evidence"]
            for lookup in evidence["asset_lookups"]:
                asset = lookup.get("asset") or {}
                if asset.get("id") not in members:
                    continue
                # Retain every matched lookup as evidence, including mismatched
                # labels. Only an own-label lookup supplies descriptive fields.
                own_label = lookup.get("lookup_kind") == "symbol" and lookup.get("lookup") == asset.get("symbol")
                observed = lookup.get("observed_at_utc")
                if observed is None and lookup.get("source") == "asset_catalogue":
                    observed = evidence.get("catalogue_observed_at_utc")
                observations.append(
                    {
                        "source_symbol": symbol,
                        "lookup": lookup,
                        "observed_at": observed,
                        "effective_from": None,
                        "effective_through": None,
                        "own_label_description": own_label,
                    }
                )
        own_assets = [item["lookup"]["asset"] for item in observations if item["own_label_description"]]
        cusips = {
            value for segment in segments if (value := (segment.get("canonical_security") or {}).get("cusip"))
        }
        cusips.update(
            item["lookup"]["lookup"] for item in observations if item["lookup"].get("lookup_kind") == "cusip"
        )
        yield {
            **security,
            "group_id": group["generation"],
            "source_symbols": group["source_symbols"],
            "names": sorted({a["name"] for a in own_assets if a.get("name")}),
            "exchanges": sorted({a["exchange"] for a in own_assets if a.get("exchange")}),
            "asset_classes": sorted({a["class"] for a in own_assets if a.get("class")}),
            "cusips": sorted(cusips),
            "metadata_observations": observations,
            "lineage_assignments": segments,
        }


def write_baseline(reader, snapshot, destination, *, progress=None):
    """Publish one self-contained run after every selected group succeeds.

    The public snapshot has accepted membership and selecting generations. Its
    owning publication gate keeps them coherent. Consume them without another
    selection, freshness pass or upstream check.
    """
    validate_version()
    roster = snapshot.roster
    destination = Path(destination).resolve()
    source_store = reader.store.resolve()
    if destination == source_store or destination.is_relative_to(source_store):
        raise ValueError("baseline output must be outside the source store")
    if destination.exists():
        raise FileExistsError(f"output already exists: {destination}")
    plan = plan_baseline(reader, roster)
    started = _now()
    definition = (ROOT / "definition.yaml").read_bytes()
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".baseline-", dir=destination.parent))
    seen_sources, public_ids = set(), set()
    group_count = 0
    try:
        with (
            (stage / "securities.jsonl").open("w", encoding="utf-8") as securities,
            (stage / "groups.jsonl").open("w", encoding="utf-8") as groups,
            (stage / "securities.csv").open("w", encoding="utf-8", newline="") as flat,
        ):
            writer = csv.DictWriter(flat, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for group in snapshot.iter_groups():
                source_symbols = set(group["source_symbols"])
                for record in _records(group):
                    public_id = record["public_id"]
                    if public_id in public_ids:
                        raise BaselineError(f"public_id occurs in multiple groups: {public_id}")
                    public_ids.add(public_id)
                    securities.write(_json(record) + "\n")
                    writer.writerow(
                        {
                            key: _json(record[key]) if isinstance(record[key], list) else record[key]
                            for key in CSV_COLUMNS
                        }
                    )
                groups.write(_json({"group_id": group["generation"], **group}) + "\n")
                group_count += 1
                seen_sources.update(source_symbols)
                if progress is not None:
                    progress(len(seen_sources), len(roster["sources"]), group_count, len(public_ids))
        (stage / "public_ids.txt").write_text(
            "".join(f"{value}\n" for value in sorted(public_ids)), encoding="utf-8"
        )
        (stage / "definition.yaml").write_bytes(definition)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "exporter_version": VERSION,
            "definition_id": "IS_ELIGIBLE_LISTED_BUSINESS_EQUITY",
            "started_at": started,
            "completed_at": _now(),
            "plan": plan,
            "source_roster": roster,
            "source_count": len(seen_sources),
            "group_count": group_count,
            "public_id_count": len(public_ids),
            "assessment_performed": False,
            "corporate_action_scope": "group_evidence_not_assigned_security_events",
            "files": {
                "securities": "securities.jsonl",
                "table": "securities.csv",
                "group_evidence": "groups.jsonl",
                "public_ids": "public_ids.txt",
                "definition": "definition.yaml",
            },
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        # Runs are immutable. The final directory is visible only after success.
        # mkdir rejects even an empty destination claimed by another exporter.
        destination.mkdir()
        try:
            os.replace(stage, destination)
        except BaseException:
            destination.rmdir()
            raise
        return {"manifest": str(destination / "manifest.json"), **manifest}
    finally:
        if stage.exists():
            shutil.rmtree(stage)
