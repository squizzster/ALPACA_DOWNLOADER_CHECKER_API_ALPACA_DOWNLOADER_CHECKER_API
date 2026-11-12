"""JSON request/result boundary shared by the public API and standalone CLI."""

import json
import math
from datetime import date
from pathlib import Path

import yaml

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, symbol_label

from . import SCHEMA_VERSION, VERSION, implementation_binding
from .export import instances
from .handoff import inspect_handoff
from .inspection import explain
from .policy import DEFAULT_POLICY, load_policy, rule_index, template
from .research import run

REQUEST_SCHEMA = "alpaca.lineage.debug.request"
REPORT_SCHEMA = "alpaca.lineage.debug.report"
FIELDS = {
    "init": set(),
    "rules": {"policy"},
    "explain": {"store", "symbol", "session", "adjustment", "max_segments", "include_metadata_handoff"},
    "run": {
        "store",
        "policy",
        "symbols",
        "all",
        "max_differences",
        "timeout_seconds",
        "control_policy",
        "screen_report",
        "reuse_report",
        "metadata_handoff",
    },
    "screen": {"store", "policy", "control_policy"},
    "export": {"manifest", "symbols", "multiple_only"},
    "handoff": {"input", "symbols"},
}


def unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def integer(value, name, maximum):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 0 and {maximum}")
    return value


def symbols(value):
    if not isinstance(value, list) or any(type(item) is not str for item in value):
        raise ValueError("symbols must be a list of ticker strings")
    return sorted({symbol_label(item) for item in value})


def path(value, name):
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a nonempty path string")
    return value


def report_file(value, name):
    document = json.loads(Path(path(value, name)).read_bytes(), object_pairs_hook=unique)
    if not isinstance(document, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return document


def execute(data):
    if type(data) is not bytes:
        raise ValueError("lineage debug requires UTF-8 JSON bytes")
    request = json.loads(data.decode("utf-8"), object_pairs_hook=unique)
    if (
        not isinstance(request, dict)
        or request.get("schema") != REQUEST_SCHEMA
        or type(request.get("schema_version")) is not int
        or request["schema_version"] != SCHEMA_VERSION
    ):
        raise ValueError("expected lineage debug request schema version 1")
    command = request.get("operation")
    if type(command) is not str or command not in FIELDS:
        raise ValueError("unknown lineage debug operation")
    unknown = set(request) - {"schema", "schema_version", "operation"} - FIELDS[command]
    if unknown:
        raise ValueError(f"unknown {command} request fields: {sorted(unknown)}")
    change_log = Path(__file__).resolve().parents[2] / "resources/alpaca_lineage_debug_change_log.json"
    entries = json.loads(change_log.read_bytes())
    if len(entries) != VERSION or any("version" in entry or not entry.get("modifications") for entry in entries):
        raise ValueError("lineage debug changelog/version mismatch")
    try:
        result = dispatch(command, request)
    except yaml.YAMLError as error:
        raise ValueError(f"invalid YAML policy: {error}") from error
    return json.dumps(
        {
            "schema": REPORT_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "debug_version": VERSION,
            "implementation_binding": implementation_binding(),
            "operation": command,
            "pipeline": [
                {"stage": "Prepare", "owns": "baseline recovery, selection and registration"},
                {
                    "stage": "Histories",
                    "owns": "acquisition, ownership, numerical choice and lineage publication",
                },
                {"stage": "Serve", "owns": "eligibility, catalogue publication and finalization"},
            ],
            "effects": {"production_writes": False, "provider_requests": False, "rule_replay": command == "run"},
            **result,
        },
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def dispatch(command, request):
    if command == "screen":
        from .screen import screen

        return screen(
            path(request.get("store"), "store"),
            load_policy(path(request.get("policy"), "policy")),
            load_policy(path(request.get("control_policy"), "control_policy")),
        )
    if command == "init":
        return {"status": "complete", "execution_complete": True, "yaml": template()}
    if command == "rules":
        return {
            "status": "complete",
            "execution_complete": True,
            **rule_index(load_policy(path(request.get("policy", str(DEFAULT_POLICY)), "policy"))),
        }
    if command == "explain":
        capture = request.get("include_metadata_handoff", False)
        if type(capture) is not bool:
            raise ValueError("include_metadata_handoff must be boolean")
        session = request.get("session")
        if session is not None and (
            type(session) is not str or date.fromisoformat(session).isoformat() != session
        ):
            raise ValueError("session must be YYYY-MM-DD")
        adjustment = request.get("adjustment", "raw")
        if adjustment not in ADJUSTMENTS:
            raise ValueError("unknown adjustment")
        return explain(
            path(request.get("store"), "store"),
            symbol_label(request.get("symbol")),
            session=session,
            adjustment=adjustment,
            maximum=integer(request.get("max_segments", 100), "max_segments", 10000),
            include_metadata_handoff=capture,
        )
    if command == "run":
        selected = symbols(request.get("symbols", []))
        all_groups = request.get("all", False)
        if type(all_groups) is not bool or (bool(selected) == all_groups):
            raise ValueError("choose a nonempty symbols list OR all: true")
        timeout = request.get("timeout_seconds")
        if timeout is not None and (
            type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number")
        policy = load_policy(path(request.get("policy", str(DEFAULT_POLICY)), "policy"))
        control = request.get("control_policy")
        control = load_policy(path(control, "control_policy")) if control is not None else None
        if ("screen_report" in request or "reuse_report" in request) and control is None:
            raise ValueError("screen and completed-report consumption require an explicit control_policy")
        if (
            getattr(policy, "membership_refinement", None)
            or getattr(policy, "control_mode", "replay") in {"accepted_publication", "documented_scopes"}
        ) and control is None:
            raise ValueError("membership refinement requires an explicit control_policy")
        if control is not None and getattr(control, "membership_refinement", None):
            raise ValueError("the control policy must disable experimental membership refinement")
        return run(
            path(request.get("store"), "store"),
            policy,
            selected,
            all_groups=all_groups,
            max_differences=integer(request.get("max_differences", 20), "max_differences", 1000),
            timeout_seconds=timeout,
            control_policy=control,
            screen_report=report_file(request["screen_report"], "screen_report")
            if "screen_report" in request
            else None,
            reuse_report=report_file(request["reuse_report"], "reuse_report")
            if "reuse_report" in request
            else None,
            metadata_handoff=report_file(request["metadata_handoff"], "metadata_handoff")
            if "metadata_handoff" in request
            else None,
        )
    if command == "export":
        multiple = request.get("multiple_only", True)
        if type(multiple) is not bool:
            raise ValueError("multiple_only must be boolean")
        return instances(
            path(request.get("manifest"), "manifest"),
            symbols(request.get("symbols", [])),
            multiple_only=multiple,
        )
    return inspect_handoff(path(request.get("input"), "input"), symbols(request.get("symbols", [])))
