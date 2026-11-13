"""Versioned, allowlisted value records with handoff-local aliases.

Only named producer types cross this boundary. No imports, executable names or
Python object IDs are accepted from input. Decoding consumes prior acceptance;
it does not replay producer validation, normalization or policy compilation.
"""

from collections import defaultdict
from dataclasses import fields
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from urllib.error import HTTPError, URLError

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import UuidLineageEdge
from libs.alpaca_lineage_build.assignment_handoff import AcceptedObservationChoices, _FrozenDict
from libs.alpaca_lineage_build.context import HistoryScope
from libs.alpaca_lineage_build.correlation_policy import CompiledPolicy
from libs.alpaca_lineage_build.market_assignments import CompletedMarketProjection
from libs.alpaca_lineage_build.numerical_completion import NumericalCompletion, NumericalInput
from libs.alpaca_lineage_build.numerical_recovery import NumericalRecoveryGroup, NumericalRecoverySelection
from libs.alpaca_lineage_build.observation_carry import ChoiceRun, RetainedChoices
from libs.alpaca_lineage_build.policy_active_suffix import ActiveSuffixRule
from libs.alpaca_lineage_build.policy_evidence_quality import EvidenceQualityRule
from libs.alpaca_lineage_build.prepared_recovery import PreparedCandidate
from libs.alpaca_lineage_build.pricing import ClosedAcceptedSource, MetadataSourceControl, PricingInput
from libs.alpaca_lineage_build.ruleset_contract import AcceptedRuleset
from libs.alpaca_lineage_build.source_choice_handoff import AcceptedSourceBundles
from libs.alpaca_lineage_build.source_equivalence import AcceptedEqualityFrame, AcceptedSourceEquivalence
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_lineage_build.storage import AcceptedStore
from libs.alpaca_pricing_download.adaptive import CompletedAdjustments
from libs.alpaca_pricing_download.adjustment_policy import AdaptiveAdjustmentDecision
from libs.alpaca_pricing_download.check_summary import AcceptedEntry, _AcceptedRows
from libs.alpaca_pricing_download.endpoints import Endpoints, EndpointSelection, HistoryComparison
from libs.alpaca_pricing_download.evidence import PublishedPricingEvidence
from libs.alpaca_pricing_download.failures import AcquisitionFailure
from libs.alpaca_pricing_download.full_publication import (
    ApprovedAppendAcceptance,
    FullPublicationAcceptance,
    PublicationAcceptance,
)
from libs.alpaca_pricing_download.responses import CompletedBars
from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence

from .source_state import ADAPTER_EXTRAS, IdentityKey, require_generic_plan


class HandoffError(ValueError):
    """Missing, incompatible or stale pipeline input."""


@lru_cache(maxsize=1)
def _rules():
    from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import OWNERSHIP_INTERVAL_RULES
    from libs.alpaca_end_of_day_security_correlation.row_correlation import ROW_CORRELATION_RULES

    return {
        type(rule).__name__ + ":" + rule.name: rule
        for rule in (*OWNERSHIP_INTERVAL_RULES, *ROW_CORRELATION_RULES)
    }


DATACLASSES = {
    c.__name__: c
    for c in (
        CompiledPolicy,
        AcceptedRuleset,
        ActiveSuffixRule,
        EvidenceQualityRule,
        CompletedBars,
        HistoryScope,
        AcceptedStore,
        EndpointSelection,
        HistoryComparison,
        PublicationAcceptance,
        FullPublicationAcceptance,
        ApprovedAppendAcceptance,
        AdaptiveAdjustmentDecision,
        PublishedPricingEvidence,
        AcquisitionFailure,
        PreparedCandidate,
        NumericalCompletion,
        NumericalInput,
        NumericalRecoveryGroup,
        NumericalRecoverySelection,
        SourceWorkPlan,
        PricingInput,
        MetadataSourceControl,
        ClosedAcceptedSource,
        RetainedChoices,
        ChoiceRun,
        CompletedMarketProjection,
        UuidLineageEdge,
    )
}
CONTAINERS = {
    "dict": dict,
    "list": list,
    "tuple": tuple,
    "set": set,
    "frozenset": frozenset,
    "accepted_rows": _AcceptedRows,
    "assignment_mapping": _FrozenDict,
    "source_equivalence": AcceptedSourceEquivalence,
    "equality_frame": AcceptedEqualityFrame,
}


ERROR_FIELDS = (
    "status",
    "code",
    "http_status",
    "request_identifiers",
    "symbol",
    "provider_message",
    "evidence",
    "failure",
    "url",
    "reason",
    "msg",
)


@lru_cache(maxsize=1)
def _errors():
    from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
    from libs.alpaca_lineage_build.contract import CorrelationPolicyError, LineageError
    from libs.alpaca_lineage_build.work_contract import RegistrationIncomplete, WorkDeferred
    from libs.alpaca_pricing_download.contract import (
        BarsSymbolUnavailable,
        DownloadError,
        InitialPricingUnavailable,
    )
    from libs.alpaca_pricing_download.correction import ProviderObservationConflict
    from libs.alpaca_pricing_download.failures import RetainedAcquisitionError
    from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
    from libs.alpaca_serving_catalogue.contract import CatalogueError

    return {
        cls.__name__: cls
        for cls in (
            RuntimeError,
            OSError,
            ValueError,
            TimeoutError,
            ConnectionError,
            HTTPError,
            URLError,
            LineageError,
            CorrelationPolicyError,
            WorkDeferred,
            RegistrationIncomplete,
            DownloadError,
            BarsSymbolUnavailable,
            InitialPricingUnavailable,
            ProviderObservationConflict,
            RetainedAcquisitionError,
            IdentityHttpError,
            SecurityCorrelationError,
            CatalogueError,
        )
    }


class Encoder:
    def __init__(self):
        self.objects = []
        self._selected = {}
        self._held = []

    def encode(self, value):
        if value is None or type(value) in (bool, int, float, str):
            return value
        previous = self._selected.get(id(value))
        if previous is not None:
            return {"ref": previous}
        slot = len(self.objects)
        self._selected[id(value)] = slot
        self._held.append(value)
        self.objects.append(None)
        record = self._record(value)
        self.objects[slot] = record
        return {"ref": slot}

    def _record(self, value):
        encode = self.encode
        if isinstance(value, SourceWorkPlan):
            require_generic_plan(value)
        # Legacy closed outputs may retain immutable provenance mappings. Carry
        # their plain values without reconstituting an active fact capability.
        if type(value).__module__ == "libs.alpaca_security_lineage.event_supplements":
            kind = {"_FrozenDict": "dict", "_FrozenList": "list"}.get(type(value).__name__)
            if kind is None:
                raise HandoffError("research assertion containers cannot cross stage JSON")
            return {
                "kind": kind,
                "items": [[encode(k), encode(v)] for k, v in value.items()]
                if isinstance(value, dict)
                else [encode(v) for v in value],
            }
        if isinstance(value, SimpleNamespace):
            names = ("path", "manifest", "rows", "pipeline_state", "pipeline_diagnostic", "integrity_failure")
            if set(value.__dict__) != set(names):
                raise HandoffError("unsupported pricing descriptor fields")
            return {
                "kind": "pricing_descriptor",
                "fields": {name: encode(getattr(value, name)) for name in names},
            }
        if isinstance(value, ProviderEvidence):
            return {"kind": "provider_evidence", "items": [[encode(k), encode(v)] for k, v in value.items()]}
        if isinstance(value, IdentityKey):
            return {"kind": "identity_key", "owner": encode(value.owner)}
        if isinstance(value, MappingProxyType):
            return {"kind": "mapping_proxy", "items": [[encode(k), encode(v)] for k, v in value.items()]}
        if isinstance(value, Path):
            return {"kind": "path", "value": str(value)}
        if isinstance(value, datetime):
            return {"kind": "datetime", "value": value.isoformat()}
        if isinstance(value, date):
            return {"kind": "date", "value": value.isoformat()}
        if isinstance(value, Decimal):
            return {"kind": "decimal", "value": str(value)}
        if isinstance(value, Endpoints):
            return {"kind": "endpoints", "values": encode(dict(value)), "tail_all": encode(value.tail_all)}
        if isinstance(value, AcceptedSourceBundles):
            return {
                "kind": "source_bundles",
                "values": encode(dict(value)),
                "observations": encode(value.observations),
            }
        if isinstance(value, CompletedAdjustments):
            # Cached producer derivations are part of the completed handoff.
            # Never evaluate a cached_property merely to populate transport.
            return {
                "kind": "completed_adjustments",
                "fields": {f.name: encode(getattr(value, f.name)) for f in fields(value)},
                "derived": {
                    name: encode(value.__dict__[name])
                    for name in ("views", "endpoints", "extent", "proof")
                    if name in value.__dict__
                },
            }
        if isinstance(value, AcceptedEntry):
            return {
                "kind": "summary_entry",
                "value": encode(dict(value)),
                "serialized": encode(value.serialized),
                "plans": encode(value.append_plans),
            }
        if isinstance(value, AcceptedObservationChoices):
            return {
                "kind": "observation_choices",
                "views": encode(value["views"]),
                "partitions": encode(value["partitions"]),
                "generation": value.generation,
            }
        if type(value).__name__ in DATACLASSES and type(value) is DATACLASSES[type(value).__name__]:
            return {
                "kind": type(value).__name__,
                "fields": {f.name: encode(getattr(value, f.name)) for f in fields(value)},
                "extras": {
                    name: encode(getattr(value, name))
                    for name in ADAPTER_EXTRAS
                    if type(value) in (PricingInput, MetadataSourceControl, ClosedAcceptedSource)
                    and name in value.__dict__
                    and name not in {f.name for f in fields(value)}
                },
            }
        if type(value).__name__ in ("OwnershipIntervalRule", "RowCorrelationRule"):
            name = type(value).__name__ + ":" + value.name
            if _rules().get(name) is not value:
                raise HandoffError("unregistered policy rule")
            return {"kind": "policy_rule", "name": name}
        if isinstance(value, BaseException):
            classes = _errors()
            name = type(value).__name__
            if classes.get(name) is not type(value):
                # Standard transport subclasses carry the same failed-observation
                # semantics. Keep their original name as diagnostic information.
                name = next(
                    (cls.__name__ for cls in (OSError, RuntimeError, ValueError) if isinstance(value, cls)), None
                )
                if name is None:
                    raise HandoffError("unsupported observed error type: " + type(value).__name__)
            return {
                "kind": "error",
                "type": name,
                "source_type": type(value).__name__,
                "message": str(value),
                "details": {k: encode(getattr(value, k)) for k in ERROR_FIELDS if hasattr(value, k)},
                "cause": encode(value.__cause__),
            }
        if isinstance(value, defaultdict):
            if value.default_factory not in (set, list, dict, None):
                raise HandoffError("unsupported default factory")
            return {
                "kind": "defaultdict",
                "factory": None if value.default_factory is None else value.default_factory.__name__,
                "items": [[encode(k), encode(v)] for k, v in value.items()],
            }
        for name, cls in CONTAINERS.items():
            if type(value) is cls:
                return {
                    "kind": name,
                    "items": [[encode(k), encode(v)] for k, v in value.items()]
                    if isinstance(value, dict)
                    else [encode(v) for v in value],
                }
        raise HandoffError("unsupported handoff value: " + type(value).__name__)


class Decoder:
    def __init__(self, objects):
        if not isinstance(objects, list):
            raise HandoffError("missing handoff object records")
        self.objects = objects
        self._values = {}
        self._active = set()

    def decode(self, value):
        if value is None or type(value) in (bool, int, float, str):
            return value
        if not isinstance(value, dict) or set(value) != {"ref"} or type(value["ref"]) is not int:
            raise HandoffError("invalid handoff reference")
        slot = value["ref"]
        if not 0 <= slot < len(self.objects) or slot in self._active:
            raise HandoffError("invalid or cyclic handoff reference")
        if slot not in self._values:
            self._active.add(slot)
            try:
                self._values[slot] = self._record(self.objects[slot])
            except (KeyError, TypeError, IndexError) as error:
                raise HandoffError("incomplete handoff record") from error
            finally:
                self._active.remove(slot)
        return self._values[slot]

    def _record(self, record):
        kind, decode = record["kind"], self.decode
        if kind == "pricing_descriptor":
            if set(record["fields"]) != {
                "path",
                "manifest",
                "rows",
                "pipeline_state",
                "pipeline_diagnostic",
                "integrity_failure",
            }:
                raise HandoffError("incomplete pricing descriptor")
            return SimpleNamespace(**{name: decode(value) for name, value in record["fields"].items()})
        if kind == "provider_evidence":
            if set(record) != {"kind", "items"} or not isinstance(record["items"], list):
                raise HandoffError("incomplete provider-evidence record")
            value = ProviderEvidence.__new__(ProviderEvidence)
            dict.__init__(value, ((decode(k), decode(v)) for k, v in record["items"]))
            return value
        if kind == "identity_key":
            return id(decode(record["owner"]))
        if kind == "mapping_proxy":
            return MappingProxyType({decode(k): decode(v) for k, v in record["items"]})
        if kind in ("path", "datetime", "date", "decimal"):
            return {
                "path": Path,
                "datetime": datetime.fromisoformat,
                "date": date.fromisoformat,
                "decimal": Decimal,
            }[kind](record["value"])
        if kind == "endpoints":
            value = Endpoints.__new__(Endpoints)
            dict.__init__(value, decode(record["values"]))
            value.tail_all = decode(record["tail_all"])
            return value
        if kind == "source_bundles":
            return AcceptedSourceBundles(decode(record["values"]), decode(record["observations"]))
        if kind == "completed_adjustments":
            if set(record["fields"]) != {f.name for f in fields(CompletedAdjustments)} or set(
                record["derived"]
            ) - {"views", "endpoints", "extent", "proof"}:
                raise HandoffError("incomplete completed-adjustment record")
            value = CompletedAdjustments(**{k: decode(v) for k, v in record["fields"].items()})
            for name, item in record["derived"].items():
                object.__setattr__(value, name, decode(item))
            return value
        if kind == "summary_entry":
            entry = AcceptedEntry(decode(record["value"]), decode(record["serialized"]))
            entry.append_plans.update(decode(record["plans"]))
            return entry
        if kind == "observation_choices":
            return AcceptedObservationChoices(
                decode(record["views"]), decode(record["partitions"]), record["generation"]
            )
        if kind in DATACLASSES:
            cls = DATACLASSES[kind]
            if set(record["fields"]) != {f.name for f in fields(cls)}:
                raise HandoffError("incomplete producer record")
            restored = {k: decode(v) for k, v in record["fields"].items()}
            if cls in (NumericalCompletion, NumericalInput, NumericalRecoveryGroup, NumericalRecoverySelection):
                # The owning numerical ingress already accepted this scope and
                # these completed values. Restoration only lends their fields;
                # no constructor, normalization or producer decision runs here.
                result = object.__new__(cls)
                for key, value in restored.items():
                    object.__setattr__(result, key, value)
            else:
                result = cls(**restored)
            if isinstance(result, SourceWorkPlan):
                require_generic_plan(result)
            extras = record.get("extras", {})
            if (
                not isinstance(extras, dict)
                or set(extras) - set(ADAPTER_EXTRAS)
                or extras
                and cls not in (PricingInput, MetadataSourceControl, ClosedAcceptedSource)
            ):
                raise HandoffError("unsupported producer adapter fields")
            for name, item in extras.items():
                if name in record["fields"]:
                    raise HandoffError("duplicate producer adapter field")
                object.__setattr__(result, name, decode(item))
            return result
        if kind == "policy_rule":
            try:
                return _rules()[record["name"]]
            except KeyError as error:
                raise HandoffError("unknown policy rule") from error
        if kind == "error":
            cls = _errors().get(record["type"])
            if cls is None:
                raise HandoffError("unsupported observed error type: " + record["type"])
            details = record["details"]
            if not isinstance(details, dict) or set(details) - set(ERROR_FIELDS):
                raise HandoffError("unknown observed error field")
            details = {key: decode(value) for key, value in details.items()}
            if cls is HTTPError:
                error = HTTPError(
                    details["url"], details["code"], details.get("msg", record["message"]), None, None
                )
                error.close()
            elif cls is URLError:
                error = URLError(details.get("reason", record["message"]))
            else:
                error = cls.__new__(cls)
                BaseException.__init__(error, record["message"])
            for key, value in details.items():
                if cls is HTTPError and key in ("url", "code", "msg", "reason", "status"):
                    continue
                setattr(error, key, value)
            error.__cause__ = decode(record["cause"])
            return error
        if kind == "defaultdict":
            return defaultdict(
                {"set": set, "list": list, "dict": dict, None: None}[record["factory"]],
                ((decode(k), decode(v)) for k, v in record["items"]),
            )
        if kind in CONTAINERS:
            cls = CONTAINERS[kind]
            values = (
                ((decode(k), decode(v)) for k, v in record["items"])
                if issubclass(cls, dict)
                else (decode(v) for v in record["items"])
            )
            return cls(values)
        raise HandoffError("unknown handoff record kind: " + str(kind))
