"""Semantic transport of the source owner's identity-bound completed caches.

Python identity is only an operation-local index. Named key positions travel as
references to the held producer object; the decoder indexes the restored object.
No key is guessed from equality and no missing object is rebuilt or audited.
"""

from dataclasses import dataclass, fields, is_dataclass, replace
from types import MappingProxyType

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.source_work import SourceWorkPlan

# Mutable adapters attach these completed decisions after construction. Their
# presence is meaningful and is retained independently of dataclass defaults.
ADAPTER_EXTRAS = (
    "_accepted_binding_applies",
    "accepted_report",
    "accepted_identity_memberships",
    "accepted_public_common_membership",
    "accepted_security_relations",
    "accepted_sourced_rename_membership",
    "accepted_documented_membership",
    "accepted_common_bounds",
    "accepted_common_routes",
    "accepted_uuid_lineage_candidates",
    "accepted_public_common_ownership",
    "accepted_current_source_ownership",
    "accepted_sourced_rename_ownership",
    "previous",
    "observation_instant_keys",
)


@dataclass(frozen=True, eq=False)
class IdentityKey:
    owner: object


def _references(roots):
    selected = {id(None): None}
    # Source-work identity keys name metadata/receipts/membership maps. Bulk
    # row views are carried by the value codec, never indexed here a second time.
    bulk = {
        "pricing",
        "partitions",
        "counterpart_partitions",
        "market_date_partitions",
        "source_bundles",
        "views",
        "downloads",
        "observation_instant_keys",
    }
    visited = set()
    pending = list(roots)
    while pending:
        value = pending.pop()
        if value is None or isinstance(value, (str, int, float, bool)) or id(value) in visited:
            continue
        visited.add(id(value))
        if isinstance(value, (dict, MappingProxyType)):
            selected[id(value)] = value
            pending.extend(item for key, item in value.items() if key not in bulk)
            pending.extend(value.keys())
        elif isinstance(value, (list, tuple, set, frozenset)):
            pending.extend(value)
        elif is_dataclass(value):
            pending.extend(getattr(value, field.name) for field in fields(value) if field.name not in bulk)
            pending.extend(getattr(value, name) for name in ADAPTER_EXTRAS if name in value.__dict__)
    return selected


def export_sourced(value, *, roots):
    """Replace every named process-ID slot with its actual held producer value."""
    if not isinstance(value, dict) or set(value) - {"metadata", "source_work", "pricing_prefix"}:
        raise LineageError("unsupported sourced-operation cache capability")
    source_work = value.get("source_work", {})
    if not isinstance(source_work, dict):
        raise LineageError("invalid source-work cache")
    if not any(source_work.values()):
        # Ordinary preparation has no identity-bound producer work. Its compact
        # universe selectors must not incur an identity-registry traversal.
        return value
    references = _references((value, *roots))
    identities, plans = {}, {}

    def identity(key):
        if type(key) is not int or key not in references:
            raise LineageError("source-work identity key lost its held producer object")
        if key not in identities:
            identities[key] = IdentityKey(references[key])
        return identities[key]

    def prior(key):
        return identity(key) if type(key) is int else key

    def bindings(key):
        return tuple((name, identity(token)) for name, token in key)

    def selected_group(key):
        return tuple(
            (name, generation, prior(receipt), identity(evidence)) for name, generation, receipt, evidence in key
        )

    def market(key):
        evidence, controls = key
        return bindings(evidence), tuple(
            (name, prior(receipt), generation) for name, receipt, generation in controls
        )

    def scope(key):
        policy, controls, auxiliary = key
        return (
            policy,
            tuple(
                (name, store, generation, prior(receipt), identity(evidence))
                for name, store, generation, receipt, evidence in controls
            ),
            bindings(auxiliary),
        )

    def plan_value(plan):
        if not isinstance(plan, SourceWorkPlan):
            return plan
        if id(plan) not in plans:
            projections = {
                (store, name, receipt, generation, identity(members), identity(documented), refine): body
                for (
                    store,
                    name,
                    receipt,
                    generation,
                    members,
                    documented,
                    refine,
                ), body in plan.closed_report_projections.items()
            }
            plans[id(plan)] = replace(plan, closed_report_projections=projections)
        return plans[id(plan)]

    stable = {
        "accepted_ownership_reports",
        "assignments",
        "donor_checkpoints",
        "donor_checkpoint_views",
        "observation_choices",
        "market_date_partitions",
        "source_bundles",
        "pricing",
        "partitions",
        "counterpart_partitions",
    }
    keyed = {
        "owner_applicability": lambda k: (k[0], prior(k[1]), identity(k[2])),
        "documented_plans": market,
        "market_plans": market,
        "recapitalization_plans": market,
        "metadata": bindings,
        "plans": lambda k: (bindings(k[0]), tuple((n, s, g, prior(p)) for n, s, g, p in k[1])),
        "recapitalization_graphs": lambda k: (k[0], identity(k[1])),
        "documented_memberships": selected_group,
        "documented_reports": lambda k: (k[0], k[1], prior(k[2]), identity(k[3])),
        "documented_continuities": bindings,
        "donor_grid_ids": lambda k: (k[0], k[1], identity(k[2]), k[3]),
        "completed_source_work": scope,
    }
    source_work = value.get("source_work", {})
    if not isinstance(source_work, dict) or set(source_work) - stable - keyed.keys():
        raise LineageError("unsupported source-work cache capability")
    packed = {}
    try:
        for name, cache in source_work.items():
            if not isinstance(cache, dict):
                raise LineageError("invalid source-work cache")
            if name in stable:
                packed[name] = cache
                continue
            packed[name] = {}
            for key, body in cache.items():
                if name == "completed_source_work":
                    body = {
                        **body,
                        "evidence": {identity(token): owner for token, owner in body["evidence"].items()},
                    }
                else:
                    body = plan_value(body)
                packed[name][keyed[name](key)] = body
    except (TypeError, KeyError, ValueError) as error:
        raise LineageError("incomplete semantic source-work cache key") from error
    return {**value, "source_work": packed}
