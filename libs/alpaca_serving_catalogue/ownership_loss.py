"""Keep unexplained loss of accepted mapped history at its owning boundary."""

from .contract import CatalogueError


class AcceptedOwnershipLost(CatalogueError):
    def __init__(self, scopes):
        self.outcome = {
            "outcome": "accepted_ownership_recovery_required",
            "missing_capability": "generation-bound ownership invalidation or accepted-owner restoration",
            "lost_scopes": scopes,
        }
        super().__init__(
            "accepted historical ownership became unsupported without a scoped invalidation handoff; "
            "owning lineage recovery required"
        )


def require_retained_owners(group, previous, inputs, offsets):
    lost = retained_owner_losses(group, previous, inputs, offsets)
    if lost:
        raise AcceptedOwnershipLost(lost)


def retained_owner_losses(group, previous, inputs, offsets, *, securities=None):
    """Recognize the known mapped-history loss using accepted metadata only.

    A surviving public ID binds the same UUID membership. Pricing ancestry binds
    old positions, but neither proves that replay was entitled to revoke their
    accepted owner. New conflicting candidates, actual owner changes and changed
    historical prices remain outside this narrow missing-capability boundary.
    """
    selected = group.summary["securities"] if securities is None else securities
    surviving = {security["public_id"] for security in selected}
    lost = []
    for prior in previous:
        completed = prior["history_validation"]
        records = {record["public_id"]: record for record in completed["prefix_records"]}
        for symbol, old in completed["inputs"].items():
            if symbol not in inputs:
                continue
            selected, position = [], 0
            for run in old["views"]["raw"]["ownership"]:
                end = position + run["rows"]
                public_id = run["owner"][0]
                if public_id in surviving and public_id in records:
                    selected.append((position + 1, end, public_id))
                position = end
            if not selected:
                continue
            for adjustment, kind, segment in group.assignments[symbol]:
                if (
                    adjustment != "raw"
                    or segment["branch"] != "uncorrelated"
                    or set(segment.get("reason_codes", ())) != {"outside_supported_name_change_history"}
                    or segment.get("candidate_securities")
                ):
                    continue
                offset = offsets[symbol]["raw"][kind]
                for start, end, public_id in selected:
                    first, last = (
                        max(start, offset + segment["row_start"]),
                        min(end, offset + segment["row_end"]),
                    )
                    if first > last or not group.preserves_pricing_prefix(symbol, old, through=segment["to"]):
                        continue
                    lost.append(
                        {
                            "source": symbol,
                            "public_id": public_id,
                            "previous_pricing_generation": old["pricing_generation"],
                            "pricing_generation": inputs[symbol]["pricing_generation"],
                            "boundary": kind,
                            "row_start": first,
                            "row_end": last,
                            "segment_from": segment["from"],
                            "segment_to": segment["to"],
                        }
                    )
    return lost
