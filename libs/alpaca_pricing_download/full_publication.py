"""Operation-scoped acceptance of completed pricing publications."""

from dataclasses import dataclass
from pathlib import Path

from libs.common.debug_trace import event


@dataclass(frozen=True)
class PublicationAcceptance:
    """Completed producer output; never a fabricated retained-history comparison.

    Publication already validated the acquired bundle and comparison tail. The
    consumer checks only applicability to its compact selectors and requirement.
    This value cannot survive into a separate invocation's freshness decision.
    """

    store: Path
    symbol: str
    generation: str
    history_start: str
    asof: str
    end_exclusive: str
    first_all: tuple | None
    last_all: tuple | None
    tail_all: tuple
    acquisition_fingerprint: str | None
    correction_id: str | None

    def applies(
        self,
        store,
        entry,
        lineage_entry,
        target,
        *,
        invalidate_mapping=False,
        acquisition_fingerprint=None,
        correction_id=None,
    ):
        return (
            self.store == store
            and self.symbol == entry["symbol"]
            and self.generation == entry.get("source_sha256")
            and self.generation == (lineage_entry or {}).get("source_sha256")
            and self.asof == entry["asof"]
            and self.end_exclusive == entry["end_exclusive"]
            and target is not None
            and self.end_exclusive[:10] == target[:10]
            and self.first_all is not None
            and tuple(entry["first_all"]) == self.first_all
            and tuple(entry["last_all"]) == self.last_all
            and tuple(tuple(row) for row in entry.get("tail_all") or ()) == self.tail_all
            and (
                not invalidate_mapping
                or acquisition_fingerprint is not None
                and acquisition_fingerprint == self.acquisition_fingerprint
            )
            and (correction_id is None or correction_id == self.correction_id)
        )


@dataclass(frozen=True)
class FullPublicationAcceptance(PublicationAcceptance):
    """A complete history freshly acquired and validated by its pricing owner."""


@dataclass(frozen=True)
class ApprovedAppendAcceptance(PublicationAcceptance):
    """Accepted retained history plus the producer's validated new observations."""

    predecessor_generation: str
    comparison: object


def _publication_fields(context, source, generation, endpoints):
    rows = endpoints["all"]
    request = source["latest_request"]
    return {
        "store": context.store,
        "symbol": source["symbol"],
        "generation": generation,
        "history_start": source["history_start"],
        "asof": request["asof"],
        "end_exclusive": request["end_exclusive"],
        "first_all": tuple(rows[0]) if rows else None,
        "last_all": tuple(rows[-1]) if rows else None,
        "tail_all": tuple(tuple(row) for row in endpoints.tail_all),
        "acquisition_fingerprint": source.get("acquisition_evidence_sha256"),
        "correction_id": source.get("history_correction_id"),
    }


def accept_full_publication(context, source, generation, endpoints):
    """Carry held full-acquisition values after successful installation.

    No upstream read or calculation belongs here. Incomplete publications,
    covered history and bounded corrections cannot enter this producer path.
    """
    accepted = FullPublicationAcceptance(**_publication_fields(context, source, generation, endpoints))
    context.full_publications[accepted.symbol] = accepted
    event(
        "pricing_full_publication_accepted",
        symbol=accepted.symbol,
        source_generation=generation,
        asof=accepted.asof,
        end_exclusive=accepted.end_exclusive,
    )
    return accepted


def accept_approved_append(context, source, generation, endpoints, *, predecessor_generation, comparison):
    """Carry the consumed retained comparison through its completed append.

    The caller owns the locked generation/mapping binding and completed matched
    comparison. The published suffix has crossed its producer validation. Do
    not repeat either acceptance when their resulting generation reaches lineage.
    """
    accepted = ApprovedAppendAcceptance(
        **_publication_fields(context, source, generation, endpoints),
        predecessor_generation=predecessor_generation,
        comparison=comparison,
    )
    context.approved_appends[accepted.symbol] = accepted
    event(
        "pricing_approved_append_accepted",
        symbol=accepted.symbol,
        previous_generation=predecessor_generation,
        source_generation=generation,
        asof=accepted.asof,
        end_exclusive=accepted.end_exclusive,
    )
    return accepted
