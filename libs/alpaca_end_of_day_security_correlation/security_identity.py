from dataclasses import dataclass
from hashlib import sha256
from typing import Iterable

from .uuid_lineage import UuidLineageEdge

SECURITY_ID_SCHEME_VERSION = 1


@dataclass(frozen=True)
class SecurityIdentity:
    security_id: str
    member_alpaca_asset_ids: tuple[str, ...]
    member_tickers: tuple[str, ...]
    basis: str

    def as_report_object(self) -> dict[str, object]:
        return {
            "security_id": self.security_id,
            "security_id_scheme": {
                "kind": "sha256",
                "version": SECURITY_ID_SCHEME_VERSION,
                "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
            },
            "member_alpaca_asset_ids": list(self.member_alpaca_asset_ids),
            "member_tickers": list(self.member_tickers),
            "basis": self.basis,
        }


def _validated_member_asset_ids(alpaca_asset_ids: Iterable[str]) -> tuple[str, ...]:
    if isinstance(alpaca_asset_ids, (str, bytes)):
        raise ValueError("a security identity requires an iterable of Alpaca asset IDs")
    supplied_members = tuple(alpaca_asset_ids)
    if not supplied_members or any(
        not isinstance(asset_id, str) or not asset_id for asset_id in supplied_members
    ):
        raise ValueError("a security identity requires non-empty Alpaca asset IDs")
    return tuple(sorted(set(supplied_members)))


def _validated_tickers(tickers: Iterable[str]) -> tuple[str, ...]:
    if isinstance(tickers, (str, bytes)):
        raise ValueError("a security identity requires an iterable of tickers")
    supplied_members = tuple(tickers)
    if not supplied_members or any(not isinstance(ticker, str) or not ticker for ticker in supplied_members):
        raise ValueError("a security identity requires non-empty tickers")
    return tuple(sorted(set(supplied_members)))


def security_id_for_alpaca_asset_ids(alpaca_asset_ids: Iterable[str]) -> str:
    """Hash a canonical Alpaca-ID set into this application's security ID."""
    # Member validation and canonical ordering are part of the stable ID contract.
    members = _validated_member_asset_ids(alpaca_asset_ids)
    return sha256("\n".join(members).encode("utf-8")).hexdigest()


def _connected_identity_data(
    seed_asset_ids: Iterable[str],
    seed_tickers: Iterable[str],
    uuid_lineage_edges: list[UuidLineageEdge],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    connected = set(_validated_member_asset_ids(seed_asset_ids))
    tickers = set(_validated_tickers(seed_tickers))
    pending = list(connected)
    while pending:
        asset_id = pending.pop()
        for edge in uuid_lineage_edges:
            if edge.predecessor_asset_id == asset_id:
                neighbor = edge.successor_asset_id
                tickers.update((edge.predecessor_symbol, edge.successor_symbol))
            elif edge.successor_asset_id == asset_id:
                neighbor = edge.predecessor_asset_id
                tickers.update((edge.predecessor_symbol, edge.successor_symbol))
            else:
                continue
            if neighbor not in connected:
                connected.add(neighbor)
                pending.append(neighbor)
    return tuple(sorted(connected)), tuple(sorted(tickers))


def security_identity_for_alpaca_assets(
    alpaca_asset_ids: Iterable[str],
    uuid_lineage_edges: list[UuidLineageEdge],
    *,
    tickers: Iterable[str],
    basis: str,
) -> SecurityIdentity:
    members, member_tickers = _connected_identity_data(
        alpaca_asset_ids,
        tickers,
        uuid_lineage_edges,
    )
    return SecurityIdentity(
        security_id=security_id_for_alpaca_asset_ids(members),
        member_alpaca_asset_ids=members,
        member_tickers=member_tickers,
        basis=basis,
    )
