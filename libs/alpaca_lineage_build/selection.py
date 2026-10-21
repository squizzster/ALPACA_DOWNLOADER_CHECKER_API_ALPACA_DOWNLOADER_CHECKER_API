"""In-memory lookup of affected ticker links, built once per lineage invocation."""

from collections import defaultdict

from .contract import MAX_CLOSURE, LineageError


class DependencyGraph:
    def __init__(self, entries):
        self.dependencies = {}
        self.assets = {}
        self.dependants = defaultdict(set)
        self.members = defaultdict(set)
        self.update(entries)

    def update(self, entries):
        for symbol, entry in entries.items():
            if "sha256" in entry and "group_members" not in entry:
                raise LineageError("lineage group membership incomplete; run alpaca-eod lineage migrate-index")
            for label in self.dependencies.get(symbol, ()):
                self.dependants[label].discard(symbol)
            for asset in self.assets.get(symbol, ()):
                self.members[asset].discard(symbol)
            # Current evidence may drop a link before its complete publication
            # has been retired. Select the previous group from the index until
            # the producer replaces every contribution in one journal.
            self.dependencies[symbol] = set(entry["dependencies"]) | set(entry.get("group_members", ()))
            self.assets[symbol] = set(entry["asset_ids"])
            for label in self.dependencies[symbol]:
                self.dependants[label].add(symbol)
            for asset in self.assets[symbol]:
                self.members[asset].add(symbol)

    def closure(self, seeds, asset_ids=()):
        pending = set(seeds)
        for asset in asset_ids:
            pending.update(self.members.get(asset, ()))
        result = set()
        while pending:
            symbol = pending.pop()
            if symbol in result:
                continue
            result.add(symbol)
            if len(result) > MAX_CLOSURE:
                raise LineageError(f"lineage closure exceeds {MAX_CLOSURE} labels; no notices consumed")
            pending.update(self.dependencies.get(symbol, ()))
            pending.update(self.dependants.get(symbol, ()))
            for asset in self.assets.get(symbol, ()):
                pending.update(self.members.get(asset, ()))
            pending.difference_update(result)
        return result
