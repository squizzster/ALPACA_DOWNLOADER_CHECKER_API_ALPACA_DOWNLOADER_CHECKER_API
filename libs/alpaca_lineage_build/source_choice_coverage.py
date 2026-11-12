"""Exact counterpart dates for newly allocated observations beside frozen choices.

This projects existing explicit numerical decisions; it never compares values.
Each donor supplies its own complete eight-view bundle. Sparse endpoint/count
coincidence and complementary partial sources cannot establish that bundle.
"""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp


class AcceptedBundleCoverage:
    def __init__(self, inputs, protected, *, identities=None):
        self.inputs = inputs
        self.protected = protected
        self.identities = identities
        self.instants = next(
            (
                value.observation_instant_keys
                for value in inputs.values()
                if hasattr(value, "observation_instant_keys")
            ),
            {},
        )
        for value in inputs.values():
            held = getattr(value, "observation_instant_keys", None)
            if held is not None and held is not self.instants:
                self.instants.update(held)
        self.positions = {}
        self.bundles = {}
        self.coverage = {}

    def _instant(self, stamp):
        if stamp not in self.instants:
            self.instants[stamp] = timestamp(stamp)
        return self.instants[stamp]

    def _positions(self, source, segment):
        rows = self.inputs[source].views[segment["adjustment"]]
        key = id(rows), segment["row_start"], segment["row_end"]
        if key not in self.positions:
            self.positions[key] = frozenset(
                self._instant(rows[index][0]) for index in range(segment["row_start"] - 1, segment["row_end"])
            )
        return self.positions[key]

    def covers(self, source, segment):
        if not self.inputs.keys() - {source}:
            return False  # No other materialized donor can require counterpart lookup.
        public_id = segment["security_identity"]["security_id"]
        if self.identities is not None and public_id not in self.identities:
            return False
        if public_id not in self.bundles:
            donors = {}
            for name, pieces in self.protected.items():
                if name not in self.inputs:
                    continue
                views = {}
                for piece in pieces:
                    if (
                        piece.get("observation_selection") != "selected"
                        or piece["security_identity"]["security_id"] != public_id
                    ):
                        continue
                    adjustment = piece["adjustment"]
                    views.setdefault(adjustment, set()).update(self._positions(name, piece))
                if set(views) == set(ADJUSTMENTS):
                    donors[name] = frozenset.intersection(*(frozenset(views[view]) for view in ADJUSTMENTS))
            self.bundles[public_id] = donors
        key = source, public_id
        if key not in self.coverage:
            self.coverage[key] = set().union(
                *(days for donor, days in self.bundles[public_id].items() if donor != source)
            )
        accepted = self.coverage[key]
        return bool(accepted) and self._positions(source, segment) <= accepted
