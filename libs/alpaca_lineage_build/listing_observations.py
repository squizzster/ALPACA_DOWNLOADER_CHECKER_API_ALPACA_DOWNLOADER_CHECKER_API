"""Durable, producer-owned observations for the explicit historical listing migration."""

import hashlib
import json
import sqlite3
import threading
import zlib
from collections import Counter, defaultdict
from datetime import date
from urllib.parse import urlencode

from libs.alpaca_data_api_solver import auction_request
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
from libs.alpaca_security_lineage.listing_trade_batch import combine_listing_samples

from . import listing_auction_ranges

VERSION = 1


def fatal_acquisition_error(error):
    return getattr(error, "status", None) in (
        401,
        403,
    ) or "ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY are required" in str(error)


def path(store):
    return store / "end_of_day" / "lineage" / "historical_listing_observations.sqlite3"


def _encode(value):
    return zlib.compress(json.dumps(value, separators=(",", ":"), allow_nan=False).encode(), level=1)


def _decode(value):
    return json.loads(zlib.decompress(value))


class Observations:
    """Serialize short SQLite transactions; concurrent HTTP uses one shared provider budget."""

    def __init__(self, store, client_factory, *, readonly=False):
        self.client_factory = client_factory
        self._http = None
        self.stats = Counter()
        self.lock = threading.RLock()
        self._auction_locks = defaultdict(threading.Lock)
        self.retained_points = {}
        self._owner_observations = defaultdict(dict)
        self._scope_bindings = {}
        self._looked_up = set()
        destination = path(store)
        self.db = None
        if readonly and not destination.is_file():
            return
        mode = "ro" if readonly else "rwc"
        self.db = sqlite3.connect(
            destination.absolute().as_uri() + "?mode=" + mode, uri=True, check_same_thread=False
        )
        (version,) = self.db.execute("PRAGMA user_version").fetchone()
        if version != VERSION and (version or readonly):
            self.db.close()
            raise ValueError("incompatible historical listing observation boundary")
        if not readonly:
            self.db.execute("PRAGMA synchronous=OFF")
            with self.db:
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS observations (kind TEXT, request TEXT, value BLOB NOT NULL, PRIMARY KEY(kind,request)) WITHOUT ROWID"
                )
                self.db.execute(f"PRAGMA user_version={VERSION}")

    @property
    def requests(self):
        return self._http.request_count if self._http is not None else 0

    def close(self):
        if self.db is not None:
            self.db.close()

    def get(self, kind, key):
        if self.db is None:
            return None
        with self.lock:
            row = self.db.execute(
                "SELECT value FROM observations WHERE kind=? AND request=?", (kind, json.dumps(key))
            ).fetchone()
        return None if not row else _decode(row[0])

    def has(self, kind, key):
        if self.db is None:
            return False
        with self.lock:
            return (
                self.db.execute(
                    "SELECT 1 FROM observations WHERE kind=? AND request=?", (kind, json.dumps(key))
                ).fetchone()
                is not None
            )

    def put(self, kind, key, value):
        body = _encode(value)
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO observations VALUES(?,?,?)", (kind, json.dumps(key), body))

    def replace_screen(self, key, value):
        with self.lock, self.db:
            self.db.execute(
                "UPDATE observations SET value=? WHERE kind='screen' AND request=?",
                (_encode(value), json.dumps(key)),
            )

    def http(self):
        with self.lock:
            if self._http is None:
                self._http = self.client_factory()
            return self._http

    def seed(self, facts):
        def retain(owner, symbol, day, sample):
            key = symbol, day
            self.retained_points[key] = combine_listing_samples(self.retained_points.get(key), sample)
            records = self._owner_observations[owner, symbol]
            records[day] = combine_listing_samples(records.get(day), sample)

        for owners in facts.values():
            for owner, item in owners.items():
                for row in item.get("trade_observations", ()):
                    symbol = row["request_symbol"]
                    if (
                        row["returned_symbol"] == symbol
                        and row["feed"] == "sip"
                        and row["asof"] == "-"
                        and row["tape_code"] in ("A", "B", "C")
                    ):
                        retain(
                            owner,
                            symbol,
                            row["session"],
                            _sample(symbol, row["session"], row["trade_timestamp"], row["tape_code"]),
                        )
                for assessment in item.get("historical_listings", ()):
                    for row in assessment["observations"]:
                        symbol = assessment["symbol"]
                        retain(
                            owner,
                            symbol,
                            row["session"],
                            _sample(symbol, row["session"], row["trade_at"], row["tape"]),
                        )

    def owner_points(self, scope):
        owner, symbol, first, last, competing = scope
        if competing:
            return ()
        return tuple(
            row
            for d, row in sorted(self._owner_observations.get((owner, symbol), {}).items())
            if first <= d <= last
        )

    def assessment_key(self, version, scope):
        binding = version, scope
        if binding not in self._scope_bindings:
            held = self.owner_points(scope)
            # Changes in unrelated peer metadata or subsequent pricing sessions
            # cannot invalidate a completed literal range. An actual new witness can.
            material = (
                hashlib.sha256(json.dumps(held, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                if held
                else None
            )
            self._scope_bindings[binding] = [version, *scope, *([material] if material else [])]
        return self._scope_bindings[binding]

    def staged_assessment_keys(self, version, scopes):
        if self.db is None:
            return []
        bounds = []
        for pid, symbol, first, last, _ in scopes:
            base = [version, pid, symbol]
            bounds.append((json.dumps([*base, first])[:-1], json.dumps([*base, last])[:-1] + "\uffff"))
        with self.lock:
            rows = self.db.execute(
                """SELECT DISTINCT o.request FROM json_each(?) q CROSS JOIN observations o
                   WHERE o.kind='assessment' AND o.request BETWEEN (q.value->>0) AND (q.value->>1)""",
                (json.dumps(bounds),),
            ).fetchall()
        return [json.loads(key) for (key,) in rows]

    def warm_points(self, pairs):
        keys = [(sym, day) for sym, day in pairs if (sym, day) not in self._looked_up]
        encoded = [json.dumps(list(key)) for key in keys]
        with self.lock:
            for offset in range(0, len(keys), 4096):
                query = "SELECT o.request,o.value FROM json_each(?) q JOIN observations o ON o.kind='tape' AND o.request=q.value"
                for key, value in self.db.execute(query, (json.dumps(encoded[offset : offset + 4096]),)):
                    point = tuple(json.loads(key))
                    self.retained_points[point] = combine_listing_samples(
                        self.retained_points.get(point), _decode(value)
                    )
            self._looked_up.update(keys)

    def known_point(self, symbol, session):
        key = symbol, session
        with self.lock:
            if key not in self._looked_up:
                self.warm_points([key])
            return self.retained_points.get(key)

    def unattempted(self, labels, day, window):
        keys = {json.dumps([label, day, window]): label for label in labels}
        with self.lock:
            completed = self.db.execute(
                "SELECT o.request FROM json_each(?) q JOIN observations o ON o.kind='window_attempt' AND o.request=q.value",
                (json.dumps(list(keys)),),
            ).fetchall()
        for (key,) in completed:
            keys.pop(key)
        return list(keys.values())

    def save_window(self, labels, day, window, samples):
        completed = _encode(True)
        markers = [("window_attempt", json.dumps([s, day, window]), completed) for s in labels]
        self._save_points(markers, day, samples, "shared_trade")

    def save_sparse(self, labels, day, samples):
        marker = ("sparse_request", json.dumps([day, labels]), _encode(True))
        self._save_points([marker], day, samples, "sparse_day")

    def _save_points(self, markers, day, samples, route):
        values = [("tape", json.dumps([s, day]), _encode(point)) for s, point in samples.items()]
        with self.lock, self.db:
            self.db.executemany("INSERT OR IGNORE INTO observations VALUES(?,?,?)", [*values, *markers])
            self.retained_points.update({(s, day): row for s, row in samples.items()})
            self.stats[route + "_requests"] += 1
            self.stats[route + "_points"] += len(samples)

    def get_trade_tape_sample(self, symbol, session):
        cached = self.known_point(symbol, session)
        if cached is not None:
            return cached
        sample = self.http().get_trade_tape_sample(symbol, session)
        if sample.get("http_status") == 403:
            raise IdentityHttpError(403, "Alpaca denied access to historical SIP trades")
        self.put("tape", [symbol, session], sample)
        with self.lock:
            self.retained_points[symbol, session] = sample
            self.stats["single_trade_requests"] += 1
        return sample

    def auction_days(self, symbol, first, last, *, selected_screen=None):
        with self.lock:
            gate = self._auction_locks[symbol]
        with gate:
            return listing_auction_ranges.auction_days(self, symbol, first, last, selected_screen)

    def read_auction_route(self, symbol, first, last, keep_first, keep_last, *, completed=False):
        route = None
        token = ""
        seen = set()
        days = []
        pages = []
        while True:
            seen.add(token)
            key = [symbol, first, last, token]
            page = self.get("auction_page", key)
            new = page is None
            if new:
                if completed:
                    raise ValueError("missing completed historical auction page")
                if route is None:
                    route = auction_request(symbol, first, last)
                params = {**route["params"], **({"page_token": token} if token else {})}
                try:
                    doc = self.http().request_json(route["url"] + "?" + urlencode(params))
                except IdentityHttpError as error:
                    if error.status != 404 or token:
                        raise
                    page = {"unavailable": True}
                else:
                    page = _auction_page(doc, symbol, first, last)
                with self.lock:
                    self.stats["auction_pages"] += 1
            if page.get("unavailable"):
                if completed:
                    raise ValueError("missing completed historical auction route")
                if new:
                    self.put("auction_page", key, page)
                self.put("auction_nonroute", key[:3], True)
                return None
            bounds = page.get("bounds")
            if bounds is None:
                # Earlier stage pages predate date bounds. Assemble this selected
                # physical route once, retaining a selector for later subranges.
                dates = [r["d"] for r in page["rows"]]
                bounds = [min(dates), max(dates)] if dates else [None, None]
            pages.append([token, *bounds])
            if (keep_first, keep_last) == (first, last):
                days.extend(page["rows"])
            else:
                days.extend(r for r in page["rows"] if keep_first <= r["d"] <= keep_last)
            token = page["next"]
            if token and token in seen:
                raise ValueError("repeated historical auction page token")
            if not token:
                # Publish the exhausted physical route with its last native page.
                rows = [("auction_route", json.dumps(key[:3]), _encode({"pages": pages}))]
                if new:
                    rows.append(("auction_page", json.dumps(key), _encode(page)))
                with self.lock, self.db:
                    self.db.executemany("INSERT OR IGNORE INTO observations VALUES(?,?,?)", rows)
                return days
            if new:
                self.put("auction_page", key, page)


def _sample(symbol, session, trade_at, tape):
    return {
        "symbol": symbol,
        "returned_symbol": symbol,
        "session": session,
        "feed": "sip",
        "asof": "-",
        "http_status": 200,
        "outcome": "observed",
        "trade": {"t": trade_at, "z": tape},
    }


def _auction_page(doc, symbol, first, last):
    if not isinstance(doc, dict) or "auctions" not in doc or "next_page_token" not in doc:
        raise ValueError("Alpaca returned an incomplete auction page")
    auctions = doc["auctions"]
    if auctions is None:
        auctions = {}
    if not isinstance(auctions, dict) or set(auctions).difference({symbol}):
        raise ValueError("Alpaca returned an unexpected auction symbol")
    rows = auctions.get(symbol, [])
    if not isinstance(rows, list):
        raise ValueError("Alpaca returned invalid auction rows")
    result = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("d"), str) or not {"o", "c"} <= row.keys():
            raise ValueError("Alpaca returned a malformed auction day")
        day = row["d"]
        if date.fromisoformat(day).isoformat() != day or not first <= day <= last:
            raise ValueError("Alpaca returned an auction outside its selected date range")
        result_row = {"d": day}
        for side, condition in (("o", "O"), ("c", "6")):
            events = row[side]
            if events is not None and not isinstance(events, list):
                raise ValueError("Alpaca returned an invalid auction event list")
            selected = []
            for event in events or ():
                if (
                    not isinstance(event, dict)
                    or not isinstance(event.get("c"), str)
                    or not isinstance(event.get("x"), str)
                ):
                    raise ValueError("Alpaca returned an invalid auction condition or venue")
                if event["c"] == condition and event["x"]:
                    selected.append({k: event[k] for k in ("c", "x", "t") if k in event})
            result_row[side] = selected
        result.append(result_row)
    token = doc["next_page_token"]
    if token is not None and not isinstance(token, str):
        raise ValueError("Alpaca returned an invalid auction page token")
    dates = [row["d"] for row in result]
    return {"rows": result, "next": token, "bounds": [min(dates), max(dates)] if dates else [None, None]}
