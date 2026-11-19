"""Immutable, date-bounded eight-view pricing outputs and resumable provider requests."""

import json
import zlib
from contextlib import closing
from datetime import date

from libs.common.sqlite_connections import connect_sqlite

from .contract import ADJUSTMENTS, DownloadError
from .dated_rename import provider_rows
from .storage import regular_path


def database(store, *, published=False):
    return regular_path(store, "end_of_day/pricing/dated_intervals.sqlite3", published=published)


def _pack(value):
    return zlib.compress(json.dumps(value, separators=(",", ":"), allow_nan=False).encode())


def _unpack(value):
    return json.loads(zlib.decompress(value))


def assemble(store, selected, adjustments, start, end, suffix):
    """Read the selected fragment and only assemble the requested dated suffix."""
    from datetime import timedelta

    cut = date.fromisoformat(selected["cut"])
    if start is not None and start >= cut:
        return suffix(start, end)
    opening = date.fromisoformat(selected["first"])
    if end is not None and end < opening:
        return suffix(start, end)
    preceding = suffix(start, opening - timedelta(days=1)) if start is None or start < opening else {}
    later = suffix(cut, end) if end is None or end >= cut else {}
    output = {}
    first = max(selected["first"], start.isoformat()) if start else selected["first"]
    last = end.isoformat() if end else selected["cut"]
    with closing(connect_sqlite(database(store, published=True), readonly=True)) as conn:
        for mode in adjustments:
            row = conn.execute(
                "SELECT payload FROM dated_price_views WHERE token=? AND view=?", (selected["token"], mode)
            ).fetchone()
            if row is None:
                raise DownloadError("missing selected dated price publication")
            bars = list(preceding.get(mode, {}).get("bars", ()))
            bars.extend(bar for day, bar in _unpack(row[0]).items() if first <= day <= last)
            bars.extend(later.get(mode, {}).get("bars", ()))
            output[mode] = {"bars": bars, "bar_count": len(bars)}
    return output


ACQUISITION_VERSION = 1


class _Restored(dict):
    """Decode a journal payload only when the resumed producer requests it."""

    def __init__(self, steps, read, prefix):
        super().__init__(dict.fromkeys(steps))
        self.read = read
        self.prefix = prefix

    def get(self, step, default=None):
        return self[step] if step in self else default

    def __getitem__(self, step):
        value = super().__getitem__(step)
        if value is None:
            value = self[step] = self.read(self.prefix + step)
            if value is None:
                raise DownloadError(f"missing selected dated acquisition stage {self.prefix}{step}")
        return value


def acquire_selected(store, binding, load_inputs, client, *, completed_only=False):
    """Stage input assembly, provider observations and completed output before handing off."""
    from uuid import uuid4

    key = json.dumps({"acquisition_version": ACQUISITION_VERSION, "selection": binding}, sort_keys=True)
    path = database(store)
    # SQLite owns these sidecars on its fixed producer-controlled path.
    for suffix in ("-journal", "-wal", "-shm"):
        regular_path(path.parent, path.name + suffix)
    if completed_only:
        if not path.exists():
            return None
        with closing(connect_sqlite(path, readonly=True)) as conn:
            row = conn.execute("SELECT descriptor FROM dated_price_completed WHERE binding=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None
    with closing(connect_sqlite(path, create=True)) as conn:
        with conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dated_price_views "
                "(token TEXT, view TEXT, payload BLOB NOT NULL, PRIMARY KEY(token,view))"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dated_price_stage "
                "(binding TEXT, step TEXT, payload BLOB NOT NULL, PRIMARY KEY(binding,step))"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dated_price_completed "
                "(binding TEXT PRIMARY KEY, descriptor TEXT NOT NULL)"
            )
        held = conn.execute("SELECT descriptor FROM dated_price_completed WHERE binding=?", (key,)).fetchone()
        if held:
            return json.loads(held[0])

        def get(step):
            row = conn.execute(
                "SELECT payload FROM dated_price_stage WHERE binding=? AND step=?", (key, step)
            ).fetchone()
            return _unpack(row[0]) if row else None

        def put(step, value):
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO dated_price_stage VALUES (?,?,?)", (key, step, _pack(value))
                )

        if binding.get("repair_kind") in (
            "split_timing",
            "split_carry",
            "duplicate_split",
            "conversion_timing",
            "retired_unit",
        ):
            bundles, result = _assess_split_timing(binding, load_inputs, get, put)
        else:
            bundles, result = _acquire_rename(conn, key, binding, load_inputs, client, get, put)
        result.update(
            token=uuid4().hex, public_id=binding["public_id"], source_decision=binding["decision_generation"]
        )
        with conn:
            conn.executemany(
                "INSERT INTO dated_price_views VALUES(?,?,?)",
                [(result["token"], mode, _pack(bars)) for mode, bars in bundles.items()],
            )
            conn.execute("INSERT INTO dated_price_completed VALUES(?,?)", (key, json.dumps(result)))
            conn.execute("DELETE FROM dated_price_stage WHERE binding=?", (key,))
        return result


def _acquire_rename(conn, key, binding, load_inputs, client, get, put):
    from .dated_rename import UnsupportedInterval, acquire

    def observe(*scope):
        request = json.dumps(scope)
        rows = get(request)
        if rows is None:
            rows = provider_rows(client, *scope)
            put(request, rows)
        return rows

    steps = [
        row[0][6:]
        for row in conn.execute(
            "SELECT step FROM dated_price_stage WHERE binding=? AND step LIKE 'proof:%'", (key,)
        )
    ]
    completed = _Restored(steps, get, "proof:")
    if (
        binding.get("repair_kind") == "carried_rename"
        and completed.get("declined") == "selected transition has no comparable split-basis discontinuity"
    ):
        # Earlier producers only considered the end of the carry. Reuse their
        # selected inputs when considering the previously unevaluated endpoint.
        del completed["declined"]
    if binding.get("repair_kind") in ("rename_split_basis", "former_label_basis"):
        if binding["repair_kind"] == "rename_split_basis":
            from .rename_split_basis import assess
        else:
            from .former_label_basis import assess

        inputs = {}
        if "result" not in completed and "declined" not in completed:
            keys = [
                row[0][6:]
                for row in conn.execute(
                    "SELECT step FROM dated_price_stage WHERE binding=? AND step LIKE 'input:%'", (key,)
                )
            ]
            if keys:
                inputs = _Restored(keys, get, "input:")
            else:
                try:
                    inputs = load_inputs()
                except UnsupportedInterval as error:
                    put("proof:declined", str(error))
                    raise
                with conn:
                    conn.executemany(
                        "INSERT INTO dated_price_stage VALUES(?,?,?)",
                        ((key, "input:" + name, _pack(value)) for name, value in inputs.items()),
                    )
        return assess(
            inputs,
            binding["cut"],
            observe,
            completed=completed,
            checkpoint=lambda step, value: put("proof:" + step, value),
        )
    views, ranges = None, None
    if "result" not in completed and "declined" not in completed:
        if (
            not steps
            and conn.execute(
                "SELECT 1 FROM dated_price_stage WHERE binding=? AND step='input:ranges'", (key,)
            ).fetchone()
            is None
        ):
            try:
                inputs = load_inputs()
            except UnsupportedInterval as error:
                put("proof:declined", str(error))
                raise
            views, ranges = inputs["views"], inputs["ranges"]
            with conn:
                conn.executemany(
                    "INSERT INTO dated_price_stage VALUES(?,?,?)",
                    ((key, "input:" + mode, _pack(views[mode])) for mode in ADJUSTMENTS),
                )
                conn.execute("INSERT INTO dated_price_stage VALUES(?,?,?)", (key, "input:ranges", _pack(ranges)))
        else:
            views = _Restored(ADJUSTMENTS, get, "input:")
            if "view:raw" not in completed:
                ranges = get("input:ranges")
    return acquire(
        views,
        ranges,
        binding["cut"],
        observe,
        carried=binding.get("repair_kind") == "carried_rename",
        same_label=binding.get("repair_kind") == "same_label_basis",
        completed=completed,
        checkpoint=lambda step, value: put("proof:" + step, value),
    )


def _assess_split_timing(binding, load_inputs, get, put):
    from .dated_rename import UnsupportedInterval

    completed = get("proof:result")
    if completed is not None:
        return completed
    declined = get("proof:declined")
    if declined is not None:
        raise UnsupportedInterval(declined)
    inputs = get("split_inputs")
    try:
        if inputs is None:
            inputs = load_inputs()
            put("split_inputs", inputs)
        if binding["repair_kind"] == "retired_unit":
            from .retired_unit import assess

            result = assess(inputs)
        elif binding["repair_kind"] == "conversion_timing":
            from .conversion_timing import assess

            result = assess(inputs)
        elif binding["repair_kind"] == "duplicate_split":
            from .duplicate_split import assess

            result = assess(inputs)
        elif binding["repair_kind"] == "split_carry":
            from .split_carry import assess

            result = assess(
                inputs["views"], inputs["actions"], inputs["endpoints"], binding["cut"], *binding["carry_window"]
            )
        else:
            from .shifted_split import assess

            result = assess(inputs["views"], inputs["actions"], binding["cut"])
        put("proof:result", result)
        return result
    except UnsupportedInterval as error:
        put("proof:declined", str(error))
        raise
