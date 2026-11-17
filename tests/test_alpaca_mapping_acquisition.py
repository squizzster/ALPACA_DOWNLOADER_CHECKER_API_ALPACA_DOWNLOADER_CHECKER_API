import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_pricing_download import adaptive, mapping
from libs.alpaca_pricing_download.contract import (
    BarsSymbolUnavailable,
    DownloadError,
    InitialPricingUnavailable,
    timestamp,
)
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.mapping import MappingAcquisition, MappingCoverageIncomplete
from libs.alpaca_pricing_download.responses import CompletedBars

START = timestamp("2016-01-01T00:00:00Z")
END = timestamp("2016-01-07T00:00:00Z")
ASOF = "2016-01-06"
DAYS = ("2016-01-04", "2016-01-05", "2016-01-06")
WITNESS_START = timestamp("2016-01-04T05:00:00Z")
WITNESS_END = WITNESS_START + timedelta(days=1)


def row(day, value="90"):
    return (day + "T05:00:00.000000000Z", value, value, value, value, "100", "2", value)


def witness(*, rows=None):
    return CompletedBars(
        "NOVA",
        "all",
        WITNESS_START,
        WITNESS_END,
        ASOF,
        (row(DAYS[0]),) if rows is None else rows,
    )


def acquisition(store="/tmp/synthetic-mapping-store", generation="source-before", **options):
    return MappingAcquisition(
        store,
        "NOVA",
        generation,
        START,
        END,
        ASOF,
        "selected-scope",
        witness(),
        required_sessions=DAYS,
        **options,
    )


class Client:
    def __init__(self, days=DAYS):
        self.days, self.calls = days, []

    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        self.calls.append((symbol, adjustment, start, end, asof))
        value = "90" if "dividend" in adjustment or adjustment == "all" else "100"
        rows = tuple(row(day, value) for day in self.days if start <= timestamp(row(day)[0]) < end)
        return CompletedBars(symbol, adjustment, start, end, asof or end.date().isoformat(), rows)

    def selected(self, adjustment, start, end):
        return self.fetch("NOVA", adjustment, start, end, asof=ASOF)


def persisted(value):
    return json.loads(json.dumps(value.document()))


class MappingAcquisitionTests(unittest.TestCase):
    def test_completed_witness_is_assembled_without_download_or_normalization(self):
        value, client = acquisition(), Client()
        accepted_row = value.fragments[0].rows[0]
        with patch(
            "libs.alpaca_pricing_download.responses.transform", side_effect=AssertionError("repeat transform")
        ):
            result = value.finish(client.selected)
        self.assertIs(result.views["all"][0], accepted_row)
        self.assertEqual(set(value.state.observed), {"raw", "all", "dividend", "split"})
        self.assertEqual(len(client.calls), 5)
        for _, adjustment, start, end, _ in client.calls:
            if adjustment == "all":
                self.assertFalse(start < WITNESS_END and WITNESS_START < end)

    def test_empty_witness_never_authorizes_full_acquisition(self):
        value = MappingAcquisition(
            "/tmp/synthetic-mapping-store",
            "NOVA",
            "before",
            START,
            END,
            ASOF,
            "scope",
            witness(rows=()),
            required_sessions=DAYS,
        )
        self.assertFalse(value.available)
        with self.assertRaises(DownloadError):
            value.finish(lambda *_: self.fail("negative witness fetched full history"))

    def test_selected_contribution_scope_is_required_before_acquisition(self):
        with self.assertRaisesRegex(DownloadError, "contribution dates"):
            MappingAcquisition(
                "/tmp/synthetic-mapping-store", "NOVA", "before", START, END, ASOF, "scope", witness()
            )

    def test_interruption_after_witness_restores_without_reaccepting_rows_or_policy(self):
        document = persisted(acquisition())
        with (
            patch.object(
                adaptive, "decide_adaptive_adjustment_downloads", side_effect=AssertionError("policy replay")
            ),
            patch.object(adaptive, "digest", side_effect=AssertionError("hash replay")),
            patch(
                "libs.alpaca_pricing_download.responses.transform",
                side_effect=AssertionError("transform replay"),
            ),
        ):
            restored = MappingAcquisition.from_document(document)
        self.assertEqual(restored.fragments[0].rows, witness().rows)
        client = Client()
        restored.finish(client.selected)
        self.assertFalse(
            any(
                name == "all" and start == WITNESS_START and end == WITNESS_END
                for _, name, start, end, _ in client.calls
            )
        )

    def test_completed_fragment_and_raw_stage_survive_a_later_fragment_failure(self):
        value, client, saved = acquisition(), Client(), []

        def fetch(name, start, end):
            if name == "all" and start == WITNESS_END:
                raise RuntimeError("peer interruption")
            return client.selected(name, start, end)

        with self.assertRaisesRegex(RuntimeError, "peer interruption"):
            value.finish(fetch, on_progress=lambda current: saved.append(persisted(current)))
        self.assertEqual(len(value.fragments), 2)
        restored = MappingAcquisition.from_document(saved[-1])
        retry = Client()
        restored.finish(retry.selected)
        self.assertFalse(
            any(name == "raw" or name == "all" and start == START for _, name, start, _, _ in retry.calls)
        )
        self.assertEqual(len(retry.calls), 3)

    def test_completed_all_and_policy_decisions_survive_later_view_failure(self):
        value, client, saved = acquisition(), Client(), []

        def fetch(name, start, end):
            if name == "dividend":
                raise RuntimeError("view interruption")
            return client.selected(name, start, end)

        with self.assertRaises(RuntimeError):
            value.finish(fetch, on_progress=lambda current: saved.append(persisted(current)))
        restored = MappingAcquisition.from_document(saved[-1])
        retry = Client()
        restored.finish(retry.selected)
        self.assertEqual([name for _, name, *_ in retry.calls], ["dividend", "split"])

    def test_complete_restore_does_not_reparse_row_timestamps_or_recalculate_projections(self):
        value, client = acquisition(), Client()
        value.finish(client.selected)
        value.confirm(client.selected)
        document = persisted(value)
        original_timestamp = mapping.timestamp

        def metadata_timestamp(value):
            if value.endswith("Z"):
                self.fail("completed row timestamp reparsed")
            return original_timestamp(value)

        with (
            patch.object(mapping, "timestamp", side_effect=metadata_timestamp),
            patch.object(adaptive, "digest", side_effect=AssertionError("hash replay")),
            patch.object(
                adaptive, "decide_adaptive_adjustment_downloads", side_effect=AssertionError("policy replay")
            ),
            patch.object(adaptive, "endpoints_of", side_effect=AssertionError("endpoint replay")),
            patch.object(adaptive, "session_extent", side_effect=AssertionError("extent replay")),
        ):
            restored = MappingAcquisition.from_document(document)
            complete = restored.finish(lambda *_: self.fail("completed acquisition requested again"))
            self.assertTrue(restored.confirm(lambda *_: self.fail("completed confirmation requested again")))
            self.assertEqual(complete.extent, value.state.completed.extent)
            self.assertEqual(complete.proof, value.state.completed.proof)
            self.assertIs(complete.views["spin-off"], complete.views["raw"])
            self.assertIs(complete.views["split,dividend"], complete.views["all"])

    def test_selected_missing_contributions_complete_negative_without_publication_or_replay(self):
        value, client, saved = acquisition(), Client((DAYS[0], DAYS[2])), []
        with self.assertRaises(MappingCoverageIncomplete) as caught:
            value.finish(client.selected, on_progress=lambda current: saved.append(persisted(current)))
        self.assertEqual(caught.exception.missing_sessions, (DAYS[1],))
        self.assertEqual([name for _, name, *_ in client.calls], ["raw"])
        restored = MappingAcquisition.from_document(saved[-1])
        with patch.object(mapping, "timestamp", side_effect=AssertionError("coverage replay")):
            with self.assertRaises(MappingCoverageIncomplete):
                restored.finish(lambda *_: self.fail("completed insufficient coverage requested again"))

    def test_missing_all_contribution_stops_before_any_later_adjustment(self):
        value, raw, adjusted = acquisition(), Client(), Client((DAYS[0], DAYS[2]))

        def fetch(name, start, end):
            if name not in {"raw", "all"}:
                self.fail("an adjustment cannot rescue already incomplete required coverage")
            return (raw if name == "raw" else adjusted).selected(name, start, end)

        with self.assertRaises(MappingCoverageIncomplete) as caught:
            value.finish(fetch)
        self.assertEqual(caught.exception.missing_sessions, (DAYS[1],))
        self.assertEqual([name for _, name, *_ in raw.calls], ["raw"])
        self.assertEqual([name for _, name, *_ in adjusted.calls], ["all", "all"])

    def test_bound_witness_and_completed_peer_confirmations_survive_interruption(self):
        value, client, saved = acquisition(), Client(), []
        value.finish(client.selected)

        def check(name, start, end):
            if name == "all":
                self.fail("accepted witness requested again")
            if name == "dividend":
                raise RuntimeError("confirmation interrupted")
            return client.selected(name, start, end)

        with self.assertRaises(RuntimeError):
            value.confirm(check, on_progress=lambda current: saved.append(persisted(current)))
        self.assertEqual(set(value.confirmations), {"raw", "all"})
        self.assertIs(value.fragments[0], value.confirmations["all"][0])
        restored = MappingAcquisition.from_document(saved[-1])
        retry = Client()
        self.assertTrue(restored.confirm(retry.selected))
        self.assertEqual([name for _, name, *_ in retry.calls], ["dividend", "split"])
        restored.confirm(lambda *_: self.fail("confirmation repeated"))

    def test_different_all_endpoint_still_requires_its_own_observation(self):
        client = Client()
        start = WITNESS_END
        end = start + timedelta(days=1)
        value = MappingAcquisition(
            "/tmp/synthetic-mapping-store",
            "NOVA",
            "before",
            START,
            END,
            ASOF,
            "scope",
            client.selected("all", start, end),
            required_sessions=DAYS,
        )
        value.finish(client.selected)

        def different(name, lower, upper):
            if name == "all":
                self.assertEqual((WITNESS_START, WITNESS_END), (lower, upper))
                return CompletedBars("NOVA", name, lower, upper, ASOF, (row(DAYS[0], "999"),))
            return client.selected(name, lower, upper)

        self.assertFalse(value.confirm(different))

    def test_confirmation_contradiction_is_not_erased_by_retry(self):
        value, client, saved = acquisition(), Client(), []
        value.finish(client.selected)

        def different(name, start, end):
            return CompletedBars("NOVA", name, start, end, ASOF, (row(DAYS[0], "999"),))

        self.assertFalse(value.confirm(different, on_progress=lambda current: saved.append(persisted(current))))
        restored = MappingAcquisition.from_document(saved[-1])
        self.assertFalse(restored.confirm(lambda *_: self.fail("contradiction was rechecked")))

    def test_actual_binding_change_rejects_before_any_request(self):
        value = acquisition()
        for changed in (
            ("/tmp/another-store", "NOVA", "source-before", START, END, ASOF),
            (value.store, "NOVA", "source-after", START, END, ASOF),
            (value.store, "NOVA", "source-before", START, END + timedelta(days=1), ASOF),
            (value.store, "NOVA", "source-before", START, END, "2016-01-05"),
        ):
            with self.assertRaisesRegex(DownloadError, "binding changed"):
                value.require_binding(*changed)

    def test_missing_capability_fails_closed_without_history_reconstruction(self):
        document = persisted(acquisition())
        del document["remaining_all_ranges"]
        with self.assertRaises(DownloadError):
            MappingAcquisition.from_document(document)
        document = persisted(acquisition())
        document["schema_version"] = 999
        with self.assertRaises(DownloadError):
            MappingAcquisition.from_document(document)
        document["schema_version"] = True
        with self.assertRaises(DownloadError):
            MappingAcquisition.from_document(document)


class MappedDownloadTests(unittest.TestCase):
    def setUp(self):
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.store = Path(temporary) / "store"
        old = Client(DAYS[1:])
        self.before = download_pricing(self.store, "NOVA", "2016-01-07", client=old)
        self.generation = self.before["source_sha256"]

    def test_real_download_consumes_mapping_without_reading_old_prices_or_validator(self):
        value = acquisition(self.store, self.generation)
        client = Client()
        with (
            patch(
                "libs.alpaca_pricing_download.download.read_validator",
                side_effect=AssertionError("old validator reread"),
            ),
            patch(
                "libs.alpaca_pricing_download.download.read_boundary",
                side_effect=AssertionError("old prices reread"),
            ),
        ):
            output = download_pricing(
                self.store,
                "NOVA",
                "2016-01-07",
                client=client,
                refresh_all=True,
                mapping_asof=ASOF,
                mapping_acquisition=value,
                selected_source_sha256=self.generation,
            )
        self.assertEqual(output["request"]["asof"], ASOF)
        self.assertEqual([row[0][:10] for row in read_pricing_views(self.store, "NOVA")["all"]], list(DAYS))
        self.assertNotIn(("NOVA", "all", WITNESS_START, WITNESS_END, ASOF), client.calls)
        self.assertEqual(len(client.calls), 8)  # Five acquisition actions and three other endpoints.

    def test_failure_before_publication_restores_completed_acquisition_and_confirmation(self):
        value, client, saved = acquisition(self.store, self.generation), Client(), []
        with patch(
            "libs.alpaca_pricing_download.download.publish_acquired", side_effect=RuntimeError("before journal")
        ):
            with self.assertRaisesRegex(RuntimeError, "before journal"):
                download_pricing(
                    self.store,
                    "NOVA",
                    "2016-01-07",
                    client=client,
                    refresh_all=True,
                    mapping_asof=ASOF,
                    mapping_acquisition=value,
                    on_mapping_progress=lambda current: saved.append(persisted(current)),
                    selected_source_sha256=self.generation,
                )
        restored = MappingAcquisition.from_document(saved[-1])
        retry = Client()
        with (
            patch(
                "libs.alpaca_pricing_download.download.read_validator",
                side_effect=AssertionError("old validator reread"),
            ),
            patch(
                "libs.alpaca_pricing_download.download.read_boundary",
                side_effect=AssertionError("old prices reread"),
            ),
            patch(
                "libs.alpaca_pricing_download.download.month_of",
                side_effect=AssertionError("month validation/assembly replay"),
            ),
            patch(
                "libs.alpaca_pricing_download.download.updated_boundaries",
                side_effect=AssertionError("partition assembly replay"),
            ),
        ):
            download_pricing(
                self.store,
                "NOVA",
                "2016-01-07",
                client=retry,
                refresh_all=True,
                mapping_asof=ASOF,
                mapping_acquisition=restored,
                selected_source_sha256=self.generation,
            )
        self.assertEqual(retry.calls, [])

    def test_omitted_required_dates_cannot_reach_publication(self):
        value = acquisition(self.store, self.generation)
        with patch(
            "libs.alpaca_pricing_download.download.publish_acquired",
            side_effect=AssertionError("incomplete history published"),
        ):
            with self.assertRaises(MappingCoverageIncomplete):
                download_pricing(
                    self.store,
                    "NOVA",
                    "2016-01-07",
                    client=Client((DAYS[0], DAYS[2])),
                    refresh_all=True,
                    mapping_asof=ASOF,
                    mapping_acquisition=value,
                    selected_source_sha256=self.generation,
                )

    def test_changed_selected_generation_rejects_before_acquisition(self):
        value, client = acquisition(self.store, "stale-generation"), Client()
        with self.assertRaisesRegex(DownloadError, "source changed"):
            download_pricing(
                self.store,
                "NOVA",
                "2016-01-07",
                client=client,
                refresh_all=True,
                mapping_asof=ASOF,
                mapping_acquisition=value,
                selected_source_sha256="stale-generation",
            )
        self.assertEqual(client.calls, [])

    def test_positive_witness_cannot_become_initial_symbol_absence(self):
        store = self.store.parent / "new-mapped-store"
        value, client = acquisition(store, None), Client()

        def rejected(*args, **kwargs):
            raise BarsSymbolUnavailable("NOVA", 400)

        client.fetch = rejected
        with self.assertRaises(DownloadError) as caught:
            download_pricing(
                store,
                "NOVA",
                "2016-01-07",
                client=client,
                refresh_all=True,
                mapping_asof=ASOF,
                mapping_acquisition=value,
                selected_source_sha256=None,
            )
        self.assertNotIsInstance(caught.exception, InitialPricingUnavailable)
        self.assertIsInstance(caught.exception.__cause__, BarsSymbolUnavailable)
