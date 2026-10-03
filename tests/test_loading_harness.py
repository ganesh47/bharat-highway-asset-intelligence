"""Failure-gate regressions; stdlib only, no Playwright/browser launch."""

import unittest
from types import SimpleNamespace

from scripts.playwright_loading import CLOSED_TARGET, Fixture, assert_diagnostics, gate_result


ORIGIN = "http://127.0.0.1:12345"
APP = ORIGIN + "/apps/web/src/app.js"
CATALOG = ORIGIN + "/data/manifests/catalog.json"


class FakeRoute:
    def __init__(self, url):
        self.request = SimpleNamespace(url=url)
        self.aborted = False

    async def abort(self, *, error_code):
        self.aborted = error_code == "failed"


class LoadingHarnessGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_unexpected_source_assertion_is_recorded_aborted_and_cannot_pass(self):
        # Even the all-reads-fail fixture must not hide an unknown source as its
        # deliberately injected 503 failure.
        fixture = Fixture("all_reads_retry", ORIGIN)
        fixture.catalog_calls = 1
        route = FakeRoute(ORIGIN + "/data/processed/unexpected-source.parquet")
        await fixture.route(route)
        self.assertTrue(route.aborted)
        self.assertEqual(fixture.route_errors[0]["kind"], "AssertionError")
        self.assertIn("unexpected-source", fixture.route_errors[0]["message"])
        self.assertEqual(fixture.active_routes, set())
        row = {"status": "passed", "request_failures": [], "page_errors": []}
        gate_result(row, fixture)
        self.assertEqual(row["status"], "failed")
        self.assertIn("Unexpected data request", row["diagnostic_error"])

    def test_expected_fault_is_exact_scenario_origin_path_and_abort_reason_once(self):
        for name, url in [("module_reload", APP), ("catalog_timeout_retry", CATALOG)]:
            record = {"url": url, "failure": "net::ERR_ABORTED", "tearing_down": False}
            # Repeated validation does not consume or reset an allowance.
            assert_diagnostics(name, ORIGIN, [], [record], [])
            assert_diagnostics(name, ORIGIN, [], [record], [])
            with self.assertRaisesRegex(AssertionError, "Repeated controlled cancellation"):
                assert_diagnostics(name, ORIGIN, [], [record, dict(record)], [])
            for changed in [
                {**record, "url": ORIGIN + "/unrelated.js"},
                {**record, "url": "https://example.org" + url[len(ORIGIN):]},
                {**record, "failure": "net::ERR_FAILED"},
                {**record, "failure": "net::ERR_CONNECTION_RESET"},
            ]:
                with self.assertRaises(AssertionError):
                    assert_diagnostics(name, ORIGIN, [], [changed], [])
            with self.assertRaises(AssertionError):
                assert_diagnostics("zero_observations", ORIGIN, [], [record], [])

    def test_unrelated_network_failure_fails_even_during_teardown(self):
        record = {"url": ORIGIN + "/src/ontology.js", "failure": "net::ERR_NAME_NOT_RESOLVED"}
        for tearing_down in (False, True):
            with self.assertRaises(AssertionError):
                assert_diagnostics("module_reload", ORIGIN, [], [{**record, "tearing_down": tearing_down}], [])

    def test_closed_target_requires_event_time_teardown_flag(self):
        record = {"url": APP, "kind": "TargetClosedError", "message": CLOSED_TARGET,
                  "tearing_down": False}
        with self.assertRaises(AssertionError):
            assert_diagnostics("module_reload", ORIGIN, [record], [], [])
        assert_diagnostics("module_reload", ORIGIN, [{**record, "tearing_down": True}], [], [])
        fixture = Fixture("module_reload", ORIGIN)
        fixture.route_errors.append(record)
        fixture.tearing_down = True  # Must not retroactively excuse the old event.
        row = {"status": "passed", "request_failures": [], "page_errors": []}
        gate_result(row, fixture)
        self.assertEqual(row["status"], "failed")

    def test_own_assertion_never_becomes_an_expected_cancellation(self):
        for message in ("net::ERR_ABORTED", CLOSED_TARGET):
            for tearing_down in (False, True):
                error = {"url": APP, "kind": "AssertionError", "message": message,
                         "tearing_down": tearing_down}
                with self.assertRaises(AssertionError):
                    assert_diagnostics("module_reload", ORIGIN, [error], [], [])

    def test_late_error_revokes_pass_and_evidence_failure_is_not_silent(self):
        fixture = Fixture("zero_observations", ORIGIN)
        row = {"status": "passed", "request_failures": [], "page_errors": []}
        gate_result(row, fixture)
        self.assertEqual(row["status"], "passed")
        fixture.route_errors.append({"url": APP, "kind": "RuntimeError", "message": "late route failure",
                                     "tearing_down": True})
        gate_result(row, fixture)
        self.assertEqual(row["status"], "failed")
        self.assertIn("late route failure", row["diagnostic_error"])
        fresh = {"status": "passed", "request_failures": [], "page_errors": [],
                 "evidence_error": "screenshot unavailable"}
        gate_result(fresh, Fixture("zero_observations", ORIGIN))
        self.assertEqual(fresh["status"], "failed")


if __name__ == "__main__":
    unittest.main()
