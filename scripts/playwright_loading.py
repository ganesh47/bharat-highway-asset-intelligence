#!/usr/bin/env python3
"""Controlled loading-state regressions for the real dashboard React UI.

Run separately from the real-data/deployed smoke test. These scenarios use an
ephemeral local HTTP server, fixture catalogs/buffers, and a fake DuckDB module.
The application, stylesheet, React CDN imports and actual React state are real.
No WASM, real Parquet, pipeline, provider settings, or published data is changed.
Fresh browser contexts isolate every scenario. Timers of exactly 15/30/60 seconds
are accelerated only inside these contexts; results are not performance metrics.

Example: python scripts/playwright_loading.py --out buildcheck/loading
Optional: --executable-path /path/to/existing/chromium --scenario catalog_retry
Requires the repository's existing Python Playwright dependency and Chromium.
This script never installs dependencies or browsers.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import functools
import hashlib
import json
import re
import threading
import traceback
from collections import Counter
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


GROWTH = "nhai_constructed_length_series_official"
FINANCE = "data_gov_in_nhai_project_finance_api"
MODEL = "highway_project_risk_and_access_panel"
QUARANTINED = "loading_fixture_quarantined"
WORKER = "self.onmessage = () => {};"

# Replace only the query engine, keeping the application's loader/React code.
# Buffers are JSON fixtures bound to the exact SHA in their catalog. Results are
# predetermined query observations, not an implementation of SQL or Parquet.
FAKE_DUCKDB = r"""
export class ConsoleLogger {}
export class AsyncDuckDBConnection {}
export async function getPlatformFeatures() {
  if (window.__fixtureEngineMode === 'features_timeout') return new Promise(() => {});
  return { wasmSIMD: false, wasmExceptions: false };
}
export class AsyncDuckDB {
  constructor(logger, worker) { this.files = new Map(); this.worker = worker; }
  async instantiate() {
    if (window.__fixtureEngineMode === 'instantiate_timeout') return new Promise(() => {});
  }
  async registerFileBuffer(alias, bytes) {
    const data = JSON.parse(new TextDecoder().decode(bytes));
    this.files.set(alias, data);
    window.__loadingFixtureMetrics.registrations.push({ alias, sourceId: data.source_id, version: data.version });
  }
  async connect() {
    if (window.__fixtureEngineMode === 'connect_timeout') return new Promise(() => {});
    const files = this.files;
    return { query: async sql => {
      const match = sql.match(/read_parquet\('([^']+)'\)/);
      const data = match && files.get(match[1]);
      if (!data) throw new Error('Fixture query has no registered source');
      const event = { sourceId: data.source_id, alias: match[1], sql, version: data.version, ok: false };
      window.__loadingFixtureMetrics.queries.push(event);
      if (sql.replace(/\s+/g, '').toUpperCase().includes('SELECTCOUNT(*)::BIGINTASROW_COUNT')) {
        throw new Error('Unexpected runtime recount in startup');
      }
      if (data.fail_query_contains && sql.includes(data.fail_query_contains)) {
        throw new Error('Controlled fixture query failure');
      }
      event.ok = true;
      event.rows = data.rows.length;
      return { toArrayOfObjects: () => data.rows };
    } };
  }
}
"""

INIT_SCRIPT = r"""
(() => {
  const originalTimeout = window.setTimeout.bind(window);
  const durations = { 15000: 500, 30000: 1400, 60000: 2200 };
  window.setTimeout = (callback, delay, ...args) =>
    originalTimeout(callback, durations[Number(delay)] ?? delay, ...args);
  window.__fixtureEngineMode = ENGINE_MODE;
  window.__loadingFixtureMetrics = { registrations: [], queries: [], workerTerminations: 0 };
  const OriginalWorker = window.Worker;
  window.Worker = class extends OriginalWorker {
    terminate() {
      window.__loadingFixtureMetrics.workerTerminations += 1;
      return super.terminate();
    }
  };
})();
"""


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def buffer(source_id: str, version: int = 1, *, fail_query: str = "", zero: bool = False) -> bytes:
    if source_id == GROWTH:
        rows = [] if zero else [{
            "period": "2024-25", "km_constructed": 100 * version,
            "series_status": "final", "series_scope": "NHAI-only",
            "source_as_of_date": "2025-03-31",
        }]
    elif source_id == FINANCE:
        rows = [{"year-wise": "2024-25", "allocation/target_-_total": 100,
                 "expenditure/release_of_funds/actuals_-_total": 90}]
    else:
        rows = []
    return json_bytes({"source_id": source_id, "version": version, "rows": rows,
                       "fail_query_contains": fail_query})


def entry(source_id: str, body: bytes, *, count: int = 1) -> dict[str, Any]:
    path = f"data/processed/{source_id}.parquet"
    model = source_id == MODEL
    skipped = source_id == QUARANTINED
    return {
        "source_id": source_id,
        "metric_category": "model_output" if model else "official_measured",
        "status": "metadata_only" if skipped else "generated" if model else "ok",
        "analytical_ready": not model and not skipped,
        "disclosure_ready": False,
        "evidence_status": "metadata_only" if skipped else "validated",
        "extraction_status": "metadata_only" if skipped else "validated",
        "output_table_path": path,
        "source_as_of_date": "2025-03-31",
        "overall_confidence_badge": "High",
        "overall_confidence_reason": ["Controlled regression fixture; not research evidence"],
        "source": {"title": f"Controlled fixture: {source_id}", "publisher": "Loading regression fixture",
                   "url": "https://example.org/loading-fixture", "official_flag": not model,
                   "retrieved_at": "2026-10-03T00:00:00Z", "license_terms": "Controlled test fixture"},
        "citations": {"permanent_identifier": source_id, "anchor": "Controlled test fixture"},
        "manifest": {"row_count": count, "columns": ["period", "km_constructed"],
                     "output_files": [{"path": path, "format": "parquet",
                                       "sha256": hashlib.sha256(body).hexdigest()}]},
    }


CONTROLLED_ABORT_PATHS = {
    "module_reload": "/apps/web/src/app.js",
    "catalog_timeout_retry": "/data/manifests/catalog.json",
}
CLOSED_TARGET = "Target page, context or browser has been closed"


def controlled_abort(name: str, origin: str, record: dict[str, Any]) -> bool:
    """Only the exact local request cancellation deliberately exercised here."""
    path = CONTROLLED_ABORT_PATHS.get(name)
    return (
        bool(origin) and bool(path)
        and record.get("kind") != "AssertionError"
        and not record.get("tearing_down", False)
        and record.get("url") == origin + path
        and record.get("failure", record.get("message")) == "net::ERR_ABORTED"
    )


def teardown_cancellation(record: dict[str, Any]) -> bool:
    if not record.get("tearing_down", False) or record.get("kind") == "AssertionError":
        return False
    if "failure" in record:
        return record["failure"] == "net::ERR_ABORTED"
    return (
        record.get("kind") in {"Error", "TargetClosedError"}
        and record.get("message", "").split(": ", 1)[-1] == CLOSED_TARGET
    )


def assert_diagnostics(
    name: str, origin: str, route_errors: list[dict[str, Any]],
    request_failures: list[dict[str, Any]], page_errors: list[str],
) -> None:
    """Pure/idempotent gate: inspect event-time records, never mutate allowances."""
    problems = [f"Uncaught UI error: {message}" for message in page_errors]
    controlled = []
    for label, records in (("Route error", route_errors), ("Request failure", request_failures)):
        for record in records:
            if controlled_abort(name, origin, record):
                controlled.append(record)
            elif not teardown_cancellation(record):
                problems.append(f"{label}: {record}")
    if len(controlled) > 1:
        problems.append(f"Repeated controlled cancellation: {len(controlled)} events")
    if problems:
        raise AssertionError("Unexpected harness diagnostics: " + " | ".join(problems))


def gate_result(row: dict[str, Any], fixture: "Fixture") -> None:
    try:
        assert_diagnostics(fixture.name, fixture.origin, fixture.route_errors,
                           row["request_failures"], row["page_errors"])
        if row.get("evidence_error"):
            raise AssertionError(f"Validation evidence failed: {row['evidence_error']}")
    except AssertionError as exc:
        row["status"] = "failed"
        row.setdefault("error", f"AssertionError: {exc}")
        row["diagnostic_error"] = str(exc)


class Fixture:
    def __init__(self, name: str, origin: str = "") -> None:
        self.name = name
        self.origin = origin
        self.tearing_down = False
        self.active_routes: set[asyncio.Task[Any]] = set()
        self.catalog_calls = 0
        self.app_calls = 0
        self.data_calls: Counter[str] = Counter()
        self.request_log: list[dict[str, Any]] = []
        self.route_errors: list[dict[str, Any]] = []
        self.app_seen = asyncio.Event()
        self.app_gate = asyncio.Event()
        self.catalog_seen = asyncio.Event()
        self.catalog_gate = asyncio.Event()
        self.app_gate.set()
        self.catalog_gate.set()
        if name == "static_shell":
            self.app_gate.clear()
        if name in {"slow_mobile", "catalog_timeout_retry"}:
            self.catalog_gate.clear()

    def snapshot(self) -> tuple[dict[str, Any], dict[str, bytes]]:
        version = 2 if self.name == "checksum_retry_cache" and self.catalog_calls > 1 else 1
        growth = buffer(GROWTH, version, zero=self.name == "zero_observations")
        finance = buffer(FINANCE, fail_query="SELECT" if self.name == "partial_query_failure" else "")
        model = buffer(MODEL, fail_query="observation_year")
        bodies = {GROWTH: growth, FINANCE: finance, MODEL: model,
                  QUARANTINED: buffer(QUARANTINED)}
        ids = [GROWTH]
        if self.name.startswith("partial_") or self.name == "checksum_retry_cache":
            ids += [FINANCE, QUARANTINED]
        if self.name == "mixed_model_queries":
            ids = [MODEL]
        entries = [entry(source_id, bodies[source_id]) for source_id in ids]
        if self.name == "empty_catalog":
            entries = []
        elif self.name == "invalid_count":
            entries[0]["manifest"]["row_count"] = "1"
        elif self.name == "missing_count":
            del entries[0]["manifest"]["row_count"]
        elif self.name == "negative_count":
            entries[0]["manifest"]["row_count"] = -1
        elif self.name == "invalid_hash":
            entries[0]["manifest"]["output_files"][0]["sha256"] = "not-a-sha256"
        elif self.name == "duplicate_id":
            entries.append(dict(entries[0]))
        return {"generated_at": "2026-10-03T00:00:00Z", "datasets": entries}, bodies

    async def route(self, route: Any) -> None:
        url = route.request.url
        path = urlparse(url).path
        task = asyncio.current_task()
        if task is not None:
            self.active_routes.add(task)
        try:
            if path.endswith("/apps/web/src/app.js"):
                self.app_calls += 1
                self.app_seen.set()
                await self.app_gate.wait()
                if self.name == "module_reload" and self.app_calls == 1:
                    await route.fulfill(status=503, content_type="text/javascript", body="// Controlled module failure")
                    return
            elif path.endswith("duckdb-browser.mjs"):
                await route.fulfill(status=200, content_type="text/javascript", body=FAKE_DUCKDB)
                return
            elif "duckdb-browser-" in path and path.endswith("worker.js"):
                await route.fulfill(status=200, content_type="text/javascript", body=WORKER)
                return
            elif path.endswith("/data/manifests/catalog.json"):
                self.catalog_calls += 1
                self.catalog_seen.set()
                await self.catalog_gate.wait()
                if self.name == "catalog_retry" and self.catalog_calls == 1:
                    await route.fulfill(status=503, content_type="application/json", body='{"controlled_failure":true}')
                    return
                payload, _ = self.snapshot()
                await route.fulfill(status=200, content_type="application/json", body=json_bytes(payload))
                return
            elif "/data/processed/" in path and path.endswith(".parquet"):
                source_id = Path(path).stem
                self.data_calls[source_id] += 1
                _, bodies = self.snapshot()
                body = bodies.get(source_id)
                if body is None:
                    raise AssertionError(f"Unexpected data request: {source_id}")
                missing = (self.name == "all_reads_retry" and self.catalog_calls == 1) or (
                    self.name == "partial_missing_retry" and source_id == FINANCE and self.catalog_calls <= 2)
                if missing:
                    await route.fulfill(status=503, body="Controlled missing evidence")
                    return
                if source_id == FINANCE and (self.name == "partial_checksum" or (
                    self.name == "checksum_retry_cache" and self.catalog_calls == 1)):
                    body = body + b" "  # Valid JSON, intentionally mismatched catalog SHA.
                await route.fulfill(status=200, content_type="application/octet-stream", body=body)
                return
            await route.continue_()
        except Exception as exc:
            record = {"url": url, "kind": type(exc).__name__, "message": str(exc),
                      "tearing_down": self.tearing_down}
            self.route_errors.append(record)
            if not controlled_abort(self.name, self.origin, record) and not teardown_cancellation(record):
                # Do not leave an unexpected request waiting for the UI timeout.
                try:
                    await route.abort(error_code="failed")
                except Exception as abort_error:
                    self.route_errors.append({"url": url, "kind": type(abort_error).__name__,
                                              "message": str(abort_error), "operation": "abort_after_error",
                                              "tearing_down": self.tearing_down})
        finally:
            if task is not None:
                self.active_routes.discard(task)


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, *_args: Any) -> None:
        pass

    def do_GET(self) -> None:
        # Dedicated-worker requests are also served here in case a browser
        # version does not expose those requests to context.route.
        if "duckdb-browser-" in self.path and self.path.split("?", 1)[0].endswith("worker.js"):
            body = WORKER.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/javascript")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()


async def ready(page: Any) -> None:
    # A loading-shell h1 is deliberately not considered data readiness.
    await page.get_by_role("combobox", name="State context filter", exact=False).wait_for(state="visible")
    await page.locator(".loading-card").wait_for(state="detached")
    await page.get_by_role("heading", name="Ontology & Provenance Coverage", exact=True).wait_for()
    assert await page.locator(".loading-marker").count() == 0, "Loading animation persisted after ready"


async def terminal(page: Any, heading: str = "Evidence could not be loaded") -> None:
    await page.get_by_role("heading", name=heading, exact=True).wait_for()
    assert await page.locator(".loading-marker").count() == 0, "Terminal state still animates"
    assert await page.locator('[aria-busy="true"]').count() == 0, "Terminal state remains busy"
    assert await page.locator(".metric-card").count() == 0, "Terminal startup displays fabricated records"


async def keyboard_retry(page: Any) -> None:
    button = page.get_by_role("button", name="Retry data", exact=True)
    await button.focus()
    assert await button.evaluate("el => el === document.activeElement"), "Retry cannot receive keyboard focus"
    async with page.expect_response(lambda response: urlparse(response.url).path.endswith("/data/manifests/catalog.json")):
        await page.keyboard.press("Enter")


async def no_partial_warning(page: Any) -> None:
    await page.locator('.load-warning[aria-label="Evidence availability"]').wait_for(state="detached")


async def partial_warning(page: Any, source_id: str) -> None:
    warning = page.locator('.load-warning[aria-label="Evidence availability"]')
    await warning.wait_for(state="visible")
    await warning.get_by_role("button", name="Retry data", exact=True).wait_for()
    if await warning.locator("details").get_attribute("open") is None:
        await warning.locator("summary").click()
    assert source_id in await warning.inner_text(), "Missing affected source identity"
    assert "published row counts" in await warning.inner_text(), "Partial state misrepresents published counts"


async def metrics(page: Any) -> dict[str, Any]:
    return await page.evaluate("() => window.__loadingFixtureMetrics")


async def exercise(name: str, page: Any, fixture: Fixture, url: str, out: Path) -> None:
    if name == "static_shell":
        await page.goto(url, wait_until="commit")
        await asyncio.wait_for(fixture.app_seen.wait(), timeout=20)
        await page.locator("#startup-status").wait_for(state="visible")
        assert fixture.catalog_calls == 0, "React/data initialization preceded static shell"
        assert await page.get_by_role("heading", name="India Highway Explorer", exact=True).count() == 1
        assert await page.locator('[role="status"]').count() == 1, "Static shell has competing live statuses"
        assert await page.locator(".loading-road").get_attribute("aria-hidden") == "true"
        assert await page.locator(".loading-skeleton").get_attribute("aria-hidden") == "true"
        assert await page.locator('[role="progressbar"][aria-valuenow]').count() == 0, "Invented numeric progress"
        assert await page.locator(".metric-card").count() == 0, "Static shell invents record values"
        # Playwright's normal screenshot waits for document.fonts.ready, which
        # can wait for DOMContentLoaded while this module is deliberately held.
        # Capture the painted Chromium frame without releasing the test gate.
        await page.wait_for_function("getComputedStyle(document.querySelector('.loading-card')).animationName === 'none'")
        await page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
        session = await page.context.new_cdp_session(page)
        capture = await session.send("Page.captureScreenshot", {"format": "png"})
        (out / "static-shell.png").write_bytes(base64.b64decode(capture["data"]))
        await session.detach()
        fixture.app_gate.set()
        await ready(page)
        return

    await page.goto(url, wait_until="domcontentloaded")
    if name == "module_reload":
        await terminal(page, "India Highway Explorer could not start")
        assert fixture.catalog_calls == 0, "Failed module initialized data"
        await page.screenshot(path=str(out / "module-error.png"), full_page=True)
        button = page.get_by_role("button", name="Reload page", exact=True)
        await button.focus()
        async with page.expect_navigation(wait_until="domcontentloaded"):
            await page.keyboard.press("Enter")
        await ready(page)
        assert fixture.app_calls == 2, "Module reload did not retry the script"
    elif name in {"catalog_retry", "all_reads_retry"}:
        await terminal(page)
        await page.screenshot(path=str(out / f"{name}-error.png"), full_page=True)
        await keyboard_retry(page)
        await ready(page)
        await no_partial_warning(page)
        assert fixture.catalog_calls == 2, "Retry did not read a new catalog"
    elif name == "catalog_timeout_retry":
        await asyncio.wait_for(fixture.catalog_seen.wait(), timeout=20)
        await terminal(page)
        await page.screenshot(path=str(out / "catalog-timeout.png"), full_page=True)
        fixture.catalog_gate.set()
        await keyboard_retry(page)
        await ready(page)
        assert fixture.catalog_calls == 2, "Timed-out request prevented a later retry"
    elif name == "empty_catalog":
        await terminal(page, "No evidence has been published yet")
        assert fixture.data_calls == Counter(), "Empty catalog requested data"
        assert not (await metrics(page))["registrations"], "Empty catalog initialized buffers"
    elif name in {"invalid_count", "missing_count", "negative_count", "invalid_hash", "duplicate_id"}:
        await terminal(page)
        assert fixture.catalog_calls == 1, "Malformed metadata initiated extra catalog reads"
        assert fixture.data_calls == Counter(), "Malformed metadata requested evidence"
    elif name == "partial_missing_retry":
        await ready(page)
        await partial_warning(page, FINANCE)
        await page.get_by_role("button", name="All Signals", exact=True).click()
        await page.get_by_role("button", name="Enlarge", exact=True).click()
        warning = page.locator('.load-warning[aria-label="Evidence availability"]')
        async with page.expect_response(lambda response: urlparse(response.url).path.endswith("/data/manifests/catalog.json")):
            await warning.get_by_role("button", name="Retry data", exact=True).evaluate("el => { el.click(); el.click(); }")
        await ready(page)
        await partial_warning(page, FINANCE)
        assert fixture.catalog_calls == 2, "Double click created overlapping attempts"
        assert await page.get_by_role("button", name="All Signals", exact=True).get_attribute("aria-pressed") == "true"
        assert await page.get_by_role("button", name="Enlarge", exact=True).get_attribute("aria-pressed") == "true"
        await page.get_by_role("combobox", name="State context filter", exact=False).select_option("All")
        await keyboard_retry(page)
        await ready(page)
        await no_partial_warning(page)
        assert fixture.catalog_calls == 3, "Busy guard prevented a later settled retry"
        assert fixture.data_calls[GROWTH] == 1, "Unchanged successful buffer redownloaded on retry"
        assert fixture.data_calls[FINANCE] == 3, "Rejected registration did not retry"
        assert fixture.data_calls[QUARANTINED] == 0, "Ineligible catalog source was fetched"
    elif name in {"partial_checksum", "partial_query_failure", "checksum_retry_cache"}:
        await ready(page)
        await partial_warning(page, FINANCE)
        assert await page.get_by_role("button", name="Analyst evidence", exact=True).get_attribute("aria-pressed") == "true"
        await page.get_by_role("button", name="All Signals", exact=True).click()
        await page.get_by_role("button", name="Analyst evidence", exact=True).click()
        await page.get_by_role("combobox", name="State context filter", exact=False).select_option("All")
        assert fixture.data_calls[QUARANTINED] == 0, "Skipped source classified as attempted"
        first = await metrics(page)
        if name in {"partial_checksum", "checksum_retry_cache"}:
            assert not any(r["sourceId"] == FINANCE for r in first["registrations"]), "Hash mismatch reached DuckDB"
        else:
            assert any(q["sourceId"] == FINANCE and not q["ok"] for q in first["queries"]), "SQL failure fixture was not exercised"
        if name == "checksum_retry_cache":
            await keyboard_retry(page)
            await ready(page)
            await no_partial_warning(page)
            current = await metrics(page)
            registrations = [r for r in current["registrations"] if r["sourceId"] == GROWTH]
            assert [r["version"] for r in registrations] == [1, 2], "New catalog SHA reused an old buffer"
            assert len({r["alias"] for r in registrations}) == 2, "Different source SHAs share a loaded alias"
            assert fixture.data_calls[GROWTH] == 2, "Changed source buffer was not fetched"
            assert any(q["sourceId"] == GROWTH and q["version"] == 2 and q["ok"] for q in current["queries"])
    elif name == "mixed_model_queries":
        await ready(page)
        await partial_warning(page, MODEL)
        queries = [q for q in (await metrics(page))["queries"] if q["sourceId"] == MODEL]
        assert len(queries) == 2 and {q["ok"] for q in queries} == {False, True}, "Did not exercise mixed reads of one source"
        assert fixture.data_calls[MODEL] == 1, "Repeated SQL read duplicated source download"
        assert await page.get_by_role("heading", name="Evidence could not be loaded", exact=True).count() == 0
        assert await page.get_by_role("button", name="Analyst evidence", exact=True).get_attribute("aria-pressed") == "true"
        await page.get_by_role("button", name="Model only", exact=True).click()
        assert await page.locator(".metric-card").count() == 1, "Partial model metadata is inaccessible"
    elif name == "zero_observations":
        await ready(page)
        await no_partial_warning(page)
        queries = (await metrics(page))["queries"]
        assert queries and all(q["ok"] and q["rows"] == 0 for q in queries), "Zero-result query was not exercised"
        assert await page.get_by_role("heading", name="Evidence could not be loaded", exact=True).count() == 0
    elif name == "slow_mobile":
        await asyncio.wait_for(fixture.catalog_seen.wait(), timeout=20)
        await page.get_by_text("This is taking longer than usual.", exact=False).wait_for()
        assert await page.locator('[role="status"]').count() == 1, "Pending phase has competing live statuses"
        assert await page.locator('[aria-busy="true"]').count() == 1
        assert await page.locator(".metric-card").count() == 0, "Pending state displays fabricated records"
        assert await page.locator(".loading-marker").evaluate("el => getComputedStyle(el).animationName") == "none"
        assert await page.locator(".loading-card").evaluate("el => getComputedStyle(el).paddingTop") == "18px", "Mobile loading layout lost its override"
        assert await page.evaluate("() => document.documentElement.scrollWidth <= window.innerWidth + 1"), "Mobile shell overflows horizontally"
        button = page.get_by_role("button", name="Reload page", exact=True)
        await page.keyboard.press("Tab")
        await button.focus()
        assert await button.evaluate("el => el.matches(':focus-visible')"), "Reload has no keyboard focus treatment"
        await page.screenshot(path=str(out / "slow-mobile-reduced-motion.png"), full_page=True)
        fixture.catalog_gate.set()
        await ready(page)
        assert await page.evaluate("() => document.documentElement.scrollWidth <= window.innerWidth + 1"), "Loaded mobile dashboard overflows"
        assert await page.locator(".metric-card").first.evaluate("el => getComputedStyle(el).animationName") == "none", "Reduced motion did not suppress existing card animation"
    elif name in {"features_timeout", "instantiate_timeout", "connect_timeout"}:
        await terminal(page)
        if name != "features_timeout":
            assert (await metrics(page))["workerTerminations"] >= 1, "Timed-out engine worker was retained"
        await page.screenshot(path=str(out / f"{name}.png"), full_page=True)
        await page.evaluate("() => { window.__fixtureEngineMode = 'success'; }")
        await keyboard_retry(page)
        await ready(page)
        await no_partial_warning(page)
    else:
        raise AssertionError(f"Unimplemented scenario: {name}")


SCENARIOS = (
    "static_shell", "module_reload", "catalog_retry", "catalog_timeout_retry", "empty_catalog",
    "invalid_count", "missing_count", "negative_count", "invalid_hash", "duplicate_id",
    "all_reads_retry", "partial_missing_retry", "partial_checksum", "partial_query_failure",
    "checksum_retry_cache", "mixed_model_queries", "zero_observations", "slow_mobile",
    "features_timeout", "instantiate_timeout", "connect_timeout",
)


async def run(args: argparse.Namespace, url: str) -> int:
    from playwright.async_api import async_playwright

    args.out.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "kind": "controlled-loading-state-regressions", "started_at": datetime.now(timezone.utc).isoformat(),
        "repo": str(args.repo), "url": url,
        "fixture_notice": "Synthetic catalog/JSON buffers and fake DuckDB; real React/UI. Not dataset equivalence or browser performance proof.",
        "accelerated_timers_ms": {"15000": 500, "30000": 1400, "60000": 2200},
        "scenarios": [],
    }
    results_path = args.out / "results.json"
    selected = args.scenario or list(SCENARIOS)
    async with async_playwright() as p:
        launch: dict[str, Any] = {"headless": True}
        if args.executable_path:
            launch["executable_path"] = str(args.executable_path)
        browser = await p.chromium.launch(**launch)
        report["browser_version"] = browser.version
        try:
            for name in selected:
                parsed = urlparse(url)
                fixture = Fixture(name, f"{parsed.scheme}://{parsed.netloc}")
                context = await browser.new_context(
                    viewport={"width": 390, "height": 844} if name == "slow_mobile" else {"width": 1280, "height": 720},
                    reduced_motion="reduce" if name == "slow_mobile" else "no-preference",
                    service_workers="block",
                )
                engine = name if name in {"features_timeout", "instantiate_timeout", "connect_timeout"} else "success"
                await context.add_init_script(script=INIT_SCRIPT.replace("ENGINE_MODE", json.dumps(engine)))
                await context.route("**/*", fixture.route)
                page = await context.new_page()
                page.set_default_timeout(20000)
                page.set_default_navigation_timeout(45000)
                row: dict[str, Any] = {"name": name, "expected_faults_are_controlled": True, "console": [], "page_errors": [], "request_failures": []}
                page.on("console", lambda msg, row=row: row["console"].append({"type": msg.type, "text": msg.text}))
                page.on("pageerror", lambda error, row=row: row["page_errors"].append(str(error)))
                page.on("requestfailed", lambda request, row=row, fixture=fixture: row["request_failures"].append({
                    "url": request.url, "failure": request.failure, "tearing_down": fixture.tearing_down}))
                page.on("request", lambda request, fixture=fixture: fixture.request_log.append({"method": request.method, "url": request.url}))
                try:
                    await exercise(name, page, fixture, url, args.out)
                    assert_diagnostics(name, fixture.origin, fixture.route_errors,
                                       row["request_failures"], row["page_errors"])
                    current = await metrics(page)
                    recount = "SELECTCOUNT(*)::BIGINTASROW_COUNT"
                    assert not any(recount in re.sub(r"\s+", "", q["sql"].upper()) for q in current["queries"]), "Startup performed runtime row counts"
                    row["status"] = "passed"
                except Exception as exc:
                    row["status"] = "failed"
                    row["error"] = f"{type(exc).__name__}: {exc}"
                    row["traceback"] = traceback.format_exc()
                finally:
                    fixture.app_gate.set()
                    fixture.catalog_gate.set()
                    try:
                        row["engine_metrics"] = await metrics(page)
                        row["visible_text"] = (await page.locator("body").inner_text())[:12000]
                        screenshot = args.out / f"{name}-{row['status']}.png"
                        await page.screenshot(path=str(screenshot), full_page=True)
                        row["screenshot"] = str(screenshot)
                    except Exception as exc:
                        row["evidence_error"] = f"{type(exc).__name__}: {exc}"
                    # Recheck after collecting evidence, then again after close:
                    # a late failed route must not retain an earlier pass label.
                    gate_result(row, fixture)
                    fixture.tearing_down = True
                    try:
                        await context.close()
                        pending = list(fixture.active_routes)
                        if pending:
                            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=5)
                    except Exception as exc:
                        fixture.route_errors.append({"url": url, "kind": type(exc).__name__,
                                                     "message": str(exc), "tearing_down": True,
                                                     "operation": "context_teardown"})
                    gate_result(row, fixture)
                    row["catalog_requests"] = fixture.catalog_calls
                    row["app_requests"] = fixture.app_calls
                    row["data_requests"] = dict(fixture.data_calls)
                    row["requests"] = fixture.request_log
                    row["route_errors"] = fixture.route_errors
                    report["scenarios"].append(row)
                    report["status"] = "failed" if any(r["status"] == "failed" for r in report["scenarios"]) else "passed"
                    results_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
                print(f"{name}: {row['status']}", flush=True)
        finally:
            await browser.close()
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    results_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Controlled loading evidence: {results_path}")
    return 1 if report["status"] == "failed" else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("buildcheck/loading"))
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--executable-path", type=Path, help="Use an already-installed Chromium executable")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS, help="Run selected scenarios only")
    args = parser.parse_args()
    args.repo = args.repo.resolve()
    args.out = args.out.resolve()
    handler = functools.partial(Handler, directory=str(args.repo))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/apps/web/"
    try:
        return asyncio.run(run(args, url))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    raise SystemExit(main())
