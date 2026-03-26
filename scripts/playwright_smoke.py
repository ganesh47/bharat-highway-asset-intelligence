#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio

from typing import Union


async def _canvas_non_transparent_pixels(canvas_locator) -> int:
    return await canvas_locator.evaluate(
        """(canvas) => {
            const ctx = canvas.getContext('2d');
            if (!ctx) {
                return 0;
            }
            const width = Math.max(1, canvas.width);
            const height = Math.max(1, canvas.height);
            const sampleWidth = Math.max(1, Math.min(width, 48));
            const rowStride = Math.max(1, Math.floor(height / 12));
            let nonTransparent = 0;
            for (let y = 0; y < height; y += rowStride) {
                const data = ctx.getImageData(0, y, width, 1).data;
                for (let x = 0; x < Math.min(data.length, sampleWidth * 4); x += 4) {
                    if (data[x + 3] > 2) {
                        nonTransparent += 1;
                    }
                }
            }
            return nonTransparent;
        }"""
    )


async def _chart_has_canvas_content(chart_card) -> bool:
    canvas_count = await chart_card.locator("canvas").count()
    if not canvas_count:
        return False
    for index in range(canvas_count):
        canvas_locator = chart_card.locator("canvas").nth(index)
        rect = await canvas_locator.bounding_box()
        if not rect or rect["width"] <= 0 or rect["height"] <= 0:
            continue
        if await _canvas_non_transparent_pixels(canvas_locator) > 0:
            return True
    return False


REQUIRED_CHARTS = [
    {
        "title": "Growth Story: NHAI Constructed Length by Year",
        "axes": True,
        "data_selector": ".line-path",
        "min_points": 1,
        "legend_labels": ["Full-year official totals", "Current-year progress (provisional)"],
        "meta_markers": ["NHAI-only construction series", "Do not compare provisional YTD progress directly with full-year totals"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "Budget vs Expenditure (All Source Years)",
        "axes": True,
        "data_selector": ".line-path",
        "min_points": 1,
        "legend_labels": ["Allocation total", "Expenditure total"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "State Portfolio: Total NH Length vs State",
        "data_selector": ".bar-row",
        "min_points": 1,
        "empty_markers": ["No records available."],
    },
    {
        "title": "MoRTH Appendix 3: CRIF Allocation vs Release",
        "axes": True,
        "data_selector": ".line-path",
        "min_points": 1,
        "legend_labels": ["Allocation", "Release"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "MoRTH Appendix 2: Number of NH Designations by State/UT",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["As of 2024-12-31"],
        "note_markers": ["non-additive across states", "same NH can appear in multiple State/UT rows"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "MoRTH Appendix 2: NH Length (km) by State",
        "data_selector": ".bar-row",
        "min_points": 1,
        "empty_markers": ["No records available."],
    },
    {
        "title": "MoRTH Appendix 5: State Permit Fee vs NH Length",
        "axes": True,
        "data_selector": ".point",
        "min_points": 1,
        "legend_labels": ["Each point: State", "Bubble size: NH count"],
        "empty_markers": ["No scatter points."],
    },
    {
        "title": "State Project Mix (active vs delayed NH projects, official March 2024 snapshot)",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["As of March 2024"],
        "legend_labels": ["Active without listed delay", "Delayed projects"],
        "note_markers": ["Active without listed delay", "Delayed projects", "Total row is excluded"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "NH Fatality Burden by State/UT (official NH fatalities, 2022)",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["Fatalities: 2022 | NH length denominator: 2024-12-31"],
        "note_markers": [
            "normalized by each State/UT's validated MoRTH Appendix 2 NH-length snapshot",
            "Higher bars mean more recorded NH deaths relative to network length",
            "not more deaths across all roads",
            "use this as burden context rather than a same-year rate card",
        ],
        "empty_markers": ["No records available."],
    },
    {
        "title": "NH Black Spot Burden × Rectification Context",
        "data_selector": ".dotplot-point",
        "min_points": 1,
        "legend_labels": ["High rectification backlog", "Medium rectification backlog", "Low rectification backlog"],
        "meta_markers": ["Black spot accident data: 2018-2020 | Reply dated 2023-12-21"],
        "note_markers": ["ranked by black spots per 1,000 km of NH", "not a same-year incident snapshot"],
        "empty_markers": ["No ranked-dot data available."],
    },
    {
        "title": "NH Fatality Trend by State/UT (official, 2020-2022)",
        "axes": True,
        "data_selector": ".line-path",
        "min_points": 1,
        "meta_markers": ["Official NH fatalities: 2020-2022"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "Synthetic Risk Scenario Score by State (exploratory)",
        "axes": True,
        "data_selector": ".line-path",
        "min_points": 1,
        "meta_markers": ["scenario planning only", "not be interpreted as an official risk ranking"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "Economic Scale vs NH Extent by State/UT",
        "axes": True,
        "data_selector": ".point",
        "min_points": 1,
        "meta_markers": ["Latest available GSDP by state: 2017-18 to 2022-23 | NH length: 2024-12-31 | Delayed projects: March 2024"],
        "legend_labels": ["Each point: State / UT", "Bubble size: Delayed NH projects"],
        "empty_markers": ["No scatter points."],
    },
    {
        "title": "Delay Burden Relative to Economic Scale",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["As of"],
        "note_markers": ["Latest available current-price GSDP year varies by state", "relative delivery burden"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "Project Economics: Land Acquisition vs Maintenance (Model Panel)",
        "axes": True,
        "data_selector": ".point",
        "min_points": 1,
        "legend_labels": ["Each point: State", "Bubble size: Sanctioned cost proxy (₹ crore)"],
        "empty_markers": ["No scatter points."],
    },
]


async def run_smoke(url: str, generate_screenshot: bool = True) -> int:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:  # pragma: no cover - environment dependent
        print(f"Playwright not available: {exc}")
        return 1

    console_errors: list[str] = []
    failed_requests: list[str] = []
    failed_responses: list[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        page.on("console", lambda message: _handle_console(message, console_errors))
        page.on(
            "requestfailed",
            lambda request: _on_request_failed(request, failed_requests),
        )

        async def on_response(response):
            try:
                if response.status >= 400:
                    failed_responses.append(f"{response.url} -> {response.status}")
            except Exception:
                pass

        page.on("response", lambda response: asyncio.create_task(on_response(response)))

        async def wait_for_dashboard_shell() -> None:
            last_exc = None
            for attempt in range(1, 4):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=120000)
                    await page.wait_for_load_state("networkidle", timeout=120000)
                    await page.wait_for_timeout(3000)
                    await page.wait_for_function(
                        """() => {
                            const header = document.querySelector('h1');
                            const metricCards = document.querySelectorAll('.metric-card').length;
                            return Boolean(
                                (header && (header.textContent || '').trim()) || metricCards >= 3
                            );
                        }""",
                        timeout=120000,
                    )
                    return
                except Exception as exc:  # pragma: no cover - environment dependent
                    last_exc = exc
                    if attempt == 3:
                        raise
                    print(
                        f"Dashboard shell not ready on attempt {attempt}/3 at {url}: {exc}. Retrying..."
                    )
                    await page.wait_for_timeout(attempt * 15000)
            if last_exc:
                raise last_exc

        try:
            await wait_for_dashboard_shell()

            header_locator = page.locator("h1").first
            if await header_locator.count() == 0:
                print("Dashboard header not found after page shell became available.")
                await browser.close()
                return 1

            header = (await header_locator.text_content()) or ""
            if "Bharat Highway Evidence Console" not in header:
                print("Unexpected header text:", header.strip())
                await browser.close()
                return 1

            if await page.locator("text=Could not load catalog").count():
                print("Catalog load error message found.")
                await browser.close()
                return 1

            if await page.locator("text=DuckDB initialization failed").count():
                print("DuckDB init error message found.")
                await browser.close()
                return 1

            chart_count = await page.locator(".metric-card").count()
            if chart_count < 3:
                print(f"Expected multiple metric cards, found {chart_count}.")
                await browser.close()
                return 1

            summary_text = await page.locator(".summary .card").all_text_contents()
            if not summary_text:
                print("Summary cards not found.")
                await browser.close()
                return 1

            for chart in REQUIRED_CHARTS:
                title_selector = chart.get("title")
                if chart.get("title_prefix"):
                    title_selector = chart["title_prefix"]

                card = page.locator(".insight-chart").filter(
                    has=page.locator(".chart-title", has_text=title_selector)
                )
                count = await card.count()
                if count == 0:
                    print(f"Missing chart by selector: {title_selector}")
                    await browser.close()
                    return 1
                if count != 1:
                    print(f"Chart appears multiple times ({count}) for selector: {title_selector}")
                    await browser.close()
                    return 1

                chart_card = card.first
                meta_text = [
                    text or ''
                    for text in await chart_card.locator('.chart-meta').all_inner_texts()
                ]
                if any(marker in ' '.join(meta_text) for marker in chart.get('empty_markers', [])):
                    print(f"Chart has empty-data marker for: {title_selector}")
                    await browser.close()
                    return 1

                marker_count = await chart_card.locator(chart['data_selector']).count()
                if marker_count < chart['min_points']:
                    if chart['data_selector'] in {".line-path", ".point"} and await _chart_has_canvas_content(chart_card):
                        pass
                    else:
                        print(f"Chart has insufficient rendered points ({marker_count}) for: {title_selector}")
                        await browser.close()
                        return 1

                for marker in chart.get("meta_markers", []):
                    if not any(marker in text for text in meta_text):
                        print(f"Chart is missing meta marker '{marker}' for: {title_selector}")
                        await browser.close()
                        return 1

                note_text = [
                    text or ''
                    for text in await chart_card.locator('.insight-note').all_inner_texts()
                ]
                for marker in chart.get("note_markers", []):
                    if not any(marker in text for text in note_text):
                        print(f"Chart is missing note marker '{marker}' for: {title_selector}")
                        await browser.close()
                        return 1

                if chart.get('axes'):
                    axis_titles = [
                        value or ''
                        for value in await chart_card.locator('.axis-title').evaluate_all('(els) => els.map((el) => el.textContent || "")')
                    ]
                    if len([label.strip() for label in axis_titles if str(label).strip()]) < 2:
                        if not await _chart_has_canvas_content(chart_card):
                            print(f"Chart is missing axis labels: {title_selector}")
                            await browser.close()
                            return 1

                legend_labels = chart.get("legend_labels", [])
                legend_min_pills = chart.get("legend_min_pills", 0)
                if legend_labels or legend_min_pills:
                    legend = chart_card.locator(".insight-legend")
                    if await legend.count() != 1:
                        print(f"Chart is missing legend container: {title_selector}")
                        await browser.close()
                        return 1
                    pill_texts = [
                        (text or "").strip()
                        for text in await legend.locator(".insight-pill").all_inner_texts()
                    ]
                    if len([text for text in pill_texts if text]) < legend_min_pills:
                        print(f"Chart legend has too few items for: {title_selector}")
                        await browser.close()
                        return 1
                    for label in legend_labels:
                        if not any(label in text for text in pill_texts):
                            print(f"Chart legend is missing label '{label}' for: {title_selector}")
                            await browser.close()
                            return 1

            if generate_screenshot:
                await page.screenshot(path="buildcheck/last-smoke.png", full_page=True)
            await browser.close()
        except Exception as exc:
            print(f"Playwright interaction failed: {exc}")
            await browser.close()
            return 1

    errors = [
        c for c in console_errors
        if "404" not in c
        and "Failed to load resource" not in c
        and "Multiple readback operations using getImageData" not in c
    ]
    if failed_requests:
        print("Request failures detected:")
        for item in failed_requests:
            print("-", item)
        return 1

    if failed_responses:
        print("HTTP error responses detected:")
        for item in failed_responses:
            print("-", item)
        return 1

    if errors:
        print("Console errors detected:")
        for item in errors:
            print("-", item)
        return 1

    print(f"Playwright smoke passed. URL={url}")
    return 0


def _handle_console(message, console_errors):
    if message.type in {"error", "warning"}:
        text = message.text
        if text:
            console_errors.append(text)


def _on_request_failed(request, failed_requests: list[str]):
    failure: Union[None, str, object] = request.failure
    if failure is None:
        reason = "unknown"
    elif isinstance(failure, str):
        reason = failure
    else:
        reason = getattr(failure, "error_text", None) or getattr(failure, "errorText", None) or str(failure)
    failed_requests.append(f"{request.url} ({reason})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a browser smoke check for the deployed dashboard.")
    parser.add_argument("--url", default="https://ganesh47.github.io/bharat-highway-asset-intelligence/apps/web/")
    parser.add_argument("--no-generate-screenshot", action="store_true")
    args = parser.parse_args()

    code = asyncio.run(run_smoke(args.url, generate_screenshot=not args.no_generate_screenshot))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
