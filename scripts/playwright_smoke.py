#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import re
from pathlib import Path

from typing import Union

import pandas as pd


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


ROOT = Path(__file__).resolve().parents[1]


def _normalize_state_key(value: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _synthetic_model_panel_ready() -> bool:
    model_path = ROOT / "data/processed/highway_project_risk_and_access_panel.parquet"
    if not model_path.exists():
        return False
    try:
        model_df = pd.read_parquet(model_path, columns=["state_assigned", "safety_risk_score"])
        states = model_df["state_assigned"].dropna().astype(str).map(_normalize_state_key)
        score_vals = pd.to_numeric(model_df["safety_risk_score"], errors="coerce").dropna()
        return states.nunique() >= 10 and score_vals.nunique() > 1
    except Exception:
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
        "meta_markers": ["YTD through 31 January 2025", "Others includes monetisation"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "State Portfolio: Total NH Network Length by State/UT",
        "note_markers": ["network stock", "Project portfolio lengths and annual construction flows are separate"],
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

]

MODEL_CHARTS = [
    {
        "title": "Project Economics: Land Acquisition vs Maintenance (Model Panel)",
        "axes": True,
        "data_selector": ".point",
        "min_points": 1,
        "legend_labels": ["Each point: State", "Bubble size: Sanctioned cost proxy (₹ crore)"],
        "empty_markers": ["No scatter points."],
    },
]

if _synthetic_model_panel_ready():
    MODEL_CHARTS.insert(
        11,
        {
            "title": "Synthetic Risk Scenario Score by State (exploratory)",
            "axes": True,
            "data_selector": ".line-path",
            "min_points": 1,
            "meta_markers": [],
            "empty_markers": ["No records available."],
        },
    )
else:
    MODEL_CHARTS.insert(
        11,
        {
            "title": "Synthetic Risk Scenario Panel (hidden pending better coverage)",
            "data_selector": ".chart-title",
            "min_points": 1,
            "meta_markers": ["too sparse or degenerate"],
            "note_markers": ["covers at least 10 states", "distinct safety scores"],
            "empty_markers": ["No records available."],
        },
    )


def _frontend_fixture_script(source: str) -> str:
    """Exercise the deployed pure calculation functions, including invalid joins."""
    names = ["num", "completeSum", "statePortfolioObservation", "unrectifiedShare", "fmtNum", "sourceTypeTag", "analyticalReady", "confidenceFromSources", "humanMetric", "disclosureTheme", "validCitation", "disclosureEligible", "disclosureMeasured", "csvText", "deriveDisclosureInsights"]
    blocks = []
    for name in names:
        start = re.search(r"^function " + re.escape(name) + r"\(", source, re.MULTILINE)
        if start is None:
            raise RuntimeError(f"Deployed frontend is missing semantic function: {name}")
        following = re.search(r"^(?:async )?function ", source[start.end():], re.MULTILINE)
        end = start.end() + following.start() if following else len(source)
        blocks.append(source[start.start():end])
    assertions = r"""
    const failures = [];
    const check = (condition, label) => { if (!condition) failures.push(label); };
    check(num(null) === null && num('') === null && num(0) === 0, 'missing values versus observed zero');
    check(completeSum([10,20,0,30,40]) === 100, 'five observed construction years summed');
    check(completeSum([0,0,0,0,0]) === 0, 'five observed zero years retained');
    check(completeSum([10,20,null,30,40]) === null, 'partial construction years are not a full five-year total');
    check(completeSum([null,null,null,null,null]) === null && completeSum([0,0,'',0,0]) === null, 'missing construction history is unavailable');
    const missingPortfolio=statePortfolioObservation({state:'Fixture'});
    check(missingPortfolio.projects === null && missingPortfolio.length_km === null && missingPortfolio.capital_outlay === null, 'missing project portfolio fields preserved');
    const zeroPortfolio=statePortfolioObservation({state:'Fixture',number_of_nh_projects:0,length_in_km:0,length__in_km_:9,capital_outlay__rs_in_cr_for_the_years_2020_to_2024:0,capital_outlay___rs_in_cr__for_the_years_2020_to_2024:12});
    check(zeroPortfolio.projects === 0 && zeroPortfolio.length_km === 0 && zeroPortfolio.capital_outlay === 0, 'portfolio zero not replaced by another alias');
    check(unrectifiedShare(10,null) === null, 'missing rectification count is not a full backlog');
    check(unrectifiedShare(10,0) === 100 && unrectifiedShare(10,10) === 0, 'observed rectification zero and full completion');
    check(unrectifiedShare(0,0) === null && unrectifiedShare(10,11) === null, 'invalid black-spot denominator or scope');
    check(fmtNum(null) === 'N/A', 'missing display');
    check(sourceTypeTag({metric_category:'model_output', source:{official_flag:false}})[1] === 'model', 'model classification');
    check(sourceTypeTag({metric_category:'issuer_disclosed', source:{official_flag:false}})[1] === 'issuer', 'issuer classification');
    check(confidenceFromSources([{overall_confidence_badge:'High'}]).badge === 'High', 'high confidence');
    check(confidenceFromSources([{overall_confidence_badge:'High'},{overall_confidence_badge:'Med'}]).badge === 'Med', 'contributing confidence floor');
    check(confidenceFromSources([]).badge === 'Low', 'missing confidence');
    check(disclosureTheme({metric:'additional_borrowings_inr_crore'}) === 'debt', 'new borrowings scope');
    check(disclosureTheme({metric:'state_government_guarantees_outstanding_inr_crore'}) === 'debt', 'state guarantees scope');
    check(disclosureTheme({metric:'sh_surfaced_length_km'}) === 'network', 'State Highway surfaced stock');
    const observed={source_id:'fixture',value:80,analytical_eligible:true,estimate_type:'actual',evidence_class:'official_measured',citation_url:'https://example.org/primary'};
    const catalog={fixture:{source_id:'fixture',metric_category:'official_measured',analytical_ready:true,manifest:{row_count:1}}};
    check(disclosureMeasured(observed,catalog), 'actual measured observation');
    check(!disclosureMeasured({...observed,estimate_type:'BE'},catalog) && !disclosureMeasured({...observed,estimate_type:'RE'},catalog), 'budget estimates are not measured actuals');
    check(!disclosureMeasured({...observed,evidence_class:'target'},catalog), 'targets are not measured actuals');
    const base = {entity_id:'P1', entity_type:'project', agency:'NHAI', state:'Odisha', road_class:'NH', period_start:'2025-04-01', period_end:'2026-03-31', period_basis:'financial_year', statement_basis:'project', source_id:'fixture', data_as_of:'2026-03-31', estimate_type:'actual', evidence_class:'official_measured'};
    const cost = {...base, metric:'sanctioned_cost_inr_crore', value:100, unit:'inr_crore'};
    const length = {...base, metric:'project_length_km', value:10, unit:'km'};
    const derive = (rows) => deriveDisclosureInsights(rows);
    check(derive([cost,length])[0]?.value === 10, 'valid cost per kilometre');
    check(derive([cost,{...length,unit:'Nos'}]).length === 0, 'bridge count is not kilometres');
    check(derive([cost,{...length,value:0}]).length === 0, 'zero denominator');
    check(derive([cost,{...length,agency:'NHIDCL'}]).length === 0, 'different agency');
    check(derive([cost,{...length,road_class:'SH'}]).length === 0, 'different road class');
    check(derive([cost,{...length,data_as_of:'2025-12-31'}]).length === 0, 'different cutoff');
    check(derive([cost,length,length]).length === 0, 'ambiguous duplicate denominator');
    check(derive([{...cost,metric:'tot_concession_value_inr_crore'}, {...length,metric:'tot_portfolio_length_km'}])[0]?.label === 'TOT concession value per route km', 'concession value uses disclosed route denominator');
    check(derive([{...cost,metric:'tot_concession_value_inr_crore'}, length]).length === 0, 'concession value does not use unrelated project denominator');
    const actual={...base,metric:'budget_maintenance_inr_crore',value:80,unit:'inr_crore'};
    const budget={...actual,value:100,estimate_type:'BE'};
    check(derive([actual,budget])[0]?.value === 80, 'matched full-period actual to BE');
    check(derive([{...actual,estimate_type:'YTD'},budget]).length === 0, 'YTD is not full-period actual');
    check(derive([actual,{...budget,data_as_of:'2025-03-31'}]).length === 0, 'budget comparison cutoff');
    check(derive([actual,{...budget,statement_basis:'consolidated'}]).length === 0, 'different accounting basis');
    check(derive([actual,actual,budget]).length === 0, 'ambiguous duplicate actual');
    check(csvText([{metric:'=1+1'}],['metric']).includes("'=1+1"), 'CSV spreadsheet text safety');
    return failures;
    """
    return "() => {\n" + "\n".join(blocks) + assertions + "\n}"


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

        async def wait_for_expected_chart_titles() -> None:
            expected_titles = [
                "NH Fatality Trend by State/UT (official, 2020-2022)",
                "Economic Scale vs NH Extent by State/UT",
            ]
            last_seen = ""
            for attempt in range(1, 6):
                visible_titles = [
                    text.strip()
                    for text in await page.locator(".chart-title").all_inner_texts()
                    if (text or "").strip()
                ]
                if all(title in visible_titles for title in expected_titles):
                    return
                last_seen = " | ".join(visible_titles[:8])
                print(
                    f"Expected deployed chart titles not visible on attempt {attempt}/5 at {url}. "
                    f"Seen: {last_seen or 'none'}. Retrying..."
                )
                await page.wait_for_timeout(attempt * 10000)
                await page.goto(url, wait_until="domcontentloaded", timeout=120000)
                await page.wait_for_load_state("networkidle", timeout=120000)
            raise RuntimeError(
                f"Expected deployed chart titles did not appear after retries. Last seen titles: {last_seen or 'none'}"
            )

        try:
            await wait_for_dashboard_shell()
            await wait_for_expected_chart_titles()

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

            async def validate_charts(charts):
                for chart in charts:
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
                        raise RuntimeError("Chart validation failed")
                    if count != 1:
                        print(f"Chart appears multiple times ({count}) for selector: {title_selector}")
                        await browser.close()
                        raise RuntimeError("Chart validation failed")

                    chart_card = card.first
                    meta_text = [
                        text or ''
                        for text in await chart_card.locator('.chart-meta').all_inner_texts()
                    ]
                    if any(marker in ' '.join(meta_text) for marker in chart.get('empty_markers', [])):
                        print(f"Chart has empty-data marker for: {title_selector}")
                        await browser.close()
                        raise RuntimeError("Chart validation failed")

                    marker_count = await chart_card.locator(chart['data_selector']).count()
                    if marker_count < chart['min_points']:
                        if chart['data_selector'] in {".line-path", ".point"} and await _chart_has_canvas_content(chart_card):
                            pass
                        else:
                            print(f"Chart has insufficient rendered points ({marker_count}) for: {title_selector}")
                            await browser.close()
                            raise RuntimeError("Chart validation failed")

                    for marker in chart.get("meta_markers", []):
                        if not any(marker in text for text in meta_text):
                            print(f"Chart is missing meta marker '{marker}' for: {title_selector}")
                            await browser.close()
                            raise RuntimeError("Chart validation failed")

                    note_text = [
                        text or ''
                        for text in await chart_card.locator('.insight-note').all_inner_texts()
                    ]
                    for marker in chart.get("note_markers", []):
                        if not any(marker in text for text in note_text):
                            print(f"Chart is missing note marker '{marker}' for: {title_selector}")
                            await browser.close()
                            raise RuntimeError("Chart validation failed")

                    if chart.get('axes'):
                        axis_titles = [
                            value or ''
                            for value in await chart_card.locator('.axis-title').evaluate_all('(els) => els.map((el) => el.textContent || "")')
                        ]
                        if len([label.strip() for label in axis_titles if str(label).strip()]) < 2:
                            if not await _chart_has_canvas_content(chart_card):
                                print(f"Chart is missing axis labels: {title_selector}")
                                await browser.close()
                                raise RuntimeError("Chart validation failed")

                    legend_labels = chart.get("legend_labels", [])
                    legend_min_pills = chart.get("legend_min_pills", 0)
                    if legend_labels or legend_min_pills:
                        legend = chart_card.locator(".insight-legend")
                        if await legend.count() != 1:
                            print(f"Chart is missing legend container: {title_selector}")
                            await browser.close()
                            raise RuntimeError("Chart validation failed")
                        pill_texts = [
                            (text or "").strip()
                            for text in await legend.locator(".insight-pill").all_inner_texts()
                        ]
                        if len([text for text in pill_texts if text]) < legend_min_pills:
                            print(f"Chart legend has too few items for: {title_selector}")
                            await browser.close()
                            raise RuntimeError("Chart validation failed")
                        for label in legend_labels:
                            if not any(label in text for text in pill_texts):
                                print(f"Chart legend is missing label '{label}' for: {title_selector}")
                                await browser.close()
                                raise RuntimeError("Chart validation failed")
            if await page.get_by_role("button", name="Analyst evidence", exact=True).get_attribute("class") != "toggle active":
                raise RuntimeError("Default view must be validated analyst evidence")
            if await page.locator('.chart-title').filter(has_text="Synthetic Risk").count() or await page.locator('.chart-title').filter(has_text="Project Economics").count():
                raise RuntimeError("Synthetic charts leaked into the default analyst view")
            if not any("model rows excluded" in text for text in summary_text):
                raise RuntimeError("Measured evidence coverage must exclude model rows")
            if not any("Issuer disclosures:" in text for text in summary_text):
                raise RuntimeError("Issuer disclosure coverage missing")
            await validate_charts(REQUIRED_CHARTS)
            module_url = await page.evaluate("new URL('src/app.js', location.href).href")
            module_response = await page.request.get(module_url)
            if not module_response.ok:
                raise RuntimeError('Cannot inspect the deployed calculation module')
            semantic_failures = await page.evaluate(_frontend_fixture_script(await module_response.text()))
            if semantic_failures:
                raise RuntimeError(f'Frontend semantic fixtures failed: {semantic_failures}')
            await page.get_by_role("heading", name="Finance & infrastructure disclosures", exact=True).wait_for()
            for label in ["Agency", "Road class", "Reporting period", "Estimate type", "Evidence class", "Metric", "Entity search"]:
                if await page.get_by_label(label, exact=True).count() != 1:
                    raise RuntimeError(f"Missing or ambiguous disclosure filter: {label}")
            table = page.locator('.evidence-table')
            if await table.count() != 1 or await table.locator('tbody tr').count() < 1:
                raise RuntimeError("Funding disclosures have no validated evidence rows")
            for label in ["Value / unit", "Period / estimate / basis", "Observation / publication", "Evidence / source"]:
                if await table.get_by_role('columnheader', name=label, exact=True).count() != 1:
                    raise RuntimeError(f"Missing evidence table context: {label}")
            if await table.locator('tbody a[href^="https://"]').count() < 1:
                raise RuntimeError("Disclosure observations lack primary citations")
            async with page.expect_download() as download_info:
                await page.get_by_role('button', name='Download filtered evidence CSV', exact=True).click()
            download = await download_info.value
            download_path = await download.path()
            csv_text = Path(download_path).read_text(encoding='utf-8-sig')
            for field in ['original_unit', 'period_basis', 'statement_basis', 'citation_url', 'table_page', 'source_document_sha256']:
                if field not in csv_text.splitlines()[0]:
                    raise RuntimeError(f"CSV lost lineage field: {field}")
            if len(csv_text.splitlines()) < 2:
                raise RuntimeError("CSV export has no observation rows")
            await page.get_by_label('Estimate type', exact=True).select_option('BE')
            if await table.locator('tbody tr').count() < 1 or 'Budget estimate' not in await table.inner_text():
                raise RuntimeError('Budget estimates must remain visible and distinct from measured actuals')
            await page.get_by_label('Estimate type', exact=True).select_option('All')
            await page.get_by_label('Evidence class', exact=True).select_option('target')
            if await table.locator('tbody tr[data-evidence-class="target"]').count() < 1 or 'excluded from measured calculations' not in await table.inner_text():
                raise RuntimeError('Validated targets must remain visible with calculation exclusions')
            await page.get_by_label('Evidence class', exact=True).select_option('All')
            await page.get_by_role('button', name='Debt & repayments', exact=False).click()
            debt_text = await page.locator('.analyst-evidence-panel').inner_text()
            if 'Disclosed debt maturity buckets are available' not in debt_text and 'No validated debt maturity schedule' not in debt_text:
                raise RuntimeError("Debt maturities must show disclosed scope or an explicit evidence gap")
            await page.get_by_label('Agency', exact=True).select_option('NHIT')
            await page.get_by_label('Metric', exact=True).select_option('debt_maturity_lt1yr_inr_crore')
            if await table.locator('tbody tr').count() < 1 or 'contractual_undiscounted_maturity' not in await table.inner_text():
                raise RuntimeError('NHIT disclosed maturities must preserve contractual statement basis')
            await page.get_by_role('button', name='State road spending', exact=False).click()
            if await table.locator('tbody tr').count() < 1 or 'roads_and_bridges_all_classes' not in await table.inner_text():
                raise RuntimeError('State road finances must retain their wider Roads and Bridges scope')
            state_selector = page.locator('.toolbar select').first
            await state_selector.select_option('Maharashtra')
            for estimate_type in ['actual', 'BE', 'RE']:
                await page.get_by_label('Estimate type', exact=True).select_option(estimate_type)
                if await table.locator('tbody tr').count() < 1 or 'Maharashtra' not in await table.inner_text():
                    raise RuntimeError(f'Maharashtra state road expenditure missing for {estimate_type}')
                if 'rbi_state_road_finances' not in await table.inner_text():
                    raise RuntimeError('State road finance rows lost their RBI primary-source lineage')
            await page.get_by_role('button', name='Debt & repayments', exact=False).click()
            await page.get_by_label('Metric', exact=True).select_option('state_government_guarantees_outstanding_inr_crore')
            guarantee_text = await table.inner_text()
            if await table.locator('tbody tr').count() < 1 or not all(marker in guarantee_text for marker in ['All sectors', 'state_government', 'rbi_state_road_finances']):
                raise RuntimeError('Maharashtra guarantees must remain all-sector state-government observations')
            if 'guarantees are contingent exposures and are not added to debt' not in await page.locator('.analyst-evidence-panel').inner_text():
                raise RuntimeError('Contingent guarantees must not be presented as highway debt')
            await state_selector.select_option('All')
            await page.get_by_role('button', name='NH & SH networks', exact=False).click()
            network_panel = page.locator('.analyst-evidence-panel')
            if await network_panel.locator('tbody tr').count() < 1:
                raise RuntimeError("Comparable highway network disclosures missing")
            road_filter = page.get_by_label('Road class', exact=True)
            road_options = await road_filter.locator('option').all_text_contents()
            sh_class = next((value for value in road_options if value in {'SH', 'State Highway', 'State Highways'}), None)
            if sh_class is None:
                raise RuntimeError('State Highway classification is absent')
            await road_filter.select_option(sh_class)
            if await network_panel.locator('tbody tr').count() < 1:
                raise RuntimeError("State Highway network evidence missing")
            await page.get_by_role('button', name='Funding & outcomes', exact=False).click()
            await page.get_by_role('button', name='Official only', exact=True).click()
            if await page.locator('.source-type.model, .source-type.proxy, .source-type.issuer').count():
                raise RuntimeError("Government-only source filter leaked other evidence types")
            if await page.locator('.chart-title').filter(has_text="Project Economics").count():
                raise RuntimeError("Government-only charts leaked a model")
            await page.get_by_role('button', name='Issuer disclosures', exact=True).click()
            if await page.locator('.source-type.official, .source-type.proxy, .source-type.model').count():
                raise RuntimeError("Issuer filter leaked other evidence types")
            await page.get_by_role('button', name='Model only', exact=True).click()
            await validate_charts(MODEL_CHARTS)
            if await page.locator('.source-type.proxy').count():
                raise RuntimeError("Model sources were incorrectly classified as proxy")
            await page.get_by_role('button', name='Analyst evidence', exact=True).click()
            # Missing observations and empty selections must not change hook order or invent zero.
            state_selector = page.locator('.toolbar select').first
            state_options = await state_selector.locator('option').all_text_contents()
            if 'Ladakh' in state_options:
                await state_selector.select_option('Ladakh')
                await page.get_by_role('button', name='Debt & repayments', exact=False).click()
                if 'No validated observations' not in (await page.locator('.analyst-evidence-panel').inner_text()):
                    raise RuntimeError("Empty state/finance selection must show unavailable evidence")
                await state_selector.select_option('All')
            await page.get_by_role('button', name='Funding & outcomes', exact=False).click()

            await page.set_viewport_size({'width': 390, 'height': 844})
            if await page.evaluate('document.documentElement.scrollWidth > innerWidth + 1'):
                raise RuntimeError('Narrow viewport has page-level horizontal overflow')
            if not await page.get_by_label('Agency', exact=True).is_visible():
                raise RuntimeError('Disclosure filters are not usable on a narrow viewport')
            await page.set_viewport_size({'width': 1280, 'height': 720})

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
