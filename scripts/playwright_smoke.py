#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import re
from pathlib import Path

from typing import Union

import pandas as pd


CANVAS_TEXT_AUDIT = r"""(() => {
    const prototype = CanvasRenderingContext2D.prototype;
    const originalClear = prototype.clearRect;
    const originalText = prototype.fillText;
    prototype.clearRect = function(...args) {
        this.canvas.__textAudit = [];
        return originalClear.apply(this, args);
    };
    prototype.fillText = function(text, x, y, ...args) {
        const result = originalText.call(this, text, x, y, ...args);
        const metrics = this.measureText(String(text));
        const transform = this.getTransform();
        const corners = [
            [x - metrics.actualBoundingBoxLeft, y - metrics.actualBoundingBoxAscent],
            [x + metrics.actualBoundingBoxRight, y - metrics.actualBoundingBoxAscent],
            [x - metrics.actualBoundingBoxLeft, y + metrics.actualBoundingBoxDescent],
            [x + metrics.actualBoundingBoxRight, y + metrics.actualBoundingBoxDescent],
        ].map(([px, py]) => transform.transformPoint(new DOMPoint(px, py)));
        (this.canvas.__textAudit ||= []).push({
            text: String(text),
            left: Math.min(...corners.map(point => point.x)),
            right: Math.max(...corners.map(point => point.x)),
            top: Math.min(...corners.map(point => point.y)),
            bottom: Math.max(...corners.map(point => point.y)),
        });
        return result;
    };
})();"""


async def _assert_canvas_text_bounds(card, integer_years: bool = False):
    canvas = card.locator('canvas')
    audit = await canvas.evaluate("""canvas => ({
        width: canvas.width, height: canvas.height,
        labels: canvas.getAttribute('data-x-tick-labels') || '',
        records: canvas.__textAudit || [],
        name: canvas.getAttribute('aria-label') || '',
    })""")
    if not audit['records']:
        raise RuntimeError('Chart has no audited text')
    clipped = [record['text'] for record in audit['records']
               if record['left'] < -1 or record['right'] > audit['width'] + 1
               or record['top'] < -1 or record['bottom'] > audit['height'] + 1]
    if clipped:
        raise RuntimeError(f"Canvas axis labels clipped: {clipped}")
    if not audit['name']:
        raise RuntimeError('Chart has no accessible unit/axis description')
    if integer_years:
        labels = audit['labels'].split('|')
        if len(labels) < 2 or any(not re.fullmatch(r'\d{4}', label) for label in labels) or labels != sorted(set(labels)):
            raise RuntimeError(f"Year ticks must use observed ungrouped integer years: {labels}")
    return audit


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
        "title": "NHIDCL Monitored NH Portfolio: Reported Stages & Schedule Exposure",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["NHIDCL monitored agency portfolio only", "As of 2026-08-31"],
        "legend_labels": ["Reported completed", "Ongoing past reported schedule", "Ongoing not past reported schedule", "Ongoing schedule unavailable", "Other reported stages"],
        "note_markers": ["not an adjudicated contractual delay", "Unknown schedules stay unavailable", "does not represent all NHAI"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "NH Fatality Burden by State/UT (official NH fatalities)",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["NH length denominator: 2024-12-31"],
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
        "title": "NH Fatality Trend by State/UT (official)",
        "axes": True,
        "data_selector": ".line-path",
        "min_points": 1,
        "meta_markers": ["Official NH fatalities:"],
        "empty_markers": ["No records available."],
    },
    {
        "title": "Economic Scale vs NH Extent by State/UT",
        "axes": True,
        "data_selector": ".point",
        "min_points": 1,
        "meta_markers": ["Latest published GSDP period: 2024-25", "NH length: 2025-12-31", "NHIDCL portfolio: 2026-08-31"],
        "note_markers": ["published national total is excluded", "coverage unavailable, not zero"],
        "legend_labels": ["Each point: State / UT", "Bubble size: NHIDCL monitored NH projects"],
        "empty_markers": ["No scatter points."],
    },
    {
        "title": "NHIDCL Schedule Exposure Relative to Economic Scale",
        "data_selector": ".bar-row",
        "min_points": 1,
        "meta_markers": ["As of"],
        "note_markers": ["latest single-period current-price GSDP", "Schedule exposure is not reported contractual delay", "not whole-NHAI or State Highway coverage"],
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
    names = ["num", "observedAxisTicks", "completeSum", "statePortfolioObservation", "unrectifiedShare", "fmtNum", "sourceTypeTag", "analyticalReady", "disclosureReady", "confidenceFromSources", "humanMetric", "disclosureTheme", "validCitation", "disclosureCutoffKnown", "disclosureEligible", "disclosureMeasured", "disclosureVisible", "disclosureQualifier", "csvText", "deriveDisclosureInsights", "analystHighlights", "netcPaymentHighlights", "netcPaymentSeries", "netcMonthTick", "wideYearFacts", "selectPeriodView", "observationLabel", "completeCalendarQuarterFlows", "normalizeState", "isAggregateStateLabel", "selectionPeriodCoverage", "latestGsdpPeriod", "reportedScheduleDate", "latestNetworkStock", "nhidclPortfolioSnapshot", "portfolioScheduleBurden"]
    aliases = re.search(r'const STATE_ALIASES = \{.*?^\};', source, re.MULTILINE | re.DOTALL)
    if not aliases: raise RuntimeError('Missing state geography aliases')
    blocks = [aliases.group(0)]
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
    check(JSON.stringify(observedAxisTicks([2022,2020,2020,2021,null,''],6)) === '[2020,2021,2022]', 'year ticks retain only observed sorted years');
    check(JSON.stringify(observedAxisTicks([2020,2021,2022],2)) === '[2020,2022]', 'narrow year axes retain observed endpoints');
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
    check(disclosureTheme({metric:'netc_payment_transactions'}) === 'toll' && disclosureTheme({metric:'netc_payment_amount_inr_crore'}) === 'toll', 'NETC payment metrics routing');
    const netc={source_id:'npci_netc_monthly_statistics',entity_type:'payment_network',agency:'NPCI',period_basis:'calendar_month',period_end:'2026-08-31',metric:'netc_payment_transactions',unit:'transactions',value:0};
    check(netcPaymentHighlights([netc])[0].row?.value === 0, 'NETC observed zero is retained');
    check(!netcPaymentHighlights([{...netc,agency:'NHAI'}])[0].row, 'NETC is distinct from NHAI receipts');
    check(!netcPaymentHighlights([{...netc,unit:'PCU'}])[0].row, 'NETC is distinct from PCU traffic');
    check(!netcPaymentHighlights([netc,netc])[0].row, 'ambiguous NETC highlight suppressed');
    const netcActual={...netc,entity_id:'NPCI_NETC',statement_basis:'NETC_payment_statistics',estimate_type:'actual',evidence_class:'issuer_disclosure',analytical_eligible:true,period_start:'2026-08-01',data_as_of:'2026-08-31',citation_url:'https://www.npci.org.in/product/netc/product-statistics'};
    const july={...netcActual,period_start:'2026-07-01',period_end:'2026-07-31',data_as_of:'2026-07-31',value:100};
    const trend=(rows)=>netcPaymentSeries(rows,'netc_payment_transactions','transactions');
    const orderedNetc=trend([netcActual,july]);
    check(orderedNetc.length===2 && orderedNetc[0].label==='2026-07-31' && netcMonthTick(orderedNetc[1].x)==='2026-08-31' && !orderedNetc[1].breakBefore, 'NETC observed month ends in time order');
    check(trend([netcActual,netcActual,july]).length===1, 'duplicate NETC month is suppressed');
    check(trend([{...netcActual,estimate_type:'BE'},{...july,analytical_eligible:false}]).length===0, 'NETC estimates and ineligible rows excluded');
    check(trend([{...netcActual,statement_basis:'NHAI_receipts'},{...july,entity_id:'different_network'}]).length===0, 'NETC incompatible entity or statement scope excluded');
    check(trend([netcActual,{...july,period_start:'2026-05-01',period_end:'2026-05-31',data_as_of:'2026-05-31'}])[1].breakBefore, 'NETC missing months are not connected');
    const observed={source_id:'fixture',value:80,analytical_eligible:true,estimate_type:'actual',evidence_class:'official_measured',data_as_of:'2025-03-31',disclosure_as_of:'2026-02-01',citation_url:'https://example.org/primary'};
    const catalog={fixture:{source_id:'fixture',metric_category:'official_measured',analytical_ready:true,manifest:{row_count:1}}};
    check(disclosureMeasured(observed,catalog), 'actual measured observation');
    check(disclosureMeasured({...observed,evidence_class:'borrower_audited_project_disclosure'},catalog), 'audited borrower actual is a measured observation');
    check(!disclosureEligible({...observed,analytical_eligible:false},catalog), 'unreconciled actual excluded from arithmetic');
    check(!disclosureMeasured({...observed,estimate_type:'BE'},catalog) && !disclosureMeasured({...observed,estimate_type:'RE'},catalog), 'budget estimates are not measured actuals');
    check(!disclosureMeasured({...observed,evidence_class:'target'},catalog), 'targets are not measured actuals');
    check(!disclosureEligible({...observed,data_as_of:''},catalog), 'later disclosure date is not an observation cutoff');
    const unknownBudget={...observed,estimate_type:'BE',data_as_of:'',estimate_vintage:'',period_start:'2024-04-01',period_end:'2025-03-31',period_basis:'financial_year',analytical_eligible:false};
    check(disclosureVisible(unknownBudget,catalog) && disclosureVisible({...unknownBudget,estimate_type:'RE'},catalog), 'supported unknown-vintage fiscal estimates remain visible');
    check(disclosureVisible({...unknownBudget,period_start:'',period_basis:'balance_sheet_snapshot'},catalog), 'unknown-vintage balance-sheet estimates retain their disclosed snapshot period');
    check(!disclosureVisible({...unknownBudget,period_start:'',period_end:''},catalog), 'undated context is not a supported fiscal estimate');
    check(disclosureQualifier(unknownBudget,catalog).includes('estimate vintage not disclosed') && disclosureQualifier(unknownBudget,catalog).includes('excluded from measured totals and comparisons'), 'unknown-vintage estimates clearly qualified');
    check(!disclosureEligible({...unknownBudget,analytical_eligible:true,data_as_of:'2026-02-01'},catalog), 'missing estimate vintage cannot enter arithmetic');
    check(!disclosureEligible({...unknownBudget,analytical_eligible:true,data_as_of:'2026-02-01',estimate_vintage:'2025-02-01'},catalog), 'inconsistent estimate vintage cannot enter arithmetic');
    check(disclosureEligible({...unknownBudget,analytical_eligible:true,data_as_of:'2026-02-01',estimate_vintage:'2026-02-01'},catalog), 'known dated budget estimate remains eligible for matched comparisons');
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
    const budget={...actual,value:100,estimate_type:'BE',estimate_vintage:'2026-03-31'};
    check(derive([actual,budget])[0]?.value === 80, 'matched full-period actual to BE');
    check(derive([{...actual,estimate_type:'YTD'},budget]).length === 0, 'YTD is not full-period actual');
    check(derive([actual,{...budget,data_as_of:'2025-03-31'}]).length === 0, 'budget comparison cutoff');
    check(derive([actual,{...budget,statement_basis:'consolidated'}]).length === 0, 'different accounting basis');
    check(derive([actual,actual,budget]).length === 0, 'ambiguous duplicate actual');
    check(derive([actual,{...budget,estimate_vintage:''}]).length === 0, 'unknown estimate vintage suppresses utilisation');
    check(derive([{...actual,data_as_of:'',disclosure_as_of:'2026-02-01'},{...budget,data_as_of:'',disclosure_as_of:'2026-02-01'}]).length === 0, 'shared later reporting date cannot substitute for missing cutoffs');
    check(csvText([{metric:'=1+1'}],['metric']).includes("'=1+1"), 'CSV spreadsheet text safety');
    const wide=wideYearFacts([{'state/ut':'Kerala','length_constructed_km._2024-25':0,'length_constructed_km._2025-26':12,'unrelated_2027':999}],/^length_constructed_km\._(\d{4}-\d{2})$/,['state/ut'],'fixture');
    check(wide.length===2 && wide[1].period==='2025-26' && wide[0].value===0,'new fiscal columns are discovered without fixed year SQL, observed zero retained');
    const annual=(entity,end,value)=>({...base,entity_id:entity,metric:'gsdp',period_start:`${Number(end.slice(0,4))-1}-04-01`,period_end:end,data_as_of:end,value,unit:'INR crore'});
    const vintage=[annual('A','2024-03-31',100),annual('B','2024-03-31',200),annual('A','2025-03-31',150)];
    check(selectPeriodView(vintage,'latest').find(row=>row.entity_id==='A').value===150,'latest observed per entity');
    check(selectPeriodView(vintage,'common').length===2 && selectPeriodView(vintage,'common').every(row=>row.period_end==='2024-03-31'),'common comparison excludes incomplete state-year coverage');
    check(completeCalendarQuarterFlows([july,netcActual]).length===0,'August partial quarter cannot become Q3 total');
    const september={...netcActual,period_start:'2026-09-01',period_end:'2026-09-30',data_as_of:'2026-09-30',value:200};
    check(completeCalendarQuarterFlows([july,netcActual,september])[0]?.value===300,'complete three-month comparable flows aggregate');
    check(completeCalendarQuarterFlows([july,netcActual,{...september,unit:'PCU'}]).length===0,'quarter requires compatible units');
    check(completeCalendarQuarterFlows([july,netcActual,{...september,observation_status:'provisional'}]).length===0,'quarter cannot silently mix observation statuses');
    check(completeCalendarQuarterFlows([july,netcActual,{...september,period_basis:'project_snapshot'}]).length===0,'project stocks are not monthly flows');
    check(completeCalendarQuarterFlows([july,netcActual,september,september]).length===0,'duplicate month breaks quarter');
    check(completeCalendarQuarterFlows([july,netcActual,september].map(row=>({...row,metric:'debt_outstanding_inr_crore'}))).length===0,'monthly-labelled debt stocks are not additive flows');
    check(selectPeriodView([annual('A','2024-03-31',1),{...annual('A','2025-03-31',2),period_basis:'calendar_quarter'}],'latest').length===2,'latest view preserves different period bases');
    check(selectPeriodView([{...annual('A','2024-03-31',1),observation_status:'final'},{...annual('A','2025-03-31',2),observation_status:'provisional'}],'latest').length===2,'latest view preserves final and provisional scopes');


    check(reportedScheduleDate('29/02/2024')==='2024-02-29' && reportedScheduleDate('31/02/2026')===null && reportedScheduleDate('-')===null, 'invalid/unknown schedules are not dates');
    check(reportedScheduleDate('2026-08-31')==='2026-08-31' && reportedScheduleDate('2026-8-31')===null, 'schedule dates require exact unambiguous format');
    const project={source_id:'nhidcl_monthly_project_progress',entity_type:'project',agency:'NHIDCL',road_class:'National Highway',period_basis:'project_snapshot',statement_basis:'PMP_Data_Lake',entity_id:'P1',state:'Assam',project_stage:'Ongoing',scheduled_completion_date:'01/08/2026',data_as_of:'2026-08-31',analytical_eligible:true};
    const portfolio=nhidclPortfolioSnapshot([project,{...project,metric:'another_metric'}, {...project,entity_id:'P2',project_stage:'Completed'}, {...project,entity_id:'P3',scheduled_completion_date:'-'}, {...project,entity_id:'P4',scheduled_completion_date:'31/08/2026'}, {...project,entity_id:'P5',project_stage:'Terminated'}, {...project,entity_id:'other',agency:'NHAI'}, {...project,entity_id:'mmlp',road_class:'multimodal_logistics'}, {...project,entity_id:'old',data_as_of:'2026-07-31'}]);
    check(portfolio.projects.length===5 && portfolio.rows[0].total_projects===5, 'portfolio counts distinct projects, not metric rows, old snapshots or other scopes');
    check(portfolio.rows[0].completed_projects===1 && portfolio.rows[0].ongoing_past_schedule===1 && portfolio.rows[0].ongoing_unknown_schedule===1 && portfolio.rows[0].ongoing_within_schedule===1 && portfolio.rows[0].other_stage_projects===1, 'reported stages, snapshot boundary and unknown schedules remain separate');
    const conflict=nhidclPortfolioSnapshot([project,{...project,project_stage:'Completed'}]);
    check(conflict.projects.length===0 && conflict.excluded===1, 'conflicting project descriptors are not counted arbitrarily');
    const gsdp={entity_id:'Assam',entity_type:'state_aggregate',state:'Assam',metric:'gsdp_current_prices_inr_crore',unit:'INR crore',price_basis:'current_prices',period_basis:'fiscal_year',period_start:'2024-04-01',period_end:'2025-03-31',data_as_of:'2025-03-31',value:100000};
    check(latestGsdpPeriod([gsdp,{...gsdp,state:'Manipur',entity_id:'Manipur',period_end:'2024-03-31'},{...gsdp,state:'Historical J&K',entity_type:'historical_state_aggregate'}, {...gsdp,state:'Unknown',value:null}]).length===1, 'latest economic comparison uses one current geographic GSDP period, never a mixed latest-per-state panel');
    check(latestGsdpPeriod([gsdp,gsdp]).length===0, 'ambiguous GSDP revisions are not chosen by row order');
    const stock={source_id:'morth_annual_report_2025_26',metric:'nh_network_length_km',entity_type:'state_ut',state:'Assam',road_class:'National Highway',unit:'km',period_basis:'stock',data_as_of:'2025-12-31',value:100};
    check(latestNetworkStock([stock,{...stock,state:'Total',entity_type:'published_total'},{...stock,state:'Manipur',data_as_of:'2024-12-31'},{...stock,state:'Other',period_basis:'project_snapshot'}]).rows.length===1, 'economic network uses latest state stock; never national totals, project lengths or mixed-date fallback');
    check(latestNetworkStock([stock,stock]).rows.length===0, 'ambiguous stock records excluded');
    check(latestNetworkStock([{...stock,value:0}]).rows[0]?.value===0, 'observed zero network stock retained');
    const burden=portfolioScheduleBurden(portfolio.rows,[{state:'Assam',gsdp_current_price:100000}]);
    check(burden.length===1 && burden[0].value===1 && burden[0].unknown_schedule===1, 'schedule exposure uses distinct ongoing projects and crore-normalized GSDP');
    check(portfolioScheduleBurden(portfolio.rows,[{state:'Assam',gsdp_current_price:null}]).length===0 && portfolioScheduleBurden(portfolio.rows,[{state:'Assam',gsdp_current_price:0}]).length===0, 'missing or zero economic denominators never create a zero burden');
    check(portfolioScheduleBurden([{state:'Assam',ongoing_past_schedule:0,ongoing_unknown_schedule:0}],[{state:'Assam',gsdp_current_price:100000}])[0].value===0, 'observed zero schedule exposure retained');
    check(selectionPeriodCoverage([gsdp],'latest',3).includes('2024-04-01 → 2025-03-31') && selectionPeriodCoverage([gsdp],'latest',3).includes('3 historical or undated'), 'latest coverage dates and retained history explicit');


    const reviewedNhai={source_id:'nhai_financial_results_2025_26_to_2026_06',entity_id:'nhai',agency:'NHAI',entity_type:'agency',metric:'debt_total_inr_crore',statement_basis:'NHAI_standalone_debt_working',assurance:'limited_review_unaudited',observation_status:'reported',value:195303.7117,unit:'INR crore',data_as_of:'2026-06-30',period_end:'2026-06-30',estimate_type:'actual',evidence_class:'official_measured',analytical_eligible:true,citation_url:'https://nhai.gov.in/financial-results'};
    check(disclosureTheme(reviewedNhai)==='debt' && disclosureTheme({...reviewedNhai,metric:'net_worth_inr_crore'})==='debt', 'NHAI reviewed debt and net worth use debt panel');
    check(disclosureTheme({...reviewedNhai,metric:'current_assets_inr_crore',statement_basis:'NHAI_standalone_balance_sheet'})==='debt', 'NHAI classic balance sheet has its own finance scope');
    check(disclosureTheme({...reviewedNhai,metric:'arbitration_contingent_claims_inr_crore',statement_basis:'NHAI_contingent_claims'})==='debt', 'NHAI contingent claims remain disclosed exposures');
    check(disclosureTheme({...reviewedNhai,metric:'toll_collection_inr_crore',statement_basis:'NHAI_gross_toll_collections'})==='toll' && disclosureTheme({...reviewedNhai,metric:'toll_deposit_cfi_inr_crore',statement_basis:'NHAI_toll_deposits_CFI'})==='toll', 'NHAI gross toll and CFI deposits route without combining accounting scopes');
    check(disclosureTheme({...reviewedNhai,metric:'toll_adjusted_ploughback_cash_inr_crore',statement_basis:'NHAI_standalone_cash_flow'})==='toll', 'NHAI annual toll cash ploughback stays distinct from quarterly collections');
    check(disclosureTheme({...reviewedNhai,metric:'other_income_inr_crore',statement_basis:'NHAI_standalone_income_expenditure'})==='funding' && disclosureTheme({...reviewedNhai,metric:'capital_road_work_cash_inr_crore',statement_basis:'NHAI_standalone_cash_flow'})==='funding', 'NHAI authority income and cash funding do not imply project delivery or InvIT earnings');
    check(disclosureTheme({...reviewedNhai,metric:'invit_proceeds_cash_inr_crore',statement_basis:'NHAI_standalone_cash_flow'})==='monetisation' && disclosureTheme({...reviewedNhai,metric:'ncd_redemption_cash_inr_crore',statement_basis:'NHAI_standalone_cash_flow'})==='debt', 'NHAI cash proceeds and debt redemption preserve their economic roles');
    check(disclosureTheme({source_id:'nhit_quarterly_operations_finance',metric:'ebitda_inr_crore'})==='monetisation' && disclosureTheme({source_id:'nhit_quarterly_operations_finance',metric:'dscr_ratio'})==='debt', 'existing NHIT routes remain unchanged');
    check(observationLabel(reviewedNhai).includes('reported · limited_review_unaudited'), 'NHAI review assurance retained without audited classification');
    const reviewedCatalog={[reviewedNhai.source_id]:{source_id:reviewedNhai.source_id,metric_category:'official_measured',analytical_ready:true,disclosure_ready:true,manifest:{row_count:1}}};
    check(disclosureMeasured(reviewedNhai,reviewedCatalog) && !disclosureVisible({...reviewedNhai,value:'-'},reviewedCatalog), 'reviewed reported facts visible; missing filing dashes never become zero');
    const earlierReviewed={...reviewedNhai,value:200000,data_as_of:'2026-03-31',period_end:'2026-03-31',observation_status:'revised'};
    check(analystHighlights([earlierReviewed,reviewedNhai,{...reviewedNhai,value:999999,statement_basis:'different_basis'}])[0].row?.value===195303.7117, 'NHAI headline uses exact reviewed standalone basis and latest observation regardless revision status');
    check(!analystHighlights([reviewedNhai,reviewedNhai])[0].row, 'ambiguous current reviewed debt headline suppressed');
    check(selectPeriodView([{...reviewedNhai,metric:'current_assets_inr_crore'},{...reviewedNhai,metric:'current_assets_inr_crore',statement_basis:'NHAI_standalone_balance_sheet'}],'latest').length===2, 'classic and working asset definitions are not folded together');

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
        await context.add_init_script(script=CANVAS_TEXT_AUDIT)
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
                "NH Fatality Trend by State/UT (official)",
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
            if "India Highway Explorer" not in header:
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
            portfolio_card = page.locator('.insight-chart').filter(has=page.locator('.chart-title', has_text='NHIDCL Monitored NH Portfolio: Reported Stages & Schedule Exposure'))
            if await portfolio_card.locator('a[href^="https://"]').count() < 1:
                raise RuntimeError('NHIDCL portfolio must retain its primary citation')
            first_portfolio_row = portfolio_card.locator('.bar-row').first
            await first_portfolio_row.focus()
            if 'Ongoing schedule unavailable:' not in (await first_portfolio_row.get_attribute('aria-label') or ''):
                raise RuntimeError('Portfolio keyboard labels must expose every stage, including unknown schedules')
            economic_card = page.locator('.insight-chart').filter(has=page.locator('.chart-title', has_text='Economic Scale vs NH Extent by State/UT'))
            if await economic_card.locator('a[href^="https://"]').count() != 3:
                raise RuntimeError('Economic comparison must cite GSDP, network stock and monitored portfolio separately')
            await economic_card.locator('canvas').focus()
            await economic_card.locator('canvas').press('End')
            economic_keyboard_text = await economic_card.get_by_role('status').inner_text()
            if not all(marker in economic_keyboard_text for marker in ['GSDP', 'NH length (km)', 'NHIDCL', '2024-25', '2026-08-31']):
                raise RuntimeError('Economic chart keyboard readout lost units, date or portfolio context')
            for viewport in [{'width': 1440, 'height': 1100}, {'width': 390, 'height': 844}]:
                await page.set_viewport_size(viewport)
                await page.wait_for_timeout(250)
                for title, integer_years in [('NH Fatality Trend by State/UT (official)', True), ('Economic Scale vs NH Extent by State/UT', False)]:
                    card = page.locator('.insight-chart').filter(has=page.locator('.chart-title', has_text=title))
                    await _assert_canvas_text_bounds(card, integer_years)
            await page.set_viewport_size({'width': 1280, 'height': 720})
            module_url = await page.evaluate("new URL('src/app.js', location.href).href")
            module_response = await page.request.get(module_url)
            if not module_response.ok:
                raise RuntimeError('Cannot inspect the deployed calculation module')
            semantic_failures = await page.evaluate(_frontend_fixture_script(await module_response.text()))
            if semantic_failures:
                raise RuntimeError(f'Frontend semantic fixtures failed: {semantic_failures}')
            await page.get_by_role("heading", name="Finance & infrastructure disclosures", exact=True).wait_for()
            for label in ["Agency", "Road class", "Reporting period", "Estimate type", "Evidence class", "Metric", "Entity search", "Period view", "Observation status", "Assurance"]:
                if await page.get_by_label(label, exact=True).count() != 1:
                    raise RuntimeError(f"Missing or ambiguous disclosure filter: {label}")
            table = page.locator('.analyst-evidence-panel .evidence-table')
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
            for field in ['observation_status', 'assurance', 'revision_identity', 'price_basis', 'base_year', 'original_unit', 'period_basis', 'statement_basis', 'disclosure_as_of', 'estimate_vintage', 'reported_period', 'citation_url', 'table_page', 'source_document_sha256']:
                if field not in csv_text.splitlines()[0]:
                    raise RuntimeError(f"CSV lost lineage field: {field}")
            if len(csv_text.splitlines()) < 2:
                raise RuntimeError("CSV export has no observation rows")
            if await page.get_by_label('Period view', exact=True).input_value() != 'latest':
                raise RuntimeError('Analyst evidence must default to latest available per compatible entity scope')
            coverage_summary = await page.get_by_test_id('selection-period-coverage').inner_text()
            if not all(marker in coverage_summary for marker in ['periods may differ', 'Selected reporting periods', 'Observation cutoffs', 'All history', 'Unknown estimate vintages']):
                raise RuntimeError('Latest table view must disclose selected periods and retained history')

            reviewed_debt_card = page.locator('.analyst-highlight').filter(has=page.get_by_role('heading', name='NHAI standalone debt (unaudited)', exact=True))
            reviewed_debt_text = await reviewed_debt_card.inner_text()
            if not all(marker in reviewed_debt_text for marker in ['1,95,303.71', 'limited_review_unaudited', 'NHAI_standalone_debt_working', '2026-06-30']):
                raise RuntimeError('Latest NHAI standalone debt card lost figure, review class, scope or cutoff')
            async def selected_csv_rows():
                async with page.expect_download() as event:
                    await page.get_by_role('button', name='Download filtered evidence CSV', exact=True).click()
                downloaded = await event.value
                return list(csv.DictReader(io.StringIO(Path(await downloaded.path()).read_text(encoding='utf-8-sig'))))
            await page.get_by_role('button', name='Debt & repayments', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('NHAI')
            await page.get_by_label('Metric', exact=True).select_option('debt_total_inr_crore')
            await page.get_by_label('Reporting period', exact=True).select_option('? → 2026-06-30 (balance_sheet_snapshot)')
            reviewed_debt = await selected_csv_rows()
            if len(reviewed_debt) != 1 or abs(float(reviewed_debt[0]['value']) - 195303.7117) > .000001 or reviewed_debt[0]['statement_basis'] != 'NHAI_standalone_debt_working':
                raise RuntimeError('June reviewed NHAI debt filter lost canonical single-scope record')
            if not all(reviewed_debt[0][field] == value for field, value in [('unit','INR crore'),('original_unit','INR lakh'),('assurance','limited_review_unaudited'),('data_as_of','2026-06-30'),('published_at','')]):
                raise RuntimeError('Reviewed NHAI debt export lost units, assurance or unknown publication date')
            if 'contingent claims are disclosed exposures, not actual borrowings' not in await page.locator('.analyst-evidence-panel').inner_text():
                raise RuntimeError('Contingent claims must not imply actual borrowings')
            await page.get_by_role('button', name='Funding & outcomes', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('NHAI')
            await page.get_by_label('Metric', exact=True).select_option('total_income_inr_crore')
            await page.get_by_label('Reporting period', exact=True).select_option('2026-04-01 → 2026-06-30 (fiscal_quarter)')
            if await table.locator('tbody tr').count() != 1 or 'NHAI_standalone_income_expenditure' not in await table.inner_text():
                raise RuntimeError('NHAI authority earnings must be scoped funding evidence')
            await page.get_by_role('button', name='Toll & traffic', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('NHAI')
            await page.get_by_label('Reporting period', exact=True).select_option('2026-04-01 → 2026-06-30 (fiscal_quarter)')
            reviewed_toll = await selected_csv_rows()
            if len(reviewed_toll) != 2 or {row['metric'] for row in reviewed_toll} != {'toll_collection_inr_crore','toll_deposit_cfi_inr_crore'} or len({row['statement_basis'] for row in reviewed_toll}) != 2:
                raise RuntimeError('NHAI quarter gross toll and CFI deposits must remain separate records')
            if {row['metric']:round(float(row['value']),4) for row in reviewed_toll} != {'toll_collection_inr_crore':10692.0625,'toll_deposit_cfi_inr_crore':10700.8666}:
                raise RuntimeError('June NHAI toll/deposit evidence differs from reviewed source figures')
            await page.get_by_role('button', name='Project delivery', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('UPEIDA')
            await page.get_by_label('Reporting period', exact=True).select_option('? → 2026-04-27 (project_snapshot)')
            ganga = await selected_csv_rows()
            if len(ganga) != 8 or any(row['source_id'] != 'upeida_ganga_progress_2026_04_27' or row['data_as_of'] != '2026-04-27' for row in ganga):
                raise RuntimeError('Ganga dated project progress lost its explicit snapshot cutoff or project scope')
            if next((float(row['value']) for row in ganga if row['metric'] == 'overall_progress_percent'), None) != 98:
                raise RuntimeError('Ganga reported overall progress must remain 98 percent')
            await page.get_by_role('button', name='Funding & outcomes', exact=False).click()

            # Explicitly retain historical and unknown-vintage estimate coverage in history mode.
            await page.get_by_label('Period view', exact=True).select_option('all')
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
                estimate_text = await table.inner_text()
                if estimate_type in {'BE', 'RE'} and not all(marker in estimate_text for marker in ['Estimate vintage: not disclosed', 'estimate vintage not disclosed', 'excluded from measured totals and comparisons', 'Disclosure as of:']):
                    raise RuntimeError('Unknown-vintage RBI estimates must remain visible with later reporting dates and comparison exclusions')
            async with page.expect_download() as rbi_download_info:
                await page.get_by_role('button', name='Download filtered evidence CSV', exact=True).click()
            rbi_download = await rbi_download_info.value
            rbi_rows = list(csv.DictReader(io.StringIO(Path(await rbi_download.path()).read_text(encoding='utf-8-sig'))))
            if not rbi_rows or any(row['estimate_type'] != 'RE' or row['data_as_of'] or row['estimate_vintage'] or row['analytical_eligible'] != 'false' or not row['disclosure_as_of'] or not row['reported_period'] for row in rbi_rows):
                raise RuntimeError('RBI unknown-vintage RE export must preserve reported periods and later disclosures without inventing observation cutoffs')
            await page.get_by_label('Estimate type', exact=True).select_option('actual')
            await page.get_by_label('Reporting period', exact=True).select_option('2023-04-01 → 2024-03-31 (fiscal_year)')
            historical_text = await table.inner_text()
            if not all(marker in historical_text for marker in ['As of: 2024-03-31', 'Disclosure as of:', 'Source period label: 2023-24']):
                raise RuntimeError('Historical RBI actuals must retain fiscal observation cutoffs separately from later disclosure dates')
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
            await page.get_by_role('button', name='Toll & traffic', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('NPCI')
            netc_cards = page.get_by_role('region', name='NETC payment-network observations', exact=True)
            if await netc_cards.count() != 1:
                raise RuntimeError('Separate NETC payment-network cards are missing')
            netc_panel_text = await page.locator('.analyst-evidence-panel').inner_text()
            for marker in ['NETC payment transactions', 'NETC payment amount', 'calendar_month', 'annual-pass', 'Maharashtra', 'million transactions', 'payment_network']:
                if marker not in netc_panel_text:
                    raise RuntimeError(f'NETC scope, periods, units or exclusions missing: {marker}')
            await validate_charts([
                {'title': 'NETC monthly payment transactions', 'axes': True, 'data_selector': 'canvas', 'min_points': 1, 'legend_labels': ['Transactions (count)', 'Points:'], 'meta_markers': ['Publication date not disclosed', 'annual-pass', 'Maharashtra', 'As of']},
                {'title': 'NETC monthly payment amount', 'axes': True, 'data_selector': 'canvas', 'min_points': 1, 'legend_labels': ['Payment amount (₹ crore)', 'Points:'], 'meta_markers': ['Publication date not disclosed', 'annual-pass', 'Maharashtra', 'As of']},
            ])
            await page.get_by_label('Period view', exact=True).select_option('latest')
            if await table.locator('tbody tr').count() != 2 or '2026-08-31' not in await table.inner_text():
                raise RuntimeError('Latest NETC evidence must expose the two latest monthly metrics')
            netc_trends = page.get_by_role('region', name='NETC monthly payment trends', exact=True)
            for chart in await netc_trends.locator('canvas').all():
                if int(await chart.get_attribute('data-point-count')) < 17 or await chart.get_attribute('data-period-start') != '2025-04-30' or (await chart.get_attribute('data-period-end')) < '2026-08-31':
                    raise RuntimeError('NETC trend lost observed monthly coverage or calendar order')
                await chart.focus()
                await chart.press('End')
                current_point = await chart.locator('..').locator('..').get_by_role('status').inner_text()
                if (await chart.get_attribute('data-period-end')) not in current_point:
                    raise RuntimeError('NETC chart keyboard readout does not expose the selected month')
            if await netc_trends.locator('a[href="https://www.npci.org.in/product/netc/product-statistics"]').count() != 2:
                raise RuntimeError('NETC trends lost their primary citations')
            await page.get_by_label('Period view', exact=True).select_option('all')
            async with page.expect_download() as netc_download_info:
                await page.get_by_role('button', name='Download filtered evidence CSV', exact=True).click()
            netc_download = await netc_download_info.value
            netc_csv = Path(await netc_download.path()).read_text(encoding='utf-8-sig')
            netc_rows = list(csv.DictReader(io.StringIO(netc_csv)))
            if len(netc_rows) < 34 or {row['metric'] for row in netc_rows} != {'netc_payment_transactions', 'netc_payment_amount_inr_crore'}:
                raise RuntimeError('NETC download must retain both separately reported monthly payment metrics')
            for row in netc_rows:
                if row['source_id'] != 'npci_netc_monthly_statistics' or row['period_basis'] != 'calendar_month' or row['entity_type'] != 'payment_network' or len(row['source_document_sha256']) != 64:
                    raise RuntimeError('NETC monthly download lost scope or governed document lineage')
                if row['metric'] == 'netc_payment_transactions' and (row['unit'] != 'transactions' or row['original_unit'] != 'million transactions' or abs(float(row['value']) - float(row['original_value']) * 1_000_000) > .01):
                    raise RuntimeError('NETC transaction normalization or original units are incorrect')
                if row['metric'] == 'netc_payment_amount_inr_crore' and row['unit'] != 'INR crore':
                    raise RuntimeError('NETC payment amount must remain separate currency observations')
            await page.get_by_label('Metric', exact=True).select_option('netc_payment_transactions')
            period_filter = page.get_by_label('Reporting period', exact=True)
            month_option = next((option for option in await period_filter.locator('option').all_text_contents() if '2026-08-01' in option and '2026-08-31' in option), None)
            if month_option is None:
                raise RuntimeError('NETC monthly reporting-period filter is absent')
            await period_filter.select_option(month_option)
            if await table.locator('tbody tr').count() != 1 or 'transactions' not in await table.inner_text():
                raise RuntimeError('NETC monthly transaction filter must show one separately scoped observation')
            await page.get_by_role('button', name='Project delivery', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('MPWD')
            async with page.expect_download() as audited_download_info:
                await page.get_by_role('button', name='Download filtered evidence CSV', exact=True).click()
            audited_download = await audited_download_info.value
            audited_rows = list(csv.DictReader(io.StringIO(Path(await audited_download.path()).read_text(encoding='utf-8-sig'))))
            measured_project_rows = [row for row in audited_rows if row['analytical_eligible'] == 'true']
            qualified_project_rows = [row for row in audited_rows if row['analytical_eligible'] == 'false']
            if len(measured_project_rows) != 70 or len(qualified_project_rows) != 1 or qualified_project_rows[0]['metric'] != 'project_cash_bank_balance_inr_crore':
                raise RuntimeError('ADB audited actuals and unreconciled reported aggregate lost eligibility distinctions')
            for row in audited_rows:
                if row['source_id'] != 'adb_state_road_projects' or not row['entity_id'].startswith('adb_52298_001') or row['estimate_type'] != 'actual' or row['evidence_class'] != 'borrower_audited_project_disclosure':
                    raise RuntimeError('ADB project statements must retain project identity and audited actual scope')
                if row['unit'] != 'INR crore' or row['original_unit'] != 'INR thousand' or abs(float(row['value']) - float(row['original_value']) * .0001) > .000001:
                    raise RuntimeError('ADB financial statement export lost original units or currency normalization')
            if '(0)' not in await page.locator('.comparable-insights summary').inner_text():
                raise RuntimeError('Project actual costs must not be divided by a 450-km target length')
            await page.get_by_label('Metric', exact=True).select_option('project_civil_works_expenditure_inr_crore')
            audited_project_text = await table.inner_text()
            if await table.locator('tbody tr').count() < 1 or not all(marker in audited_project_text for marker in ['borrower audited project disclosure', 'INR thousand', 'adb_state_road_projects']):
                raise RuntimeError('Audited borrower project expenditure lost its evidence class, original units or ADB lineage')
            if 'excluded from measured calculations' in audited_project_text:
                raise RuntimeError('Eligible audited borrower actuals must not be labelled as estimates')
            await page.get_by_role('button', name='Funding & outcomes', exact=False).click()
            await page.get_by_label('Agency', exact=True).select_option('MPWD')
            await page.get_by_label('Evidence class', exact=True).select_option('target')
            if await table.locator('tbody tr[data-evidence-class="target"]').count() < 1 or 'excluded from measured calculations' not in await table.inner_text():
                raise RuntimeError('ADB project targets must remain separate from audited actuals')
            async with page.expect_download() as target_download_info:
                await page.get_by_role('button', name='Download filtered evidence CSV', exact=True).click()
            target_download = await target_download_info.value
            target_rows = list(csv.DictReader(io.StringIO(Path(await target_download.path()).read_text(encoding='utf-8-sig'))))
            if len(target_rows) != 6 or any(row['analytical_eligible'] != 'false' or row['evidence_class'] != 'target' for row in target_rows):
                raise RuntimeError('ADB target export must preserve all six planning observations as ineligible')
            await page.get_by_role('button', name='Debt & repayments', exact=False).click()
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
            await page.get_by_role('button', name='Safety', exact=False).click()
            await page.get_by_label('Reporting period', exact=True).select_option('2024-01-01 → 2024-12-31 (calendar_year)')
            await page.get_by_label('Road class', exact=True).select_option('National Highway')
            if 'morth_road_accidents_2024_final' not in await table.inner_text():
                raise RuntimeError('Final 2024 NH safety evidence is missing')
            await page.get_by_role('button', name='Economic context', exact=False).click()
            await page.get_by_label('Period view', exact=True).select_option('latest')
            await page.get_by_label('Reporting period', exact=True).select_option('2024-04-01 → 2025-03-31 (fiscal_year)')
            latest_gsdp_text = await table.inner_text()
            if not all(marker in latest_gsdp_text for marker in ['rbi_gsdp_current_prices_2024_25', 'current', '2025-03-31']):
                raise RuntimeError('Latest GSDP evidence lost source, price basis or observation cutoff')
            await page.get_by_label('Reporting period', exact=True).select_option('2022-04-01 → 2023-03-31 (fiscal_year)')
            if await table.locator('tbody tr').count() < 1 or 'As of: 2023-03-31' not in await table.inner_text() or await page.get_by_label('Period view', exact=True).input_value() != 'latest':
                raise RuntimeError('Explicit historical reporting-period filters must retain their own latest compatible observations')

            await page.get_by_role('button', name='State road spending', exact=False).click()
            for state, sid in [('Karnataka','cag_karnataka_road_finances_2024_25'), ('Maharashtra','cag_maharashtra_road_finances_2024_25'), ('Gujarat','cag_gujarat_road_finances_2024_25'), ('Uttar Pradesh','cag_uttar_pradesh_road_finances_2024_25'), ('Telangana','cag_telangana_road_finances_2024_25')]:
                await state_selector.select_option(state)
                await page.get_by_label('Reporting period', exact=True).select_option('2024-04-01 → 2025-03-31 (fiscal_year)')
                if sid not in await table.inner_text():
                    raise RuntimeError(f'Audited FY2024-25 road finance evidence missing for {state}')
            await state_selector.select_option('All')
            coverage = page.get_by_test_id('metric-coverage')
            await coverage.locator('summary').click()
            coverage_text = await coverage.inner_text()
            if not all(marker in coverage_text for marker in ['2026-08-31', 'Published:', 'Checked:', 'publication lag:', 'Schedule not disclosed', 'not_yet_reported']):
                raise RuntimeError('Metric freshness lost publication gaps or separate observation/check dates')
            await coverage.locator('summary').click()
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
