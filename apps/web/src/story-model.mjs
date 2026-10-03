export const EVIDENCE_HASHES = Object.freeze({
  'observations.v1.json': '931a71fa74cb0f3e0af66739d70922139d2725a17758f691332bd2cd169a1cd4',
  'calculations.v1.json': '6366e2e0c61cd0c6ac4fe0973eedf3d00cf0c6ca143b76629d8893af3619740a',
  'sources.v1.json': 'bb6ffc7dc5ce3f7b94cfbaf93ce3261420dc8ded83309a8346617abaa43f2714',
});

export function evidenceIndex(observations, calculations, sources) {
  if ([observations, calculations, sources].some(document => document?.schema_version !== '1.0.0')) throw new Error('Unsupported evidence version');
  if (observations.observations?.length !== 548 || calculations.calculations?.length !== 55 || sources.sources?.length !== 28) throw new Error('Incomplete evidence bundle');
  const rows = new Map(), references = new Map(), recipes = new Map();
  for (const source of sources.sources) {
    const url = new URL(source.url);
    if (url.protocol !== 'https:' || url.username || url.password || references.has(source.url)) throw new Error('Invalid source reference');
    references.set(source.url, source);
  }
  for (const row of observations.observations) {
    if (!/^[A-Za-z0-9_-]+$/.test(row.id) || rows.has(row.id)) throw new Error('Invalid observation ID');
    for (const field of ['metric','unit','entity','perimeter','reporting_period','as_of','audit_status','definition','uncertainty','document_page']) {
      if (typeof row[field] !== 'string' || !row[field]) throw new Error('Missing observation context');
    }
    if (!['reported','reported_estimate','derived','unavailable'].includes(row.value_status)) throw new Error('Unknown evidence status');
    if ((row.value === null) !== (row.value_status === 'unavailable')) throw new Error('Missing evidence must remain unavailable');
    if (row.value !== null && (typeof row.value !== 'string' || !/^-?\d+(?:\.\d+)?$/.test(row.value) || !Number.isFinite(Number(row.value)))) throw new Error('Invalid observation value');
    const source = references.get(row.source_url);
    if (!source?.observation_ids.includes(row.id) || !source.entities.includes(row.entity) || !source.page_references.includes(row.document_page)) throw new Error('Source lineage differs');
    rows.set(row.id, row);
  }
  for (const recipe of calculations.calculations) {
    const row = rows.get(recipe.id);
    if (recipes.has(recipe.id) || row?.value_status !== 'derived' || row.value !== recipe.expected_decimal || row.unit !== recipe.unit || JSON.stringify(row.calculation_input_ids) !== JSON.stringify(recipe.input_ids) || recipe.input_ids.some(id => !rows.has(id))) throw new Error('Calculation lineage differs');
    recipes.set(recipe.id, recipe);
  }
  return {rows, references, recipes, cutoff:observations.research_as_of, get(id) { const row = rows.get(id); if (!row) throw new Error('Missing required observation: '+id); return row; }};
}

export function formatValue(row, decimals) {
  if (row.value === null) return 'Unavailable';
  if (decimals !== undefined) return Number(row.value).toLocaleString('en-IN',{minimumFractionDigits:decimals, maximumFractionDigits:decimals});
  const negative = row.value.startsWith('-');
  const [whole, fraction] = row.value.replace(/^-/, '').split('.');
  return (negative ? '−' : '') + BigInt(whole).toLocaleString('en-IN') + (fraction ? '.'+fraction : '');
}

const table = (title, columns, rows) => ({title, columns, rows});
const fact = (label, id, decimals) => ({label,id,decimals});

export function exhibits(index) {
  const history = ['2020','2021','2022','2023','2024'].map(year => {
    const ids = ['government_capital','borrowings_including_ADB'].map(metric => [...index.rows.values()].find(row => row.entity==='NHAI' && row.metric===metric && row.as_of===year+'-03-31')?.id);
    if (ids.some(id => !id)) throw new Error('Missing classic NHAI history');
    return {label:'FY'+year, ids};
  });
  const nhit = ['2022','2023','2024','2025','2026'].map(year => ({label:'FY'+year+(year==='2022'?' (partial operations)':''), ids:['operating_revenue','book_assets','external_borrowings_carrying','DPU'].map(metric => 'NHIT-FY'+year+'_'+metric)}));
  const chapters = [
    {id:'public-authority', title:'Funding the public authority', description:'Government capital and borrowing are different sources of finance. Read each later disclosure within its own accounting scope.',
      warning:'FY2020–FY2024 uses NHAI statutory standalone classic sources-and-applications accounts, with CAG accounting observations. Government capital is not market equity and excludes the separate grant balance. The budget debt series and the latest gross-assets working table have different scopes; they are shown separately.',
      tables:[table('NHAI classic statutory balance-sheet history — stocks at 31 March, INR crore',['Period','Government capital (INR crore)','Borrowings including ADB (INR crore)'],history),
        table('Budget disclosure — a separate debt perimeter, INR crore',['Observation','Value'],[fact('FY2024 outstanding liabilities','NHAI-C130'),fact('FY2025 outstanding liabilities','NHAI-C131'),fact('Change, FY2025 versus FY2024 (%)','CALC-nhai_budget_debt_change_FY25',2)]),
        table('FY2025–26 administrative funding — INR crore; official release, not audited accounts',['Observation','Value'],[fact('Government budget support','NHAI-C188'),fact('Own resources','NHAI-C189'),fact('Administrative expenditure','NHAI-C187'),fact('Budget support / expenditure (%)','NHAI-C191',2)]),
        table('NHAI at 30 June 2026 — limited-review unaudited working table, INR crore',['Observation','Value'],[fact('Gross assets','NHAI-C148'),fact('Net worth / other equity','NHAI-C145'),fact('Total liabilities','NHAI-C147'),fact('Reported debt','NHAI-C144'),fact('Current liabilities','NHAI-C146')]),
        table('Separate future commitments — nominal, undiscounted; not equivalent borrowing',['Observation','Value'],[fact('Approved unpaid annuity obligations at 31 March 2025','NHAI-C195')]),
        table('Comparable annual accounting evidence gap',['Observation','Evidence'],[fact('FY2025 classic audited-basis balance sheet','MISSING-NHAI_FY25_classic_comparable')])],
      notes:['Budget support includes recycled toll and monetisation receipts; its share is not a fresh-taxpayer-subsidy share. Budget debt excludes some statutory borrowing items.', 'The old and new asset tables have different accounting bases. The government ownership and operational-control interpretations are recorded in the underlying accounting disclosures. Future annuity commitments must remain separate from borrowing.']},
    {id:'portfolio-scale',title:'Scale from acquisitions and cash per unit',description:'A larger portfolio can raise revenue without measuring organic growth on the same roads.',
      warning:'FY2022 includes partial operations. The portfolio perimeter changes with acquisitions. These figures do not establish traffic growth, a like-for-like CAGR, or a return on closing book assets. DPU uses fiscal-attributable distributions, not distributions inferred from closing units.',
      tables:[table('NHIT annual reported series — consolidated book values and fiscal-attributable DPU',['Period','Operating revenue (INR crore)','Book assets (INR crore)','Carrying external borrowing (INR crore)','DPU (INR/unit)'],nhit),
        table('FY2026 revenue bridge — rounded SPV disclosures, INR crore except share',['Observation','Value'],[fact('Consolidated revenue increase','CALC-FY26_revenue_delta'),fact('New NSPPL reported revenue','SPV-FY2026-NSPPL'),fact('Existing NWPPL + NEPPL revenue increase','CALC-existing_spv_delta'),fact('Source rounding residual','CALC-growth_rounding_residual'),fact('Approximate new-SPV share (%)','CALC-new_spv_share',2),fact('Existing-SPV reported revenue change (%)','CALC-existing_spv_growth',2)]),
        table('NHIT FY2026 book capital — INR crore, ratios with separate denominators',['Observation','Value'],[fact('Book equity','NHIT-FY2026_book_equity'),fact('Net unit capital — a separate capital definition','NHIT-FY2026_unit_capital_net_costs'),fact('Carrying external borrowing','NHIT-FY2026_external_borrowings_carrying'),fact('Other liabilities residual','CALC-nhit_other_liabilities'),fact('Book debt / equity (times)','CALC-nhit_book_debt_equity',2),fact('Book debt / assets (%)','CALC-nhit_book_debt_assets',2)]),
        table('Fair-value regulatory perimeter — separate from book values',['Observation','Value'],[fact('31 March 2026 SPV enterprise value (INR crore)','NHIT-FY26_spvev'),fact('31 March Trust component (INR crore)','NHIT-FY26_trust_ev_component'),fact('31 March total InvIT asset value (INR crore)','CALC-FY26_evbridge'),fact('30 June total InvIT asset value (INR crore)','CALC-Q1FY27_evbridge'),fact('31 March fair NAV (INR/unit)','NHIT-FY26_fairnav'),fact('31 March book NAV (INR/unit)','NHIT-FY26_booknav')]),
        table('Latest unaudited quarter — changed portfolio scope; INR crore',['Observation','Value'],[fact('Q1 FY2026 reported operating revenue comparative','NHIT-Q1FY26_oprev'),fact('Q1 FY2027 operating revenue','NHIT-Q1FY27_oprev'),fact('Q1 FY2027 Round 5 contribution','NHIT-Q1FY27_R5_revenue')])],
      notes:['New-SPV attribution is an approximation from rounded issuer figures. The source rounding residual is retained; this bridge is not a traffic-growth estimate.', 'Latest-quarter figures are unaudited and have a changed portfolio scope. Book equity, net unit capital, fair NAV and regulatory net debt retain separate definitions. Intra-group SPV receivables must not be added to consolidated external borrowing.', ...nhit.map(row=>row.label+': '+index.get(row.ids[0]).uncertainty)]},
    {id:'distribution',title:'Assemble the distribution',description:'The Trust cash calculation and the legal character of distributions answer different questions.',
      warning:'Trust NDCF includes specified SPV cash received after year end and before the board meeting. Adjusted cash is not consolidated in-year cash generation or cash paid during the fiscal year. Reserve release does not change the legal DPU classification into return of capital.',
      tables:[table('FY2026 Trust distribution-cash bridge — INR crore',['Component','Value'],[fact('Base Trust NDCF','NHIT-FY26_ndcf_base'),fact('Unpaid ZCB interest adjustment','NHIT-FY26_zcb_addback'),fact('First DSRA release','NHIT-FY26_dsra_release_q1'),fact('Second DSRA release','NHIT-FY26_dsra_release_q2'),fact('Adjusted cash available','CALC-ndcf_bridge'),fact('Adjustments / adjusted cash (%)','CALC-ndcf_adjustment_share',3)]),
        table('FY2026 fiscal-attributable DPU — INR/unit',['Component','Value'],[fact('Interest','NHIT-FY2026_DPU_interest'),fact('Other income','NHIT-FY2026_DPU_other'),fact('Dividend','NHIT-FY2026_DPU_dividend'),fact('Return of capital','NHIT-FY2026_DPU_return_of_capital'),fact('Total DPU','NHIT-FY2026_DPU')]),
        table('Annual-pass timing — INR crore; timing gap does not establish delinquency',['Observation','Value'],[fact('Recognised compensation','NHIT-FY26_pass'),fact('Cash received within fiscal year','NHIT-FY26_annual_pass_cash_received'),fact('Timing gap','CALC-pass_timing')]),
        table('Issuer NAV-based return decomposition — percent; not market return or investor IRR',['Observation','Value'],[fact('Distribution yield on opening NAV','NHIT-FY26_nav_yield'),fact('Appraisal NAV change','NHIT-FY26_nav_change'),fact('Issuer NAV-based total return','NHIT-FY26_nav_total_return'),fact('Issuer adjusted reallocation method','NHIT-FY26_nav_adjusted_return')]),
        table('Separate reserves retained for major maintenance — INR crore',['Observation','Value'],[fact('Retained unitholder-funded reserves','NHIT-FY26_dsra_unit_retain')])],
      notes:['Operating-cash-funded reserves were replaced by bank guarantees. Their costs and conditions are not measured by this cash bridge. Unitholder-funded reserves retained for major maintenance are a separate amount.', 'Distribution methods and timing have changed. Do not infer total distributions by multiplying annual DPU by closing units, or turn these observations into a homogeneous long NDCF series.']},
    {id:'resilience',title:'Test resilience at the measurement boundary',description:'Reported road-level traffic, toll receipts, fuel prices and interest sensitivity keep their own denominators.',
      warning:'GM and MH have a counting break: the prior quarter uses ETC only; the current quarter includes ETC and non-ETC. Earlier revenue spans transition remittances; current revenue includes annual-pass compensation and adjustments. The reported changes below are not organic traffic growth. Never subtract newly counted exempt traffic from a reported growth rate.',
      tables:[table('Issuer reported Q1 FY2027 versus Q1 FY2026 changes — percent; rounded, unaudited',['Road','Reported PCU change (%)','Reported revenue change (%)'],[['GM','Gandhidham–Mundra'],['AP','Palanpur–Abu Road'],['AS','Abu Road–Swaroopganj'],['MH','Muzaffarnagar–Haridwar']].map(([code,name])=>({label:name+' ('+code+')',ids:['TRAFFIC-'+code+'-pcu_yoy_reported','TRAFFIC-'+code+'-revenue_yoy_reported']}))),
        table('Indian crude basket — native USD/barrel observations; no diesel pass-through estimate',['Period','Value'],[fact('August 2025','OIL-2025-08'),fact('July 2026','OIL-2026-07'),fact('August 2026','OIL-2026-08')]),
        table('Issuer FY2026 financing sensitivity — INR crore',['Observation','Value'],[fact('Floating-rate principal','NHIT-FY26_float'),fact('PBT sensitivity to a 25-basis-point rate change','NHIT-FY26_ratesensitivity')])],
      notes:['The issuer attributes GM multi-axle-vehicle weakness to West Asia disruption and AP/AS weakness to free-alternative diversion. These are issuer explanations, not independently estimated causal effects. PCU is not a unique vehicle count or capacity utilisation.', 'The interest-rate sensitivity is an issuer PBT sensitivity, not a DPU forecast. Delhi retail fuel observations apply only to their listed dates and outlets. NETC payments exclude annual-pass and Maharashtra EV-exempt transactions; payment counts are not unique vehicles.']},
    {id:'monitoring',title:'Monitoring opportunity and implementation stage',description:'Programme ambition and procurement evidence are useful signals, with deployment and financial outcomes still to measure.',
      warning:'Announcements, tender requirements, technical qualification and reported process throughput are different stages. They do not establish accepted deployment, paid capex or independently measured net financial ROI. Unknown outcomes are unavailable, not zero.',
      tables:[table('Technology evidence at 3 October 2026 — stage, date and original units retained',['Observation','Value'],[fact('Announced AI dashcam scope','TECH-dashcam_scope'),fact('Government-reported NSV maximum throughput','TECH-nsv_throughput'),fact('Government-reported NSV actionable-report turnaround','TECH-nsv_result_days'),fact('ATMS tender corridor length','TECH-atms_scope'),fact('ATMS tender TMCS units','TECH-atms_tmcs'),fact('ATMS tender VIDES units','TECH-atms_vides'),fact('Bahadarabad MLFF estimated capex, excluding GST','TECH-mlff_estimate'),fact('Technically qualified MLFF bidders, 30 September 2026','TECH-mlff_qualified')]),
        table('Evidence gaps at the research cutoff',['Outcome','Evidence'],[fact('Confirmed installed dashcam coverage','MISSING-dashcam_installed_km'),fact('Paid monitoring / AI capex','MISSING-AI_paid_capex'),fact('Causal financial ROI','MISSING-AI_ROI'),fact('Same-asset national highway capacity utilisation','MISSING-national_utilisation')])],
      notes:['The June announcement says dashcam rollout was initiated; completed installation is not established. NSV throughput and turnaround are government process claims. The ATMS tender was issued by NWPPL for roads of NWPPL and NEPPL; existing equipment is a separate inventory.', 'MLFF financial opening was scheduled for 1 October. No award or commissioning was verified in the reviewed record by the cutoff; that is not proof those events never occurred.', 'Future measures: acceptance date, unique operational coverage, paid capex, recurring costs, detection accuracy, validation and repair-closure lags, repeated defects, condition, lane-hours unavailable, and safety rates with denominators and comparison periods. Better monitoring may initially raise necessary maintenance spending; lower spending alone does not prove productivity.']},
  ];
  for (const chapter of chapters) for (const t of chapter.tables) for (const row of t.rows) for (const id of row.ids || [row.id]) index.get(id);
  return chapters;
}
