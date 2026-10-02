"""Dated MoSPI monitored road portfolios, never a census of all NHAI projects."""
from __future__ import annotations
import re
from pathlib import Path

SOURCE_IDS = ('mospi_paimana_monthly_projects',)
DOCUMENTS = {SOURCE_IDS[0]: 'https://paimana-proj.mospi.gov.in/ReportPage/ViewPdf?id=1323&path=Content%5CArchiveReport%5Cflash%5C2026-27/FlashReport_August_2026.pdf'}
DISCOVERY_URL = 'https://paimana-proj.mospi.gov.in/ReportPage/Report?fyear=N&month=0&quater=0&reportType=F'
EXPECTED_SHA = 'af9fad425ca081ab4c1fa49fedc1640ac0e6b199f04372761a31507754a03b02'


def numeric(value):
    if value is None or str(value).strip() in {'', '-', '(-)'}: return None
    value = float(str(value).replace(',', '').strip())
    if value < 0: raise ValueError('Negative PAIMANA cost/progress')
    return value


def parse_project(row):
    """Only structurally identified Table6 road-agency rows; no title length guesses."""
    if len(row) != 8 or not str(row[0] or '').isdigit(): return None
    text = row[1] or ''
    match = re.search(r'\((MoRTH|NHAI|NHIDCL|National Highways Authority of India[^)]*|National Highways[^)]*Infrastructure[^)]*)\)\s*\n\((\d+)\)', text, re.I)
    if not match: return None
    agency_raw, code = match.groups()
    agency = 'NHIDCL' if 'Infrastructure' in agency_raw or agency_raw.upper() == 'NHIDCL' else 'NHAI' if 'Authority' in agency_raw or agency_raw.upper() == 'NHAI' else 'MoRTH'
    costs = (row[5] or '').split('\n')
    if len(costs) != 2: raise ValueError(f'Ambiguous project cost cells: {code}')
    physical = numeric(row[7])
    if physical is not None and physical > 100: raise ValueError('Invalid physical percentage')
    return dict(code=code, agency=agency, name=re.sub(r'\s+', ' ', text[:match.start()]).strip(), state=re.sub(r'\s+', ' ', row[2] or '').strip(), original=numeric(costs[0]), revised=numeric(costs[1]), expenditure=numeric(row[6]), physical=physical, approval=row[3] or '', completion=row[4] or '')


def extend_snapshots(builder):
    from pipelines.common import sha256_for_file
    import pdfplumber
    sid = SOURCE_IDS[0]
    path = builder.raw_root / 'primary_disclosures' / sid / 'document.pdf'
    builder.rows.setdefault(sid, []); builder.documents.setdefault(sid, [])
    if not path.exists():
        builder.notes[sid] = 'Public August2026 report available; validated local download/extraction missing.'
        return
    if sha256_for_file(path) != EXPECTED_SHA:
        raise ValueError('PAIMANA document changed: discover and validate new publication before extraction')
    if not builder.documents[sid]: builder.pin(sid, path, DOCUMENTS[sid])
    projects = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages[88:148]:
            for table in page.extract_tables():
                for row in table:
                    project = parse_project(row)
                    if project: projects.append((page.page_number, project))
    if len(projects) != 982 or len({p['code'] for _, p in projects}) != 982:
        raise ValueError(f'PAIMANA MoRTH portfolio must reconcile to982 unique projects, got{len(projects)}')
    for page, p in projects:
        attrs = dict(entity_id='paimana_'+p['code'], entity_name=p['name'], entity_type='project', agency=p['agency'], state=p['state'], road_class='Roads & Highways monitored portfolio', end='2026-08-31', basis='project_snapshot', asof='2026-08-31', published='2026-09-25', disclosure_as_of='2026-09-24', statement='PAIMANA_CRIP_monitored_projects_ge_150_crore', asset_id=p['code'], project_name=p['name'], project_stage='ongoing', implementing_agency_id=p['agency'].lower(), observation_status='reported', assurance='administrative', notes=f'PAIMANA project code {p["code"]}; agency reported by publisher. Projects≥₹150crore; excludes smaller/unreported works. Original/revised cost is project-wide, not current-year spending. Revised cost0 means no separate revision reported, not a zero-cost project. Approval/start month source cells: {p["approval"]}; original/revised completion month: {p["completion"]}. Month dates do not imply exact completion days. Snapshot reported by agencies through24September; observations remain31August. No contract length inferred from title; cross-source project linkage requires verified IDs.')
        anchor = f'PDF p{page}; Table6 All Ongoing Projects; project{p["code"]}'
        for key, metric, unit in [('original','project_original_cost_inr_crore','INR crore'),('revised','project_reported_revised_cost_inr_crore','INR crore'),('expenditure','project_cumulative_expenditure_inr_crore','INR crore'),('physical','physical_progress_percent','percent')]:
            if p[key] is not None: builder.fact(sid, metric, p[key], unit, anchor, **attrs)
    builder.notes[sid] = '982 unique MoRTH-monitored road projects reconcile toAugust2026 report. Administrative reported snapshots, not audited accounts/allNHAI census. Stable PAIMANA IDs retained; NHIDCL/PMP and legacy OCMS IDs are distinct. September report announced26October2026; no September observations inferred.'
