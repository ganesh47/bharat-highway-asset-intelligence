"""Audited road expenditure recovered from regional CAG and UT publications.

The central discovery tab misses regional year-heading layouts. These pinned
publications preserve UT boundaries, table order, signed net expenditures and
unapportioned legacy balances; annual flows never include cumulative stocks.
"""
from __future__ import annotations
import hashlib
import math
import re
from pathlib import Path
from typing import Any
import pdfplumber
from pipelines.state_freshness import _numeric_tokens

ROOT = 'https://cag.gov.in/'
DOCUMENTS = {
 'cag_rajasthan_road_finances_2024_25': ROOT+'uploads/state_accounts_report/account-report-FA-2024-25-Vol-I-E-002-06985d5ed9f3be2-90457272.pdf',
 'cag_jammu_and_kashmir_road_finances_2024_25': ROOT+'uploads/state_accounts_report/account-report-JK-Finance-Accounts-2024-25-Vol-I-15-12-2025-069d79132701351-89156562.pdf',
 'cag_puducherry_road_finances_2024_25': ROOT+'uploads/download_audit_report/2025/UTFAR-ENGLISH-06a9995af301932.89506359.pdf',
 'cag_delhi_road_finances_2024_25': ROOT+'webroot/uploads/download_audit_report/2025/Report_No_2_of_2026_SFAR_English-06a7ae58e197ee5.14833566.pdf',
}
SOURCE_IDS = tuple(DOCUMENTS)
CONTRACTS = {
 'cag_rajasthan_road_finances_2024_25': dict(state='Rajasthan',directory='rajasthan',sha256='44f59878f57f8640217e3ba84691491032e0dbe7348f1f7c59b053b06b09c547',function=37,capital=46,current_first=True),
 'cag_jammu_and_kashmir_road_finances_2024_25': dict(state='Jammu and Kashmir',directory='jammu_and_kashmir',sha256='658c6fb7d2289942b1ca7612e0691a25fef2cd68bf6175363df3cd60652d9e93',function=35,capital=47,current_first=False),
 'cag_puducherry_road_finances_2024_25': dict(state='Puducherry',directory='puducherry',sha256='b3d9726e438fab89d26d2548e2eaaae6fb28760a5483a02f08a59246e22ef8be',published='2026-08-31'),
 'cag_delhi_road_finances_2024_25': dict(state='Delhi',directory='delhi',sha256='8db2505cae00b0d29892cbfa88b96655dd0f37534a9c8ca4eef0c827cb43e274',published='2026-08-10'),
}

def document_path(raw_root: Path, sid: str) -> Path:
 return raw_root/'state_supplement_completion'/CONTRACTS[sid]['directory']/'document.pdf'

def parse_accounts(function: str, capital: str, *, current_first: bool) -> dict[str,float]:
 function=' '.join(function.split());capital=' '.join(capital.split())
 if not all(x in function for x in ['Revenue','Capital','Total','crore']) or not any(x in function for x in ['Loans','L & A']):
  raise ValueError('Functional headings or units changed')
 if not all(x in capital for x in ['2024-25','2023-24','crore','Expenditure']):raise ValueError('Capital headings or units changed')
 row=re.search(r'Roads and Bridges(.*?)Road Transport',function)
 cap=re.search(r'5054\s*[.-]?\s*Capital Outlay on Roads and Bridges(.*?)5055',capital)
 if not row or not cap:raise ValueError('Road major-head contract changed')
 vals=_numeric_tokens(row[1]);outlay=_numeric_tokens(cap[1])
 if len(vals) not in {3,4}:raise ValueError('Functional columns changed')
 if current_first:
  if len(outlay)!=5:raise ValueError('Capital columns changed')
  current,_,previous,_,_=outlay
 else:
  # Jammu & Kashmir: two extra bold cumulative legacy balances. Neither is
  # annual expenditure nor an amount allocated during this fiscal year.
  if not all(x in capital for x in ['30 October 2019','yet to be apportioned','Amount','allocated']):raise ValueError('UT legacy-balance headings changed')
  if len(outlay)!=6 or outlay[-2]!=outlay[-1]:raise ValueError('UT legacy-balance columns changed')
  previous,_,current,_,_,_=outlay
 if not math.isclose(vals[1],current,abs_tol=.005) or not math.isclose(sum(vals[:-1]),vals[-1],abs_tol=.015):raise ValueError('Annual capital reconciliation failed')
 return dict(revenue_2024_25=vals[0],capital_2024_25=current,capital_2023_24=previous)

def parse_rajasthan_state_highways(revenue: str, capital: str) -> dict[str,float]:
 """Minor head03 actuals are net cash flows, including published recoveries."""
 if not all(x in revenue for x in ['2024-25','2023-24','State Highways','lakh','Minus expenditure']) or not all(x in capital for x in ['2024-25','2023-24','State Highways','lakh','Deduct']):raise ValueError('SH net expenditure headings changed')
 def row(text):
  lines=[line for line in text.splitlines() if 'TOTAL - 03' in line]
  if len(lines)!=1:raise ValueError('SH subtotal ambiguous')
  return _numeric_tokens(lines[0])
 r,c=row(revenue),row(capital)
 if len(r)!=4 or len(c)!=6 or not math.isclose(c[0]+c[1],c[2],abs_tol=.005):raise ValueError('SH subtotal columns/reconciliation changed')
 return {'revenue_2024_25':r[1],'revenue_2023_24':r[2],'capital_2024_25':c[2],'capital_2023_24':c[3]}

def parse_delhi(text: str) -> dict[str,float]:
 text=' '.join(text.split())
 if not all(x in text for x in ['2023-24','2024-25','crore','5054','Roads and Bridges']):raise ValueError('Delhi capital narrative contract changed')
 match=re.search(r'from ₹\s*([\d,]+\.\d{2}) crore in 2023-24 to ₹\s*([\d,]+\.\d{2}) crore in 2024-25 under head MH 5054',text)
 if not match:raise ValueError('Delhi annual capital amounts absent')
 return dict(capital_2023_24=float(match[1].replace(',','')),capital_2024_25=float(match[2].replace(',','')))

def parse_puducherry(text: str) -> dict[str,float]:
 text=' '.join(text.split())
 if not all(x in text for x in ['2024-25','Capital Expenditure','Roads','and Bridges','misclassifications','25.59 crore']):raise ValueError('Puducherry narrative/qualification contract changed')
 match=re.search(r'Expenditure amounting to [^0-9]{0,4}(\d+) crore was spent towards Transport under Roads and Bridges',text)
 if not match:raise ValueError('Puducherry Roads/Bridges capital actual absent')
 return {'capital_2024_25':float(match[1])}

def extend_snapshots(builder: Any) -> None:
 for sid in SOURCE_IDS:
  c=CONTRACTS[sid];path=document_path(builder.raw_root,sid);builder.rows.setdefault(sid,[]);builder.documents.setdefault(sid,[])
  if not path.exists():builder.notes[sid]='Pinned audited publication unavailable; no inferred amounts.';continue
  if hashlib.sha256(path.read_bytes()).hexdigest()!=c['sha256']:raise ValueError(sid+': publication changed; reviewed extraction required')
  if not builder.documents[sid]:builder.pin(sid,path,DOCUMENTS[sid])
  with pdfplumber.open(path) as pdf:
   if c['state']=='Puducherry':
    ocr=path.parent/'reviewed_capital_ocr.txt'
    if hashlib.sha256(ocr.read_bytes()).hexdigest()!='a800687970841878fe806c69a775a194008370bd2f10136b63d75c617795aa34':raise ValueError('Reviewed OCR checksum changed')
    values=parse_puducherry(ocr.read_text());pages={'capital_2024_25':51}
   elif c['state']=='Delhi':values=parse_delhi(pdf.pages[28].extract_text());pages={'capital_2024_25':29,'capital_2023_24':29}
   else:
    values=parse_accounts(pdf.pages[c['function']-1].extract_text(layout=True),pdf.pages[c['capital']-1].extract_text(layout=True),current_first=c['current_first'])
    pages={key:c['function'] if key.startswith('revenue') else c['capital'] for key in values}
  for key,value in values.items():
   year=2024 if key.endswith('2024_25') else 2023
   metric='roads_bridges_revenue_expenditure_inr_crore' if key.startswith('revenue') else 'roads_bridges_capital_outlay_inr_crore'
   notes='Audited government cash-account annual expenditure for all road classes, excluding separate Road Transport head5055. Not SH-only spending, corporation debt or construction cost. Annual capital excludes progressive balances. '
   if c['state']=='Jammu and Kashmir':notes+='Present UT boundary; pre-31October2019 unapportioned legacy balances are excluded. '
   if c['state']=='Puducherry':notes+='Reported rounded whole-crore road capital figure, visually verified in scanned SFAR printedp25. Audit reports aggregate capital misclassification INR25.59crore; no unsupported deduction from the road subtotal. Revenue3054/prior5054 remain unavailable in this disclosure. '
   if c['state']=='Delhi':notes+='State Finances Audit Report capital narrative; revenue head3054 is not separately disclosed in this extract and remains a gap. '
   builder.fact(sid,metric,value,'INR crore',f"PDF p{pages[key]}; "+('SFAR chapter1 printedp25 Roads and Bridges narrative' if c['state']=='Puducherry' else ('SFAR chapter1 printedp7 MH5054 narrative' if c['state']=='Delhi' else 'Finance Accounts Statements4A/5 Roads and Bridges')),entity_id='state_'+c['directory'],entity_name=c['state'],entity_type='state_aggregate',agency='CAG / Government of '+c['state'],state=c['state'],road_class='roads_and_bridges_all_classes',start=f'{year}-04-01',end=f'{year+1}-03-31',basis='fiscal_year',estimate='actual',statement='state_finance_audit_report_rounded_cash_capital_expenditure' if c['state']=='Puducherry' else ('state_finance_audit_report_cash_capital_narrative' if c['state']=='Delhi' else 'state_finance_accounts_cash_expenditure_as_reported'),reported_period=f'{year}-{str(year+1)[2:]}',published=c.get('published',''),observation_status='final',assurance='audited_state_finances_report' if c['state'] in ('Puducherry','Delhi') else 'audited_finance_accounts',notes=notes)
  if c['state']=='Rajasthan':
   with pdfplumber.open(path) as pdf:
    sh=parse_rajasthan_state_highways(pdf.pages[208].extract_text(layout=True),pdf.pages[275].extract_text(layout=True))
   for key,value in sh.items():
    year=2024 if key.endswith('2024_25') else 2023
    is_revenue=key.startswith('revenue')
    builder.fact(sid,'roads_bridges_revenue_expenditure_inr_crore' if is_revenue else 'roads_bridges_capital_outlay_inr_crore',value,'INR crore',f"PDF p{209 if is_revenue else 276}; printedp{181 if is_revenue else 248}; Statement{15 if is_revenue else 16}, major head{3054 if is_revenue else 5054}, minorhead03 State Highways TOTAL",original_unit='INR lakh',entity_id='state_rajasthan',entity_name='Rajasthan',entity_type='state_aggregate',agency='CAG / Government of Rajasthan',state='Rajasthan',road_class='State Highway',start=f'{year}-04-01',end=f'{year+1}-03-31',basis='fiscal_year',estimate='actual',statement='state_finance_accounts_net_minor_head_03_expenditure',reported_period=f'{year}-{str(year+1)[2:]}',published='',observation_status='final',assurance='audited_finance_accounts',notes='Audited SH minorhead03 net annual cash expenditure, not gross construction cost. Revenue is negative because receipts/recoveries exceed expenditure as footnote(a) explicitly reports. Capital is net of Central Road Fund and State Road Development Fund recoveries. Included in the all-road-class major-head subtotal; never add both scopes together. Original INRlakh normalized to crore.')
  builder.notes[sid]='Recovered regional/UT primary publication; dated audited annual actuals with labelled reconciliation and no inferred publication day.'

if __name__=='__main__':
 from pipelines.connectors.primary_disclosures import SnapshotBuilder
 b=SnapshotBuilder(Path('data/raw'));b.document_urls.update(DOCUMENTS);extend_snapshots(b);b.finish(SOURCE_IDS);print({s:len(b.rows[s]) for s in SOURCE_IDS})
