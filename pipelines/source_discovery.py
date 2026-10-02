"""Discover publisher releases without promoting unvalidated documents to facts."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from html import unescape
import json
from pathlib import Path
import re
from urllib.parse import urljoin, urlsplit
import requests
from pipelines.common import write_json
from pipelines.connectors.primary_disclosures import research_cutoff
from research.loader import load_inventory

INDEXES = {
 'morth_reports': 'https://morth.gov.in/backend/api/reports',
 'paimana_reports': 'https://paimana-proj.mospi.gov.in/ReportPage/Report?fyear=N&month=0&quater=0&reportType=F',
 'nhidcl_projects': 'https://www.nhidcl.com/en/current-status',
 'nhit_reports': 'https://nhit.co.in/reports/',
 'npci_netc': 'https://www.npci.org.in/product/netc/product-statistics',
 'cag_state_accounts': 'https://cag.gov.in/en/state-accounts-report?defuat_state_id=79',
}
SUCCESSORS = {
 'data_gov_in_nh_fatalities_injuries_state_year':['morth_road_accidents_2024_final'],
 'data_gov_in_road_accidents_nhs_2003_2016':['morth_road_accidents_2024_final'],
 'data_gov_in_road_accidents_india_2003_2016':['morth_road_accidents_2024_final'],
 'data_gov_in_road_fatal_accidents_2003_2016':['morth_road_accidents_2024_final'],
 'data_gov_in_gsdp_stateut_current_prices_2017_23':['rbi_gsdp_current_prices_2024_25'],
 'data_gov_in_nhai_project_finance_api':['nhai_fy2025_26_performance'],
 'morth_annual_report_pdf':['morth_annual_report_2025_26'],
 'nhai_audited_results_pdf':['nhai_financial_results_2025_03_unaudited'],
 'rbi_state_road_finances':['cag_karnataka_road_finances_2024_25','cag_maharashtra_road_finances_2024_25'],
}
HISTORICAL = {'cag_bharatmala_performance_audit','adb_state_road_projects','data_gov_in_nhai_state_projects_api','data_gov_in_nhai_projects_district_target_2023'}


def extract_candidates(name, content):
    payload = json.loads(content) if name in {'morth_reports','paimana_reports'} else None
    candidates = []
    if name == 'morth_reports':
        def walk(value):
            if isinstance(value, dict):
                filename = value.get('file')
                title = str(value.get('title') or '')
                if isinstance(filename,str) and filename.lower().endswith('.pdf') and re.search(r'annual.report|road.accident|basic.road',title,re.I):
                    candidates.append({'title':title,'url':urljoin('https://morth.gov.in/backend/',filename),'publisher_date':value.get('published_date') or value.get('publication_date'),'date_kind':'publisher_metadata' if value.get('published_date') or value.get('publication_date') else 'unknown','extraction_status':'not_validated_by_discovery'})
                for item in value.values(): walk(item)
            elif isinstance(value,list):
                for item in value: walk(item)
        walk(payload)
    else:
        text = payload.get('html','') if isinstance(payload,dict) else content
        for href,label in re.findall(r'<a[^>]+href=[\"\x27]([^\"\x27]+)[\"\x27][^>]*>(.*?)</a>',text,re.S|re.I):
            url = urljoin(INDEXES[name],unescape(href).replace('\\','/'))
            label = re.sub(r'\s+',' ',unescape(re.sub('<[^>]+>',' ',label))).strip()
            if urlsplit(url).scheme == 'https' and ('.pdf' in url.lower() or '.xlsx' in url.lower()): candidates.append({'title':label or 'Publisher linked document','url':url,'publisher_date':None,'date_kind':'unknown','extraction_status':'not_validated_by_discovery'})
    return list({item['url']:item for item in candidates}.values())


def check_index(item):
    name,url = item
    result={'index':name,'url':url,'checked_at':datetime.now(timezone.utc).isoformat()}
    try:
        response=requests.get(url,timeout=25,headers={'User-Agent':'BHAI-publication-discovery/1.0'},stream=True)
        result['http_status']=response.status_code
        response.raise_for_status()
        if urlsplit(response.url).scheme != 'https' or urlsplit(response.url).hostname != urlsplit(url).hostname: raise ValueError('Unapproved index redirect')
        content=response.raw.read(3_000_001,decode_content=True)
        if len(content)>3_000_000: raise ValueError('Index exceeds3MBlimit')
        text=content.decode('utf-8',errors='replace')
        if any(x in text.lower() for x in ['verify you are human','access denied','what code is in the image']): raise ValueError('Restricted/challenge response')
        result.update(status='checked',checksum=hashlib.sha256(content).hexdigest(),candidates=extract_candidates(name,text))
        if name=='nhit_reports': result['qualification']='HTTP index can be cached; browser verification required before claiming no newer quarter release.'
        if name=='npci_netc': result['qualification']='Rendered table and exclusions require governed browser capture; index success alone does not verify a new month.'
    except Exception as exc: result.update(status='retrieval_blocked' if result.get('http_status') in {401,403,429} else 'retrieval_failed',error=str(exc),candidates=[])
    return result


def build(inventory, catalog, out):
    sources=load_inventory(inventory).sources
    entries={row['source_id']:row for row in json.loads(Path(catalog).read_text()).get('datasets',[])}
    with ThreadPoolExecutor(max_workers=6) as pool: indexes=list(pool.map(check_index,INDEXES.items()))
    checked=datetime.now(timezone.utc).isoformat()
    rows=[]
    for source in sources:
        sid=source['source_id'];entry=entries.get(sid,{})
        companions=[candidate for candidate in SUCCESSORS.get(sid,[]) if candidate in entries]
        rows.append({'source_id':sid,'refresh_outcome':entry.get('refresh_outcome','not_checked'),'last_checked_at':entry.get('last_checked_at'),'observation_cutoff':entry.get('source_data_cutoff') or entry.get('source_as_of_date'),'publication_date':entry.get('publication_date'),'analytical_ready':entry.get('analytical_ready',False),'successor_source_ids':companions,'successor_policy':'Separate scopes/vintages; compatible metric selection only, never concatenate totals' if companions else None,'research_outcome':'historical_context' if sid in HISTORICAL else 'newer_companion_verified' if companions else 'no_verified_successor_recorded','gap_reason':entry.get('refresh_error') or entry.get('quarantine_reason') or entry.get('note') or entry.get('skip_reason')})
    report={'checked_at':checked,'research_cutoff':research_cutoff(),'registered_source_count':len(sources),'source_count':len(rows),'sources':rows,'publisher_indexes':indexes,'policy':'Candidate links are discovery evidence only. No new observation or publication date is inferred; unvalidated disclosures do not replace data.'}
    write_json(report,Path(out));return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--inventory',default='research/source_inventory.yaml');p.add_argument('--catalog',default='data/manifests/catalog.json');p.add_argument('--out',default='data/manifests/publication_discovery.json');a=p.parse_args();r=build(a.inventory,a.catalog,a.out);print(json.dumps({'registered_sources':r['source_count'],'indexes':{x['index']:x['status'] for x in r['publisher_indexes']}}))
