import {evidenceIndex, exhibits, formatValue, EVIDENCE_HASHES} from './story-model.mjs';
window.dispatchEvent(new Event('story-runtime-started'));

const create = (tag, text, className) => {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
};
const root = document.getElementById('story-exhibits');
const status = document.getElementById('story-status');
const retry = document.getElementById('story-retry');
const stages = {announced:'Announced programme',reported_process_outcome:'Government-reported process outcome',tendered:'Tender specification',tendered_estimate:'Tender estimate',technical_qualification:'Technical qualification'};
let busy = false;

function dataRoot() {
  const pathname = location.pathname;
  const apps = pathname.lastIndexOf('/apps/web/');
  const prefix = apps >= 0 ? pathname.slice(0, apps) : pathname.slice(0, pathname.lastIndexOf('/'));
  return new URL(prefix+'/data/research/nhai-nhit/', location.origin);
}

function claimLink(row, decimals) {
  const link = create('a', formatValue(row, decimals));
  link.href = '#claim-'+row.id;
  link.dataset.observation = row.id;
  link.setAttribute('aria-label', row.metric+': '+formatValue(row,decimals)+' '+row.unit+'. Read source and context.');
  return link;
}

function tableView(config, index) {
  const region = create('div', undefined, 'story-table-scroll');
  region.tabIndex = 0;
  region.setAttribute('role','region');
  region.setAttribute('aria-label',config.title);
  const table = create('table');
  table.append(create('caption',config.title));
  const head = create('tr');
  for (const column of config.columns) {const cell=create('th',column);cell.scope='col';head.append(cell);}
  const thead=create('thead');thead.append(head);table.append(thead);
  const body=create('tbody');
  for (const item of config.rows) {
    const row=create('tr');
    const label=create('th',item.label);label.scope='row';row.append(label);
    for (const id of item.ids || [item.id]) {
      const observation=index.get(id), cell=create('td');
      cell.append(claimLink(observation,item.decimals));
      if (!item.ids) {
        if (observation.value !== null) cell.append(create('span',' '+observation.unit, 'value-unit'));
        cell.append(create('small',observation.reporting_period+' · as of '+observation.as_of+' · '+observation.value_status.replaceAll('_',' '),'value-context'));
        if (observation.evidence_stage) cell.append(create('small',stages[observation.evidence_stage] || observation.evidence_stage,'stage-label'));
      }
      row.append(cell);
    }
    body.append(row);
  }
  table.append(body);region.append(table);return region;
}

function historyFigure(chapter, index) {
  const table=chapter.tables[0];
  const pairs=table.rows.map(row=>row.ids.map(id=>index.get(id)));
  const maximum=Math.max(...pairs.flat().map(row=>Number(row.value)));
  const figure=create('figure',undefined,'story-figure');
  const title=create('figcaption','Government capital and borrowings — INR crore, stocks at 31 March');
  figure.append(title);
  const chart=create('div',undefined,'paired-chart');chart.setAttribute('aria-hidden','true');
  for (let i=0;i<pairs.length;i++) {
    const group=create('div',undefined,'chart-period');group.append(create('span',table.rows[i].label));
    pairs[i].forEach((row,j)=>{
      const bar=create('div',undefined,'chart-bar '+(j?'borrowing':'capital'));
      bar.style.width=(Number(row.value)/maximum*100)+'%';
      group.append(bar);
    });chart.append(group);
  }
  figure.append(chart);
  figure.append(create('p','Government capital (blue) · Borrowings including ADB (amber). Zero baseline; the longest bar equals '+formatValue(pairs.flat().reduce((a,b)=>Number(a.value)>Number(b.value)?a:b),2)+' INR crore. Exact values, source links and dates are in the table below.','figure-key'));
  return figure;
}

function cashWaterfall(index) {
  const ids=['NHIT-FY26_ndcf_base','NHIT-FY26_zcb_addback','NHIT-FY26_dsra_release_q1','NHIT-FY26_dsra_release_q2'];
  const total=Number(index.get('CALC-ndcf_bridge').value);
  const figure=create('figure',undefined,'story-figure');
  figure.append(create('figcaption','FY2026 Trust adjusted distribution cash — INR crore'));
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 620 200');svg.setAttribute('aria-hidden','true');
  let previous=0;
  [...ids,'CALC-ndcf_bridge'].forEach((id,i)=>{
    const value=Number(index.get(id).value), start=i===4?0:previous, end=i===4?total:previous+value;
    const rect=document.createElementNS(svg.namespaceURI,'rect');
    rect.setAttribute('x',String(24+i*119));rect.setAttribute('width','75');
    rect.setAttribute('y',String(165-end/total*140));rect.setAttribute('height',String((end-start)/total*140));
    rect.setAttribute('fill',i===4?'#184c82':i===0?'#3773aa':'#a97520');svg.append(rect);
    previous=end;
  });
  figure.append(svg);
  const labels=create('div',undefined,'waterfall-labels');labels.setAttribute('aria-hidden','true');for(const label of ['Base','ZCB','DSRA 1','DSRA 2','Total'])labels.append(create('span',label));figure.append(labels);
  figure.append(create('p','Zero baseline; total '+formatValue(index.get('CALC-ndcf_bridge'))+' INR crore. Small reserve releases remain visible in the exact-value table below. This is the Trust distribution calculation, not consolidated in-year cash generation.','figure-key'));
  return figure;
}

function sourceDetail(row,index) {
  const detail=create('details',undefined,'source-record');detail.id='claim-'+row.id;
  detail.append(create('summary',row.metric+' · '+formatValue(row)+(row.value===null?'':' '+row.unit)+' · '+row.entity+' · '+row.reporting_period));
  const dl=create('dl');
  for (const [label,value] of [['Observation ID',row.id],['Entity and perimeter',row.entity+' · '+row.perimeter],['Period and cutoff',row.reporting_period+' · '+row.as_of],['Value status',row.value_status.replaceAll('_',' ')],['Assurance',row.audit_status],['Definition',row.definition],['Caveats',row.uncertainty],['Comparison context',row.comparison_status || row.comparability_group || 'Not supplied; this does not imply comparability'],['Evidence stage',stages[row.evidence_stage] || 'Read the stated evidence context'],['Source document date',row.source_document_date || 'Not supplied'],['Original value / normalization',String(row.original_value ?? 'Not supplied')+' · '+(row.normalization || 'Not supplied')]]) {
    dl.append(create('dt',label),create('dd',value));
  }
  detail.append(dl);
  const source=create('a',new URL(row.source_url).hostname+' · '+row.document_page);source.href=row.source_url;source.target='_blank';source.rel='noopener noreferrer';detail.append(source);
  const recipe=index.recipes.get(row.id);
  if (recipe) {
    detail.append(create('p','Analyst calculation: '+recipe.operation+'. Decimal precision 28; growth uses (current / prior − 1) × 100. Assurance limits are inherited from the inputs.'));
    const inputs=create('ul');
    for (const id of recipe.input_ids) {const li=create('li'),input=index.get(id);li.append(create('span',input.metric+': '),claimLink(input),create('span',' '+input.unit));inputs.append(li);}
    detail.append(inputs);
  }
  const permalink=create('a','Link to this observation');permalink.href='#'+detail.id;permalink.className='permalink';detail.append(permalink);
  return detail;
}

function sourceCatalog(index) {
  const catalog=create('details');catalog.id='evidence-catalog';
  catalog.append(create('summary','Browse '+index.rows.size+' source-linked observations and '+index.references.size+' primary sources'));
  const label=create('label','Search observations '),search=create('input');search.type='search';search.placeholder='Entity, metric, period or observation ID';label.append(search);catalog.append(label);
  const count=create('p',index.rows.size+' matching observations','value-context');count.setAttribute('role','status');count.setAttribute('aria-live','polite');catalog.append(count);
  const records=create('div');
  for (const row of index.rows.values()) records.append(sourceDetail(row,index));
  catalog.append(records);
  search.addEventListener('input',()=>{
    const query=search.value.toLowerCase().trim();let visible=0;
    for (const child of records.children) {child.hidden=query!=='' && !child.textContent.toLowerCase().includes(query);if(!child.hidden)visible++;}
    count.textContent=visible+' matching observations';
  });
  for (const name of Object.keys(EVIDENCE_HASHES)) {
    const link=create('a','Read '+name);link.href=new URL(name,dataRoot());link.className='data-link';catalog.append(link);
  }
  return catalog;
}

function openClaim() {
  if (!/^#claim-[A-Za-z0-9_-]+$/.test(location.hash)) return;
  const target=document.getElementById(location.hash.slice(1));
  if (!target) return;
  const catalog=document.getElementById('evidence-catalog');catalog.open=true;
  const search=catalog.querySelector('input');search.value='';search.dispatchEvent(new Event('input'));
  target.open=true;target.querySelector('summary').focus();target.scrollIntoView({block:'start'});
}
window.addEventListener('hashchange',openClaim);
document.addEventListener('click',event=>{
  const link=event.target.closest('a[href^="#claim-"]');
  if (link && link.getAttribute('href')===location.hash) openClaim();
});

async function load() {
  if (busy) return;busy=true;retry.disabled=true;retry.hidden=true;root.setAttribute('aria-busy','true');
  status.textContent='Reading the versioned observations, calculations and primary sources…';
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),30000);
  try {
    const documents=await Promise.all(Object.entries(EVIDENCE_HASHES).map(async([name,expected])=>{
      const response=await fetch(new URL(name,dataRoot()),{cache:'no-cache',signal:controller.signal});
      if (!response.ok) throw new Error('Evidence file unavailable');
      const bytes=await response.arrayBuffer();
      const digest=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(value=>value.toString(16).padStart(2,'0')).join('');
      if (digest!==expected) throw new Error('Evidence checksum differs');
      return JSON.parse(new TextDecoder().decode(bytes));
    }));
    const index=evidenceIndex(...documents),chapters=exhibits(index);
    const prepared=chapters.map(chapter=>{
      const panel=document.createDocumentFragment();
      panel.append(create('p',chapter.description),create('p',chapter.warning,'comparison-warning'));
      if (chapter.id==='public-authority') panel.append(historyFigure(chapter,index));
      if (chapter.id==='distribution') panel.append(cashWaterfall(index));
      for (const table of chapter.tables) panel.append(tableView(table,index));
      const notes=create('ul',undefined,'exhibit-notes');for(const text of chapter.notes)notes.append(create('li',text));panel.append(notes);
      return {target:document.querySelector('[data-exhibit="'+chapter.id+'"]'),panel};
    });
    const catalog=sourceCatalog(index);
    if (prepared.some(item=>!item.target)) throw new Error('An exhibit container is missing');
    for (const item of prepared) item.target.replaceChildren(item.panel);
    document.getElementById('story-source-list').replaceChildren(catalog);
    document.getElementById('evidence-status-title').textContent='Source-linked evidence, as of '+index.cutoff;
    status.textContent=index.rows.size+' observations · '+index.recipes.size+' reproducible calculations · '+index.references.size+' primary sources. Values link to their source, definition and caveats. Calculations were checked for internal consistency; this does not independently authenticate the primary documents.';
    document.body.dataset.storyReady='true';openClaim();
  } catch(error) {
    controller.abort();
    document.getElementById('evidence-status-title').textContent='Story evidence could not be loaded';
    status.textContent='No new figures were loaded. Check your connection and retry. '+(error.name==='AbortError'?'The request timed out.':error.message);
    retry.hidden=false;
  } finally {clearTimeout(timer);busy=false;retry.disabled=false;root.setAttribute('aria-busy','false');}
}
retry.addEventListener('click',load);
load();
