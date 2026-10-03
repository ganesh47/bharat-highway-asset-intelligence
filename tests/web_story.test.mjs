import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import test from 'node:test';
import assert from 'node:assert/strict';
import {EVIDENCE_HASHES, evidenceIndex, exhibits, formatValue} from '../apps/web/src/story-model.mjs';

const bytes=Object.keys(EVIDENCE_HASHES).map(name=>readFileSync(new URL('../data/research/nhai-nhit/'+name,import.meta.url)));
const documents=()=>bytes.map(buffer=>JSON.parse(buffer));

test('approved bytes, five exhibits and every claim resolve to a primary citation',()=>{
  bytes.forEach((buffer,i)=>assert.equal(createHash('sha256').update(buffer).digest('hex'),Object.values(EVIDENCE_HASHES)[i]));
  const index=evidenceIndex(...documents()),chapters=exhibits(index);
  assert.equal(chapters.length,5);
  const displayed=chapters.flatMap(chapter=>chapter.tables.flatMap(table=>table.rows.flatMap(row=>row.ids||[row.id])));
  for(const id of displayed) {
    const row=index.get(id),source=index.references.get(row.source_url);
    assert.ok(source.observation_ids.includes(id));
    assert.ok(source.page_references.includes(row.document_page));
  }
  assert.equal(new Set(displayed.filter(id=>index.get(id).value===null)).size,5);
  assert.equal(index.get('CALC-ndcf_bridge').value,'2234.2400');
});

test('reported decimal precision is preserved; missing values have no numeric presentation',()=>{
  assert.equal(formatValue({value:'1465842.545'}),'14,65,842.545');
  assert.equal(formatValue({value:'12345678901234567890.0010'}),'1,23,45,67,89,01,23,45,67,890.0010');
  assert.equal(formatValue({value:'-27.040'}),'−27.040');
  assert.equal(formatValue({value:null},2),'Unavailable');
});

test('failed lineage and removed accounting context cannot render',()=>{
  let docs=documents();docs[0].observations[0].perimeter='';
  assert.throws(()=>evidenceIndex(...docs),/context/);
  docs=documents();docs[1].calculations[0].input_ids=['invented-input'];
  assert.throws(()=>evidenceIndex(...docs),/lineage/);
  docs=documents();docs[2].sources[0].url='javascript:alert(1)';
  assert.throws(()=>evidenceIndex(...docs),/source reference/);
});

test('missing disclosure cannot silently become a measured zero',()=>{
  const docs=documents();docs[0].observations.find(row=>row.value===null).value='0';
  assert.throws(()=>evidenceIndex(...docs),/remain unavailable/);
});

test('a chapter cannot render if its required observation disappeared',()=>{
  const index=evidenceIndex(...documents());index.rows.delete('NHAI-C195');
  assert.throws(()=>exhibits(index),/Missing required observation/);
});
