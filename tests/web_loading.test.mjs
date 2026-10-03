import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { catalogSnapshot, fetchEvidence, verifyBuffer, withDeadline } from '../apps/web/src/loading.mjs';

const bytes = new TextEncoder().encode('published fixture');
const checksum = createHash('sha256').update(bytes).digest('hex');
const entry = { source_id: 'fixture', output_table_path: 'data/processed/fixture.parquet', manifest: { row_count: 0, output_files: [{ path: 'data/processed/fixture.parquet', sha256: checksum }] } };

test('published zero counts and snapshot hashes remain explicit', () => {
  const snapshot = catalogSnapshot({ datasets: [entry] });
  assert.equal(snapshot.counts.fixture, 0);
  assert.equal(snapshot.checksums.get(entry.output_table_path), checksum);
  assert.equal(snapshot.catalog.fixture, entry);
  assert.deepEqual(Object.keys(catalogSnapshot({ datasets: [] }).catalog), []);
});

test('malformed counts, IDs and hashes fail rather than become empty evidence', () => {
  for (const row_count of [undefined, null, '0', -1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => catalogSnapshot({ datasets: [{ ...entry, manifest: { ...entry.manifest, row_count } }] }));
  }
  assert.throws(() => catalogSnapshot({ datasets: [entry, entry] }));
  assert.throws(() => catalogSnapshot({ datasets: [{ ...entry, manifest: { ...entry.manifest, output_files: [] } }] }));
  assert.throws(() => catalogSnapshot({ datasets: {} }));
});

test('mixed deployment bytes cannot enter the evidence engine', async () => {
  await verifyBuffer(bytes, checksum);
  await assert.rejects(verifyBuffer(new TextEncoder().encode('different publication'), checksum), /catalog snapshot/);
});

test('the request deadline also covers a stalled response body', async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async (_, options) => ({ ok: true, arrayBuffer: () => new Promise((_, reject) => {
    options.signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
  }) });
  try { await assert.rejects(fetchEvidence('/fixture', 5), /aborted/); }
  finally { globalThis.fetch = original; }
});

test('engine stages time out and preserve immediate success or rejection', async () => {
  await assert.rejects(withDeadline(new Promise(() => {}), 5, 'engine timed out'), /engine timed out/);
  assert.equal(await withDeadline(Promise.resolve('ready'), 1000, 'timeout'), 'ready');
  await assert.rejects(withDeadline(Promise.reject(new Error('worker failed')), 1000, 'timeout'), /worker failed/);
});
