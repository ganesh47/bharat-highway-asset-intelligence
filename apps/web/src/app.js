import React, { useEffect, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser.mjs';

function resolveAssetPath(relPath) {
  const pathname = window.location.pathname || '/';
  const normalizedPath = pathname.endsWith('/') ? pathname : `${pathname}/`;
  const rootPath = normalizedPath.replace(/\/?apps\/web\/?$/, '/');
  const sanitizedRoot = rootPath.endsWith('/') ? rootPath : `${rootPath}/`;
  const sanitizedRel = relPath.replace(/^\/+/, '');
  return `${sanitizedRoot}${sanitizedRel}`;
}

const CATALOG_PATH = resolveAssetPath('data/manifests/catalog.json');
const METHOD_URL = new URL('methodology.html', window.location.href).pathname;

function sourceTypeTag(item) {
  const category = (item?.metric_category || item?.source_type || item?.source?.source_type || '').toLowerCase();
  if (item?.source?.official_flag === false) {
    return ['Proxy-derived', 'proxy'];
  }
  if (category.includes('proxy')) {
    return ['Proxy-derived', 'proxy'];
  }
  if (category.includes('model')) {
    return ['Model outputs', 'model'];
  }
  if (item?.source?.official_flag) {
    return ['Official measured', 'official'];
  }
  return ['Official measured', 'official'];
}

async function initDuckDB() {
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const selected = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger);
  const worker = new Worker(selected.mainWorker);
  await db.instantiate(selected.mainModule, selected.pthreadWorker, worker);
  return db;
}

function readResultRows(result) {
  if (!result) return [];
  if (typeof result.toArray === 'function') {
    return result.toArray();
  }
  if (typeof result.toArrayOfObjects === 'function') {
    return result.toArrayOfObjects();
  }
  if (typeof result.toJSON === 'function') {
    try {
      return JSON.parse(result.toJSON());
    } catch (error) {
      return [];
    }
  }
  return [];
}

async function queryRows(db, path) {
  const conn = await db.connect();
  const candidates = [path, `/${path.replace(/^\/+/, '')}`];
  for (const candidate of candidates) {
    try {
      const file = candidate.split('/').pop();
      const resp = await fetch(candidate);
      if (!resp.ok) {
        continue;
      }
      const buffer = await resp.arrayBuffer();
      await db.registerFileBuffer(file, new Uint8Array(buffer));
      const result = await conn.query(`SELECT COUNT(*)::BIGINT AS row_count FROM read_parquet('${file}')`);
      const rows = readResultRows(result);
      await conn.close();
      if (!rows.length) return 0;
      const first = rows[0] || {};
      return Number(first.row_count ?? 0);
    } catch (error) {
      continue;
    }
  }
  await conn.close();
  return null;
}

async function readCatalog(path) {
  const catalogCandidates = [path, '/data/manifests/catalog.json'];
  for (const candidate of catalogCandidates) {
    try {
      const response = await fetch(candidate);
      if (!response.ok) {
        continue;
      }
      return await response.json();
    } catch {
      continue;
    }
  }
  return null;
}

function MetricCard({ item, rowCount }) {
  const [label, kind] = sourceTypeTag(item);

  const citation = item?.citations || {};
  const source = item?.source || {};
  const quality = {
    overall_confidence_badge: item?.overall_confidence_badge,
    overall_confidence_reason: item?.overall_confidence_reason || [],
  };
  const retrievalDate = source.retrieved_at || item?.retrieved_at || 'Unknown';
  const license = source.license_terms || 'To be confirmed in source notes';
  const reasons = quality.overall_confidence_reason || [];
  const permanentIdentifier = citation.permanent_identifier || source.permanent_identifier_hint || 'N/A';
  const anchor = citation.anchor || 'pending';
  const primarySource = `${source.publisher || item.source_id} / ${source.title || item?.source_id || 'Unknown source'}`;

  return (
    React.createElement('div', { className: 'metric-card card' },
      React.createElement('div', { className: `badge ${quality.overall_confidence_badge?.toLowerCase() || 'low'}`,
        title: (reasons.length ? reasons.join(' | ') : 'Confidence reasons available in methodology.'),
      },
      quality.overall_confidence_badge || 'Low',
      React.createElement('span', { style: { fontSize: '0.75rem', opacity: 0.95 } }, ' / Why this badge? '),
      React.createElement('a', { href: METHOD_URL, target: '_blank', style: {color: 'inherit', textDecoration: 'underline'} }, 'Methodology')
      ),
      React.createElement('h3', null, source.title || item.source_id),
      React.createElement('div', { className: `source-type ${kind}` }, label),
      React.createElement('div', { className: 'metric-meta' }, `Rows in parquet: ${rowCount}`),
      React.createElement('div', { className: 'metric-meta' }, `Primary source: ${primarySource}`),
      React.createElement('div', { className: 'metric-meta' }, `Retrieval date: ${retrievalDate}`),
      React.createElement('div', { className: 'metric-meta' }, `Permanent identifier: ${permanentIdentifier}`),
      React.createElement('div', { className: 'metric-meta' }, `Citation anchor: ${anchor}`),
      React.createElement('div', { className: 'metric-meta' }, `License: ${license}`),
      React.createElement('div', { className: `status ${item.skip_reason ? 'warn' : 'success'}` },
        item.skip_reason ? `Skipped: ${item.skip_reason}` : 'Ready'
      )
    )
  );
}

function App() {
  const [catalog, setCatalog] = useState({});
  const [rowCounts, setRowCounts] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let mounted = true;

    const run = async () => {
      try {
        const payload = await readCatalog(CATALOG_PATH);
        if (!payload || !payload.datasets) {
          throw new Error('manifest missing');
        }
        const items = payload.datasets || [];
        if (!mounted) return;
        const map = {};
        items.forEach((item) => {
          map[item.source_id] = item;
        });

        const db = await initDuckDB();
        const counts = {};
        for (const item of items) {
          const outputPath = resolveAssetPath(item.output_table_path || `data/processed/${item.source_id}.parquet`);
          try {
            const value = await queryRows(db, outputPath);
            counts[item.source_id] = value;
          } catch (error) {
            counts[item.source_id] = item?.manifest?.row_count || 0;
          }
        }

        if (mounted) {
          setCatalog(map);
          setRowCounts(counts);
          setLoading(false);
        }
      } catch (err) {
        if (mounted) {
          setError(`Could not load catalog. Tried: ${CATALOG_PATH}. Run python -m pipelines.ingest and refresh.`);
          setLoading(false);
        }
      }
    };

    run();
    return () => {
      mounted = false;
    };
  }, []);

  const officialCount = Object.values(catalog).filter((x) => {
    const cat = (x?.metric_category || x?.source_type || '').toLowerCase();
    return x?.source?.official_flag !== false && !cat.includes('proxy') && !cat.includes('model');
  }).length;
  const proxyCount = Object.values(catalog).filter((x) => (x?.source?.official_flag === false) || (x?.metric_category || '').toLowerCase().includes('proxy')).length;
  const modelCount = Object.values(catalog).filter((x) => (x?.metric_category || '').toLowerCase().includes('model')).length;

  return (
    React.createElement('div', { className: 'app-shell' },
      React.createElement('header', null,
        React.createElement('h1', null, 'Bharat Highway Evidence Console'),
        React.createElement('p', { className: 'metric-meta' }, 'Official-first ingestion with source discovery, citations, and confidence scoring.')
      ),
      React.createElement('section', { className: 'summary' },
        React.createElement('div', { className: 'card' }, `Official measured sources: ${officialCount}`),
        React.createElement('div', { className: 'card' }, `Proxy-derived signals: ${proxyCount}`),
        React.createElement('div', { className: 'card' }, `Model output signals: ${modelCount}`),
        React.createElement('div', { className: 'card' }, `Catalog entries: ${Object.keys(catalog).length}`),
        React.createElement('div', { className: 'card' }, loading ? 'Loading…' : `Data rows: ${Object.values(rowCounts).reduce((a, b) => a + (Number(b) || 0), 0)}`)
      ),
      React.createElement('section', { className: 'panel-grid' },
        loading ? React.createElement('div', { className: 'card' }, 'Loading data from DuckDB-WASM...') : null,
        error ? React.createElement('div', { className: 'card' }, error) : null,
        ...Object.keys(catalog).map((sourceId) =>
          React.createElement(MetricCard, {
            key: sourceId,
            item: catalog[sourceId],
            rowCount: rowCounts[sourceId] || 0,
          })
        )
      )
    )
  );
}

createRoot(document.getElementById('root')).render(React.createElement(App));
