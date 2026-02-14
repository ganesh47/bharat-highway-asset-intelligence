import React, { useEffect, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

function detectSiteBase() {
  const path = window.location.pathname || '/';
  const normalized = path.replace(/\/+/g, '/');
  const markerIndex = normalized.indexOf('/apps/web/');
  if (markerIndex >= 0) {
    return normalized.slice(0, markerIndex + 1);
  }
  if (normalized.endsWith('/')) {
    return normalized;
  }
  return `${normalized.replace(/\/+[^/]*$/, '')}/`;
}

const SITE_BASE = detectSiteBase();

function sitePath(relPath) {
  const normalized = relPath.replace(/^\/+/, '');
  const base = SITE_BASE || '/';
  return `${base}${normalized}`.replace(/\/{2,}/g, '/');
}

function candidatePaths(relPath) {
  const clean = relPath.replace(/^\/+/, '');
  const candidates = [sitePath(clean), `/${clean}`];
  if (SITE_BASE && SITE_BASE.length > 1 && SITE_BASE !== '/'){
    const firstSegment = SITE_BASE.split('/').filter(Boolean)[0];
    if (firstSegment) {
      candidates.push(`/${firstSegment}/${clean}`);
    }
  }
  const firstSegment = (window.location.pathname || '').split('/').filter(Boolean)[0];
  const alt = firstSegment ? `/${firstSegment}/${clean}` : null;
  if (alt && !candidates.includes(alt)) {
    candidates.push(alt);
  }

  // Handle accidental duplicated repository base segments (e.g. /repo/repo/...).
  const parts = (window.location.pathname || '/').split('/').filter(Boolean);
  if (parts.length >= 2 && parts[0] === parts[1] && parts[0]) {
    const dedup = `/${parts[0]}/${clean}`;
    if (!candidates.includes(dedup)) {
      candidates.push(dedup);
    }
  }

  return [...new Set(candidates)];
}

async function readCatalog(path) {
  const candidates = candidatePaths(path);
  for (const candidate of candidates) {
    try {
      const response = await fetch(candidate, { cache: 'no-store' });
      if (!response.ok) {
        throw new Error(`${candidate}: ${response.status}`);
      }
      const payload = await response.json();
      if (!payload || !payload.datasets) {
        throw new Error(`${candidate}: missing datasets field`);
      }
      return payload;
    } catch (error) {
      if (typeof window !== 'undefined') {
        window.__catalogLoadAttempts = window.__catalogLoadAttempts || [];
        window.__catalogLoadAttempts.push(error.message || String(error));
      }
    }
  }
  return Promise.reject(new Error(`Catalog load failed for ${path}.`));
}

function getDuckDBBundleCandidates() {
  return {
    eh: {
      mainModule: sitePath('duckdb/duckdb-eh.wasm'),
      mainWorker: sitePath('duckdb/duckdb-browser-eh.worker.js'),
      pthreadWorker: null,
    },
    mvp: {
      mainModule: sitePath('duckdb/duckdb-mvp.wasm'),
      mainWorker: sitePath('duckdb/duckdb-browser-mvp.worker.js'),
      pthreadWorker: null,
    },
  };
}

async function initDuckDB() {
  const features = await duckdb.getPlatformFeatures();
  const bundles = getDuckDBBundleCandidates();
  const selected = features.wasmSIMD && features.wasmExceptions ? bundles.eh : bundles.mvp;
  const logger = new duckdb.ConsoleLogger();

  const instantiate = async (bundle) => {
    const worker = new Worker(bundle.mainWorker, { type: 'module' });
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return db;
  };

  try {
    return await instantiate(selected);
  } catch (error) {
    if (selected !== bundles.mvp) {
      return await instantiate(bundles.mvp);
    }
    throw error;
  }
}

function extractRows(result) {
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
    } catch {
      return [];
    }
  }
  return [];
}

async function queryParquetRowCount(db, path) {
  const candidates = candidatePaths(path);
  const conn = await db.connect();

  for (const candidate of candidates) {
    try {
      const fileName = candidate.split('/').pop() || 'table.parquet';
      const response = await fetch(candidate, { cache: 'no-store' });
      if (!response.ok) {
        continue;
      }
      const buffer = await response.arrayBuffer();
      await conn.registerFileBuffer(fileName, new Uint8Array(buffer));
      const query = await conn.query(`SELECT COUNT(*)::BIGINT AS row_count FROM read_parquet('${fileName}')`);
      const rows = extractRows(query);
      if (!rows.length) return 0;
      return Number(rows[0]?.row_count || 0);
    } catch {
      continue;
    }
  }
  return null;
}

function clamp(n, min, max) {
  if (n == null || Number.isNaN(n)) {
    return min;
  }
  return Math.max(min, Math.min(max, n));
}

function SourceRowCountChart({ catalog, rowCounts }) {
  const items = Object.values(catalog || {})
    .map((item) => ({ item, rows: Number(rowCounts[item.source_id] || 0) }))
    .filter((entry) => entry.rows > 0)
    .sort((a, b) => b.rows - a.rows);

  if (!items.length) {
    return React.createElement('div', { className: 'card' }, 'No row-count data available yet.');
  }

  const maxRows = Math.max(...items.map((entry) => entry.rows), 1);
  return React.createElement(
    'div',
    { className: 'chart card' },
    React.createElement('h2', null, 'Top Source Row Counts'),
    React.createElement(
      'div',
      { className: 'bars' },
      ...items.slice(0, 12).map(({ item, rows }) => {
        const width = clamp(Math.round((rows / maxRows) * 100), 5, 100);
        const label = item?.source?.title || item.source_id || 'Source';
        return React.createElement(
          'div',
          { key: item.source_id, className: 'bar-row' },
          React.createElement('div', { className: 'bar-label', title: label }, label),
          React.createElement(
            'div',
            { className: 'bar-track' },
            React.createElement('div', { className: 'bar-fill', style: { width: `${width}%` } })
          ),
          React.createElement('div', { className: 'bar-value' }, rows.toLocaleString())
        );
      })
    )
  );
}

function QualityBreakdown({ catalog }) {
  const official = [];
  const proxy = [];
  const model = [];

  Object.values(catalog || {}).forEach((item) => {
    const category = (item?.metric_category || item?.source_type || item?.source?.source_type || '').toLowerCase();
    if (item?.source?.official_flag === false || category.includes('proxy')) {
      proxy.push(item);
      return;
    }
    if (category.includes('model')) {
      model.push(item);
      return;
    }
    official.push(item);
  });

  return React.createElement(
    'div',
    { className: 'chart card' },
    React.createElement('h2', null, 'Coverage by Type'),
    React.createElement(
      'div',
      { className: 'coverage-grid' },
      React.createElement('div', { className: 'coverage-cell' }, `Official measured: ${official.length}`),
      React.createElement('div', { className: 'coverage-cell' }, `Proxy-derived: ${proxy.length}`),
      React.createElement('div', { className: 'coverage-cell' }, `Model outputs: ${model.length}`)
    )
  );
}

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
  const methodologyUrl = sitePath('methodology.html');

  return React.createElement(
    'div',
    { className: 'metric-card card' },
    React.createElement(
      'div',
      {
        className: `badge ${quality.overall_confidence_badge?.toLowerCase() || 'low'}`,
        title: reasons.length ? reasons.join(' | ') : 'Confidence reasons available in methodology.',
      },
      quality.overall_confidence_badge || 'Low',
      React.createElement('span', { style: { fontSize: '0.75rem', opacity: 0.95 } }, ' / Why this badge? '),
      React.createElement('a', { href: methodologyUrl, target: '_blank', style: { color: 'inherit', textDecoration: 'underline' } }, 'Methodology')
    ),
    React.createElement('h3', null, source.title || item.source_id),
    React.createElement('div', { className: `source-type ${kind}` }, label),
    React.createElement('div', { className: 'metric-meta' }, `Rows in parquet: ${rowCount}`),
    React.createElement('div', { className: 'metric-meta' }, `Primary source: ${primarySource}`),
    React.createElement('div', { className: 'metric-meta' }, `Retrieval date: ${retrievalDate}`),
    React.createElement('div', { className: 'metric-meta' }, `Permanent identifier: ${permanentIdentifier}`),
    React.createElement('div', { className: 'metric-meta' }, `Citation anchor: ${anchor}`),
    React.createElement('div', { className: 'metric-meta' }, `License: ${license}`),
    React.createElement('div', { className: `status ${item.skip_reason ? 'warn' : 'success'}` }, item.skip_reason ? `Skipped: ${item.skip_reason}` : 'Ready')
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
        const payload = await readCatalog('data/manifests/catalog.json');
        const items = payload.datasets || [];
        if (!mounted) return;

        const map = {};
        items.forEach((item) => {
          map[item.source_id] = item;
        });

        let db;
        try {
          db = await initDuckDB();
        } catch (duckDbErr) {
          throw new Error(`DuckDB initialization failed: ${duckDbErr?.message || 'Unknown error'}`);
        }

        const counts = {};
        for (const item of items) {
          const outputPath = item.output_table_path || `data/processed/${item.source_id}.parquet`;
          try {
            const value = await queryParquetRowCount(db, outputPath);
            counts[item.source_id] = Number.isFinite(value) ? value : Number(item?.manifest?.row_count || 0);
          } catch {
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
          setError(`${err?.message || 'Data pipeline init failed'}. Hard refresh (Ctrl/Cmd+Shift+R). If running locally: python -m pipelines.ingest then refresh.`);
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
    const category = (x?.metric_category || x?.source_type || '').toLowerCase();
    return x?.source?.official_flag !== false && !category.includes('proxy') && !category.includes('model');
  }).length;
  const proxyCount = Object.values(catalog).filter((x) => (x?.source?.official_flag === false) || (x?.metric_category || '').toLowerCase().includes('proxy')).length;
  const modelCount = Object.values(catalog).filter((x) => (x?.metric_category || '').toLowerCase().includes('model')).length;

  return React.createElement(
    'div',
    { className: 'app-shell' },
    React.createElement('header', null, React.createElement('h1', null, 'Bharat Highway Evidence Console'), React.createElement('p', { className: 'metric-meta' }, 'Official-first ingestion with source discovery, citations, and confidence scoring.')),
    React.createElement('section', { className: 'summary' },
      React.createElement('div', { className: 'card' }, `Official measured sources: ${officialCount}`),
      React.createElement('div', { className: 'card' }, `Proxy-derived signals: ${proxyCount}`),
      React.createElement('div', { className: 'card' }, `Model output signals: ${modelCount}`),
      React.createElement('div', { className: 'card' }, `Catalog entries: ${Object.keys(catalog).length}`),
      React.createElement('div', { className: 'card' }, loading ? 'Loading…' : `Data rows: ${Object.values(rowCounts).reduce((a, b) => a + (Number(b) || 0), 0)}`)
    ),
    React.createElement('section', { className: 'charts-grid' },
      React.createElement(QualityBreakdown, { catalog }),
      React.createElement(SourceRowCountChart, { catalog, rowCounts })
    ),
    React.createElement('section', { className: 'panel-grid' },
      loading ? React.createElement('div', { className: 'card' }, 'Loading data from DuckDB-WASM...') : null,
      error ? React.createElement('div', { className: 'card' }, error) : null,
      ...Object.keys(catalog).map((sourceId) =>
        React.createElement(MetricCard, { key: sourceId, item: catalog[sourceId], rowCount: rowCounts[sourceId] || 0 })
      )
    )
  );
}

createRoot(document.getElementById('root')).render(React.createElement(App));
