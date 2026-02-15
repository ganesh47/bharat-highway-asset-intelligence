import React, { useEffect, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';

const DUCKDB_MODULE_CANDIDATES = [
  'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser.mjs',
];

const DUCKDB_CDN_ROOT = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist';

let cachedDuckDBModule = null;

function normalizePath(path) {
  return String(path || '/').replace(/\/+/g, '/');
}

function getRepoPrefix() {
  const parts = normalizePath(new URL(window.location.href).pathname).split('/').filter(Boolean);
  const appsIndex = parts.lastIndexOf('apps');
  if (appsIndex > 0) {
    return `/${parts.slice(0, appsIndex).join('/')}`;
  }
  if (parts.length > 0 && parts[0] !== 'apps') {
    return `/${parts[0]}`;
  }
  return '';
}

function dedupeConsecutiveSegments(path) {
  const parts = normalizePath(path).split('/').filter(Boolean);
  const out = [];
  parts.forEach((part) => {
    if (out.length === 0 || out[out.length - 1] !== part) {
      out.push(part);
    }
  });
  return `/${out.join('/')}`;
}

function dedupeList(items) {
  const out = [];
  items.forEach((item) => {
    if (!out.includes(item)) {
      out.push(item);
    }
  });
  return out;
}

function rewriteLegacyDataPath(candidatePath) {
  const candidate = normalizePath(candidatePath);
  if (!candidate || !candidate.startsWith('/data/')) {
    return candidate;
  }

  const repoPrefix = getRepoPrefix();
  if (!repoPrefix) {
    return candidate;
  }
  const candidatePathParts = candidate.replace(/^\/+/, '').split('/').filter(Boolean);
  const prefixParts = repoPrefix.replace(/^\/+/, '').split('/').filter(Boolean);

  if (
    prefixParts.length > 0
    && candidatePathParts.length >= prefixParts.length
    && candidatePathParts.slice(0, prefixParts.length).join('/') === prefixParts.join('/')
  ) {
    return candidate;
  }

  if (
    prefixParts.length > 0
    && candidatePathParts.length >= 2 * prefixParts.length
    && candidatePathParts.slice(0, prefixParts.length).join('/') === prefixParts.join('/')
    && candidatePathParts.slice(prefixParts.length, 2 * prefixParts.length).join('/') === prefixParts.join('/')
  ) {
    return normalizePath(`/${candidatePathParts.slice(prefixParts.length).join('/')}`);
  }

  return `${repoPrefix}${candidate}`;
}

function getAssetRoots() {
  const pathname = normalizePath(new URL(window.location.href).pathname);
  const repoPrefix = getRepoPrefix();
  const pageDir = pathname.endsWith('/') ? pathname : pathname.slice(0, pathname.lastIndexOf('/') + 1) || '/';
  const parts = pageDir.split('/').filter(Boolean);
  const appsIndex = parts.lastIndexOf('apps');
  const isRepoHostedApp = appsIndex >= 0 && parts[appsIndex + 1] === 'web';
  const roots = new Set();

  roots.add(dedupeConsecutiveSegments(pageDir) + (pageDir.endsWith('/') ? '/' : ''));

  if (parts.length) {
    if (parts[0] !== 'apps') {
      roots.add(`/${parts[0]}/`);
    }
  }

  if (appsIndex >= 0 && parts[appsIndex + 1] === 'web') {
    const repoRootParts = parts.slice(0, appsIndex);
    if (repoRootParts.length > 0) {
      roots.add(`/${repoRootParts.join('/')}/`);
    } else if (parts.length === 2) {
      roots.add('/');
    }
  } else if (!isRepoHostedApp && !repoPrefix) {
    roots.add('/');
  }

  return Array.from(roots).map((root) => normalizePath(root));
}

function candidateAssetPaths(relPath) {
  const raw = String(relPath || '').trim();
  if (!raw) {
    return ['/'];
  }
  if (/^https?:\/\//i.test(raw)) {
    return [raw];
  }

  const clean = raw.replace(/^\/+/, '');
  const candidates = [];
  const seen = new Set();
  const pathParts = normalizePath(new URL(window.location.href).pathname).split('/').filter(Boolean);
  const pathAppsIndex = pathParts.lastIndexOf('apps');
  const isRepoHostedApp = pathAppsIndex >= 0 && pathParts[pathAppsIndex + 1] === 'web';
  const repoPrefix = getRepoPrefix();
  const hasRepoPrefix = Boolean(repoPrefix && repoPrefix !== '/');
  const add = (value) => {
    const normalized = rewriteLegacyDataPath(value);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    candidates.push(normalized);
  };

  if (isRepoHostedApp) {
    add(normalizePath(new URL(clean, window.location.href).pathname));
    if (repoPrefix) {
      add(normalizePath(`${repoPrefix}/apps/web/${clean}`));
      add(normalizePath(`${repoPrefix}/${clean}`));
    } else {
      add(normalizePath(`/${clean}`));
    }
  } else {
    add(normalizePath(new URL(clean, window.location.href).pathname));
    if (!hasRepoPrefix) {
      add(normalizePath(`/${clean}`));
    }
  }

  getAssetRoots().forEach((root) => {
    add(normalizePath(`${normalizePath(root)}/${clean}`));
  });

  return dedupeList(candidates);
}

async function readCatalog(path) {
  const candidates = candidateAssetPaths(path);
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
      window.__catalogLoadAttempts = window.__catalogLoadAttempts || [];
      window.__catalogLoadAttempts.push(error.message || String(error));
    }
  }
  return Promise.reject(new Error(`Catalog load failed for ${path}.`));
}

function getDuckDBBundleCandidates() {
  return {
    eh: {
      mainModuleCandidates: [
        ...candidateAssetPaths('duckdb/duckdb-eh.wasm'),
        `${DUCKDB_CDN_ROOT}/duckdb-eh.wasm`,
      ],
      mainWorkerCandidates: [
        ...candidateAssetPaths('duckdb/duckdb-browser-eh.worker.js'),
        `${DUCKDB_CDN_ROOT}/duckdb-browser-eh.worker.js`,
      ],
      mainModule: `${DUCKDB_CDN_ROOT}/duckdb-eh.wasm`,
      mainWorker: `${DUCKDB_CDN_ROOT}/duckdb-browser-eh.worker.js`,
      pthreadWorker: null,
    },
    mvp: {
      mainModuleCandidates: [
        ...candidateAssetPaths('duckdb/duckdb-mvp.wasm'),
        `${DUCKDB_CDN_ROOT}/duckdb-mvp.wasm`,
      ],
      mainWorkerCandidates: [
        ...candidateAssetPaths('duckdb/duckdb-browser-mvp.worker.js'),
        `${DUCKDB_CDN_ROOT}/duckdb-browser-mvp.worker.js`,
      ],
      mainModule: `${DUCKDB_CDN_ROOT}/duckdb-mvp.wasm`,
      mainWorker: `${DUCKDB_CDN_ROOT}/duckdb-browser-mvp.worker.js`,
      pthreadWorker: null,
    },
  };
}

async function loadDuckDBModule() {
  if (cachedDuckDBModule) {
    return cachedDuckDBModule;
  }

  const moduleSources = [];
  const seen = new Set();
  DUCKDB_MODULE_CANDIDATES.forEach((candidate) => {
    if (/^https?:\/\//i.test(candidate)) {
      if (!seen.has(candidate)) {
        seen.add(candidate);
        moduleSources.push(candidate);
      }
    } else {
      candidateAssetPaths(candidate).forEach((candidatePath) => {
        if (!seen.has(candidatePath)) {
          seen.add(candidatePath);
          moduleSources.push(candidatePath);
        }
      });
    }
  });

  const importErrors = [];
  for (const source of moduleSources) {
    try {
      const duckdb = await import(source);
      if (duckdb?.getPlatformFeatures && duckdb?.AsyncDuckDB && duckdb?.AsyncDuckDBConnection) {
        cachedDuckDBModule = duckdb;
        return duckdb;
      }
      importErrors.push(`${source}: missing DuckDB exports`);
    } catch (error) {
      importErrors.push(`${source}: ${error?.message || error}`);
    }
  }

  throw new Error(`DuckDB module load failed (${importErrors.join(' | ')})`);
}

async function initDuckDB() {
  const duckdb = await loadDuckDBModule();
  const features = await duckdb.getPlatformFeatures();
  const bundles = getDuckDBBundleCandidates();
  const selected = features.wasmSIMD && features.wasmExceptions ? bundles.eh : bundles.mvp;
  const logger = new duckdb.ConsoleLogger();

  const instantiate = async (mainModule, mainWorker, selectedBundle) => {
    const worker = new Worker(mainWorker, { type: 'module' });
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(mainModule, selectedBundle.pthreadWorker);
    return db;
  };

  const tryBundle = async (bundle) => {
    const moduleCount = Math.max(bundle.mainModuleCandidates.length, 1);
    const workerCount = Math.max(bundle.mainWorkerCandidates.length, 1);
    const total = Math.max(moduleCount, workerCount);

    for (let i = 0; i < total; i += 1) {
      const moduleUrl = bundle.mainModuleCandidates[i] || bundle.mainModule;
      const workerUrl = bundle.mainWorkerCandidates[i] || bundle.mainWorker;
      try {
        return await instantiate(moduleUrl, workerUrl, bundle);
      } catch (error) {
        window.__duckDbLoadAttempts = window.__duckDbLoadAttempts || [];
        window.__duckDbLoadAttempts.push(`${moduleUrl} | ${workerUrl}: ${error?.message || error}`);
      }
    }
    throw new Error('All DuckDB bootstrap candidates failed');
  };

  try {
    return await tryBundle(selected);
  } catch (error) {
    if (selected === bundles.mvp) {
      throw error;
    }
    return tryBundle(bundles.mvp);
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
  const candidates = candidateAssetPaths(path);
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
  const methodologyUrl = candidateAssetPaths('methodology.html')[0] || 'methodology.html';

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
