import React, { useEffect, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser.mjs';

function pickAssetPath(pathCandidates) {
  return pickCanonicalAssetPath(pathCandidates) || resolveAssetPath(pathCandidates);
}

function pickDuckDBAssetPath(relPath) {
  return pickAssetPath(assetCandidates(relPath));
}

function getDuckDBBundleCandidates() {
  const eh = {
    mainModule: pickDuckDBAssetPath('duckdb/duckdb-eh.wasm'),
    mainWorker: pickDuckDBAssetPath('duckdb/duckdb-browser-eh.worker.js'),
    pthreadWorker: null,
  };
  const mvp = {
    mainModule: pickDuckDBAssetPath('duckdb/duckdb-mvp.wasm'),
    mainWorker: pickDuckDBAssetPath('duckdb/duckdb-browser-mvp.worker.js'),
    pthreadWorker: null,
  };
  return { eh, mvp };
}

function getDuckDBBundle() {
  const bundles = getDuckDBBundleCandidates();
  return { eh: { ...bundles.eh }, mvp: { ...bundles.mvp } };
}

function resolveAssetPath(relPath) {
  const pathname = window.location.pathname || '/';
  const normalizedPath = pathname.endsWith('/') ? pathname : `${pathname}/`;
  const rootPath = normalizedPath.replace(/\/?apps\/web\/?$/, '/');
  const sanitizedRoot = rootPath.endsWith('/') ? rootPath : `${rootPath}/`;
  const sanitizedRel = relPath.replace(/^\/+/, '');
  return `${sanitizedRoot}${sanitizedRel}`;
}

function moduleRootPath() {
  try {
    const modulePath = new URL(import.meta.url).pathname;
    const stripped = modulePath.replace(/\/src\/app\.js$/, '').replace(/\/+$/, '');
    const normalized = stripped.replace(/\/?apps\/web\/?$/, '/');
    return normalized.endsWith('/') ? normalized : `${normalized}/`;
  } catch {
    return '/';
  }
}

function collapseLeadingDuplicateSegments(pathname) {
  const trimmed = pathname.replace(/^\/+|\/+$/g, '');
  const parts = trimmed.split('/').filter(Boolean);
  if (parts.length >= 2 && parts[0] === parts[1]) {
    return `/${parts.slice(1).join('/')}`;
  }
  return null;
}

function assetCandidates(relPath) {
  const direct = relPath.replace(/^\/+/, '');
  const seen = new Set();
  const candidates = [];
  const add = (value) => {
    if (!value) {
      return;
    }
    const normalizedValues = [];
    const normalized = (value.startsWith('/') ? value : `/${value}`).replace(/\/{2,}/g, '/');
    normalizedValues.push(normalized);
    const collapsed = collapseLeadingDuplicateSegments(normalized);
    if (collapsed && collapsed !== normalized) {
      normalizedValues.push(collapsed);
    }

    for (const candidate of normalizedValues) {
      if (!seen.has(candidate)) {
        seen.add(candidate);
        candidates.push(candidate);
      }
    }
  };

  add(resolveAssetPath(direct));
  add(`${moduleRootPath()}/${direct}`);

  const scriptPath = document.currentScript?.src;
  if (scriptPath && scriptPath.includes('/apps/web/')) {
    try {
      const root = new URL(scriptPath).pathname.split('/apps/web/')[0];
      add(`${root}/${direct}`);
    } catch {
      // keep best-effort
    }
  }

  const pathname = (window.location.pathname || '/').replace(/^\/+/, '');
  const firstSegment = pathname.split('/').filter(Boolean)[0];
  if (firstSegment && !['apps', 'data', 'src', 'methodology.html'].includes(firstSegment)) {
    add(`/${firstSegment}/${direct}`);
  }

  add(`/${direct}`);
  return candidates;
}

function pickCanonicalAssetPath(candidates) {
  if (!Array.isArray(candidates) || !candidates.length) {
    return null;
  }
  for (const candidate of candidates) {
    if (!collapseLeadingDuplicateSegments(candidate)) {
      return candidate;
    }
  }
  return candidates[0];
}

const CATALOG_PATH = pickCanonicalAssetPath(assetCandidates('data/manifests/catalog.json')) || resolveAssetPath('data/manifests/catalog.json');
const METHOD_URL = pickCanonicalAssetPath(assetCandidates('methodology.html')) || new URL('methodology.html', window.location.href).pathname;

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
  const features = await duckdb.getPlatformFeatures();
  const localBundles = getDuckDBBundle();
  const selected = (features.wasmSIMD && features.wasmExceptions)
    ? localBundles.eh
    : localBundles.mvp;

  if (!selected.mainModule || !selected.mainWorker) {
    throw new Error('Local DuckDB assets are missing (manifested paths not found).');
  }

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
    if (selected !== localBundles.mvp && localBundles.mvp.mainModule && localBundles.mvp.mainWorker) {
      return await instantiate(localBundles.mvp);
    } else {
      throw error;
    }
  }
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
  const candidates = assetCandidates(path);
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
  const catalogCandidates = assetCandidates(path);
  const failures = [];
  for (const candidate of catalogCandidates) {
    try {
      const response = await fetch(candidate);
      if (!response.ok) {
        failures.push(`${candidate}: ${response.status}`);
        continue;
      }
      const payload = await response.json();
      if (!payload || !payload.datasets) {
        failures.push(`${candidate}: missing datasets`);
        continue;
      }
      return payload;
    } catch (error) {
      failures.push(`${candidate}: ${error?.message || 'parse error'}`);
      continue;
    }
  }
  if (typeof window !== 'undefined') {
    window.__catalogLoadFailures = failures;
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
        const payload = await readCatalog('data/manifests/catalog.json');
        if (!payload || !payload.datasets) {
          const attempts = window.__catalogLoadFailures || [];
          const details = attempts.length ? ` Attempts: ${attempts.join(' | ')}` : '';
          throw new Error(`Catalog load failed for ${CATALOG_PATH}.${details}`);
        }
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
