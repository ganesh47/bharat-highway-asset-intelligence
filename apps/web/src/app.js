import React, { useEffect, useMemo, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';
import { inferOntologyCoverage } from './ontology.js';

const DUCKDB_CDN_ROOT = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist';
const ESM_ARROW_IMPORTMAP_NOTE = 'Ensure importmap keeps apache-arrow local path.';
const DUCKDB_MODULE_CANDIDATES = [
  './duckdb/duckdb-browser.mjs',
  '/duckdb/duckdb-browser.mjs',
  `${DUCKDB_CDN_ROOT}/duckdb-browser.mjs`,
];

let cachedDuckDBModule = null;
let cachedDuckDBInstance = null;
let cachedDuckDBConnection = null;
let sourceAliasCache = new Map();

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
  const candidateParts = candidate.replace(/^\/+/, '').split('/').filter(Boolean);
  const repoParts = repoPrefix.replace(/^\/+/, '').split('/').filter(Boolean);

  if (
    repoParts.length > 0 &&
    candidateParts.length >= repoParts.length &&
    candidateParts.slice(0, repoParts.length).join('/') === repoParts.join('/')
  ) {
    return candidate;
  }

  if (
    repoParts.length > 0 &&
    candidateParts.length >= 2 * repoParts.length &&
    candidateParts.slice(0, repoParts.length).join('/') === repoParts.join('/') &&
    candidateParts.slice(repoParts.length, 2 * repoParts.length).join('/') === repoParts.join('/')
  ) {
    return normalizePath(`/${candidateParts.slice(repoParts.length).join('/')}`);
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
  } else if (repoPrefix) {
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

  const clean = raw
    .replace(/^\/+/, '')
    .replace(/^\.\/+/, '')
    .replace(/^\/+/, '');
  const candidates = [];
  const seen = new Set();
  const pathParts = normalizePath(new URL(window.location.href).pathname).split('/').filter(Boolean);
  const pathAppsIndex = pathParts.lastIndexOf('apps');
  const isRepoHostedApp = pathAppsIndex >= 0 && pathParts[pathAppsIndex + 1] === 'web';
  const repoPrefix = getRepoPrefix();
  const add = (value) => {
    const normalized = rewriteLegacyDataPath(value);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    candidates.push(normalized);
  };

  if (isRepoHostedApp) {
    if (clean.startsWith('data/')) {
      if (repoPrefix) {
        add(normalizePath(`${repoPrefix}/${clean}`));
      } else {
        add(normalizePath(`/${clean}`));
      }
    } else {
      add(normalizePath(new URL(clean, window.location.href).pathname));
      if (repoPrefix) {
        add(normalizePath(`${repoPrefix}/apps/web/${clean}`));
        add(normalizePath(`${repoPrefix}/${clean}`));
      } else {
        add(normalizePath(`/apps/web/${clean}`));
      }
    }
  } else {
    if (clean.startsWith('data/')) {
      add(normalizePath(`/${clean}`));
    } else {
      add(normalizePath(new URL(clean, window.location.href).pathname));
    }

    if (!repoPrefix) {
      add(normalizePath(`/${clean}`));
    }
  }

  if (!clean.startsWith('data/')) {
    getAssetRoots().forEach((root) => {
    add(normalizePath(`${normalizePath(root)}/${clean}`));
  });
  }

  return dedupeList(candidates);
}

function stableAlias(seed) {
  const text = String(seed || '');
  let acc = 0;
  for (let i = 0; i < text.length; i += 1) {
    acc = (acc * 31 + text.charCodeAt(i)) % 2147483647;
  }
  const safe = text
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 45) || 'dataset';
  return `${safe}_${Math.abs(acc)}`;
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

function getDuckDBBundleCandidates() {
  return {
    eh: {
      mainModuleCandidates: [...candidateAssetPaths('duckdb/duckdb-eh.wasm'), `${DUCKDB_CDN_ROOT}/duckdb-eh.wasm`],
      mainWorkerCandidates: [
        ...candidateAssetPaths('duckdb/duckdb-browser-eh.worker.js'),
        `${DUCKDB_CDN_ROOT}/duckdb-browser-eh.worker.js`,
      ],
      mainModule: `${DUCKDB_CDN_ROOT}/duckdb-eh.wasm`,
      mainWorker: `${DUCKDB_CDN_ROOT}/duckdb-browser-eh.worker.js`,
      pthreadWorker: null,
    },
    mvp: {
      mainModuleCandidates: [...candidateAssetPaths('duckdb/duckdb-mvp.wasm'), `${DUCKDB_CDN_ROOT}/duckdb-mvp.wasm`],
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

async function initDuckDB() {
  if (cachedDuckDBInstance && cachedDuckDBConnection) {
    return cachedDuckDBConnection;
  }
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
        const db = await instantiate(moduleUrl, workerUrl, bundle);
        const conn = await db.connect();
        cachedDuckDBInstance = db;
        cachedDuckDBConnection = conn;
        return conn;
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
  if (typeof result.toArrayOfObjects === 'function') {
    return result.toArrayOfObjects();
  }
  if (typeof result.toArray === 'function') {
    const rows = result.toArray();
    if (!rows.length) {
      return [];
    }
    const firstRow = rows[0];
    if (firstRow && typeof firstRow === 'object' && !Array.isArray(firstRow)) {
      return rows;
    }
    if (!Array.isArray(firstRow)) {
      return [];
    }
    const names = result.columnNames || [];
    if (!names.length) {
      return rows.map((row) => Object.fromEntries(row.map((value, index) => [`column_${index}`, value])));
    }
    return rows.map((row) => {
      const obj = {};
      names.forEach((name, index) => {
        obj[name] = row[index];
      });
      return obj;
    });
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

function ensureSourceAlias(conn, sourcePath) {
  const cacheKey = normalizePath(sourcePath || '');
  if (sourceAliasCache.has(cacheKey)) {
    return sourceAliasCache.get(cacheKey);
  }
  const alias = stableAlias(cacheKey);
  sourceAliasCache.set(cacheKey, alias);
  return alias;
}

async function registerSourceBuffer(conn, sourcePath) {
  const alias = ensureSourceAlias(conn, sourcePath);
  if (alias && sourceAliasCache.get(`${sourcePath}::loaded`)) {
    return alias;
  }

  const candidates = candidateAssetPaths(sourcePath);
  const lastError = [];
  for (const candidate of candidates) {
    try {
      const response = await fetch(candidate, { cache: 'no-store' });
      if (!response.ok) {
        lastError.push(`${candidate}: ${response.status}`);
        continue;
      }
      const buffer = await response.arrayBuffer();
      if (!cachedDuckDBInstance || typeof cachedDuckDBInstance.registerFileBuffer !== 'function') {
        throw new Error('DuckDB instance is not ready for file registration');
      }
      await cachedDuckDBInstance.registerFileBuffer(alias, new Uint8Array(buffer));
      sourceAliasCache.set(`${sourcePath}::loaded`, true);
      return alias;
    } catch (error) {
      lastError.push(`${candidate}: ${error?.message || error}`);
    }
  }
  throw new Error(`Parquet unavailable: ${sourcePath}. Tried: ${lastError.join(' | ')}`);
}

async function queryParquetRows(conn, sourcePath, queryFactory) {
  const alias = await registerSourceBuffer(conn, sourcePath);
  const sql = typeof queryFactory === 'function' ? queryFactory(alias) : queryFactory;
  let query;
  try {
    query = await conn.query(sql);
  } catch (error) {
    throw error;
  }
  const rows = extractRows(query);
  return rows;
}

async function countRows(conn, sourcePath) {
  const rows = await queryParquetRows(conn, sourcePath, (alias) => `SELECT COUNT(*)::BIGINT AS row_count FROM read_parquet('${alias}')`);
  if (!rows.length) {
    return 0;
  }
  return Number(rows[0]?.row_count || 0);
}

function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function fmtNum(value, options = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return 'N/A';
  }
  const { compact = true, decimals = 2 } = options;
  if (compact && Math.abs(n) >= 10000000) {
    return new Intl.NumberFormat('en-IN', { notation: 'compact', maximumFractionDigits: 1 }).format(n);
  }
  return new Intl.NumberFormat('en-IN', {
    maximumFractionDigits: decimals,
  }).format(n);
}

function clamp(v, min, max) {
  if (v == null || Number.isNaN(v)) {
    return min;
  }
  return Math.max(min, Math.min(max, v));
}

function safeLabel(v) {
  return String(v || 'Unknown');
}

function normalizeState(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, '')
    .replace(/\s+/g, ' ');
}

function toYearNumeric(value) {
  const text = String(value || '').trim();
  if (!text) {
    return NaN;
  }
  const match = text.match(/(\d{4})/);
  return match ? Number(match[1]) : NaN;
}

function sourceTypeTag(item) {
  const category = String(item?.metric_category || item?.source_type || item?.source?.source_type || '').toLowerCase();
  if (item?.source?.official_flag === false || category.includes('proxy')) {
    return ['Proxy-derived', 'proxy'];
  }
  if (category.includes('model')) {
    return ['Model outputs', 'model'];
  }
  return ['Official measured', 'official'];
}

function confidenceFromSources(entries) {
  if (!entries.length) {
    return {
      badge: 'Low',
      reasons: ['No source confidence metadata available yet.'],
      score: 0,
    };
  }

  let minScore = 1;
  const reasons = new Set();
  entries.forEach((entry) => {
    const badge = String(entry?.overall_confidence_badge || 'Low').toLowerCase();
    if (badge === 'high') minScore = Math.min(minScore, 3);
    else if (badge === 'med') minScore = Math.min(minScore, 2);
    else minScore = Math.min(minScore, 1);
    (entry?.overall_confidence_reason || []).forEach((reason) => reasons.add(String(reason)));
  });

  const scoreMap = { 1: 'Low', 2: 'Med', 3: 'High' };
  const badge = scoreMap[minScore] || 'Low';
  if (!reasons.size) {
    reasons.add(`Lowest contributing source confidence is ${badge}.`);
  }
  return { badge, reasons: Array.from(reasons), score: minScore };
}

function tooltipText(lines) {
  return lines.filter(Boolean).join('\n');
}

function ChartTooltip({ tooltip }) {
  if (!tooltip?.visible) return null;
  const style = {
    left: `${tooltip.x + 10}px`,
    top: `${tooltip.y + 10}px`,
  };
  return React.createElement('div', { className: 'tooltip', style }, tooltip.text.split('\n').map((line) => React.createElement('div', { key: line }, line)));
}

function LineChart({ title, description, series, xTick, tooltipKey, confidence, onHover, chartScale = 1 }) {
  const points = (series || []).filter((item) => num(item.y, null) !== null);
  if (!points.length) {
    return React.createElement('div', { className: 'card insight-chart' }, React.createElement('div', { className: 'chart-title' }, title), React.createElement('div', { className: 'chart-meta' }, description || 'No records available.'));
  }

  const xValues = points.map((item) => num(item.x));
  const yValues = points.map((item) => num(item.y));
  const xMin = Math.min(...xValues);
  const xMax = Math.max(...xValues);
  const yMin = Math.min(...yValues);
  const yMax = Math.max(...yValues);
  const width = 980;
  const height = Math.round(270 * chartScale);
  const pad = { top: 16, right: 16, bottom: 26, left: 42 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  const xScale = (v) => pad.left + ((num(v) - xMin) / (xMax - xMin || 1)) * plotW;
  const yScale = (v) => pad.top + (1 - (num(v) - yMin) / (yMax - yMin || 1)) * plotH;

  const d = points
    .map((point, index) => `${index === 0 ? 'M' : 'L'} ${xScale(point.x)} ${yScale(point.y)}`)
    .join(' ');

  const labels = points.filter((_, index) => index % Math.max(1, Math.floor(points.length / 6)) === 0);

  return React.createElement('div', { className: 'card insight-chart' },
    React.createElement(
      'div',
      { className: 'source-line' },
      React.createElement('div', { className: 'chart-title' }, title),
      React.createElement('span', { className: `badge ${String(confidence.badge || 'low').toLowerCase()}` }, `${confidence.badge || 'Low'} confidence`)
    ),
    React.createElement('div', { className: 'chart-meta' }, description || ''),
    React.createElement('div', { className: 'insight-legend' },
      React.createElement('span', { className: 'insight-pill' }, `Points: ${points.length}`),
      React.createElement('span', { className: 'insight-pill' }, `Source trust: ${confidence.badge || 'Low'}`)),
    React.createElement('div', { className: 'chart-svg-wrap' },
      React.createElement(
        'svg',
        { viewBox: `0 0 ${width} ${Math.round(270 * chartScale)}`, width: '100%', height: `${height}px`, style: { height: `${height}px`, width: '100%' } },
        React.createElement('line', { x1: pad.left, y1: pad.top + plotH, x2: width - pad.right, y2: pad.top + plotH, className: 'axis-line' }),
        React.createElement('line', { x1: pad.left, y1: pad.top, x2: pad.left, y2: pad.top + plotH, className: 'axis-line' }),
        labels.map((point) =>
          React.createElement('text', {
            key: `x-${point.x}`,
            x: xScale(point.x),
            y: pad.top + plotH + 14,
            fontSize: 10,
            textAnchor: 'middle',
            fill: '#3b5068',
          }, xTick ? xTick(point.x) : safeLabel(point.x))
        ),
        points.map((point, i) =>
          React.createElement('circle', {
            key: `${i}-${point.x}`,
            className: 'point',
            cx: xScale(point.x),
            cy: yScale(point.y),
            r: 3,
            fill: '#2f5f99',
            onMouseEnter: (event) => onHover({
              visible: true,
              x: event.clientX,
              y: event.clientY,
              text: tooltipText([tooltipKey || 'value', `${safeLabel(point.label)}`, `x: ${safeLabel(point.x)}`, `y: ${fmtNum(point.y, { compact: false })}`]),
            }),
            onMouseMove: (event) => onHover({
              visible: true,
              x: event.clientX,
              y: event.clientY,
              text: tooltipText([tooltipKey || 'value', `${safeLabel(point.label)}`, `x: ${safeLabel(point.x)}`, `y: ${fmtNum(point.y, { compact: false })}`]),
            }),
            onMouseLeave: () => onHover({ visible: false }),
          })
        ),
        React.createElement('path', { d, className: 'line-path', stroke: '#2f5f99', fill: 'none', strokeWidth: 2.4 })
      )
    ),
    React.createElement('div', { className: 'insight-note' }, confidence.reasons.slice(0, 2).map((r) => r).join(' '))
  );
}

function MultiLineChart({ title, description, layers, xTick, confidence, onHover, tooltipTextLabel, chartScale = 1 }) {
  const allSeries = layers.flatMap((layer) => layer.points || []);
  if (!allSeries.length) {
    return React.createElement('div', { className: 'card insight-chart' }, React.createElement('div', { className: 'chart-title' }, title), React.createElement('div', { className: 'chart-meta' }, description || 'No records available.'));
  }

  const xExtent = {
    min: Math.min(...allSeries.map((item) => num(item.x))),
    max: Math.max(...allSeries.map((item) => num(item.x))),
  };
  const yExtent = {
    min: Math.min(...allSeries.map((item) => num(item.y))),
    max: Math.max(...allSeries.map((item) => num(item.y))),
  };

  const width = 980;
  const height = Math.round(290 * chartScale);
  const pad = { top: 16, right: 16, bottom: 26, left: 42 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;
  const xScale = (value) => pad.left + ((num(value) - xExtent.min) / (xExtent.max - xExtent.min || 1)) * plotW;
  const yScale = (value) => pad.top + (1 - (num(value) - yExtent.min) / (yExtent.max - yExtent.min || 1)) * plotH;
  const palette = ['#1b4d91', '#0a8f52', '#b07a00', '#a0182d', '#5f4eeb', '#5f8a4e'];

  return React.createElement('div', { className: 'card insight-chart' },
    React.createElement('div', { className: 'source-line' },
      React.createElement('div', { className: 'chart-title' }, title),
      React.createElement('span', { className: `badge ${String(confidence.badge || 'low').toLowerCase()}` }, `${confidence.badge || 'Low'} confidence`)
    ),
    React.createElement('div', { className: 'chart-meta' }, description || ''),
    React.createElement(
      'div',
      { className: 'insight-legend' },
      ...layers.map((layer, idx) =>
        React.createElement('span', { key: layer.key, className: 'insight-pill', style: { borderColor: palette[idx % palette.length], color: '#1f3650' } }, layer.name)
      )
    ),
    React.createElement(
      'div',
      { className: 'chart-svg-wrap' },
      React.createElement(
        'svg',
        { viewBox: `0 0 ${width} ${Math.round(290 * chartScale)}`, width: '100%', height: `${height}px`, style: { height: `${height}px`, width: '100%' } },
        React.createElement('line', { x1: pad.left, y1: pad.top + plotH, x2: width - pad.right, y2: pad.top + plotH, className: 'axis-line' }),
        React.createElement('line', { x1: pad.left, y1: pad.top, x2: pad.left, y2: pad.top + plotH, className: 'axis-line' }),
        layers.map((layer, layerIndex) => {
          const points = (layer.points || [])
            .filter((point) => Number.isFinite(num(point.x)) && Number.isFinite(num(point.y)))
            .sort((a, b) => num(a.x) - num(b.x));
          if (!points.length) {
            return null;
          }
          const d = points
            .map((point, idx) => `${idx === 0 ? 'M' : 'L'} ${xScale(point.x)} ${yScale(point.y)}`)
            .join(' ');

          return React.createElement(
            React.Fragment,
            { key: layer.key },
            React.createElement('path', {
              d,
              stroke: palette[layerIndex % palette.length],
              className: 'line-path',
              fill: 'none',
              strokeWidth: 2.3,
            }),
            points.map((point, i) =>
              React.createElement('circle', {
                key: `${layer.key}-${i}`,
                className: 'line-point',
                cx: xScale(point.x),
                cy: yScale(point.y),
                r: 3,
                fill: palette[layerIndex % palette.length],
                onMouseEnter: (event) => onHover({
                  visible: true,
                  x: event.clientX,
                  y: event.clientY,
                  text: tooltipText([tooltipTextLabel || layer.name, `${safeLabel(point.label)}`, `x: ${safeLabel(point.x)}`, `y: ${fmtNum(point.y, { compact: false })}`]),
                }),
                onMouseMove: (event) => onHover({
                  visible: true,
                  x: event.clientX,
                  y: event.clientY,
                  text: tooltipText([tooltipTextLabel || layer.name, `${safeLabel(point.label)}`, `x: ${safeLabel(point.x)}`, `y: ${fmtNum(point.y, { compact: false })}`]),
                }),
                onMouseLeave: () => onHover({ visible: false }),
              })
            )
          );
        })
      )
    ),
    React.createElement('div', { className: 'insight-note' }, `Why this badge?: ${tooltipTextLabel || confidence.reasons?.[0] || 'Use source provenance, recency, and consistency checks.'}`)
  );
}

function HorizontalBars({ title, rows, xLabel, yLabel, confidence, onHover, tooltipLines }) {
  const series = (rows || [])
    .filter((item) => num(item.value, null) !== null)
    .sort((a, b) => num(b.value) - num(a.value));
  const top = series;
  const max = Math.max(...top.map((item) => num(item.value)), 1);

  return React.createElement('div', { className: 'card insight-chart' },
    React.createElement('div', { className: 'source-line' },
      React.createElement('div', { className: 'chart-title' }, title),
      React.createElement('span', { className: `badge ${String(confidence.badge || 'low').toLowerCase()}` }, `${confidence.badge || 'Low'} confidence`)
    ),
    React.createElement('div', { className: 'chart-meta' }, `${xLabel || ''} by ${yLabel || 'category'}`),
    React.createElement('div', { className: 'bars' },
      ...top.map((row) => {
        const width = Math.round((num(row.value) / max) * 100);
        return React.createElement('div', { key: row.label, className: 'bar-row' },
          React.createElement('div', { className: 'bar-label', title: `${safeLabel(row.label)}: ${fmtNum(row.value)}` }, safeLabel(row.label)),
          React.createElement('div', { className: 'bar-track' },
            React.createElement('div', {
              className: 'bar-fill',
              style: { width: `${clamp(width, 5, 100)}%` },
              title: `${safeLabel(row.label)}: ${fmtNum(row.value)}`,
            })
          ),
          React.createElement('div', { className: 'bar-value' }, fmtNum(row.value))
        );
      })
    ),
    React.createElement('div', { className: 'insight-note' }, tooltipLines || '')
  );
  }

function StackedStateStatus({ title, rows, confidence, onHover }) {
  const cleaned = (rows || [])
    .map((row) => ({
      state: safeLabel(row.state),
      under: num(row.under_construction_length, 0),
      completed: num(row.completed_length, 0),
      approved: num(row.approved_length, 0),
    }))
    .filter((row) => row.under + row.completed + row.approved > 0);

  const max = Math.max(...cleaned.map((row) => row.under + row.completed + row.approved), 1);
  const ordered = cleaned.sort((a, b) => b.under + b.completed + b.approved - (a.under + a.completed + a.approved));

  return React.createElement('div', { className: 'card insight-chart' },
    React.createElement('div', { className: 'source-line' },
      React.createElement('div', { className: 'chart-title' }, title),
      React.createElement('span', { className: `badge ${String(confidence.badge || 'low').toLowerCase()}` }, `${confidence.badge || 'Low'} confidence`)),
    React.createElement('div', { className: 'chart-meta' }, 'Segmented by reported NH work-stage length (km)'),
    React.createElement('div', { className: 'bars' },
      ...ordered.map((row) => {
        const underPct = (row.under / max) * 100;
        const completedPct = (row.completed / max) * 100;
        const approvedPct = (row.approved / max) * 100;
        const barSegments = [
          { color: '#2f5f99', value: underPct },
          { color: '#0a8f52', value: completedPct },
          { color: '#b07a00', value: approvedPct },
        ];
        const barTrack = React.createElement(
          'div',
          { className: 'bar-track', style: { height: 16, display: 'flex' } },
          ...barSegments.map((segment) =>
            React.createElement('div', {
              className: 'bar-fill',
              style: { width: `${clamp(segment.value, 0, 100)}%`, background: segment.color },
            })
          )
        );
        return React.createElement(
          'div',
          { key: row.state, className: 'bar-row', style: { alignItems: 'start' } },
          React.createElement('div', { className: 'bar-label', title: row.state }, row.state),
          barTrack,
          React.createElement('div', { className: 'bar-value' }, fmtNum(row.under + row.completed + row.approved))
        );
      })),
    React.createElement('div', { className: 'insight-note' }, 'Blue=Under construction, Green=Completed, Amber=Approved but not commenced')
  );
}

function ScatterChart({ title, rows, confidence, onHover, chartScale = 1, xLabel = 'X', yLabel = 'Y', pointLabel = 'value' }) {
  const points = (rows || [])
    .filter((r) => Number.isFinite(num(r.x)) && Number.isFinite(num(r.y)))
    .sort((a, b) => num(a.x) - num(b.x));
  if (!points.length) {
    return React.createElement('div', { className: 'card insight-chart' }, React.createElement('div', { className: 'chart-title' }, title), React.createElement('div', { className: 'chart-meta' }, 'No scatter points.'));
  }

  const width = 980;
  const height = Math.round(300 * chartScale);
  const pad = { top: 20, right: 20, bottom: 28, left: 42 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  const xMin = Math.min(...points.map((p) => num(p.x)));
  const xMax = Math.max(...points.map((p) => num(p.x)));
  const yMin = Math.min(...points.map((p) => num(p.y)));
  const yMax = Math.max(...points.map((p) => num(p.y)));
  const rMin = Math.min(...points.map((p) => num(p.radius) || 3));
  const rMax = Math.max(...points.map((p) => num(p.radius) || 3));
  const xScale = (x) => pad.left + ((num(x) - xMin) / (xMax - xMin || 1)) * plotW;
  const yScale = (y) => pad.top + (1 - (num(y) - yMin) / (yMax - yMin || 1)) * plotH;
  const radiusScale = (v) => clamp(((num(v) - rMin) / (rMax - rMin || 1)) * 7 + 3, 3, 12);

  return React.createElement('div', { className: 'card insight-chart' },
    React.createElement('div', { className: 'source-line' },
      React.createElement('div', { className: 'chart-title' }, title),
      React.createElement('span', { className: `badge ${String(confidence.badge || 'low').toLowerCase()}` }, `${confidence.badge || 'Low'} confidence`)
    ),
    React.createElement('div', { className: 'chart-meta' }, `X=${xLabel}; Y=${yLabel}`),
    React.createElement('div', { className: 'chart-svg-wrap' },
      React.createElement('svg', {
        viewBox: `0 0 ${width} ${Math.round(300 * chartScale)}`,
        width: '100%',
        height: `${height}px`,
        style: { height: `${height}px`, width: '100%' },
      },
        React.createElement('line', { x1: pad.left, y1: pad.top + plotH, x2: width - pad.right, y2: pad.top + plotH, className: 'axis-line' }),
        React.createElement('line', { x1: pad.left, y1: pad.top, x2: pad.left, y2: pad.top + plotH, className: 'axis-line' }),
        points.map((point, idx) => React.createElement('circle', {
          key: `${point.state}-${idx}`,
          className: 'point',
          cx: xScale(point.x),
          cy: yScale(point.y),
          r: radiusScale(point.radius || 1),
          fill: point.radius ? '#a0182d' : '#2f5f99',
          onMouseEnter: (event) => onHover({
            visible: true,
            x: event.clientX,
            y: event.clientY,
            text: tooltipText([safeLabel(point.state), `${xLabel}: ${fmtNum(point.x)}`, `${yLabel}: ${fmtNum(point.y)}`, `${pointLabel}: ${safeLabel(point.modelConfidence)}`]),
          }),
          onMouseMove: (event) => onHover({
            visible: true,
            x: event.clientX,
            y: event.clientY,
            text: tooltipText([safeLabel(point.state), `${xLabel}: ${fmtNum(point.x)}`, `${yLabel}: ${fmtNum(point.y)}`, `${pointLabel}: ${safeLabel(point.modelConfidence)}`]),
          }),
          onMouseLeave: () => onHover({ visible: false }),
        }))
      )
    )
  );
}

function MethodologyBadge({ label, href }) {
  return React.createElement('a', {
    href,
    target: '_blank',
    rel: 'noreferrer',
    className: 'insight-pill',
    title: 'Why this badge? Data confidence and methodology.',
  }, `${label} · Why this badge?`);
}

function MetricCard({ item, rowCount, sourceFilter }) {
  const [label, kind] = sourceTypeTag(item);
  const citation = item?.citations || {};
  const source = item?.source || {};
  const reasons = item?.overall_confidence_reason || [];
  const primarySource = `${source.publisher || item?.source_id} / ${source.title || item?.source_id || 'Unknown source'}`;
  const retrievalDate = source.retrieved_at || item?.retrieved_at || 'Unknown';
  const license = source.license_terms || 'To be confirmed in source notes';
  const confidence = item?.overall_confidence_badge || 'Low';
  const temporaryLicense = String(license || '').slice(0, 160);
  const permanentIdentifier = citation.permanent_identifier || source.permanent_identifier_hint || 'N/A';
  const anchor = citation.anchor || 'pending';
  const isVisible = sourceFilter === 'all' || String(kind) === sourceFilter;
  if (!isVisible) return null;

  return React.createElement(
    'div',
    { className: 'metric-card card' },
    React.createElement(
      'div',
      {
        className: `badge ${String(confidence || 'low').toLowerCase()}`,
        title: reasons.length ? reasons.join(' | ') : 'Confidence reasons available in methodology.',
      },
      `${confidence} / `,
      React.createElement(MethodologyBadge, { label: 'Why this badge?', href: candidateAssetPaths('methodology.html')[0] || 'methodology.html' })
    ),
    React.createElement('h3', null, source.title || item.source_id),
    React.createElement('div', { className: `source-type ${kind}` }, label),
    React.createElement('div', { className: 'metric-meta' }, `Rows in parquet: ${rowCount}`),
    React.createElement('div', { className: 'metric-meta' }, `Primary source: ${primarySource}`),
    React.createElement('div', { className: 'metric-meta' }, `Retrieval date: ${retrievalDate}`),
    React.createElement('div', { className: 'metric-meta' }, `Permanent identifier: ${permanentIdentifier}`),
    React.createElement('div', { className: 'metric-meta' }, `Citation anchor: ${anchor}`),
    React.createElement('div', { className: 'metric-meta' }, `License: ${temporaryLicense}`),
    React.createElement('div', { className: `status ${item.skip_reason ? 'warn' : 'success'}` }, item.skip_reason ? `Skipped: ${item.skip_reason}` : 'Ready')
  );
}

function SourceMetaFooter({ label, confidence }) {
  return React.createElement(
    'div',
    { className: 'source-line' },
    React.createElement('div', { className: 'chart-meta' }, label),
    React.createElement(MethodologyBadge, { label: `Why this badge (${confidence})`, href: candidateAssetPaths('methodology.html')[0] || 'methodology.html' })
  );
}

function OntologyPanel({ catalog }) {
  const inferred = inferOntologyCoverage(catalog);
  return React.createElement(
    'section',
    { className: 'card' },
    React.createElement('h2', null, 'Ontology & Provenance Coverage'),
    React.createElement('p', { className: 'metric-meta' }, 'Entity references are inferred from canonical manifest columns and source titles. Use this to quickly confirm what each story can be grounded against.'),
    React.createElement(
      'div',
      { className: 'ontology' },
      Object.entries(inferred.entityCounts).map(([entityId, count]) =>
        React.createElement(
          'div',
          { key: entityId, className: 'ontology-card' },
          React.createElement('h4', null, entityId),
          React.createElement('p', null, `${count} source(s) expose ${entityId} references`)
        )
      )
    ),
    React.createElement(
      'div',
      { className: 'metric-meta ontology-card', style: { marginTop: '8px' } },
      React.createElement('p', { style: { marginTop: 0 } }, 'Relation evidence from manifest-level schemas'),
      React.createElement(
        'ul',
        { className: 'relation-list' },
        ...inferred.relationCoverage.map((relation) =>
          React.createElement(
            'li',
            { key: `${relation.from}-${relation.to}` },
            `${relation.name}: ${relation.evidence_count} supporting dataset(s)`
          )
        )
      )
    )
  );
}

function CoverageCards({ catalog, rowCounts }) {
  const officialCount = Object.values(catalog).filter((entry) => sourceTypeTag(entry)[1] === 'official').length;
  const proxyCount = Object.values(catalog).filter((entry) => sourceTypeTag(entry)[1] === 'proxy').length;
  const modelCount = Object.values(catalog).filter((entry) => sourceTypeTag(entry)[1] === 'model').length;
  const rowTotal = Object.values(rowCounts).reduce((acc, value) => acc + (Number(value) || 0), 0);

  return React.createElement(
    'section',
    { className: 'summary' },
    React.createElement('div', { className: 'card' }, `Official measured sources: ${officialCount}`),
    React.createElement('div', { className: 'card' }, `Proxy-derived signals: ${proxyCount}`),
    React.createElement('div', { className: 'card' }, `Model output signals: ${modelCount}`),
    React.createElement('div', { className: 'card' }, `Catalog entries: ${Object.keys(catalog).length}`),
    React.createElement('div', { className: 'card' }, `Ingested rows: ${rowTotal.toLocaleString()}`)
  );
}

function QualityBreakdown({ catalog }) {
  const official = [];
  const proxy = [];
  const model = [];
  Object.values(catalog || {}).forEach((item) => {
    const tag = sourceTypeTag(item)[1];
    if (tag === 'official') official.push(item);
    else if (tag === 'proxy') proxy.push(item);
    else model.push(item);
  });

  return React.createElement('div', { className: 'card chart' },
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

function toTopStates(analyticsRows, selectedState) {
  if (!selectedState || selectedState === 'All') {
    return analyticsRows;
  }
  return (analyticsRows || []).filter((row) => normalizeState(row.state) === normalizeState(selectedState));
}

async function loadAnalyticCatalog(conn, catalog) {
  const fetchById = async (sourceId, queryFactory) => {
    const manifest = catalog[sourceId];
    const sourcePath = manifest?.output_table_path || `data/processed/${sourceId}.parquet`;
    try {
      return await queryParquetRows(conn, sourcePath, queryFactory);
    } catch {
      return [];
    }
  };

  const growth = await fetchById('data_gov_in_nhai_yearwise_nh_constructed_2014_15', (alias) => `
    SELECT
      "financial_year" AS period,
      CAST("length_of_nh_constructed_in_km" AS DOUBLE) AS km_constructed
    FROM read_parquet('${alias}')
    WHERE "financial_year" IS NOT NULL AND "length_of_nh_constructed_in_km" IS NOT NULL
  `);

  const finance = await fetchById('data_gov_in_nhai_project_finance_api', (alias) => `
    SELECT
      "year-wise" AS year_label,
      CAST("allocation/target_-_total" AS DOUBLE) AS allocation_total,
      CAST("expenditure/release_of_funds/actuals_-_total" AS DOUBLE) AS expenditure_total
    FROM read_parquet('${alias}')
    WHERE "year-wise" IS NOT NULL
    ORDER BY "year-wise"
  `);

  const statePortfolio = await fetchById('data_gov_in_nhai_state_projects_api', (alias) => `
    SELECT
      "state" AS state,
      CAST("number_of_nh_projects" AS BIGINT) AS projects,
      CAST("length_in_km" AS DOUBLE) AS length_km,
      CAST("capital_outlay__rs_in_cr_for_the_years_2020_to_2024" AS DOUBLE) AS capital_outlay
    FROM read_parquet('${alias}')
    WHERE "state" IS NOT NULL
  `);

  const stateStatus = await fetchById('data_gov_in_nhai_statewise_nh_project_status_2024_25', (alias) => `
    SELECT
      "state-wise" AS state,
      CAST("details_of_under_construction_projects_-_length_kilometer" AS DOUBLE) AS under_construction_length,
      CAST("details_of_projects_completed_during_2024-25_-_length_kilometer" AS DOUBLE) AS completed_length,
      CAST("details_of_projects_approved_but_yet_to_be_commenced_-_length_kilometer" AS DOUBLE) AS approved_length
    FROM read_parquet('${alias}')
    WHERE "state-wise" IS NOT NULL
  `);

  const accidents = await fetchById('ncrb_road_accidents_state_year', (alias) => `
    SELECT
      "state" AS state,
      CAST("total_killed" AS DOUBLE) AS total_killed,
      CAST("total_injured" AS DOUBLE) AS total_injured,
      CAST("fatal_crashes" AS DOUBLE) AS fatal_crashes,
      CAST("year" AS INTEGER) AS year
    FROM read_parquet('${alias}')
    WHERE "state" IS NOT NULL AND "year" IS NOT NULL
  `);

  const maintenance = await fetchById('quality_maintenance_indicators', (alias) => `
    SELECT
      "state" AS state,
      "metric_name" AS metric_name,
      CAST("metric_value" AS DOUBLE) AS value
    FROM read_parquet('${alias}')
    WHERE "state" IS NOT NULL AND "metric_name" IS NOT NULL AND "metric_value" IS NOT NULL
  `);

  const modelRiskSeries = await fetchById('highway_project_risk_and_access_panel', (alias) => `
    SELECT
      "state_assigned" AS state,
      CAST("observation_year" AS INTEGER) AS observation_year,
      AVG(CAST("safety_risk_score" AS DOUBLE)) AS avg_safety_risk_score,
      AVG(CAST("delay_risk_score" AS DOUBLE)) AS avg_delay_risk_score,
      AVG(CAST("construction_progress_pct" AS DOUBLE)) AS avg_progress,
      AVG(CAST("estimated_revenue_generated_cr" AS DOUBLE)) AS avg_revenue_cr,
      AVG(CAST("quality_score" AS DOUBLE)) AS avg_quality_score
    FROM read_parquet('${alias}')
    WHERE "state_assigned" IS NOT NULL AND "observation_year" IS NOT NULL
    GROUP BY "state_assigned", CAST("observation_year" AS INTEGER)
  `);

  const modelProjectCosts = await fetchById('highway_project_risk_and_access_panel', (alias) => `
    SELECT
      "state_assigned" AS state,
      AVG(CAST("sanctioned_cost_cr" AS DOUBLE)) AS avg_sanctioned_cost_cr,
      AVG(CAST("road_length_km" AS DOUBLE)) AS avg_road_length_km,
      AVG(CAST("estimated_revenue_generated_cr" AS DOUBLE)) AS avg_revenue_cr,
      AVG(CAST("land_acquisition_cost_cr" AS DOUBLE)) AS avg_land_acquisition_cost_cr,
      AVG(CAST("maintenance_cost_cr" AS DOUBLE)) AS avg_maintenance_cost_cr
    FROM read_parquet('${alias}')
    WHERE "state_assigned" IS NOT NULL
    GROUP BY "state_assigned"
  `);

  const macro = await fetchById('rbi_mospi_macro_indicators', (alias) => `
    SELECT
      CAST("year" AS INTEGER) AS year,
      CAST("metric_name" AS VARCHAR) AS metric_name,
      CAST("metric_value" AS DOUBLE) AS metric_value
    FROM read_parquet('${alias}')
    WHERE CAST("year" AS INTEGER) IS NOT NULL
      AND metric_name IS NOT NULL
      AND metric_value IS NOT NULL
  `);

  const morthAppendix = await fetchById('morth_annual_report_pdf', (alias) => `
    SELECT
      CAST("state" AS VARCHAR) AS state,
      CAST("year" AS VARCHAR) AS year,
      CAST("metric_name" AS VARCHAR) AS metric_name,
      CAST("metric_value" AS DOUBLE) AS metric_value
    FROM read_parquet('${alias}')
    WHERE "metric_name" IN (
      'appendix2_statewise_nh_count',
      'appendix2_statewise_nh_length_km',
      'appendix3_crif_allocation',
      'appendix3_crif_release',
      'appendix5_statewise_national_permit_fee'
    )
  `);

  const growthRows = growth
    .map((row) => ({
      x: toYearNumeric(row.period),
      y: num(row.km_constructed),
      label: row.period,
      source: 'data_gov_in_nhai_yearwise_nh_constructed_2014_15',
    }))
    .filter((row) => Number.isFinite(row.x) && Number.isFinite(row.y))
    .sort((a, b) => a.x - b.x);

  const financeRows = finance
    .map((row) => ({
      x: toYearNumeric(row.year_label),
      label: row.year_label,
      allocation: num(row.allocation_total),
      expenditure: num(row.expenditure_total),
      source: 'data_gov_in_nhai_project_finance_api',
    }))
    .filter((row) => Number.isFinite(row.x));

  const portfolioRows = statePortfolio
    .map((row) => ({
      state: row.state,
      projects: num(row.projects),
      length: num(row.length_km),
      capital: num(row.capital_outlay),
      source: 'data_gov_in_nhai_state_projects_api',
    }))
    .filter((row) => row.state);

  const stateStatusRows = stateStatus
    .map((row) => ({
      state: row.state,
      under_construction_length: num(row.under_construction_length),
      completed_length: num(row.completed_length),
      approved_length: num(row.approved_length),
      source: 'data_gov_in_nhai_statewise_nh_project_status_2024_25',
    }))
    .filter((row) => row.state);

  const accidentLatestYear = accidents.reduce((acc, row) => {
    const year = num(row.year);
    if (!acc || year > acc) {
      return year;
    }
    return acc;
  }, null);

  const accidentRows = accidents
    .filter((row) => num(row.year) === accidentLatestYear)
    .map((row) => ({
      state: row.state,
      total_killed: num(row.total_killed),
      fatal_crashes: num(row.fatal_crashes),
      total_injured: num(row.total_injured),
      source: 'ncrb_road_accidents_state_year',
    }))
    .filter((row) => row.state);

  const proxyByState = {};
  maintenance.forEach((row) => {
    const state = row.state;
    if (!state) return;
    const key = normalizeState(state);
    proxyByState[key] = proxyByState[key] || { state, roughness: [], bridge: [], urban: [] };
    if (row.metric_name === 'roughness_index') {
      proxyByState[key].roughness.push(num(row.value));
    } else if (row.metric_name === 'bridge_defect_index') {
      proxyByState[key].bridge.push(num(row.value));
    } else if (row.metric_name === 'urban_activity_index') {
      proxyByState[key].urban.push(num(row.value));
    }
  });
  Object.keys(proxyByState).forEach((key) => {
    const bucket = proxyByState[key];
    bucket.roughness_index = bucket.roughness.length ? bucket.roughness.reduce((a, b) => a + b, 0) / bucket.roughness.length : null;
    bucket.bridge_defect_index = bucket.bridge.length ? bucket.bridge.reduce((a, b) => a + b, 0) / bucket.bridge.length : null;
    bucket.urban_activity_index = bucket.urban.length ? bucket.urban.reduce((a, b) => a + b, 0) / bucket.urban.length : null;
  });

  const modelStateRisk = [];
  const byStateYear = new Map();
  modelRiskSeries.forEach((row) => {
    const key = `${normalizeState(row.state)}::${num(row.observation_year)}`;
    byStateYear.set(key, row);
  });
  Array.from(byStateYear.values())
    .sort((a, b) => num(a.observation_year) - num(b.observation_year))
    .forEach((row) => {
      modelStateRisk.push({
        state: row.state,
        year: num(row.observation_year),
        safety_risk: num(row.avg_safety_risk_score),
        delay_risk: num(row.avg_delay_risk_score),
        progress: num(row.avg_progress),
        revenue: num(row.avg_revenue_cr),
        quality: num(row.avg_quality_score),
      });
    });

  const modelByStateSummary = [];
  const byStateCost = new Map();
  modelProjectCosts.forEach((row) => {
    const key = normalizeState(row.state);
    const arr = byStateCost.get(key) || [];
    arr.push({
      state: row.state,
      avg_sanctioned_cost_cr: num(row.avg_sanctioned_cost_cr),
      avg_road_length_km: num(row.avg_road_length_km),
      avg_revenue_cr: num(row.avg_revenue_cr),
      avg_land_acquisition_cost_cr: num(row.avg_land_acquisition_cost_cr),
      avg_maintenance_cost_cr: num(row.avg_maintenance_cost_cr),
    });
    byStateCost.set(key, arr);
  });
  byStateCost.forEach((items, key) => {
    const row = items[0];
    modelByStateSummary.push({
      state: row.state,
      avg_sanctioned_cost_cr: row.avg_sanctioned_cost_cr,
      avg_road_length_km: row.avg_road_length_km,
      avg_revenue_cr: row.avg_revenue_cr,
      avg_land_acquisition_cost_cr: row.avg_land_acquisition_cost_cr,
      avg_maintenance_cost_cr: row.avg_maintenance_cost_cr,
    });
  });

  const macroSeries = {
    cpi: macro.filter((row) => row.metric_name === 'CPI_Index'),
    capex: macro.filter((row) => row.metric_name === 'Highway_CapEx_Growth'),
    fuel: macro.filter((row) => row.metric_name === 'Fuel_Price_Index'),
  };

  const morthAppendix2CountRows = morthAppendix
    .filter((row) => row.metric_name === 'appendix2_statewise_nh_count' && row.state)
    .map((row) => ({ state: row.state, value: num(row.metric_value), source: 'morth_annual_report_pdf' }))
    .filter((row) => Number.isFinite(row.value));

  const morthAppendix2LengthRows = morthAppendix
    .filter((row) => row.metric_name === 'appendix2_statewise_nh_length_km' && row.state)
    .map((row) => ({ state: row.state, value: num(row.metric_value), source: 'morth_annual_report_pdf' }))
    .filter((row) => Number.isFinite(row.value));

  const morthCrifRows = morthAppendix
    .filter((row) => row.metric_name && row.metric_name.startsWith('appendix3_crif'))
    .map((row) => ({
      year: toYearNumeric(row.year),
      metric_name: row.metric_name,
      metric_value: num(row.metric_value),
      source: 'morth_annual_report_pdf',
    }))
    .filter((row) => Number.isFinite(row.year) && Number.isFinite(row.metric_value));

  const morthPermitRows = morthAppendix
    .filter((row) => row.metric_name === 'appendix5_statewise_national_permit_fee' && row.state)
    .map((row) => ({ state: row.state, value: num(row.metric_value), source: 'morth_annual_report_pdf' }))
    .filter((row) => Number.isFinite(row.value));

  const morthPermitByState = {};
  morthPermitRows.forEach((row) => {
    const key = normalizeState(row.state);
    morthPermitByState[key] = row.value;
  });

  const morthNHLengthByState = {};
  morthAppendix2LengthRows.forEach((row) => {
    morthNHLengthByState[normalizeState(row.state)] = num(row.value);
  });

  const stateList = new Set();
  portfolioRows.forEach((row) => stateList.add(row.state));
  stateStatusRows.forEach((row) => stateList.add(row.state));
  accidentRows.forEach((row) => stateList.add(row.state));
  modelStateRisk.forEach((row) => stateList.add(row.state));
  modelByStateSummary.forEach((row) => stateList.add(row.state));
  morthAppendix2CountRows.forEach((row) => stateList.add(row.state));
  morthAppendix2LengthRows.forEach((row) => stateList.add(row.state));
  morthPermitRows.forEach((row) => stateList.add(row.state));
  return {
    growthRows,
    financeRows,
    portfolioRows,
    stateStatusRows,
    accidentRows,
    proxyByState,
    modelStateRisk,
    modelByStateSummary,
    macroSeries,
    stateList: Array.from(stateList).sort(),
    morthAppendix2CountRows,
    morthAppendix2LengthRows,
    morthCrifRows,
    morthPermitRows,
    morthPermitByState,
    morthNHLengthByState,
    accidentLatestYear,
  };
}

function App() {
  const [catalog, setCatalog] = useState({});
  const [rowCounts, setRowCounts] = useState({});
  const [analytics, setAnalytics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [analyticsLoading, setAnalyticsLoading] = useState(true);
  const [error, setError] = useState('');
  const [sourceFilter, setSourceFilter] = useState('all');
  const [chartScale, setChartScale] = useState('normal');
  const [selectedState, setSelectedState] = useState('All');
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, text: '' });

  const chartScaleValue = { compact: 0.86, normal: 1, large: 1.35, xlarge: 2.0 };
  const activeChartScale = chartScaleValue[chartScale] || 1;

  useEffect(() => {
    let mounted = true;

    const run = async () => {
      try {
        const payload = await readCatalog('data/manifests/catalog.json');
        const items = payload.datasets || [];
        const map = {};
        items.forEach((item) => {
          map[item.source_id] = item;
        });

        let conn;
        try {
          conn = await initDuckDB();
        } catch (duckDbErr) {
          throw new Error(`DuckDB initialization failed: ${duckDbErr?.message || 'Unknown error'}`);
        }

        const counts = {};
        for (const item of items) {
          const outputPath = item.output_table_path || `data/processed/${item.source_id}.parquet`;
          try {
            const value = await countRows(conn, outputPath);
            counts[item.source_id] = Number.isFinite(value) ? value : Number(item?.manifest?.row_count || 0);
          } catch {
            counts[item.source_id] = item?.manifest?.row_count || 0;
          }
        }

        const analyticsData = await loadAnalyticCatalog(conn, map);

        if (mounted) {
          setCatalog(map);
          setRowCounts(counts);
          setAnalytics(analyticsData);
          setLoading(false);
          setAnalyticsLoading(false);
        }
      } catch (err) {
        if (mounted) {
          setError(`${err?.message || 'Data pipeline init failed'}. Hard refresh (Ctrl/Cmd+Shift+R). If running locally: python -m pipelines.ingest then refresh.`);
          setLoading(false);
          setAnalyticsLoading(false);
        }
      }
    };

    run();

    return () => {
      mounted = false;
    };
  }, []);

  const confidenceCatalog = useMemo(() => {
    const entries = Object.values(catalog || {});
    return {
      growth: confidenceFromSources(entries.filter((item) => String(item.source_id).includes('yearwise_nh_constructed_2014_15') || String(item.source_id).includes('nhi_yearwise'))),
      finance: confidenceFromSources(entries.filter((item) => String(item.source_id).includes('project_finance_api'))),
      portfolio: confidenceFromSources(entries.filter((item) => String(item.source_id).includes('state_projects_api'))),
      status: confidenceFromSources(entries.filter((item) => String(item.source_id).includes('statewise_nh_project_status'))),
      safety: confidenceFromSources(entries.filter((item) => ['ncrb_road_accidents_state_year', 'quality_maintenance_indicators', 'highway_project_risk_and_access_panel'].includes(item.source_id))),
      morthReport: confidenceFromSources(entries.filter((item) => item.source_id === 'morth_annual_report_pdf')),
      model: confidenceFromSources(entries.filter((item) => String(item.source_id).includes('highway_project_risk_and_access_panel') || item.source_type === 'model_output')),
      macro: confidenceFromSources(entries.filter((item) => String(item.source_id).includes('rbi_mospi_macro_indicators') || String(item.source_id).includes('ncrb_road_accidents_state_year') || String(item.source_id).includes('quality_maintenance_indicators'))),
    };
  }, [catalog]);

  const filteredStateRows = useMemo(() => {
    if (!analytics) return null;
    const state = selectedState || 'All';
    return {
      modelStateRisk: toTopStates(analytics.modelStateRisk, state),
      modelByStateSummary: toTopStates(analytics.modelByStateSummary, state),
      accidentRows: toTopStates(analytics.accidentRows, state),
      portfolioRows: toTopStates(analytics.portfolioRows, state),
      stateStatusRows: toTopStates(analytics.stateStatusRows, state),
      morthAppendix2CountRows: toTopStates(analytics.morthAppendix2CountRows, state),
      morthAppendix2LengthRows: toTopStates(analytics.morthAppendix2LengthRows, state),
      morthPermitRows: toTopStates(analytics.morthPermitRows, state),
    };
  }, [analytics, selectedState]);

  if (loading) {
    return React.createElement(
      'div',
      { className: 'app-shell' },
      React.createElement('section', { className: 'card' }, 'Loading catalog and DuckDB...')
    );
  }

  if (error) {
    return React.createElement('div', { className: 'app-shell' }, React.createElement('section', { className: 'card' }, error));
  }

  const methodologyUrl = candidateAssetPaths('methodology.html')[0] || 'methodology.html';
  const confidenceByAll = confidenceFromSources(Object.values(catalog));

  const financeChartData = (analytics?.financeRows || []);
  const growthLineData = (analytics?.growthRows || []).map((row) => ({
    x: row.x,
    y: row.y,
    label: row.label,
  }));
  const allocationSeries = financeChartData.map((row) => ({
    x: row.x,
    y: row.allocation,
    label: row.label,
  }));
  const expenditureSeries = financeChartData.map((row) => ({
    x: row.x,
    y: row.expenditure,
    label: row.label,
  }));

  const portfolioBars = (analytics?.portfolioRows || [])
    .map((row) => ({ label: row.state, value: row.length }))
    .filter((item) => item.value > 0);
  const statusBars = filteredStateRows?.stateStatusRows || [];

  const safetyChartRows = ((analytics?.accidentRows || []).map((acc) => {
    const key = normalizeState(acc.state);
    const proxy = analytics.proxyByState[key] || {};
    const modelRows = (analytics.modelByStateSummary || []).filter((row) => normalizeState(row.state) === key);
    const model = modelRows[0] || {};
    const qualityProxy = num(proxy.roughness_index);
    const risk = num(proxy.roughness_index) ? num(proxy.roughness_index) : null;
    const incident = num(acc.total_killed) || 0;
    const riskScore = num(model.avg_maintenance_cost_cr) ? num(model.avg_maintenance_cost_cr) : 0;
    const cityAccess = num(model.avg_road_length_km);
    const riskLabel = Number.isFinite(risk) ? risk.toFixed(1) : 'n/a';
    const modelConfidence = riskScore > 0 ? `${riskLabel} proxy` : 'official+model blend';
    return {
      state: acc.state,
      x: incident > 0 ? incident : qualityProxy || 0.1,
      y: riskScore ? riskScore * 0.01 : num(acc.total_killed),
      radius: cityAccess ? cityAccess / 10 : 4,
      modelConfidence,
    };
  })).filter((row) => Number.isFinite(row.x) && Number.isFinite(row.y));

  const modelSeriesByState = filteredStateRows?.modelStateRisk || [];
  const modelLinesByState = activeStateList.length
    ? activeStateList.map((state) => {
      const rows = modelSeriesByState
        .filter((item) => item.state === state)
        .map((item) => ({ x: item.year, y: item.safety_risk, label: `${item.state} ${item.year}` }));
      return { key: state, name: state, points: rows };
    }).filter((series) => series.points.length > 0)
    : [];

  const activeStateList = analytics?.stateList || [];

  const macroLines = [
    {
      name: 'CPI Index',
      key: 'cpi',
      points: (analytics?.macroSeries?.cpi || []).map((item) => ({
        x: num(item.year),
        y: num(item.metric_value),
        label: `CPI ${item.year}`,
      })),
    },
    {
      name: 'Highway CapEx growth',
      key: 'capex',
      points: (analytics?.macroSeries?.capex || []).map((item) => ({
        x: num(item.year),
        y: num(item.metric_value),
        label: `CapEx ${item.year}`,
      })),
    },
    {
      name: 'Fuel Price Index',
      key: 'fuel',
      points: (analytics?.macroSeries?.fuel || []).map((item) => ({
        x: num(item.year),
        y: num(item.metric_value),
        label: `Fuel ${item.year}`,
      })),
    },
  ].filter((series) => series.points.length > 0);

  const modelCostRows = (analytics?.modelByStateSummary || []).map((row) => ({
    label: row.state,
    x: num(row.avg_land_acquisition_cost_cr) || 0,
    y: num(row.avg_maintenance_cost_cr) || 0,
    radius: num(row.avg_sanctioned_cost_cr) || 1,
  })).filter((r) => Number.isFinite(r.x) && Number.isFinite(r.y));

  const morthCrifAllocationSeries = (analytics?.morthCrifRows || [])
    .filter((row) => row.metric_name === 'appendix3_crif_allocation')
    .sort((a, b) => a.year - b.year)
    .map((row) => ({ x: row.year, y: row.metric_value, label: `${row.year}` }));

  const morthCrifReleaseSeries = (analytics?.morthCrifRows || [])
    .filter((row) => row.metric_name === 'appendix3_crif_release')
    .sort((a, b) => a.year - b.year)
    .map((row) => ({ x: row.year, y: row.metric_value, label: `${row.year}` }));

  const morthStateCountBars = (filteredStateRows?.morthAppendix2CountRows || analytics?.morthAppendix2CountRows || [])
    .map((row) => ({ label: row.state, value: row.value, source: 'morth_annual_report_pdf' }))
    .filter((row) => row.value > 0)
    .sort((a, b) => b.value - a.value);

  const morthStateLengthBars = (filteredStateRows?.morthAppendix2LengthRows || analytics?.morthAppendix2LengthRows || [])
    .map((row) => ({ label: row.state, value: row.value, source: 'morth_annual_report_pdf' }))
    .filter((row) => row.value > 0)
    .sort((a, b) => b.value - a.value);

  const morthPermitVsLength = (filteredStateRows?.morthPermitRows || analytics?.morthPermitRows || [])
    .map((row) => {
      const key = normalizeState(row.state);
      const stateCount = num((filteredStateRows?.morthAppendix2CountRows || analytics?.morthAppendix2CountRows || []).find((item) => normalizeState(item.state) === key)?.value);
      return {
        state: row.state,
        x: num((analytics?.morthNHLengthByState || {})[key]) || 0,
        y: row.value,
        radius: stateCount || 4,
        modelConfidence: stateCount ? `NH count: ${stateCount}` : 'official',
      };
    })
    .filter((row) => Number.isFinite(row.x) && Number.isFinite(row.y));

  const portfolioConfidence = confidenceFromSources(Object.values(catalog).filter((item) => ['data_gov_in_nhai_state_projects_api', 'data_gov_in_nhai_tamil_nh_major_ongoing_2024_2026', 'data_gov_in_nhai_projects_api'].includes(item.source_id)));
  const stateStatusConfidence = confidenceFromSources(Object.values(catalog).filter((item) => item.source_id === 'data_gov_in_nhai_statewise_nh_project_status_2024_25'));
  const growthConfidence = confidenceFromSources(Object.values(catalog).filter((item) => item.source_id === 'data_gov_in_nhai_yearwise_nh_constructed_2014_15'));
  const modelConfidence = confidenceFromSources(Object.values(catalog).filter((item) => item.source_id === 'highway_project_risk_and_access_panel'));
  const morthReportConfidence = confidenceFromSources(Object.values(catalog).filter((item) => item.source_id === 'morth_annual_report_pdf'));

  return React.createElement(
    'div',
    { className: 'app-shell' },
    React.createElement(
      'header',
      null,
      React.createElement('h1', null, 'Bharat Highway Evidence Console'),
      React.createElement('p', { className: 'subhead' }, `Official-first visual analytics for highways growth, safety, finance and project-risk planning. Every chart is tied to a source manifest with citations and confidence scoring.`),
      React.createElement(SourceMetaFooter, { label: `All dashboards confidence floor: ${confidenceByAll.badge}`, confidence: confidenceByAll.badge }),
      React.createElement(MethodologyBadge, { label: 'Why these badges?', href: methodologyUrl })
    ),
    React.createElement(CoverageCards, { catalog, rowCounts }),
    React.createElement(
      'div',
      { className: 'toolbar' },
      React.createElement(
        'label',
        null,
        'State context filter',
        React.createElement(
          'select',
          { value: selectedState, onChange: (event) => setSelectedState(event.target.value) },
          React.createElement('option', { value: 'All' }, 'All states / all entities'),
          ...activeStateList.map((state) => React.createElement('option', { value: state, key: state }, state))
        )
      ),
      React.createElement(
        'div',
        { className: 'toggle-group' },
        React.createElement('button', {
          type: 'button',
          className: `toggle ${sourceFilter === 'all' ? 'active' : ''}`,
          onClick: () => setSourceFilter('all'),
        }, 'All Signals'),
        React.createElement('button', {
          type: 'button',
          className: `toggle ${sourceFilter === 'official' ? 'active' : ''}`,
          onClick: () => setSourceFilter('official'),
        }, 'Official only'),
        React.createElement('button', {
          type: 'button',
          className: `toggle ${sourceFilter === 'proxy' ? 'active' : ''}`,
          onClick: () => setSourceFilter('proxy'),
        }, 'Proxy only'),
        React.createElement('button', {
          type: 'button',
          className: `toggle ${sourceFilter === 'model' ? 'active' : ''}`,
          onClick: () => setSourceFilter('model'),
        }, 'Model only')
      ),
      React.createElement(
        'div',
        { className: 'toggle-group' },
        React.createElement('button', {
          type: 'button',
          className: `toggle ${chartScale === 'compact' ? 'active' : ''}`,
          onClick: () => setChartScale('compact'),
        }, 'Compact'),
        React.createElement('button', {
          type: 'button',
          className: `toggle ${chartScale === 'normal' ? 'active' : ''}`,
          onClick: () => setChartScale('normal'),
        }, 'Normal'),
        React.createElement('button', {
          type: 'button',
          className: `toggle ${chartScale === 'large' ? 'active' : ''}`,
          onClick: () => setChartScale('large'),
        }, 'Enlarge')
        ,
        React.createElement('button', {
          type: 'button',
          className: `toggle ${chartScale === 'xlarge' ? 'active' : ''}`,
          onClick: () => setChartScale('xlarge'),
          title: 'Show a large single-column chart layout for close reading and zoomed inspection',
        }, 'Focus')
      )
    ),
    React.createElement(
      'section',
      { className: 'charts-grid' },
      React.createElement(QualityBreakdown, { catalog })
    ),
    React.createElement(
      'section',
      { className: 'chart-grid', 'data-size': chartScale },
      React.createElement(LineChart, {
        title: 'Growth Story: NH Constructed Length by Year',
        description: 'Official time series from MoRTH/NHAI annual construct tables. This is a direct infrastructure growth indicator.',
        series: growthLineData,
        xTick: (value) => safeLabel(value),
        confidence: growthConfidence,
        onHover: setTooltip,
        chartScale: activeChartScale,
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(MultiLineChart, {
        title: 'Budget vs Expenditure (All Source Years)',
        description: `Official budget and release movement (year-wise) from NHAI project finance dataset. Strong lag between allocation and actual release indicates pipeline pressure.`,
        layers: [
          { key: 'allocation', name: 'Allocation total', points: allocationSeries },
          { key: 'expenditure', name: 'Expenditure total', points: expenditureSeries },
        ],
        confidence: confidenceCatalog.finance,
        onHover: setTooltip,
        tooltipTextLabel: 'Yearly budget metric',
        chartScale: activeChartScale,
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(HorizontalBars, {
        title: 'State Portfolio: Total NH Length vs State',
        rows: portfolioBars,
        confidence: portfolioConfidence,
        onHover: setTooltip,
        xLabel: 'Length (km)',
        yLabel: 'State',
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(MultiLineChart, {
        title: 'MoRTH Appendix 3: CRIF Allocation vs Release',
        description: 'Official MoRTH annual report appendix values for state roads CRIF movement.',
        layers: [
          { key: 'appendix3_crif_allocation', name: 'Allocation', points: morthCrifAllocationSeries },
          { key: 'appendix3_crif_release', name: 'Release', points: morthCrifReleaseSeries },
        ],
        confidence: morthReportConfidence,
        onHover: setTooltip,
        tooltipTextLabel: 'CRIF year metric',
        chartScale: activeChartScale,
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(HorizontalBars, {
        title: 'MoRTH Appendix 2: NH Count by State',
        rows: morthStateCountBars,
        confidence: morthReportConfidence,
        onHover: setTooltip,
        xLabel: 'Number of NHs',
        yLabel: 'State',
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(HorizontalBars, {
        title: 'MoRTH Appendix 2: NH Length (km) by State',
        rows: morthStateLengthBars,
        confidence: morthReportConfidence,
        onHover: setTooltip,
        xLabel: 'Length (km)',
        yLabel: 'State',
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(ScatterChart, {
        title: 'MoRTH Appendix 5: State Permit Fee vs NH Length',
        rows: morthPermitVsLength,
        confidence: morthReportConfidence,
        onHover: setTooltip,
        xLabel: 'NH length (km)',
        yLabel: 'Permit fee (₹ in actuals)',
        pointLabel: 'NH count',
        chartScale: activeChartScale,
      }),
      React.createElement(ChartTooltip, { tooltip }),
      React.createElement(StackedStateStatus, {
        title: 'State Project Mix (2024-25 official status snapshot)',
        rows: statusBars,
        confidence: stateStatusConfidence,
      }),
      React.createElement(ScatterChart, {
        title: `Safety Context: Fatality Intensity × Safety Risk Signals (${analytics?.accidentLatestYear || 'latest'})`,
        rows: safetyChartRows,
        confidence: confidenceCatalog.safety,
        onHover: setTooltip,
        xLabel: 'Incident intensity',
        yLabel: 'Safety risk score',
        pointLabel: 'Model confidence',
        chartScale: activeChartScale,
      }),
      React.createElement(MultiLineChart, {
        title: 'Model Risk Trajectory by State (proxy-informed)',
        description: 'Model-only signal for scenario planning. Officially measured crash/finance metrics should stay dominant for policy decisions.',
        layers: modelLinesByState,
        confidence: modelConfidence,
        onHover: setTooltip,
        tooltipTextLabel: 'State safety trend',
        chartScale: activeChartScale,
      }),
      React.createElement(MultiLineChart, {
        title: 'GDP & Infrastructure Context',
        description: 'Macro backdrop (official national indicators) and highway-related investment context.',
        layers: macroLines,
        confidence: confidenceCatalog.macro,
        onHover: setTooltip,
        tooltipTextLabel: 'Macro metric',
        chartScale: activeChartScale,
      }),
      React.createElement(ScatterChart, {
        title: 'Project Economics: Land Acquisition vs Maintenance (Model Panel)',
        rows: modelCostRows,
        confidence: modelConfidence,
        onHover: setTooltip,
        chartScale: activeChartScale,
      })
    ),
    React.createElement(
      'section',
      { className: 'panel-grid' },
      analyticsLoading ? React.createElement('div', { className: 'card' }, 'Loading insight panels...') : null,
      React.createElement(OntologyPanel, { catalog }),
      ...Object.values(catalog)
        .filter((item) => {
          if (sourceFilter === 'all') return true;
          const tag = sourceTypeTag(item)[1];
          return tag === sourceFilter;
        })
        .map((item) =>
          React.createElement(MetricCard, {
            key: item.source_id,
            item,
            rowCount: rowCounts[item.source_id] || 0,
            sourceFilter,
          })
        )
    ),
    React.createElement('div', { style: { height: '20px' } })
  );
}

createRoot(document.getElementById('root')).render(React.createElement(App));
