// Published counts are validated against Parquet by the Pages artifact gate.
// Bind downloads to that same catalog snapshot before querying them.
export function catalogSnapshot(payload) {
  if (!Array.isArray(payload?.datasets)) throw new Error('Invalid evidence catalog');
  const catalog = Object.create(null);
  const counts = Object.create(null);
  const checksums = new Map();
  for (const item of payload.datasets) {
    const id = item?.source_id;
    const count = item?.manifest?.row_count;
    const path = item?.output_table_path || `data/processed/${id}.parquet`;
    const checksum = item?.manifest?.output_files?.find(file => file.path === path)?.sha256;
    if (typeof id !== 'string' || !id || Object.hasOwn(catalog, id) || !Number.isSafeInteger(count) || count < 0
        || !/^[a-f0-9]{64}$/i.test(checksum || '')) {
      throw new Error('Evidence catalog metadata is incomplete');
    }
    if (checksums.has(path) && checksums.get(path) !== checksum.toLowerCase()) throw new Error('Conflicting evidence checksums');
    catalog[id] = item;
    counts[id] = count;
    checksums.set(path, checksum.toLowerCase());
  }
  return { catalog, counts, checksums };
}

export async function verifyBuffer(buffer, expected) {
  const digest = await crypto.subtle.digest('SHA-256', buffer);
  const actual = Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
  if (actual !== expected) throw new Error('Evidence file does not match the catalog snapshot');
}

export async function fetchEvidence(url, timeoutMs = 60000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { cache: 'no-cache', signal: controller.signal });
    if (!response.ok) throw new Error(`${url}: ${response.status}`);
    // Keep the timeout active while receiving the body, not just the headers.
    return await response.arrayBuffer();
  } finally {
    clearTimeout(timer);
  }
}

export async function withDeadline(pending, timeoutMs, message) {
  let timer;
  try {
    return await Promise.race([pending, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}
