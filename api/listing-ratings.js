const fs = require('fs');
const path = require('path');

const LOCAL_STORE = path.join(process.cwd(), 'data', 'listing-ratings.local.json');
const PROJECT_KEY = 'sl-map-site';
const TABLE = 'sl_map_site_listing_ratings';

function nowIso() {
  return new Date().toISOString();
}

function hasSupabase() {
  return Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);
}

function supabaseHeaders(extra = {}) {
  return {
    apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    ...extra,
  };
}

function supabaseBase() {
  return process.env.SUPABASE_URL.replace(/\/$/, '');
}

async function supabaseReadStore() {
  const url = `${supabaseBase()}/rest/v1/${TABLE}?project_key=eq.${encodeURIComponent(PROJECT_KEY)}&select=listing_id,rating,rejected,updated_at`;
  const response = await fetch(url, { headers: supabaseHeaders() });
  if (!response.ok) throw new Error(`Supabase read ${response.status}: ${await response.text()}`);
  const rows = await response.json();
  const items = {};
  for (const row of rows) {
    items[row.listing_id] = {
      id: row.listing_id,
      rating: row.rating == null ? undefined : row.rating,
      rejected: Boolean(row.rejected),
      updatedAt: row.updated_at,
    };
    if (items[row.listing_id].rating == null) delete items[row.listing_id].rating;
  }
  return { items };
}

async function supabaseWriteItem(item) {
  const row = {
    listing_id: item.id,
    project_key: PROJECT_KEY,
    rating: item.rating == null ? null : item.rating,
    rejected: Boolean(item.rejected),
  };
  const response = await fetch(`${supabaseBase()}/rest/v1/${TABLE}?on_conflict=listing_id`, {
    method: 'POST',
    headers: supabaseHeaders({ prefer: 'resolution=merge-duplicates,return=representation' }),
    body: JSON.stringify(row),
  });
  if (!response.ok) throw new Error(`Supabase write ${response.status}: ${await response.text()}`);
  const rows = await response.json();
  const saved = rows[0] || row;
  return {
    id: saved.listing_id,
    rating: saved.rating == null ? undefined : saved.rating,
    rejected: Boolean(saved.rejected),
    updatedAt: saved.updated_at || nowIso(),
  };
}

function normalizeStore(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return { items: {} };
  const items = payload.items && typeof payload.items === 'object' && !Array.isArray(payload.items) ? payload.items : payload;
  return { items };
}

async function readStore() {
  if (hasSupabase()) return supabaseReadStore();

  // Local/dev fallback. Production cross-browser persistence uses Supabase.
  if (!fs.existsSync(LOCAL_STORE)) return { items: {} };
  return normalizeStore(JSON.parse(fs.readFileSync(LOCAL_STORE, 'utf8')));
}

async function writeLocalItem(item) {
  const store = await readStore();
  store.items[item.id] = item;
  fs.mkdirSync(path.dirname(LOCAL_STORE), { recursive: true });
  fs.writeFileSync(LOCAL_STORE, JSON.stringify(normalizeStore(store)) + '\n');
  return item;
}

function sanitizeUpdate(body) {
  const id = String(body.id || '').trim();
  if (!id) throw new Error('Missing listing id');
  const patch = { id, updatedAt: nowIso() };
  if ('rating' in body) {
    if (body.rating === null || body.rating === '') patch.rating = null;
    else {
      const rating = Number(body.rating);
      if (!Number.isInteger(rating) || rating < 0 || rating > 10) throw new Error('Rating must be an integer from 0 to 10');
      patch.rating = rating;
    }
  }
  if ('rejected' in body) patch.rejected = Boolean(body.rejected);
  return patch;
}

module.exports = async function handler(req, res) {
  res.setHeader('cache-control', 'no-store');
  try {
    if (req.method === 'GET') {
      const store = await readStore();
      return res.status(200).json({ ...store, storage: hasSupabase() ? 'supabase:sl_map_site_listing_ratings' : 'local' });
    }

    if (req.method !== 'POST' && req.method !== 'PUT') {
      res.setHeader('Allow', 'GET, POST, PUT');
      return res.status(405).json({ error: 'Use GET or POST' });
    }

    if (!hasSupabase() && process.env.VERCEL) {
      return res.status(503).json({
        error: 'Supabase is not configured. Add SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in Vercel.',
      });
    }

    const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
    const patch = sanitizeUpdate(body);
    const previous = (await readStore()).items[patch.id] || { id: patch.id };
    const next = { ...previous, ...patch };
    if (next.rating == null) delete next.rating;
    const item = hasSupabase() ? await supabaseWriteItem(next) : await writeLocalItem(next);
    return res.status(200).json({ item, storage: hasSupabase() ? 'supabase:sl_map_site_listing_ratings' : 'local' });
  } catch (error) {
    return res.status(500).json({ error: error.message || String(error) });
  }
};
