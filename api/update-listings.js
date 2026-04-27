const fs = require('fs');
const path = require('path');

const USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36';

function normalize(value = '') {
  return String(value)
    .toLowerCase()
    .replace(/\|/g, ' ')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\b(van|vån|tr|trappa)\b/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function slugText(url) {
  const slug = new URL(url).pathname.split('/').pop().replace(/-\d+$/, '');
  return normalize(slug.replace(/-/g, ' '));
}

function scoreListingToUrl(listing, url) {
  const title = normalize(listing.title || '');
  const location = normalize(String(listing.location || '').split(',')[0]);
  const slug = slugText(url);
  if (!title || !slug) return 0;
  const titleTokens = new Set(title.split(' '));
  const slugTokens = new Set(slug.split(' '));
  let overlap = 0;
  for (const token of titleTokens) if (slugTokens.has(token)) overlap += 1;
  const tokenScore = overlap / Math.max(1, titleTokens.size);
  const locationBonus = location.split(' ').some((token) => slugTokens.has(token)) ? 0.08 : 0;
  return tokenScore + locationBonus;
}

async function fetchText(url) {
  const response = await fetch(url, { headers: { 'user-agent': USER_AGENT } });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText} for ${url}`);
  return response.text();
}

async function collectHemnetUrls(searchUrl, maxPages = 18) {
  const urls = new Set();
  for (let page = 1; page <= maxPages; page += 1) {
    const url = page === 1 ? searchUrl : `${searchUrl}&page=${page}`;
    const html = await fetchText(url);
    const matches = [...html.matchAll(/href=["'](\/bostad\/[^"']+)["']/g)].map((match) => `https://www.hemnet.se${match[1]}`);
    if (!matches.length) break;
    const before = urls.size;
    matches.forEach((match) => urls.add(match));
    if (urls.size === before && page > 2) break;
  }
  return [...urls].sort();
}

function readSearchUrls() {
  try {
    const payload = JSON.parse(fs.readFileSync(path.join(process.cwd(), 'data', 'search-parameters.json'), 'utf8'));
    const urls = (payload.items || []).filter((item) => item.source === 'hemnet' && item.url).map((item) => item.url);
    if (urls.length) return urls;
  } catch (_) {}
  return ['https://www.hemnet.se/bostader?location_ids%5B%5D=17744&price_max=3000000&rooms_min=4'];
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Use POST' });
  }
  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
    const listings = Array.isArray(body.listings) ? body.listings.filter((item) => item.source === 'hemnet') : [];
    const searchUrls = readSearchUrls();
    const allUrls = new Set();
    for (const searchUrl of searchUrls) {
      const urls = await collectHemnetUrls(searchUrl, Number(body.maxPages || 18));
      urls.forEach((url) => allUrls.add(url));
    }
    const urls = [...allUrls];
    const used = new Set();
    const matches = [];
    for (const listing of listings) {
      if (listing.listing_url) continue;
      const ranked = urls
        .filter((url) => !used.has(url))
        .map((url) => ({ score: scoreListingToUrl(listing, url), url }))
        .sort((a, b) => b.score - a.score);
      const best = ranked[0];
      if (best && best.score >= 0.92) {
        used.add(best.url);
        matches.push({ source_id: listing.source_id, title: listing.title, location: listing.location, listing_url: best.url, score: Number(best.score.toFixed(3)) });
      }
    }
    return res.status(200).json({ fetched_at: new Date().toISOString(), searched_urls: urls.length, matches });
  } catch (error) {
    return res.status(500).json({ error: error.message || String(error) });
  }
};
