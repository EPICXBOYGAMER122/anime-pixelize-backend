// =====================================================================
//  Anime pixelize backend (v2 - optimised)
//  ------------------------------------------------------------------
//  Preserves the original behaviour: host allowlist, 10s fetch timeout,
//  5 MB image cap, sharp resize+flatten to solid RGB, response shape
//  { width, height, pixels: [[r,g,b], ...] }.
//
//  Adds:
//    * True LRU memory cache
//    * Persistent SQLite cache (gzip'd raw RGB)
//    * In-flight request dedup per cache key
//    * Concurrency semaphore + queue (503 on overflow)
//    * Per-IP rate limit (token bucket, no dep)
//    * /stats route (safe read-only counters)
//    * Canonical cache keys aligned with the Roblox server:
//        "<imageUrl>|<W>|<H>|<fit>|preview"
//        "<imageUrl>|<W>|<H>|<fit>|<colors>colors"
//
//  All tunables read from env vars (see the CONFIG block below).
//  Sensible defaults for a low-resource host (Render free tier etc.).
// =====================================================================

const express = require("express");
const sharp   = require("sharp");
const zlib    = require("zlib");
const path    = require("path");
const fs      = require("fs");

// SQLite is optional. If better-sqlite3 fails to load (e.g. no native
// build tools on your host), we run without disk persistence rather than
// crash.
let sqlite = null;
try { sqlite = require("better-sqlite3"); }
catch (e) { console.warn("[cache] better-sqlite3 unavailable; disk cache disabled -", e.message); }

// ---------- CONFIG (env-driven so you can retune without editing code) ----------
const PORT                    = Number(process.env.PORT || 3000);
const MAX_CACHE_ITEMS         = Number(process.env.MAX_CACHE_ITEMS || 200);
const MAX_CONCURRENT_PIXELIZE = Number(process.env.MAX_CONCURRENT_PIXELIZE || 1);
const MAX_QUEUE_SIZE          = Number(process.env.MAX_QUEUE_SIZE || 30);
const RATE_LIMIT_PER_MIN      = Number(process.env.RATE_LIMIT_PER_MIN || 120);
const FETCH_TIMEOUT_MS        = Number(process.env.FETCH_TIMEOUT_MS || 10_000);
const MAX_IMAGE_BYTES         = Number(process.env.MAX_IMAGE_BYTES || 5 * 1024 * 1024);
const MAX_DIM                 = Number(process.env.MAX_DIM || 512);   // was 256 - raised so 300x300 canvas works
const MIN_DIM                 = Number(process.env.MIN_DIM || 8);
const DB_PATH                 = process.env.DB_PATH || path.join(__dirname, "cache.sqlite");

// Preview cache TTL (ms). Full canvases live longer.
const TTL_PREVIEW_MS = Number(process.env.TTL_PREVIEW_MS || 30 * 60 * 1000);
const TTL_CANVAS_MS  = Number(process.env.TTL_CANVAS_MS  || 2  * 60 * 60 * 1000);
// Disk cache prune sweep interval + max age (favours popular canvases).
const DISK_PRUNE_INTERVAL_MS  = Number(process.env.DISK_PRUNE_INTERVAL_MS  || 6 * 60 * 60 * 1000);
const DISK_PREVIEW_MAX_AGE_MS = Number(process.env.DISK_PREVIEW_MAX_AGE_MS || 7  * 24 * 60 * 60 * 1000);
const DISK_CANVAS_MAX_AGE_MS  = Number(process.env.DISK_CANVAS_MAX_AGE_MS  || 60 * 24 * 60 * 60 * 1000);

// Only allow anime image hosts you expect. From the original file.
// AniList cover URLs usually use s4.anilist.co.
// Jikan/MAL images usually use cdn.myanimelist.net.
const ALLOWED_HOSTS = new Set([
  "s4.anilist.co",
  "cdn.myanimelist.net",
]);

// ---------- SQLite (persistent cache) ----------
// Schema: one row per cache key, pixels gzip'd raw RGB (3 bytes/pixel).
let db = null;
let getSql, putSql, touchSql, countSql, pruneSql;
if (sqlite) {
  try {
    db = new sqlite(DB_PATH);
    db.pragma("journal_mode = WAL");
    db.exec(`
      CREATE TABLE IF NOT EXISTS cache (
        key         TEXT PRIMARY KEY,
        imageUrl    TEXT NOT NULL,
        w           INTEGER NOT NULL,
        h           INTEGER NOT NULL,
        fit         TEXT NOT NULL,
        mode        TEXT NOT NULL,          -- "preview" or "<N>colors"
        pixelsGz    BLOB NOT NULL,          -- gzip'd raw RGB bytes
        generatedAt INTEGER NOT NULL,
        lastUsedAt  INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_lastUsed ON cache(lastUsedAt);
      CREATE INDEX IF NOT EXISTS idx_mode     ON cache(mode);
    `);
    getSql   = db.prepare("SELECT pixelsGz, w, h FROM cache WHERE key = ?");
    putSql   = db.prepare("INSERT OR REPLACE INTO cache (key, imageUrl, w, h, fit, mode, pixelsGz, generatedAt, lastUsedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
    touchSql = db.prepare("UPDATE cache SET lastUsedAt = ? WHERE key = ?");
    countSql = db.prepare("SELECT COUNT(*) AS n FROM cache");
    pruneSql = db.prepare("DELETE FROM cache WHERE (mode = 'preview' AND lastUsedAt < ?) OR (mode != 'preview' AND lastUsedAt < ?)");
    console.log(`[cache] disk cache at ${DB_PATH}`);
  } catch (e) {
    console.warn("[cache] failed to open SQLite; running without disk cache -", e.message);
    db = null;
  }
}

function packPixels(pixels) {
  const raw = Buffer.alloc(pixels.length * 3);
  for (let i = 0; i < pixels.length; i++) {
    raw[i * 3]     = pixels[i][0];
    raw[i * 3 + 1] = pixels[i][1];
    raw[i * 3 + 2] = pixels[i][2];
  }
  return zlib.gzipSync(raw);
}
function unpackPixels(gzBuf) {
  const raw = zlib.gunzipSync(gzBuf);
  const pixels = new Array(raw.length / 3);
  for (let i = 0, j = 0; i < raw.length; i += 3, j++) {
    pixels[j] = [raw[i], raw[i + 1], raw[i + 2]];
  }
  return pixels;
}
function loadDisk(key) {
  if (!db) return null;
  const row = getSql.get(key);
  if (!row) return null;
  touchSql.run(Date.now(), key);
  return { width: row.w, height: row.h, pixels: unpackPixels(row.pixelsGz) };
}
function saveDisk(key, imageUrl, w, h, fit, mode, pixels) {
  if (!db) return;
  const gz  = packPixels(pixels);
  const now = Date.now();
  putSql.run(key, imageUrl, w, h, fit, mode, gz, now, now);
}

// ---------- Memory LRU (true LRU: promote on read) ----------
class LRU {
  constructor(max, ttlMs) { this.max = max; this.ttl = ttlMs; this.map = new Map(); }
  get(key) {
    const entry = this.map.get(key);
    if (!entry) return null;
    if (this.ttl && Date.now() > entry.expiresAt) { this.map.delete(key); return null; }
    // Re-insert so the key becomes the most recently used.
    this.map.delete(key); this.map.set(key, entry);
    return entry.value;
  }
  set(key, value, ttlMs) {
    if (this.map.has(key)) this.map.delete(key);
    const expiresAt = (ttlMs != null ? ttlMs : this.ttl) + Date.now();
    this.map.set(key, { value, expiresAt });
    while (this.map.size > this.max) this.map.delete(this.map.keys().next().value);
  }
  invalidate(key) { this.map.delete(key); }
  size() { return this.map.size; }
}
const memCache = new LRU(MAX_CACHE_ITEMS, TTL_PREVIEW_MS);   // per-entry TTL set on write

// ---------- In-flight dedup ----------
// Two concurrent requests for the same cache key share ONE sharp job.
const inflight = new Map();   // key -> Promise
async function dedupe(key, fn) {
  const pending = inflight.get(key);
  if (pending) { stats.inFlightDedupes++; return pending; }
  const p = (async () => { try { return await fn(); } finally { inflight.delete(key); } })();
  inflight.set(key, p);
  return p;
}

// ---------- Concurrency semaphore + bounded queue ----------
// Not p-limit: rolled inline so we can hard-cap the queue and 503 fast.
let running = 0;
let queued  = 0;
const waiters = [];
async function withSlot(fn) {
  if (queued >= MAX_QUEUE_SIZE) {
    const e = new Error("server busy"); e.code = 503; throw e;
  }
  queued++;
  try {
    if (running >= MAX_CONCURRENT_PIXELIZE) {
      await new Promise((resolve) => waiters.push(resolve));
    }
    running++;
    try { return await fn(); }
    finally {
      running--;
      const next = waiters.shift();
      if (next) next();
    }
  } finally {
    queued--;
  }
}

// ---------- Rate limit (token bucket per IP, no dep) ----------
const buckets = new Map();   // ip -> { tokens, updatedAt }
function rateLimit(ip) {
  const now = Date.now();
  const b   = buckets.get(ip) || { tokens: RATE_LIMIT_PER_MIN, updatedAt: now };
  const refill = (now - b.updatedAt) / 60_000 * RATE_LIMIT_PER_MIN;
  b.tokens    = Math.min(RATE_LIMIT_PER_MIN, b.tokens + refill);
  b.updatedAt = now;
  if (b.tokens < 1) { buckets.set(ip, b); return false; }
  b.tokens -= 1;
  buckets.set(ip, b);
  return true;
}
setInterval(() => {   // periodic prune of stale bucket entries
  const cutoff = Date.now() - 10 * 60 * 1000;
  for (const [ip, b] of buckets) if (b.updatedAt < cutoff) buckets.delete(ip);
}, 5 * 60 * 1000).unref();

// ---------- Stats ----------
const startedAt = Date.now();
const stats = {
  memCacheHits: 0, memCacheMisses: 0,
  diskCacheHits: 0, diskCacheMisses: 0,
  inFlightDedupes: 0,
  totalPixelizeRequests: 0,
  rateLimited: 0, serverBusy: 0,
  failedJobs: 0, hostBlocked: 0, sizeBlocked: 0,
  totalPixelsProcessed: 0,
};

// ---------- Helpers preserved from the original ----------
function isAllowedImageUrl(imageUrl) {
  try {
    const url = new URL(imageUrl);
    return url.protocol === "https:" && ALLOWED_HOSTS.has(url.hostname);
  } catch { return false; }
}
function cacheKeyFor({ imageUrl, w, h, fit, colors }) {
  const mode = (colors != null && Number(colors) > 0) ? `${Number(colors)}colors` : "preview";
  return `${imageUrl}|${w}|${h}|${fit}|${mode}`;
}

// ---------- Express ----------
const app = express();
app.set("trust proxy", true);   // honour X-Forwarded-For on Render/Railway/Fly
app.use(express.json({ limit: "1mb" }));

app.get("/", (req, res) => {
  res.json({ status: "ok", message: "Anime pixelize backend is running" });
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.get("/stats", (req, res) => {
  const diskRow = db ? countSql.get() : { n: 0 };
  res.json({
    uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
    memoryUsageMB: Object.fromEntries(
      Object.entries(process.memoryUsage()).map(([k, v]) => [k, +(v / 1024 / 1024).toFixed(1)])
    ),
    memCacheSize:  memCache.size(),
    diskCacheSize: diskRow.n,
    diskCacheEnabled: !!db,
    inFlightJobs:  inflight.size,
    queued, running,
    concurrency: { max: MAX_CONCURRENT_PIXELIZE, queueMax: MAX_QUEUE_SIZE },
    rateLimit: { perMinute: RATE_LIMIT_PER_MIN, bucketsTracked: buckets.size },
    counters: { ...stats },
  });
});

app.post("/pixelize", async (req, res) => {
  const ip = req.ip || req.connection?.remoteAddress || "unknown";
  if (!rateLimit(ip)) {
    stats.rateLimited++;
    return res.status(429).json({ error: "rate limited" });
  }
  stats.totalPixelizeRequests++;

  try {
    const { imageUrl, width, height, fit, colors } = req.body || {};
    if (!imageUrl || typeof imageUrl !== "string") {
      return res.status(400).json({ error: "Missing imageUrl" });
    }
    if (!isAllowedImageUrl(imageUrl)) {
      stats.hostBlocked++;
      return res.status(400).json({
        error: "Image URL host is not allowed",
        allowedHosts: Array.from(ALLOWED_HOSTS),
      });
    }

    const w = Math.max(MIN_DIM, Math.min(MAX_DIM, Number(width)  || 120));
    const h = Math.max(MIN_DIM, Math.min(MAX_DIM, Number(height) || 120));
    // "cover" fills the whole square but can crop the anime poster.
    // "contain" shows the full poster but can create side borders.
    const resizeFit = fit === "contain" ? "contain" : "cover";
    const isCanvas = colors != null && Number(colors) > 0;
    const key = cacheKeyFor({ imageUrl, w, h, fit: resizeFit, colors });

    // 1) Memory cache
    const memHit = memCache.get(key);
    if (memHit) { stats.memCacheHits++; return res.json(memHit); }
    stats.memCacheMisses++;

    // 2) Disk cache -> promote to memory
    const diskHit = loadDisk(key);
    if (diskHit) {
      stats.diskCacheHits++;
      memCache.set(key, diskHit, isCanvas ? TTL_CANVAS_MS : TTL_PREVIEW_MS);
      return res.json(diskHit);
    }
    stats.diskCacheMisses++;

    // 3) In-flight dedup wrapped around the concurrency-limited sharp job
    const result = await dedupe(key, () => withSlot(async () => {
      // Re-check both caches under the dedup lock (another leader may have
      // finished between the miss above and here).
      const late = memCache.get(key) || loadDisk(key);
      if (late) return late;

      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
      let imageResponse;
      try {
        imageResponse = await fetch(imageUrl, {
          signal: controller.signal,
          headers: { "User-Agent": "AnimePixelizeBackend/2.0" },
        });
      } finally { clearTimeout(timeout); }

      if (!imageResponse.ok) {
        const e = new Error(`Failed to download image: ${imageResponse.status}`);
        e.code = 400; throw e;
      }
      const contentType = imageResponse.headers.get("content-type") || "";
      if (!contentType.startsWith("image/")) {
        const e = new Error("URL did not return an image"); e.code = 400; throw e;
      }
      const contentLength = Number(imageResponse.headers.get("content-length") || 0);
      if (contentLength > MAX_IMAGE_BYTES) {
        stats.sizeBlocked++;
        const e = new Error("Image too large"); e.code = 400; throw e;
      }
      const buffer = Buffer.from(await imageResponse.arrayBuffer());

      const raw = await sharp(buffer)
        .resize(w, h, {
          fit: resizeFit,
          position: "center",
          background: { r: 255, g: 255, b: 255, alpha: 1 },
        })
        .flatten({ background: { r: 255, g: 255, b: 255 } })
        .raw()
        .toBuffer({ resolveWithObject: true });

      const { data, info } = raw;
      const pixels = new Array(info.width * info.height);
      for (let i = 0; i < info.width * info.height; i++) {
        const o = i * info.channels;
        pixels[i] = [data[o], data[o + 1], data[o + 2]];
      }
      const out = { width: info.width, height: info.height, pixels };

      const mode = isCanvas ? `${Number(colors)}colors` : "preview";
      memCache.set(key, out, isCanvas ? TTL_CANVAS_MS : TTL_PREVIEW_MS);
      saveDisk(key, imageUrl, info.width, info.height, resizeFit, mode, pixels);
      stats.totalPixelsProcessed += pixels.length;
      return out;
    }));

    res.json(result);
  } catch (error) {
    if (error && error.code === 503) {
      stats.serverBusy++;
      return res.status(503).json({ error: "server busy" });
    }
    if (error && error.code === 400) {
      return res.status(400).json({ error: String(error.message || error) });
    }
    stats.failedJobs++;
    console.error("[pixelize]", error);
    res.status(500).json({ error: String(error.message || error) });
  }
});

// ---------- Periodic disk prune ----------
if (db) {
  setInterval(() => {
    const now = Date.now();
    try {
      const info = pruneSql.run(now - DISK_PREVIEW_MAX_AGE_MS, now - DISK_CANVAS_MAX_AGE_MS);
      if (info.changes > 0) console.log(`[cache] pruned ${info.changes} stale disk entries`);
    } catch (e) { console.warn("[cache] prune failed", e.message); }
  }, DISK_PRUNE_INTERVAL_MS).unref();
}

app.listen(PORT, () => {
  console.log(`Pixelize backend v2 on :${PORT}`);
  console.log(`  memCache max=${MAX_CACHE_ITEMS}  diskCache=${db ? "on" : "off"}`);
  console.log(`  concurrency max=${MAX_CONCURRENT_PIXELIZE}  queue max=${MAX_QUEUE_SIZE}`);
  console.log(`  rateLimit=${RATE_LIMIT_PER_MIN}/min  MAX_DIM=${MAX_DIM}`);
});
