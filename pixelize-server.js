// =====================================================================
//  Anime pixelize backend (v2.3 - streamed compressed transport)
//  ------------------------------------------------------------------
//  Backward-compatible response formats:
//
//    formatVersion 1 (default; current live Roblox game):
//      { width, height, pixels: [[r,g,b], ...] }
//
//    formatVersion 2 (opt-in; for the updated Roblox scripts):
//      {
//        formatVersion: 2,
//        encoding: "rgb24",
//        compression: "zstd",
//        width,
//        height,
//        channels: 3,
//        pixelCount,
//        uncompressedBytes,
//        compressedBytes,
//        pixelsBase64
//      }
//
//  Version 2 keeps the exact same RGB pixels, but avoids creating and
//  transmitting hundreds of thousands of nested JSON arrays. The raw
//  RGB bytes are Zstandard-compressed and Base64-wrapped for JSON.
//
//  Existing SQLite rows remain valid. The existing gzip RGB column is
//  preserved and a nullable Zstd column is added automatically. Old rows
//  are upgraded lazily the first time formatVersion 2 requests them.
// =====================================================================

const express = require("express");
const sharp   = require("sharp");
const zlib    = require("zlib");
const path    = require("path");
const { performance, monitorEventLoopDelay } = require("node:perf_hooks");

// SQLite remains optional so the service can still start without native
// SQLite support, although Oracle should have it installed.
let sqlite = null;
try {
  sqlite = require("better-sqlite3");
} catch (error) {
  console.warn("[cache] better-sqlite3 unavailable; disk cache disabled -", error.message);
}

// Zstandard is optional at runtime for safe deployment. If installation
// fails, the existing formatVersion 1 API continues working. Explicit
// formatVersion 2 requests receive 503 until the dependency is available.
let zstdApi = null;
let zstdReady = false;

async function initialiseZstd() {
  try {
    zstdApi = require("@bokuweb/zstd-wasm");
    await zstdApi.init();
    zstdReady = true;
    console.log("[compression] Zstandard WebAssembly initialised");
  } catch (error) {
    zstdApi = null;
    zstdReady = false;
    console.warn(
      "[compression] @bokuweb/zstd-wasm unavailable; formatVersion 2 disabled -",
      error.message
    );
  }
}

function hasZstd() {
  return Boolean(zstdReady && zstdApi && typeof zstdApi.compress === "function");
}

// ---------- CONFIG ----------
const PORT = Number(process.env.PORT || 3000);

// Legacy MAX_CACHE_ITEMS is retained only as a preview-cache fallback.
const LEGACY_MAX_CACHE_ITEMS = Number(process.env.MAX_CACHE_ITEMS || 0);
const MAX_PREVIEW_CACHE_ITEMS = Number(
  process.env.MAX_PREVIEW_CACHE_ITEMS || LEGACY_MAX_CACHE_ITEMS || 500
);
const MAX_CANVAS_CACHE_ITEMS = Number(process.env.MAX_CANVAS_CACHE_ITEMS || 20);
const MAX_LARGE_CANVAS_CACHE_ITEMS = Number(
  process.env.MAX_LARGE_CANVAS_CACHE_ITEMS || 2
);
const LARGE_CANVAS_MIN_PIXELS = Number(
  process.env.LARGE_CANVAS_MIN_PIXELS || 200_000
);

const MAX_CONCURRENT_PIXELIZE = Number(process.env.MAX_CONCURRENT_PIXELIZE || 1);
const MAX_QUEUE_SIZE = Number(process.env.MAX_QUEUE_SIZE || 30);
const RATE_LIMIT_PER_MIN = Number(process.env.RATE_LIMIT_PER_MIN || 120);
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 10_000);
const MAX_IMAGE_BYTES = Number(process.env.MAX_IMAGE_BYTES || 5 * 1024 * 1024);
const MAX_DIM = Number(process.env.MAX_DIM || 512);
const MIN_DIM = Number(process.env.MIN_DIM || 8);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "cache.sqlite");

// Stream Base64 in 48 KiB chunks by default. The chunk size is rounded down
// to a multiple of three so concatenated Base64 chunks never contain padding
// in the middle of the payload.
const BASE64_STREAM_CHUNK_BYTES = Math.max(
  3,
  Math.floor(
    Number(process.env.BASE64_STREAM_CHUNK_BYTES || 48 * 1024) / 3
  ) * 3
);

// Disk-hit timestamps are deduplicated in memory and written in batches. This
// removes a synchronous SQLite write from the hottest cache-hit path.
const DISK_TOUCH_FLUSH_MS = Number(
  process.env.DISK_TOUCH_FLUSH_MS || 60_000
);
const DISK_TOUCH_MIN_INTERVAL_MS = Number(
  process.env.DISK_TOUCH_MIN_INTERVAL_MS || 10 * 60 * 1000
);

// Compression levels are deliberately moderate to avoid wasting CPU.
// Zstd level 3 is a good real-time default; gzip remains the persistent
// compatibility representation already used by the current database.
const ZSTD_LEVEL = Math.max(1, Math.min(10, Number(process.env.ZSTD_LEVEL || 3)));
const GZIP_LEVEL = Math.max(1, Math.min(9, Number(process.env.GZIP_LEVEL || 6)));

// Preview cache TTL (ms). Full canvases live longer.
const TTL_PREVIEW_MS = Number(process.env.TTL_PREVIEW_MS || 30 * 60 * 1000);
const TTL_CANVAS_MS = Number(process.env.TTL_CANVAS_MS || 2 * 60 * 60 * 1000);

// Disk cache prune sweep interval + max age.
const DISK_PRUNE_INTERVAL_MS = Number(
  process.env.DISK_PRUNE_INTERVAL_MS || 6 * 60 * 60 * 1000
);
const DISK_PREVIEW_MAX_AGE_MS = Number(
  process.env.DISK_PREVIEW_MAX_AGE_MS || 7 * 24 * 60 * 60 * 1000
);
const DISK_CANVAS_MAX_AGE_MS = Number(
  process.env.DISK_CANVAS_MAX_AGE_MS || 60 * 24 * 60 * 60 * 1000
);

const ALLOWED_HOSTS = new Set([
  "s4.anilist.co",
  "cdn.myanimelist.net",
]);

// ---------- SQLite persistent cache ----------
// pixelsGz is the original gzip-compressed raw RGB storage.
// pixelsZstd is optional and filled lazily for transport format v2.
let db = null;
let getV1Sql;
let getV2Sql;
let putSql;
let touchSql;
let countSql;
let pruneSql;
let updateZstdSql;
let flushTouchesTransaction;

const pendingDiskTouches = new Map();
const lastPersistedDiskTouch = new Map();

if (sqlite) {
  try {
    db = new sqlite(DB_PATH);
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = NORMAL");

    db.exec(`
      CREATE TABLE IF NOT EXISTS cache (
        key         TEXT PRIMARY KEY,
        imageUrl    TEXT NOT NULL,
        w           INTEGER NOT NULL,
        h           INTEGER NOT NULL,
        fit         TEXT NOT NULL,
        mode        TEXT NOT NULL,
        pixelsGz    BLOB NOT NULL,
        pixelsZstd  BLOB,
        generatedAt INTEGER NOT NULL,
        lastUsedAt  INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_lastUsed ON cache(lastUsedAt);
      CREATE INDEX IF NOT EXISTS idx_mode ON cache(mode);
    `);

    // Migrate existing databases created before v2.2 without deleting or
    // rebuilding any cached artwork.
    const columns = db.prepare("PRAGMA table_info(cache)").all();
    if (!columns.some((column) => column.name === "pixelsZstd")) {
      db.exec("ALTER TABLE cache ADD COLUMN pixelsZstd BLOB");
      console.log("[cache] added pixelsZstd column to existing database");
    }

    getV1Sql = db.prepare(
      "SELECT pixelsGz, w, h FROM cache WHERE key = ?"
    );
    getV2Sql = db.prepare(
      "SELECT pixelsZstd, w, h FROM cache WHERE key = ?"
    );
    putSql = db.prepare(`
      INSERT OR REPLACE INTO cache
        (key, imageUrl, w, h, fit, mode, pixelsGz, pixelsZstd, generatedAt, lastUsedAt)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    touchSql = db.prepare("UPDATE cache SET lastUsedAt = ? WHERE key = ?");
    flushTouchesTransaction = db.transaction((entries) => {
      for (const [key, usedAt] of entries) {
        touchSql.run(usedAt, key);
      }
    });
    updateZstdSql = db.prepare(
      "UPDATE cache SET pixelsZstd = ?, lastUsedAt = ? WHERE key = ?"
    );
    countSql = db.prepare("SELECT COUNT(*) AS n FROM cache");
    pruneSql = db.prepare(`
      DELETE FROM cache
      WHERE (mode = 'preview' AND lastUsedAt < ?)
         OR (mode != 'preview' AND lastUsedAt < ?)
    `);

    console.log(`[cache] disk cache at ${DB_PATH}`);
  } catch (error) {
    console.warn("[cache] failed to open SQLite; running without disk cache -", error.message);
    db = null;
  }
}

function gzipRawRgb(rawRgb) {
  return zlib.gzipSync(rawRgb, { level: GZIP_LEVEL });
}

function gunzipRawRgb(pixelsGz) {
  return zlib.gunzipSync(pixelsGz);
}

function queueDiskTouch(key) {
  if (!db) return;

  const now = Date.now();
  const lastPersisted = lastPersistedDiskTouch.get(key) || 0;
  if (now - lastPersisted < DISK_TOUCH_MIN_INTERVAL_MS) return;

  // Map#set deduplicates repeat hits while preserving the newest timestamp.
  pendingDiskTouches.set(key, now);
}

function flushDiskTouches() {
  if (!db || !flushTouchesTransaction || pendingDiskTouches.size === 0) {
    return;
  }

  const entries = Array.from(pendingDiskTouches.entries());
  pendingDiskTouches.clear();

  try {
    flushTouchesTransaction(entries);
    for (const [key, usedAt] of entries) {
      lastPersistedDiskTouch.set(key, usedAt);
    }
    stats.diskTouchBatches++;
    stats.diskRowsTouched += entries.length;
  } catch (error) {
    stats.diskTouchFailures++;
    console.warn("[cache] touch batch failed -", error.message);

    // Retry later without replacing a newer queued timestamp.
    for (const [key, usedAt] of entries) {
      const current = pendingDiskTouches.get(key) || 0;
      pendingDiskTouches.set(key, Math.max(current, usedAt));
    }
  }
}

function loadDiskEntry(key, formatVersion) {
  if (!db) return null;

  if (formatVersion === 2) {
    const zstdRow = getV2Sql.get(key);
    if (!zstdRow) return null;

    queueDiskTouch(key);

    if (zstdRow.pixelsZstd) {
      return {
        width: zstdRow.w,
        height: zstdRow.h,
        pixelsGz: null,
        pixelsZstd: Buffer.from(zstdRow.pixelsZstd),
        pixels: null,
        rawRgb: null,
      };
    }

    // An older row may not have been lazily upgraded to Zstd yet.
    const gzipRow = getV1Sql.get(key);
    if (!gzipRow) return null;

    return {
      width: gzipRow.w,
      height: gzipRow.h,
      pixelsGz: Buffer.from(gzipRow.pixelsGz),
      pixelsZstd: null,
      pixels: null,
      rawRgb: null,
    };
  }

  const gzipRow = getV1Sql.get(key);
  if (!gzipRow) return null;

  queueDiskTouch(key);
  return {
    width: gzipRow.w,
    height: gzipRow.h,
    pixelsGz: Buffer.from(gzipRow.pixelsGz),
    pixelsZstd: null,
    pixels: null,
    rawRgb: null,
  };
}

function saveDiskEntry(key, imageUrl, w, h, fit, mode, entry) {
  if (!db) return;

  const now = Date.now();
  putSql.run(
    key,
    imageUrl,
    w,
    h,
    fit,
    mode,
    entry.pixelsGz,
    entry.pixelsZstd,
    now,
    now
  );
  lastPersistedDiskTouch.set(key, now);
}

function saveDiskZstd(key, pixelsZstd) {
  if (!db) return;
  const now = Date.now();
  updateZstdSql.run(pixelsZstd, now, key);
  lastPersistedDiskTouch.set(key, now);
}

// ---------- Memory LRU ----------
class LRU {
  constructor(max, ttlMs) {
    this.max = max;
    this.ttl = ttlMs;
    this.map = new Map();
  }

  get(key) {
    const entry = this.map.get(key);
    if (!entry) return null;

    if (this.ttl && Date.now() > entry.expiresAt) {
      this.map.delete(key);
      return null;
    }

    this.map.delete(key);
    this.map.set(key, entry);
    return entry.value;
  }

  set(key, value, ttlMs) {
    if (this.map.has(key)) this.map.delete(key);

    const ttl = ttlMs != null ? ttlMs : this.ttl;
    this.map.set(key, {
      value,
      expiresAt: Date.now() + ttl,
    });

    while (this.map.size > this.max) {
      this.map.delete(this.map.keys().next().value);
    }
  }

  invalidate(key) {
    this.map.delete(key);
  }

  size() {
    return this.map.size;
  }
}

const previewMemCache = new LRU(MAX_PREVIEW_CACHE_ITEMS, TTL_PREVIEW_MS);
const canvasMemCache = new LRU(MAX_CANVAS_CACHE_ITEMS, TTL_CANVAS_MS);
const largeCanvasMemCache = new LRU(
  MAX_LARGE_CANVAS_CACHE_ITEMS,
  TTL_CANVAS_MS
);

function chooseMemoryCache(isCanvas, w, h) {
  if (!isCanvas) {
    return { cache: previewMemCache, kind: "preview" };
  }
  if (w * h >= LARGE_CANVAS_MIN_PIXELS) {
    return { cache: largeCanvasMemCache, kind: "largeCanvas" };
  }
  return { cache: canvasMemCache, kind: "canvas" };
}

function totalMemCacheSize() {
  return (
    previewMemCache.size() +
    canvasMemCache.size() +
    largeCanvasMemCache.size()
  );
}

// ---------- In-flight dedup ----------
// The cache key intentionally does not include formatVersion. A v1 and v2
// request for the same artwork share the same image download/Sharp job.
const inflight = new Map();

async function dedupe(key, fn) {
  const pending = inflight.get(key);
  if (pending) {
    stats.inFlightDedupes++;
    return pending;
  }

  const promise = (async () => {
    try {
      return await fn();
    } finally {
      inflight.delete(key);
    }
  })();

  inflight.set(key, promise);
  return promise;
}

// ---------- Concurrency semaphore + bounded queue ----------
let running = 0;
let queued = 0;
let activePixelizeResponses = 0;
let maxActivePixelizeResponses = 0;
let activeV2Streams = 0;
let maxActiveV2Streams = 0;
const waiters = [];

async function withSlot(fn) {
  if (queued >= MAX_QUEUE_SIZE) {
    const error = new Error("server busy");
    error.code = 503;
    throw error;
  }

  queued++;
  try {
    if (running >= MAX_CONCURRENT_PIXELIZE) {
      await new Promise((resolve) => waiters.push(resolve));
    }

    running++;
    try {
      return await fn();
    } finally {
      running--;
      const next = waiters.shift();
      if (next) next();
    }
  } finally {
    queued--;
  }
}

// ---------- Rate limit ----------
const buckets = new Map();

function rateLimit(ip) {
  const now = Date.now();
  const bucket = buckets.get(ip) || {
    tokens: RATE_LIMIT_PER_MIN,
    updatedAt: now,
  };

  const refill = ((now - bucket.updatedAt) / 60_000) * RATE_LIMIT_PER_MIN;
  bucket.tokens = Math.min(RATE_LIMIT_PER_MIN, bucket.tokens + refill);
  bucket.updatedAt = now;

  if (bucket.tokens < 1) {
    buckets.set(ip, bucket);
    return false;
  }

  bucket.tokens -= 1;
  buckets.set(ip, bucket);
  return true;
}

setInterval(() => {
  const cutoff = Date.now() - 10 * 60 * 1000;
  for (const [ip, bucket] of buckets) {
    if (bucket.updatedAt < cutoff) buckets.delete(ip);
  }
}, 5 * 60 * 1000).unref();

// ---------- Latency + event-loop telemetry ----------
class LatencyWindow {
  constructor(limit = 2048) {
    this.limit = limit;
    this.samples = new Array(limit);
    this.next = 0;
    this.size = 0;
    this.totalCount = 0;
    this.totalMs = 0;
    this.maxLifetimeMs = 0;
  }

  record(ms) {
    if (!Number.isFinite(ms) || ms < 0) return;

    this.samples[this.next] = ms;
    this.next = (this.next + 1) % this.limit;
    this.size = Math.min(this.size + 1, this.limit);
    this.totalCount++;
    this.totalMs += ms;
    this.maxLifetimeMs = Math.max(this.maxLifetimeMs, ms);
  }

  snapshot() {
    const values = this.samples
      .slice(0, this.size)
      .filter(Number.isFinite)
      .sort((a, b) => a - b);

    const percentile = (p) => {
      if (values.length === 0) return null;
      const index = Math.min(
        values.length - 1,
        Math.floor((p / 100) * values.length)
      );
      return Number(values[index].toFixed(1));
    };

    return {
      lifetimeCount: this.totalCount,
      lifetimeAverageMs:
        this.totalCount > 0
          ? Number((this.totalMs / this.totalCount).toFixed(1))
          : null,
      windowSamples: values.length,
      p50Ms: percentile(50),
      p95Ms: percentile(95),
      p99Ms: percentile(99),
      windowMaxMs:
        values.length > 0
          ? Number(values[values.length - 1].toFixed(1))
          : null,
      lifetimeMaxMs: Number(this.maxLifetimeMs.toFixed(1)),
    };
  }
}

const latency = {
  total: new LatencyWindow(),
  previewMemory: new LatencyWindow(),
  previewDisk: new LatencyWindow(),
  previewGenerated: new LatencyWindow(),
  canvasMemory: new LatencyWindow(),
  canvasDisk: new LatencyWindow(),
  canvasGenerated: new LatencyWindow(),
  largeCanvasMemory: new LatencyWindow(),
  largeCanvasDisk: new LatencyWindow(),
  largeCanvasGenerated: new LatencyWindow(),
  v2StreamWrite: new LatencyWindow(),
};

const eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
eventLoopDelay.enable();
let lastEventLoopDelayWindow = null;

function readEventLoopDelay() {
  const toMs = (nanoseconds) =>
    Number.isFinite(nanoseconds)
      ? Number((nanoseconds / 1e6).toFixed(1))
      : null;

  return {
    meanMs: toMs(eventLoopDelay.mean),
    p50Ms: toMs(eventLoopDelay.percentile(50)),
    p95Ms: toMs(eventLoopDelay.percentile(95)),
    p99Ms: toMs(eventLoopDelay.percentile(99)),
    maxMs: toMs(eventLoopDelay.max),
  };
}

setInterval(() => {
  lastEventLoopDelayWindow = readEventLoopDelay();
  eventLoopDelay.reset();
}, 60_000).unref();

// ---------- Stats ----------
const startedAt = Date.now();
const stats = {
  memCacheHits: 0,
  memCacheMisses: 0,
  previewMemCacheHits: 0,
  canvasMemCacheHits: 0,
  largeCanvasMemCacheHits: 0,
  previewMemCacheMisses: 0,
  canvasMemCacheMisses: 0,
  largeCanvasMemCacheMisses: 0,
  diskCacheHits: 0,
  diskCacheMisses: 0,
  inFlightDedupes: 0,
  totalPixelizeRequests: 0,
  rateLimited: 0,
  serverBusy: 0,
  failedJobs: 0,
  hostBlocked: 0,
  sizeBlocked: 0,
  totalPixelsProcessed: 0,

  formatV1Requests: 0,
  formatV2Requests: 0,
  formatV1Responses: 0,
  formatV2Responses: 0,
  formatV2Unavailable: 0,
  v1PixelArraysMaterialized: 0,
  zstdPayloadCacheHits: 0,
  zstdPayloadCompressions: 0,
  v2RawBytesRepresented: 0,
  v2CompressedBytesSent: 0,
  v2Base64BytesSent: 0,
  v2StreamedResponses: 0,
  v2StreamWriteMs: 0,
  clientAborts: 0,

  diskTouchBatches: 0,
  diskRowsTouched: 0,
  diskTouchFailures: 0,
};

if (db) {
  setInterval(flushDiskTouches, DISK_TOUCH_FLUSH_MS).unref();

  setInterval(() => {
    const cutoff = Date.now() - DISK_TOUCH_MIN_INTERVAL_MS * 2;
    for (const [key, usedAt] of lastPersistedDiskTouch) {
      if (usedAt < cutoff) lastPersistedDiskTouch.delete(key);
    }
  }, 30 * 60 * 1000).unref();
}

// ---------- Helpers ----------
function isAllowedImageUrl(imageUrl) {
  try {
    const url = new URL(imageUrl);
    return url.protocol === "https:" && ALLOWED_HOSTS.has(url.hostname);
  } catch {
    return false;
  }
}

function cacheKeyFor({ imageUrl, w, h, fit, colors }) {
  const mode =
    colors != null && Number(colors) > 0
      ? `${Number(colors)}colors`
      : "preview";
  return `${imageUrl}|${w}|${h}|${fit}|${mode}`;
}

function parseFormatVersion(value) {
  if (value == null || value === "" || Number(value) === 1) return 1;
  if (Number(value) === 2) return 2;
  return null;
}

function validateRawLength(entry, rawRgb) {
  const expectedBytes = entry.width * entry.height * 3;
  if (rawRgb.length !== expectedBytes) {
    const error = new Error(
      `Corrupt pixel cache entry: expected ${expectedBytes} RGB bytes, got ${rawRgb.length}`
    );
    error.code = 500;
    throw error;
  }
}

function getRawRgb(entry) {
  if (entry.rawRgb) {
    validateRawLength(entry, entry.rawRgb);
    return entry.rawRgb;
  }

  if (entry.pixelsGz) {
    const rawRgb = gunzipRawRgb(entry.pixelsGz);
    validateRawLength(entry, rawRgb);
    return rawRgb;
  }

  if (
    entry.pixelsZstd &&
    zstdApi &&
    typeof zstdApi.decompress === "function"
  ) {
    const rawRgb = Buffer.from(zstdApi.decompress(entry.pixelsZstd));
    validateRawLength(entry, rawRgb);
    return rawRgb;
  }

  const error = new Error("Pixel cache entry has no decodable payload");
  error.code = 500;
  throw error;
}

function materializeV1Pixels(entry) {
  const rawRgb = getRawRgb(entry);
  const pixelCount = entry.width * entry.height;
  const pixels = new Array(pixelCount);

  for (let i = 0, offset = 0; i < pixelCount; i++, offset += 3) {
    pixels[i] = [rawRgb[offset], rawRgb[offset + 1], rawRgb[offset + 2]];
  }

  // The nested v1 array is intentionally temporary. Retaining it in the
  // shared LRU would keep hundreds of thousands of small arrays alive.
  stats.v1PixelArraysMaterialized++;
  return pixels;
}

async function ensureZstdPayload(key, entry) {
  if (entry.pixelsZstd) {
    stats.zstdPayloadCacheHits++;

    // SQLite retains the gzip compatibility copy, so v2 hot-cache entries
    // only need the Zstd representation.
    if (db) {
      entry.pixelsGz = null;
      entry.pixels = null;
      entry.rawRgb = null;
    }

    return entry.pixelsZstd;
  }

  if (!hasZstd()) {
    const error = new Error("compressed format temporarily unavailable");
    error.code = 503;
    error.compressionUnavailable = true;
    throw error;
  }

  const rawRgb = getRawRgb(entry);
  const compressed = zstdApi.compress(rawRgb, ZSTD_LEVEL);
  entry.pixelsZstd = Buffer.from(compressed);

  saveDiskZstd(key, entry.pixelsZstd);

  if (db) entry.pixelsGz = null;
  entry.pixels = null;
  entry.rawRgb = null;

  stats.zstdPayloadCompressions++;
  return entry.pixelsZstd;
}

function sendV1Response(res, entry) {
  stats.formatV1Responses++;
  return res.json({
    width: entry.width,
    height: entry.height,
    pixels: materializeV1Pixels(entry),
  });
}

function writeResponseChunk(res, chunk) {
  if (res.destroyed || res.writableEnded) {
    const error = new Error("client disconnected");
    error.clientAborted = true;
    return Promise.reject(error);
  }

  if (res.write(chunk)) return Promise.resolve();

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      res.off("drain", onDrain);
      res.off("close", onClose);
      res.off("error", onError);
    };

    const onDrain = () => {
      cleanup();
      resolve();
    };

    const onClose = () => {
      cleanup();
      const error = new Error("client disconnected");
      error.clientAborted = true;
      reject(error);
    };

    const onError = (error) => {
      cleanup();
      reject(error);
    };

    res.once("drain", onDrain);
    res.once("close", onClose);
    res.once("error", onError);
  });
}

async function sendV2Response(res, key, entry) {
  const responseStartedAt = performance.now();
  const pixelsZstd = await ensureZstdPayload(key, entry);
  const rawBytes = entry.width * entry.height * 3;
  const base64Bytes = Math.ceil(pixelsZstd.length / 3) * 4;

  const metadata = {
    formatVersion: 2,
    encoding: "rgb24",
    compression: "zstd",
    width: entry.width,
    height: entry.height,
    channels: 3,
    pixelCount: entry.width * entry.height,
    uncompressedBytes: rawBytes,
    compressedBytes: pixelsZstd.length,
  };

  const metadataJson = JSON.stringify(metadata);
  const prefix = metadataJson.slice(0, -1) + ',"pixelsBase64":"';
  const suffix = '"}';
  const contentLength =
    Buffer.byteLength(prefix, "utf8") +
    base64Bytes +
    Buffer.byteLength(suffix, "utf8");

  activeV2Streams++;
  maxActiveV2Streams = Math.max(maxActiveV2Streams, activeV2Streams);

  try {
    res.status(200);
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Content-Length", String(contentLength));

    await writeResponseChunk(res, prefix);

    for (
      let offset = 0;
      offset < pixelsZstd.length;
      offset += BASE64_STREAM_CHUNK_BYTES
    ) {
      const end = Math.min(
        offset + BASE64_STREAM_CHUNK_BYTES,
        pixelsZstd.length
      );
      const encodedChunk = pixelsZstd.subarray(offset, end).toString("base64");
      await writeResponseChunk(res, encodedChunk);
    }

    res.end(suffix);

    const elapsed = performance.now() - responseStartedAt;
    stats.formatV2Responses++;
    stats.v2RawBytesRepresented += rawBytes;
    stats.v2CompressedBytesSent += pixelsZstd.length;
    stats.v2Base64BytesSent += base64Bytes;
    stats.v2StreamedResponses++;
    stats.v2StreamWriteMs += elapsed;
    latency.v2StreamWrite.record(elapsed);
  } finally {
    activeV2Streams--;
  }
}

async function sendPixelResponse(res, key, entry, formatVersion) {
  if (formatVersion === 2) {
    return sendV2Response(res, key, entry);
  }
  return sendV1Response(res, entry);
}

// ---------- Express ----------
const app = express();
app.set("trust proxy", true);
app.use(express.json({ limit: "1mb" }));

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    message: "Anime pixelize backend is running",
    apiVersion: "2.3",
    pixelFormats: {
      default: 1,
      supported: hasZstd() ? [1, 2] : [1],
      format2Available: hasZstd(),
    },
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    format2Available: hasZstd(),
  });
});

app.get("/stats", (req, res) => {
  const diskRow = db ? countSql.get() : { n: 0 };
  const compressionRatio =
    stats.v2CompressedBytesSent > 0
      ? Number(
          (
            stats.v2RawBytesRepresented / stats.v2CompressedBytesSent
          ).toFixed(2)
        )
      : null;

  res.json({
    uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
    memoryUsageMB: Object.fromEntries(
      Object.entries(process.memoryUsage()).map(([key, value]) => [
        key,
        Number((value / 1024 / 1024).toFixed(1)),
      ])
    ),
    memCacheSize: totalMemCacheSize(),
    previewMemCacheSize: previewMemCache.size(),
    canvasMemCacheSize: canvasMemCache.size(),
    largeCanvasMemCacheSize: largeCanvasMemCache.size(),
    totalMemCacheSize: totalMemCacheSize(),
    memoryCacheLimits: {
      preview: MAX_PREVIEW_CACHE_ITEMS,
      canvas: MAX_CANVAS_CACHE_ITEMS,
      largeCanvas: MAX_LARGE_CANVAS_CACHE_ITEMS,
      largeCanvasMinPixels: LARGE_CANVAS_MIN_PIXELS,
    },
    diskCacheSize: diskRow.n,
    diskCacheEnabled: Boolean(db),
    compression: {
      format2Available: hasZstd(),
      algorithm: "zstd",
      zstdLevel: ZSTD_LEVEL,
      gzipLevel: GZIP_LEVEL,
      cumulativeRawToCompressedRatio: compressionRatio,
    },
    inFlightJobs: inflight.size,
    queued,
    running,
    concurrency: {
      max: MAX_CONCURRENT_PIXELIZE,
      queueMax: MAX_QUEUE_SIZE,
    },
    rateLimit: {
      perMinute: RATE_LIMIT_PER_MIN,
      bucketsTracked: buckets.size,
    },
    pendingDiskTouches: pendingDiskTouches.size,
    httpResponses: {
      activePixelize: activePixelizeResponses,
      maxActivePixelize: maxActivePixelizeResponses,
      activeV2Streams,
      maxActiveV2Streams,
    },
    latencyMs: Object.fromEntries(
      Object.entries(latency).map(([name, collector]) => [
        name,
        collector.snapshot(),
      ])
    ),
    eventLoopDelay: {
      last60Seconds: lastEventLoopDelayWindow,
      currentWindow: readEventLoopDelay(),
    },
    counters: { ...stats },
  });
});

app.post("/pixelize", async (req, res) => {
  const requestStartedAt = performance.now();
  const requestTelemetry = { kind: null, source: null };
  let responseFinalized = false;

  activePixelizeResponses++;
  maxActivePixelizeResponses = Math.max(
    maxActivePixelizeResponses,
    activePixelizeResponses
  );

  const finalizeResponseTelemetry = () => {
    if (responseFinalized) return;
    responseFinalized = true;

    activePixelizeResponses = Math.max(0, activePixelizeResponses - 1);
    const elapsed = performance.now() - requestStartedAt;
    latency.total.record(elapsed);

    if (requestTelemetry.kind && requestTelemetry.source) {
      const key =
        requestTelemetry.kind +
        requestTelemetry.source[0].toUpperCase() +
        requestTelemetry.source.slice(1);
      if (latency[key]) latency[key].record(elapsed);
    }
  };

  res.once("finish", finalizeResponseTelemetry);
  res.once("close", finalizeResponseTelemetry);

  const ip = req.ip || req.connection?.remoteAddress || "unknown";
  if (!rateLimit(ip)) {
    stats.rateLimited++;
    return res.status(429).json({ error: "rate limited" });
  }

  stats.totalPixelizeRequests++;

  try {
    const {
      imageUrl,
      width,
      height,
      fit,
      colors,
      formatVersion: requestedFormatVersion,
    } = req.body || {};

    const formatVersion = parseFormatVersion(requestedFormatVersion);
    if (formatVersion == null) {
      return res.status(400).json({
        error: "Unsupported formatVersion",
        supportedFormatVersions: hasZstd() ? [1, 2] : [1],
      });
    }

    if (formatVersion === 2) {
      stats.formatV2Requests++;
      if (!hasZstd()) {
        stats.formatV2Unavailable++;
        return res.status(503).json({
          error: "compressed format temporarily unavailable",
          fallbackFormatVersion: 1,
        });
      }
    } else {
      stats.formatV1Requests++;
    }

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

    const w = Math.max(
      MIN_DIM,
      Math.min(MAX_DIM, Math.round(Number(width) || 120))
    );
    const h = Math.max(
      MIN_DIM,
      Math.min(MAX_DIM, Math.round(Number(height) || 120))
    );

    const resizeFit = fit === "contain" ? "contain" : "cover";
    const isCanvas = colors != null && Number(colors) > 0;
    const key = cacheKeyFor({ imageUrl, w, h, fit: resizeFit, colors });
    const selectedMem = chooseMemoryCache(isCanvas, w, h);
    const memCache = selectedMem.cache;
    const cacheTtl = isCanvas ? TTL_CANVAS_MS : TTL_PREVIEW_MS;

    // 1) Memory cache.
    const memHit = memCache.get(key);
    if (memHit) {
      stats.memCacheHits++;
      if (selectedMem.kind === "preview") stats.previewMemCacheHits++;
      else if (selectedMem.kind === "canvas") stats.canvasMemCacheHits++;
      else stats.largeCanvasMemCacheHits++;

      requestTelemetry.kind = selectedMem.kind;
      requestTelemetry.source = "memory";
      return await sendPixelResponse(res, key, memHit, formatVersion);
    }

    stats.memCacheMisses++;
    if (selectedMem.kind === "preview") stats.previewMemCacheMisses++;
    else if (selectedMem.kind === "canvas") stats.canvasMemCacheMisses++;
    else stats.largeCanvasMemCacheMisses++;

    // 2) Format-aware disk cache. v2 normally reads only the Zstd column.
    const diskHit = loadDiskEntry(key, formatVersion);
    if (diskHit) {
      stats.diskCacheHits++;
      memCache.set(key, diskHit, cacheTtl);
      requestTelemetry.kind = selectedMem.kind;
      requestTelemetry.source = "disk";
      return await sendPixelResponse(res, key, diskHit, formatVersion);
    }

    stats.diskCacheMisses++;

    // 3) Shared image-processing job. A late cache result is returned with
    // its source so latency telemetry does not mislabel it as generated.
    const resolved = await dedupe(key, () =>
      withSlot(async () => {
        const lateMem = memCache.get(key);
        if (lateMem) return { entry: lateMem, source: "memory" };

        const lateDisk = loadDiskEntry(key, formatVersion);
        if (lateDisk) {
          memCache.set(key, lateDisk, cacheTtl);
          return { entry: lateDisk, source: "disk" };
        }

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
        let imageResponse;

        try {
          imageResponse = await fetch(imageUrl, {
            signal: controller.signal,
            headers: {
              "User-Agent": "AnimePixelizeBackend/2.3",
            },
          });
        } finally {
          clearTimeout(timeout);
        }

        if (!imageResponse.ok) {
          const error = new Error(
            `Failed to download image: ${imageResponse.status}`
          );
          error.code = 400;
          throw error;
        }

        const contentType = imageResponse.headers.get("content-type") || "";
        if (!contentType.startsWith("image/")) {
          const error = new Error("URL did not return an image");
          error.code = 400;
          throw error;
        }

        const contentLength = Number(
          imageResponse.headers.get("content-length") || 0
        );
        if (contentLength > MAX_IMAGE_BYTES) {
          stats.sizeBlocked++;
          const error = new Error("Image too large");
          error.code = 400;
          throw error;
        }

        const imageBuffer = Buffer.from(await imageResponse.arrayBuffer());
        if (imageBuffer.length > MAX_IMAGE_BYTES) {
          stats.sizeBlocked++;
          const error = new Error("Image too large");
          error.code = 400;
          throw error;
        }

        const raw = await sharp(imageBuffer)
          .resize(w, h, {
            fit: resizeFit,
            position: "center",
            background: { r: 255, g: 255, b: 255, alpha: 1 },
          })
          .flatten({ background: { r: 255, g: 255, b: 255 } })
          .removeAlpha()
          .raw()
          .toBuffer({ resolveWithObject: true });

        // raw.data is already a Buffer; avoid a second full RGB allocation.
        const rawRgb = raw.data;
        const generatedEntry = {
          width: raw.info.width,
          height: raw.info.height,
          pixelsGz: gzipRawRgb(rawRgb),
          pixelsZstd: null,
          pixels: null,
          rawRgb,
        };

        validateRawLength(generatedEntry, rawRgb);

        if (formatVersion === 2) {
          generatedEntry.pixelsZstd = Buffer.from(
            zstdApi.compress(rawRgb, ZSTD_LEVEL)
          );
          stats.zstdPayloadCompressions++;
        }

        const mode = isCanvas ? `${Number(colors)}colors` : "preview";

        // Save the required gzip compatibility representation first.
        saveDiskEntry(
          key,
          imageUrl,
          generatedEntry.width,
          generatedEntry.height,
          resizeFit,
          mode,
          generatedEntry
        );

        // Once persisted, a v2 hot entry keeps only Zstd in memory.
        if (formatVersion === 2 && db) {
          generatedEntry.pixelsGz = null;
          generatedEntry.rawRgb = null;
          generatedEntry.pixels = null;
        }

        memCache.set(key, generatedEntry, cacheTtl);
        stats.totalPixelsProcessed +=
          generatedEntry.width * generatedEntry.height;

        return { entry: generatedEntry, source: "generated" };
      })
    );

    requestTelemetry.kind = selectedMem.kind;
    requestTelemetry.source = resolved.source;
    return await sendPixelResponse(
      res,
      key,
      resolved.entry,
      formatVersion
    );
  } catch (error) {
    if (error?.clientAborted || res.destroyed) {
      stats.clientAborts++;
      return;
    }

    if (res.headersSent) {
      stats.failedJobs++;
      console.error("[pixelize-after-headers]", error);
      if (!res.destroyed) res.destroy(error);
      return;
    }

    if (error && error.code === 503) {
      if (error.compressionUnavailable) stats.formatV2Unavailable++;
      else stats.serverBusy++;
      return res.status(503).json({
        error: String(error.message || "server busy"),
        fallbackFormatVersion: error.compressionUnavailable ? 1 : undefined,
      });
    }

    if (error && error.code === 400) {
      return res.status(400).json({
        error: String(error.message || error),
      });
    }

    stats.failedJobs++;
    console.error("[pixelize]", error);
    return res.status(500).json({
      error: String(error.message || error),
    });
  }
});

// ---------- Periodic disk prune ----------
if (db) {
  setInterval(() => {
    const now = Date.now();
    try {
      const info = pruneSql.run(
        now - DISK_PREVIEW_MAX_AGE_MS,
        now - DISK_CANVAS_MAX_AGE_MS
      );
      if (info.changes > 0) {
        console.log(`[cache] pruned ${info.changes} stale disk entries`);
      }
      db.pragma("optimize");
    } catch (error) {
      console.warn("[cache] prune failed", error.message);
    }
  }, DISK_PRUNE_INTERVAL_MS).unref();
}

let httpServer = null;
let shuttingDown = false;

async function startServer() {
  // Initialise the portable WebAssembly compressor before accepting v2
  // traffic. If it fails, v1 still starts normally.
  await initialiseZstd();

  httpServer = app.listen(PORT, () => {
    console.log(`Pixelize backend v2.3 on :${PORT}`);
    console.log(
      `  memCache preview=${MAX_PREVIEW_CACHE_ITEMS} ` +
        `canvas=${MAX_CANVAS_CACHE_ITEMS} ` +
        `large=${MAX_LARGE_CANVAS_CACHE_ITEMS} ` +
        `(large >= ${LARGE_CANVAS_MIN_PIXELS} px) ` +
        `diskCache=${db ? "on" : "off"}`
    );
    console.log(
      `  compressed format v2=${hasZstd() ? "available" : "unavailable"} ` +
        `zstdLevel=${ZSTD_LEVEL}`
    );
    console.log(
      `  concurrency max=${MAX_CONCURRENT_PIXELIZE} queue max=${MAX_QUEUE_SIZE}`
    );
    console.log(
      `  rateLimit=${RATE_LIMIT_PER_MIN}/min MAX_DIM=${MAX_DIM}`
    );
  });
}

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[shutdown] ${signal}`);

  try {
    flushDiskTouches();
  } catch (error) {
    console.warn("[shutdown] final touch flush failed -", error.message);
  }

  const finish = () => {
    try {
      flushDiskTouches();
      if (db && db.open) db.close();
    } catch (error) {
      console.warn("[shutdown] cache close failed -", error.message);
    }
    process.exit(0);
  };

  if (httpServer) {
    httpServer.close(finish);
    setTimeout(() => {
      console.warn("[shutdown] forcing exit after timeout");
      finish();
    }, 10_000).unref();
  } else {
    finish();
  }
}

process.once("SIGTERM", () => shutdown("SIGTERM"));
process.once("SIGINT", () => shutdown("SIGINT"));

startServer().catch((error) => {
  console.error("[startup] fatal error", error);
  process.exitCode = 1;
});
