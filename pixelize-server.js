// =====================================================================
//  Anime pixelize backend (v2.2 - compressed transport)
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
  process.env.MAX_PREVIEW_CACHE_ITEMS || LEGACY_MAX_CACHE_ITEMS || 120
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
let getSql;
let putSql;
let touchSql;
let countSql;
let pruneSql;
let updateZstdSql;

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

    getSql = db.prepare(
      "SELECT pixelsGz, pixelsZstd, w, h FROM cache WHERE key = ?"
    );
    putSql = db.prepare(`
      INSERT OR REPLACE INTO cache
        (key, imageUrl, w, h, fit, mode, pixelsGz, pixelsZstd, generatedAt, lastUsedAt)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    touchSql = db.prepare("UPDATE cache SET lastUsedAt = ? WHERE key = ?");
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

function loadDiskEntry(key) {
  if (!db) return null;

  const row = getSql.get(key);
  if (!row) return null;

  touchSql.run(Date.now(), key);
  return {
    width: row.w,
    height: row.h,
    pixelsGz: Buffer.from(row.pixelsGz),
    pixelsZstd: row.pixelsZstd ? Buffer.from(row.pixelsZstd) : null,
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
}

function saveDiskZstd(key, pixelsZstd) {
  if (!db) return;
  updateZstdSql.run(pixelsZstd, Date.now(), key);
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
};

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

  const rawRgb = gunzipRawRgb(entry.pixelsGz);
  validateRawLength(entry, rawRgb);
  return rawRgb;
}

function materializeV1Pixels(entry) {
  if (entry.pixels) return entry.pixels;

  const rawRgb = getRawRgb(entry);
  const pixelCount = entry.width * entry.height;
  const pixels = new Array(pixelCount);

  for (let i = 0, offset = 0; i < pixelCount; i++, offset += 3) {
    pixels[i] = [rawRgb[offset], rawRgb[offset + 1], rawRgb[offset + 2]];
  }

  entry.pixels = pixels;
  entry.rawRgb = null;
  stats.v1PixelArraysMaterialized++;
  return pixels;
}

async function ensureZstdPayload(key, entry) {
  if (entry.pixelsZstd) {
    stats.zstdPayloadCacheHits++;
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
  entry.rawRgb = null;

  saveDiskZstd(key, entry.pixelsZstd);
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

async function sendV2Response(res, key, entry) {
  const pixelsZstd = await ensureZstdPayload(key, entry);
  const pixelsBase64 = pixelsZstd.toString("base64");
  const rawBytes = entry.width * entry.height * 3;

  const payload = {
    formatVersion: 2,
    encoding: "rgb24",
    compression: "zstd",
    width: entry.width,
    height: entry.height,
    channels: 3,
    pixelCount: entry.width * entry.height,
    uncompressedBytes: rawBytes,
    compressedBytes: pixelsZstd.length,
    pixelsBase64,
  };

  stats.formatV2Responses++;
  stats.v2RawBytesRepresented += rawBytes;
  stats.v2CompressedBytesSent += pixelsZstd.length;
  stats.v2Base64BytesSent += Buffer.byteLength(pixelsBase64, "ascii");

  // Sending a prepared JSON string lets /stats account for the exact
  // Base64 payload size and avoids Express inspecting a Buffer object.
  return res.type("application/json").send(JSON.stringify(payload));
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
    apiVersion: "2.2",
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
    const key = cacheKeyFor({
      imageUrl,
      w,
      h,
      fit: resizeFit,
      colors,
    });

    const selectedMem = chooseMemoryCache(isCanvas, w, h);
    const memCache = selectedMem.cache;

    // 1) Memory cache
    const memHit = memCache.get(key);
    if (memHit) {
      stats.memCacheHits++;
      if (selectedMem.kind === "preview") stats.previewMemCacheHits++;
      else if (selectedMem.kind === "canvas") stats.canvasMemCacheHits++;
      else stats.largeCanvasMemCacheHits++;

      return await sendPixelResponse(res, key, memHit, formatVersion);
    }

    stats.memCacheMisses++;
    if (selectedMem.kind === "preview") stats.previewMemCacheMisses++;
    else if (selectedMem.kind === "canvas") stats.canvasMemCacheMisses++;
    else stats.largeCanvasMemCacheMisses++;

    // 2) Disk cache. Version 2 can return pixelsZstd directly without
    // rebuilding the giant pixels array.
    const diskHit = loadDiskEntry(key);
    if (diskHit) {
      stats.diskCacheHits++;
      memCache.set(
        key,
        diskHit,
        isCanvas ? TTL_CANVAS_MS : TTL_PREVIEW_MS
      );
      return await sendPixelResponse(res, key, diskHit, formatVersion);
    }

    stats.diskCacheMisses++;

    // 3) Shared image processing job. Response formatting happens after
    // dedupe, so v1 and v2 callers can share the same generated RGB data.
    const entry = await dedupe(key, () =>
      withSlot(async () => {
        const lateMem = memCache.get(key);
        if (lateMem) return lateMem;

        const lateDisk = loadDiskEntry(key);
        if (lateDisk) {
          memCache.set(
            key,
            lateDisk,
            isCanvas ? TTL_CANVAS_MS : TTL_PREVIEW_MS
          );
          return lateDisk;
        }

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
        let imageResponse;

        try {
          imageResponse = await fetch(imageUrl, {
            signal: controller.signal,
            headers: {
              "User-Agent": "AnimePixelizeBackend/2.2",
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

        const rawRgb = Buffer.from(raw.data);
        const generatedEntry = {
          width: raw.info.width,
          height: raw.info.height,
          pixelsGz: gzipRawRgb(rawRgb),
          pixelsZstd: null,
          pixels: null,
          rawRgb,
        };

        validateRawLength(generatedEntry, rawRgb);

        // Only create Zstd during a v2 request. This means deploying the
        // backend update does not add compression CPU cost to the current
        // live v1 game before the Roblox scripts are updated.
        if (formatVersion === 2) {
          generatedEntry.pixelsZstd = Buffer.from(
            zstdApi.compress(rawRgb, ZSTD_LEVEL)
          );
          generatedEntry.rawRgb = null;
          stats.zstdPayloadCompressions++;
        }

        const mode = isCanvas ? `${Number(colors)}colors` : "preview";
        memCache.set(
          key,
          generatedEntry,
          isCanvas ? TTL_CANVAS_MS : TTL_PREVIEW_MS
        );
        saveDiskEntry(
          key,
          imageUrl,
          generatedEntry.width,
          generatedEntry.height,
          resizeFit,
          mode,
          generatedEntry
        );

        stats.totalPixelsProcessed +=
          generatedEntry.width * generatedEntry.height;

        return generatedEntry;
      })
    );

    return await sendPixelResponse(res, key, entry, formatVersion);
  } catch (error) {
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

async function startServer() {
  // Initialise the portable WebAssembly compressor before accepting v2
  // traffic. If it fails, v1 still starts normally.
  await initialiseZstd();

  app.listen(PORT, () => {
    console.log(`Pixelize backend v2.2 on :${PORT}`);
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

startServer().catch((error) => {
  console.error("[startup] fatal error", error);
  process.exitCode = 1;
});
