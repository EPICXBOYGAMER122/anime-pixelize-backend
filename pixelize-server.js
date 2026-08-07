// =====================================================================
//  Paint Any Anime backend (v2.6.3 - Studio-scoped real enforcement override + Roblox-specific Rekognition policy + moderation + safe-first MangaDex matching + exact AniList suggestive fallback)
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
const crypto  = require("node:crypto");
const {
  performance,
  monitorEventLoopDelay,
  PerformanceObserver,
  constants: perfConstants,
} = require("node:perf_hooks");

// SQLite remains optional so the service can still start without native
// SQLite support, although Oracle should have it installed.
let sqlite = null;
try {
  sqlite = require("better-sqlite3");
} catch (error) {
  console.warn("[cache] better-sqlite3 unavailable; disk cache disabled -", error.message);
}

// Amazon Rekognition is optional at process-start so the existing backend can
// still run with moderation disabled while dependencies or credentials are
// being deployed. When moderation is enabled, missing SDK/configuration is
// surfaced through health telemetry and enforcement fails closed.
let rekognitionSdk = null;
try {
  rekognitionSdk = require("@aws-sdk/client-rekognition");
} catch (error) {
  console.warn(
    "[moderation] @aws-sdk/client-rekognition unavailable; moderation disabled -",
    error.message
  );
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
const API_VERSION = "2.6.3";
const IMAGE_FETCH_USER_AGENT =
  process.env.IMAGE_FETCH_USER_AGENT || "PaintAnyAnimeBackend/2.6.3";


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
const MAX_IMAGE_REDIRECTS = Math.max(0, Number(process.env.MAX_IMAGE_REDIRECTS || 3));
const MAX_INPUT_PIXELS = Math.max(1, Number(process.env.MAX_INPUT_PIXELS || 40_000_000));
const MAX_DIM = Number(process.env.MAX_DIM || 512);
const MIN_DIM = Number(process.env.MIN_DIM || 8);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "cache.sqlite");

// ---------- Amazon Rekognition image moderation ----------
function envBoolean(name, fallback = false) {
  const raw = process.env[name];
  if (raw == null || raw === "") return fallback;
  return /^(1|true|yes|on)$/i.test(String(raw));
}

const AWS_REGION = String(process.env.AWS_REGION || "eu-west-2").trim();
const AWS_REKOGNITION_ENABLED = envBoolean("AWS_REKOGNITION_ENABLED", false);
const AWS_REKOGNITION_ENFORCE =
  AWS_REKOGNITION_ENABLED && envBoolean("AWS_REKOGNITION_ENFORCE", false);

// Controls whether ordinary non-enforcing public requests still perform paid
// Rekognition scans in shadow mode. Keep this false during Studio-only testing
// so live players do not consume Rekognition calls. Global enforcement and a
// valid Studio override always take precedence and still run moderation.
const AWS_REKOGNITION_PUBLIC_SHADOW_SCAN =
  AWS_REKOGNITION_ENABLED &&
  envBoolean("AWS_REKOGNITION_PUBLIC_SHADOW_SCAN", false);

// Optional request-scoped enforcement for unpublished Roblox Studio testing.
// Global enforcement remains authoritative. The token is read only from the
// process environment and is never included in health/stats/log output.
const AWS_REKOGNITION_STUDIO_ENFORCE_ENABLED =
  AWS_REKOGNITION_ENABLED &&
  envBoolean("AWS_REKOGNITION_STUDIO_ENFORCE_ENABLED", false);
const AWS_REKOGNITION_STUDIO_ENFORCE_TOKEN = String(
  process.env.AWS_REKOGNITION_STUDIO_ENFORCE_TOKEN || ""
).trim();
const AWS_REKOGNITION_STUDIO_ENFORCE_HEADER =
  "X-PAA-Studio-Enforce-Token";
const AWS_REKOGNITION_STUDIO_TOKEN_MIN_BYTES = 32;
const AWS_REKOGNITION_STUDIO_TOKEN_CONFIGURED = Boolean(
  AWS_REKOGNITION_STUDIO_ENFORCE_ENABLED &&
  Buffer.byteLength(AWS_REKOGNITION_STUDIO_ENFORCE_TOKEN, "utf8") >=
    AWS_REKOGNITION_STUDIO_TOKEN_MIN_BYTES
);

function hasValidStudioEnforcementToken(req) {
  if (!AWS_REKOGNITION_STUDIO_TOKEN_CONFIGURED || !req) return false;

  const supplied = req.get(AWS_REKOGNITION_STUDIO_ENFORCE_HEADER);
  if (typeof supplied !== "string" || supplied.length === 0) return false;

  const expectedBuffer = Buffer.from(
    AWS_REKOGNITION_STUDIO_ENFORCE_TOKEN,
    "utf8"
  );
  const suppliedBuffer = Buffer.from(supplied, "utf8");
  if (expectedBuffer.length !== suppliedBuffer.length) return false;

  return crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
}

function effectiveModerationEnforcementForRequest(req) {
  return Boolean(
    AWS_REKOGNITION_ENFORCE || hasValidStudioEnforcementToken(req)
  );
}

function shouldModerateRequest(req) {
  return Boolean(
    AWS_REKOGNITION_ENABLED &&
    (AWS_REKOGNITION_ENFORCE ||
      hasValidStudioEnforcementToken(req) ||
      AWS_REKOGNITION_PUBLIC_SHADOW_SCAN)
  );
}

const AWS_REKOGNITION_MIN_CONFIDENCE = Math.max(0, Math.min(100, Number(
  process.env.AWS_REKOGNITION_MIN_CONFIDENCE || 50
)));
const AWS_REKOGNITION_MAX_CONCURRENCY = Math.max(1, Number(
  process.env.AWS_REKOGNITION_MAX_CONCURRENCY || 4
));
const AWS_REKOGNITION_TIMEOUT_MS = Math.max(1000, Number(
  process.env.AWS_REKOGNITION_TIMEOUT_MS || 5000
));
const AWS_REKOGNITION_MAX_QUEUE = Math.max(1, Number(
  process.env.AWS_REKOGNITION_MAX_QUEUE || 100
));
const AWS_REKOGNITION_MAX_IMAGE_BYTES = Math.max(256 * 1024, Math.min(
  5 * 1024 * 1024,
  Number(process.env.AWS_REKOGNITION_MAX_IMAGE_BYTES || 4_500_000)
));
const AWS_REKOGNITION_MAX_DIM = Math.max(256, Number(
  process.env.AWS_REKOGNITION_MAX_DIM || 1600
));
const AWS_REKOGNITION_JPEG_QUALITY = Math.max(40, Math.min(95, Number(
  process.env.AWS_REKOGNITION_JPEG_QUALITY || 85
)));

// Policy v1 intentionally targets sexual/nudity risk for Roblox artwork.
// Generic parent labels, male nipples, bare backs, ordinary swimwear,
// violence, weapons, alcohol and kissing do not block by themselves.
const MODERATION_POLICY_VERSION = "roblox-sexual-safety-v1";
const MODERATION_POLICY_BACKFILL_BATCH = Math.max(25, Math.min(2000, Number(
  process.env.MODERATION_POLICY_BACKFILL_BATCH || 250
)));

const moderationConfigured = Boolean(
  rekognitionSdk &&
  rekognitionSdk.RekognitionClient &&
  rekognitionSdk.DetectModerationLabelsCommand &&
  AWS_REGION
);

const rekognitionClient = AWS_REKOGNITION_ENABLED && moderationConfigured
  ? new rekognitionSdk.RekognitionClient({
      region: AWS_REGION,
      // The backend performs one explicit retry for transient failures.
      // Disable hidden SDK retries so latency and call counts remain bounded.
      maxAttempts: 1,
    })
  : null;

let moderationLastSuccessAt = 0;
let moderationLastErrorAt = 0;
let moderationLastErrorCode = null;

// Bound libvips working memory. The pixel cache remains separate and is
// already bounded by entry count. These defaults trade a small amount of
// throughput for much lower RSS spikes on small Oracle instances.
const SHARP_CACHE_MEMORY_MB = Math.max(0, Number(process.env.SHARP_CACHE_MEMORY_MB || 16));
const SHARP_CACHE_FILES = Math.max(0, Number(process.env.SHARP_CACHE_FILES || 0));
const SHARP_CACHE_ITEMS = Math.max(0, Number(process.env.SHARP_CACHE_ITEMS || 64));
const SHARP_CONCURRENCY = Math.max(1, Number(process.env.SHARP_CONCURRENCY || 1));
sharp.cache({
  memory: SHARP_CACHE_MEMORY_MB,
  files: SHARP_CACHE_FILES,
  items: SHARP_CACHE_ITEMS,
});
sharp.concurrency(SHARP_CONCURRENCY);

// MangaDex integration is deliberately disabled by default. Its current API
// acceptable-use policy requires credit and disallows paid services/apps.
// Enable only after confirming your Roblox experience is permitted to use it.
const ENABLE_MANGADEX = /^(1|true|yes)$/i.test(
  String(process.env.ENABLE_MANGADEX || "false")
);
const MANGADEX_API_BASE = process.env.MANGADEX_API_BASE || "https://api.mangadex.org";
const MANGADEX_COVER_BASE =
  process.env.MANGADEX_COVER_BASE || "https://uploads.mangadex.org/covers";
const MANGADEX_USER_AGENT =
  process.env.MANGADEX_USER_AGENT || "PaintAnyAnimeBackend/2.6.3";
const MANGADEX_COVER_VARIANT = String(
  process.env.MANGADEX_COVER_VARIANT || "512"
).trim();
const CATALOG_FETCH_TIMEOUT_MS = Math.max(1000, Number(
  process.env.CATALOG_FETCH_TIMEOUT_MS || 8_000
));
const CATALOG_MAX_RESPONSE_BYTES = Math.max(16 * 1024, Number(
  process.env.CATALOG_MAX_RESPONSE_BYTES || 1024 * 1024
));
const CATALOG_CACHE_MAX_ITEMS = Math.max(10, Number(
  process.env.CATALOG_CACHE_MAX_ITEMS || 250
));
const CATALOG_CACHE_MAX_BYTES = Math.max(256 * 1024, Number(
  process.env.CATALOG_CACHE_MAX_BYTES || 4 * 1024 * 1024
));
const CATALOG_CACHE_TTL_MS = Math.max(60_000, Number(
  process.env.CATALOG_CACHE_TTL_MS || 6 * 60 * 60 * 1000
));
const CATALOG_CACHE_STALE_MS = Math.max(CATALOG_CACHE_TTL_MS, Number(
  process.env.CATALOG_CACHE_STALE_MS || 24 * 60 * 60 * 1000
));
const CATALOG_RATE_LIMIT_PER_MIN = Math.max(1, Number(
  process.env.CATALOG_RATE_LIMIT_PER_MIN || 120
));
const MANGADEX_MATCH_LIMIT = Math.max(10, Math.min(100, Number(
  process.env.MANGADEX_MATCH_LIMIT || 50
)));
const MANGADEX_MATCH_SCAN_MAX = Math.max(
  MANGADEX_MATCH_LIMIT,
  Math.min(100, Number(process.env.MANGADEX_MATCH_SCAN_MAX || 100))
);
const MANGADEX_COVER_FETCH_LIMIT = Math.max(10, Math.min(100, Number(
  process.env.MANGADEX_COVER_FETCH_LIMIT || 100
)));
const MANGADEX_COVER_MAX_RECORDS = Math.max(100, Number(
  process.env.MANGADEX_COVER_MAX_RECORDS || 1000
));
const MANGADEX_PREFERRED_COVER_LOCALES = Array.from(new Set(
  String(process.env.MANGADEX_PREFERRED_COVER_LOCALES || "ja,en")
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
));

// Memory telemetry is intentionally bounded and kept only in process memory.
// RSS is sampled more frequently because it represents the complete resident
// process size, while the full V8 memory breakdown is sampled every 250 ms.
const MEMORY_RSS_SAMPLE_MS = Math.max(
  50,
  Number(process.env.MEMORY_RSS_SAMPLE_MS || 100)
);
const MEMORY_FULL_SAMPLE_MS = Math.max(
  100,
  Number(process.env.MEMORY_FULL_SAMPLE_MS || 250)
);
const MEMORY_WINDOW_MS = 15 * 60 * 1000;
const MEMORY_SPIKE_LOOKBACK_MS = Math.max(
  500,
  Number(process.env.MEMORY_SPIKE_LOOKBACK_MS || 1000)
);
const MEMORY_HEAP_SPIKE_MB = Math.max(
  1,
  Number(process.env.MEMORY_HEAP_SPIKE_MB || 15)
);
const MEMORY_RSS_SPIKE_MB = Math.max(
  1,
  Number(process.env.MEMORY_RSS_SPIKE_MB || 40)
);
const MEMORY_SPIKE_RECOVERY_HEAP_MB = Math.max(
  1,
  Number(process.env.MEMORY_SPIKE_RECOVERY_HEAP_MB || 5)
);
const MEMORY_SPIKE_RECOVERY_RSS_MB = Math.max(
  1,
  Number(process.env.MEMORY_SPIKE_RECOVERY_RSS_MB || 15)
);
const MEMORY_SPIKE_MAX_DURATION_MS = Math.max(
  1000,
  Number(process.env.MEMORY_SPIKE_MAX_DURATION_MS || 30_000)
);
const MEMORY_RECENT_SPIKE_LIMIT = Math.max(
  5,
  Number(process.env.MEMORY_RECENT_SPIKE_LIMIT || 50)
);
const GC_LONG_PAUSE_MS = Math.max(
  1,
  Number(process.env.GC_LONG_PAUSE_MS || 50)
);

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
  "uploads.mangadex.org",
]);


// ---------- Active-operation telemetry ----------
const OPERATION_NAMES = [
  "imageDownloads",
  "sharpJobs",
  "gzipCompressions",
  "gzipDecompressions",
  "zstdCompressions",
  "zstdDecompressions",
  "v1PixelArrays",
  "v1Responses",
  "previewGenerations",
  "canvasGenerations",
  "largeCanvasGenerations",
  "catalogRequests",
  "mangaDexRequests",
  "moderationDownloads",
  "moderationSharpJobs",
  "rekognitionRequests",
];

const operationTelemetry = {
  active: Object.fromEntries(OPERATION_NAMES.map((name) => [name, 0])),
  lifetimePeak: Object.fromEntries(OPERATION_NAMES.map((name) => [name, 0])),
};

function beginOperation(name) {
  if (!(name in operationTelemetry.active)) return;
  operationTelemetry.active[name]++;
  operationTelemetry.lifetimePeak[name] = Math.max(
    operationTelemetry.lifetimePeak[name],
    operationTelemetry.active[name]
  );
}

function endOperation(name) {
  if (!(name in operationTelemetry.active)) return;
  operationTelemetry.active[name] = Math.max(
    0,
    operationTelemetry.active[name] - 1
  );
}

function withSyncOperation(name, fn) {
  beginOperation(name);
  try {
    return fn();
  } finally {
    endOperation(name);
  }
}

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
let getModerationByUrlSql;
let getModerationByHashSql;
let putModerationUrlSql;
let putModerationResultSql;
let touchModerationUrlSql;
let touchModerationResultSql;
let countModerationResultsSql;
let updateModerationPolicySql;
let selectModerationPolicyBackfillSql;
let countModerationPolicySql;
let moderationPolicyBackfillTransaction;

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

      CREATE TABLE IF NOT EXISTS moderation_results (
        imageHash          TEXT PRIMARY KEY,
        verdict            TEXT NOT NULL,
        labelsJson         TEXT NOT NULL,
        moderationModelVersion TEXT,
        contentTypesJson   TEXT NOT NULL,
        checkedAt          INTEGER NOT NULL,
        lastUsedAt         INTEGER NOT NULL,
        policyVersion      TEXT,
        policyDecision     TEXT,
        matchedRule        TEXT,
        matchedLabel       TEXT,
        matchedConfidence  REAL,
        policyEvaluatedAt  INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_moderation_lastUsed
        ON moderation_results(lastUsedAt);

      CREATE TABLE IF NOT EXISTS moderation_urls (
        imageUrl    TEXT PRIMARY KEY,
        imageHash   TEXT NOT NULL,
        createdAt   INTEGER NOT NULL,
        lastUsedAt  INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_moderation_url_hash
        ON moderation_urls(imageHash);
    `);

    // Migrate existing databases created before v2.2 without deleting or
    // rebuilding any cached artwork.
    const columns = db.prepare("PRAGMA table_info(cache)").all();
    if (!columns.some((column) => column.name === "pixelsZstd")) {
      db.exec("ALTER TABLE cache ADD COLUMN pixelsZstd BLOB");
      console.log("[cache] added pixelsZstd column to existing database");
    }

    // Add moderation policy audit columns without deleting existing AWS
    // labels. Existing rows are re-evaluated locally under the current policy;
    // no additional Rekognition calls are required.
    const moderationColumns = db.prepare(
      "PRAGMA table_info(moderation_results)"
    ).all();
    const moderationColumnNames = new Set(
      moderationColumns.map((column) => column.name)
    );
    const moderationPolicyColumns = [
      ["policyVersion", "TEXT"],
      ["policyDecision", "TEXT"],
      ["matchedRule", "TEXT"],
      ["matchedLabel", "TEXT"],
      ["matchedConfidence", "REAL"],
      ["policyEvaluatedAt", "INTEGER"],
    ];
    for (const [columnName, columnType] of moderationPolicyColumns) {
      if (!moderationColumnNames.has(columnName)) {
        db.exec(
          `ALTER TABLE moderation_results ADD COLUMN ${columnName} ${columnType}`
        );
        console.log(
          `[moderation] added ${columnName} column to existing database`
        );
      }
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

    getModerationByUrlSql = db.prepare(`
      SELECT
        r.imageHash, r.verdict, r.labelsJson, r.moderationModelVersion,
        r.contentTypesJson, r.checkedAt, r.lastUsedAt,
        r.policyVersion, r.policyDecision, r.matchedRule, r.matchedLabel,
        r.matchedConfidence, r.policyEvaluatedAt
      FROM moderation_urls AS u
      JOIN moderation_results AS r ON r.imageHash = u.imageHash
      WHERE u.imageUrl = ?
    `);
    getModerationByHashSql = db.prepare(`
      SELECT
        imageHash, verdict, labelsJson, moderationModelVersion,
        contentTypesJson, checkedAt, lastUsedAt,
        policyVersion, policyDecision, matchedRule, matchedLabel,
        matchedConfidence, policyEvaluatedAt
      FROM moderation_results
      WHERE imageHash = ?
    `);
    putModerationUrlSql = db.prepare(`
      INSERT INTO moderation_urls (imageUrl, imageHash, createdAt, lastUsedAt)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(imageUrl) DO UPDATE SET
        imageHash = excluded.imageHash,
        lastUsedAt = excluded.lastUsedAt
    `);
    putModerationResultSql = db.prepare(`
      INSERT INTO moderation_results
        (imageHash, verdict, labelsJson, moderationModelVersion,
         contentTypesJson, checkedAt, lastUsedAt)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(imageHash) DO UPDATE SET
        verdict = excluded.verdict,
        labelsJson = excluded.labelsJson,
        moderationModelVersion = excluded.moderationModelVersion,
        contentTypesJson = excluded.contentTypesJson,
        checkedAt = excluded.checkedAt,
        lastUsedAt = excluded.lastUsedAt
    `);
    touchModerationUrlSql = db.prepare(
      "UPDATE moderation_urls SET lastUsedAt = ? WHERE imageUrl = ?"
    );
    touchModerationResultSql = db.prepare(
      "UPDATE moderation_results SET lastUsedAt = ? WHERE imageHash = ?"
    );
    countModerationResultsSql = db.prepare(
      "SELECT COUNT(*) AS n FROM moderation_results"
    );
    updateModerationPolicySql = db.prepare(`
      UPDATE moderation_results
      SET policyVersion = ?,
          policyDecision = ?,
          matchedRule = ?,
          matchedLabel = ?,
          matchedConfidence = ?,
          policyEvaluatedAt = ?,
          lastUsedAt = ?
      WHERE imageHash = ?
    `);
    selectModerationPolicyBackfillSql = db.prepare(`
      SELECT imageHash, verdict, labelsJson
      FROM moderation_results
      WHERE policyVersion IS NULL OR policyVersion != ?
      LIMIT ?
    `);
    countModerationPolicySql = db.prepare(`
      SELECT policyDecision, COUNT(*) AS n
      FROM moderation_results
      WHERE policyVersion = ?
      GROUP BY policyDecision
    `);
    moderationPolicyBackfillTransaction = db.transaction((rows, evaluatedAt) => {
      for (const row of rows) {
        const result = {
          imageHash: row.imageHash,
          verdict: row.verdict,
          labels: parseJsonArray(row.labelsJson),
        };
        const policy = evaluateModerationPolicy(result);
        updateModerationPolicySql.run(
          policy.policyVersion,
          policy.policyDecision,
          policy.matchedRule,
          policy.matchedLabel,
          policy.matchedConfidence,
          evaluatedAt,
          evaluatedAt,
          row.imageHash
        );
      }
    });

    console.log(`[cache] disk cache at ${DB_PATH}`);
  } catch (error) {
    console.warn("[cache] failed to open SQLite; running without disk cache -", error.message);
    db = null;
  }
}

function gzipRawRgb(rawRgb) {
  return withSyncOperation("gzipCompressions", () =>
    zlib.gzipSync(rawRgb, { level: GZIP_LEVEL })
  );
}

function gunzipRawRgb(pixelsGz) {
  return withSyncOperation("gzipDecompressions", () =>
    zlib.gunzipSync(pixelsGz)
  );
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

class ByteBoundedLRU {
  constructor(maxItems, maxBytes, ttlMs, staleMs) {
    this.maxItems = maxItems;
    this.maxBytes = maxBytes;
    this.ttlMs = ttlMs;
    this.staleMs = staleMs;
    this.map = new Map();
    this.bytes = 0;
  }

  _sizeOf(value) {
    try {
      return Buffer.byteLength(JSON.stringify(value), "utf8");
    } catch {
      return 0;
    }
  }

  _remove(key) {
    const entry = this.map.get(key);
    if (!entry) return;
    this.bytes = Math.max(0, this.bytes - entry.bytes);
    this.map.delete(key);
  }

  get(key, allowStale = false) {
    const entry = this.map.get(key);
    if (!entry) return null;

    const now = Date.now();
    if (now > entry.staleUntil) {
      this._remove(key);
      return null;
    }
    if (!allowStale && now > entry.expiresAt) return null;

    this.map.delete(key);
    this.map.set(key, entry);
    return { value: entry.value, stale: now > entry.expiresAt };
  }

  set(key, value) {
    this._remove(key);
    const bytes = this._sizeOf(value);
    const now = Date.now();
    this.map.set(key, {
      value,
      bytes,
      expiresAt: now + this.ttlMs,
      staleUntil: now + this.staleMs,
    });
    this.bytes += bytes;

    while (this.map.size > this.maxItems || this.bytes > this.maxBytes) {
      const oldest = this.map.keys().next().value;
      if (oldest == null) break;
      this._remove(oldest);
    }
  }

  size() {
    return this.map.size;
  }
}

const catalogCache = new ByteBoundedLRU(
  CATALOG_CACHE_MAX_ITEMS,
  CATALOG_CACHE_MAX_BYTES,
  CATALOG_CACHE_TTL_MS,
  CATALOG_CACHE_STALE_MS
);
const catalogInflight = new Map();

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
const catalogBuckets = new Map();

function consumeRateToken(bucketMap, ip, perMinute) {
  const now = Date.now();
  const bucket = bucketMap.get(ip) || {
    tokens: perMinute,
    updatedAt: now,
  };

  const refill = ((now - bucket.updatedAt) / 60_000) * perMinute;
  bucket.tokens = Math.min(perMinute, bucket.tokens + refill);
  bucket.updatedAt = now;

  if (bucket.tokens < 1) {
    bucketMap.set(ip, bucket);
    return false;
  }

  bucket.tokens -= 1;
  bucketMap.set(ip, bucket);
  return true;
}

function rateLimit(ip) {
  return consumeRateToken(buckets, ip, RATE_LIMIT_PER_MIN);
}

function catalogRateLimit(ip) {
  return consumeRateToken(catalogBuckets, ip, CATALOG_RATE_LIMIT_PER_MIN);
}

setInterval(() => {
  const cutoff = Date.now() - 10 * 60 * 1000;
  for (const bucketMap of [buckets, catalogBuckets]) {
    for (const [ip, bucket] of bucketMap) {
      if (bucket.updatedAt < cutoff) bucketMap.delete(ip);
    }
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
  moderation: new LatencyWindow(),
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

  catalogRequests: 0,
  catalogRateLimited: 0,
  catalogCacheHits: 0,
  catalogCacheMisses: 0,
  catalogStaleFallbacks: 0,
  catalogInflightDedupes: 0,
  catalogFailures: 0,
  mangaDexRequests: 0,
  mangaDexFailures: 0,
  mangaDexResponseBytes: 0,
  mangaDexAmbiguousMatches: 0,
  mangaDexCoverRecordsFetched: 0,
  mangaDexCoverDuplicatesRemoved: 0,
  mangaDexCoverCatalogTruncated: 0,
  mangaDexSuggestiveFallbackRequests: 0,
  mangaDexSuggestiveFallbackMatches: 0,

  moderationCalls: 0,
  moderationSkippedPublicShadow: 0,
  moderationCacheHits: 0,
  moderationCacheMisses: 0,
  moderationNoLabels: 0,
  moderationLabelsDetected: 0,
  moderationBlocked: 0,
  moderationErrors: 0,
  moderationThrottles: 0,
  moderationRetries: 0,
  moderationInflightDedupes: 0,
  moderationPolicyEvaluations: 0,
  moderationWouldAllow: 0,
  moderationWouldBlock: 0,
  moderationWouldUncertain: 0,
  moderationEnforcedBlock: 0,
  moderationEnforcedUncertain: 0,
  moderationPolicyBackfilled: 0,
  moderationPolicyBackfillFailures: 0,
};

// ---------- Peak memory, spike and garbage-collection telemetry ----------
const BYTES_PER_MB = 1024 * 1024;
const fullMemorySamples = [];
const rssSamples = [];
const recentMemorySpikes = [];
const recentGcEvents = [];
const recentLongGcPauses = [];
let activeMemorySpike = null;
let spikeRecoverySamples = 0;

const lifetimeMemoryPeaksMB = {
  rss: 0,
  heapTotal: 0,
  heapUsed: 0,
  external: 0,
  arrayBuffers: 0,
};

const gcTelemetry = {
  supported: typeof PerformanceObserver === "function",
  lifetimeCount: 0,
  lifetimeTotalMs: 0,
  lifetimeMaxMs: 0,
};

function bytesToMB(bytes) {
  return Number((Number(bytes || 0) / BYTES_PER_MB).toFixed(1));
}

function memoryUsageToMB(memory) {
  return {
    rss: bytesToMB(memory.rss),
    heapTotal: bytesToMB(memory.heapTotal),
    heapUsed: bytesToMB(memory.heapUsed),
    external: bytesToMB(memory.external),
    arrayBuffers: bytesToMB(memory.arrayBuffers),
  };
}

function memoryValuesOnly(sample) {
  return {
    rss: sample.rss,
    heapTotal: sample.heapTotal,
    heapUsed: sample.heapUsed,
    external: sample.external,
    arrayBuffers: sample.arrayBuffers,
  };
}

function operationSnapshot() {
  return {
    ...operationTelemetry.active,
    running,
    queued,
    inFlightJobs: inflight.size,
    activePixelizeResponses,
    activeV2Streams,
  };
}

function updateOperationPeaks(target) {
  const current = operationSnapshot();
  for (const [name, value] of Object.entries(current)) {
    target[name] = Math.max(target[name] || 0, Number(value) || 0);
  }
}

function updateLifetimeMemoryPeaks(sample) {
  for (const key of Object.keys(lifetimeMemoryPeaksMB)) {
    lifetimeMemoryPeaksMB[key] = Math.max(
      lifetimeMemoryPeaksMB[key],
      Number(sample[key]) || 0
    );
  }
}

function pruneTimedSamples(samples, now, maxAgeMs = MEMORY_WINDOW_MS) {
  const cutoff = now - maxAgeMs;
  let removeCount = 0;
  while (
    removeCount < samples.length &&
    samples[removeCount].timestampMs < cutoff
  ) {
    removeCount++;
  }
  if (removeCount > 0) samples.splice(0, removeCount);
}

function memoryPeakSnapshot(windowMs) {
  const cutoff = Date.now() - windowMs;
  const result = {
    rss: null,
    heapTotal: null,
    heapUsed: null,
    external: null,
    arrayBuffers: null,
  };

  for (const sample of fullMemorySamples) {
    if (sample.timestampMs < cutoff) continue;
    for (const key of ["heapTotal", "heapUsed", "external", "arrayBuffers"]) {
      result[key] = Math.max(result[key] ?? 0, sample[key]);
    }
    result.rss = Math.max(result.rss ?? 0, sample.rss);
  }

  // The dedicated 100 ms RSS sampler may catch a shorter native-memory peak.
  for (const sample of rssSamples) {
    if (sample.timestampMs < cutoff) continue;
    result.rss = Math.max(result.rss ?? 0, sample.rss);
  }

  return result;
}

function gcKindName(kind) {
  const mapping = new Map([
    [perfConstants?.NODE_PERFORMANCE_GC_MAJOR, "major"],
    [perfConstants?.NODE_PERFORMANCE_GC_MINOR, "minor"],
    [perfConstants?.NODE_PERFORMANCE_GC_INCREMENTAL, "incremental"],
    [perfConstants?.NODE_PERFORMANCE_GC_WEAKCB, "weakCallbacks"],
  ]);
  return mapping.get(kind) || `kind-${kind ?? "unknown"}`;
}

function garbageCollectionSnapshot() {
  const cutoff = Date.now() - 60_000;
  const lastMinute = recentGcEvents.filter((event) => event.timestampMs >= cutoff);
  const lastMinuteTotalMs = lastMinute.reduce(
    (total, event) => total + event.durationMs,
    0
  );
  const lastMinuteMaxMs = lastMinute.reduce(
    (maximum, event) => Math.max(maximum, event.durationMs),
    0
  );

  return {
    supported: gcTelemetry.supported,
    lifetimeCount: gcTelemetry.lifetimeCount,
    lifetimeTotalMs: Number(gcTelemetry.lifetimeTotalMs.toFixed(1)),
    lifetimeMaxMs: Number(gcTelemetry.lifetimeMaxMs.toFixed(1)),
    last60SecondsCount: lastMinute.length,
    last60SecondsTotalMs: Number(lastMinuteTotalMs.toFixed(1)),
    last60SecondsMaxMs: Number(lastMinuteMaxMs.toFixed(1)),
    longPauseThresholdMs: GC_LONG_PAUSE_MS,
    recentLongPauses: recentLongGcPauses.slice(-20).reverse(),
  };
}

function startMemorySpike(current, before, trigger, now) {
  activeMemorySpike = {
    startedAtMs: now,
    startedAt: new Date(now).toISOString(),
    trigger,
    beforeMB: memoryValuesOnly(before),
    peakMB: memoryValuesOnly(current),
    operationsAtStart: operationSnapshot(),
    operationPeaks: {},
    gcStartCount: gcTelemetry.lifetimeCount,
    gcStartTotalMs: gcTelemetry.lifetimeTotalMs,
    gcStartMaxMs: gcTelemetry.lifetimeMaxMs,
  };
  updateOperationPeaks(activeMemorySpike.operationPeaks);
  spikeRecoverySamples = 0;
}

function finishMemorySpike(current, now, reason) {
  if (!activeMemorySpike) return;

  const spike = activeMemorySpike;
  const gcEventsDuring = recentGcEvents.filter(
    (event) => event.timestampMs >= spike.startedAtMs && event.timestampMs <= now
  );

  const completed = {
    startedAt: spike.startedAt,
    endedAt: new Date(now).toISOString(),
    durationMs: Math.max(0, now - spike.startedAtMs),
    endedReason: reason,
    trigger: spike.trigger,
    beforeMB: spike.beforeMB,
    peakMB: spike.peakMB,
    afterMB: memoryValuesOnly(current),
    operationsAtStart: spike.operationsAtStart,
    operationPeaks: spike.operationPeaks,
    garbageCollection: {
      count: gcTelemetry.lifetimeCount - spike.gcStartCount,
      totalMs: Number(
        (gcTelemetry.lifetimeTotalMs - spike.gcStartTotalMs).toFixed(1)
      ),
      maxMs: Number(
        gcEventsDuring.reduce(
          (maximum, event) => Math.max(maximum, event.durationMs),
          0
        ).toFixed(1)
      ),
    },
  };

  recentMemorySpikes.push(completed);
  while (recentMemorySpikes.length > MEMORY_RECENT_SPIKE_LIMIT) {
    recentMemorySpikes.shift();
  }

  if (completed.peakMB.heapUsed >= 75 || completed.peakMB.rss >= 750) {
    console.warn(
      `[memory] notable spike heap=${completed.peakMB.heapUsed}MB ` +
        `rss=${completed.peakMB.rss}MB duration=${completed.durationMs}ms`
    );
  }

  activeMemorySpike = null;
  spikeRecoverySamples = 0;
}

function recordFullMemorySample() {
  const now = Date.now();
  const current = {
    timestampMs: now,
    ...memoryUsageToMB(process.memoryUsage()),
  };

  fullMemorySamples.push(current);
  pruneTimedSamples(fullMemorySamples, now);
  updateLifetimeMemoryPeaks(current);

  const lookbackCutoff = now - MEMORY_SPIKE_LOOKBACK_MS;
  const lookback = fullMemorySamples.filter(
    (sample) => sample.timestampMs >= lookbackCutoff && sample.timestampMs < now
  );

  if (!activeMemorySpike && lookback.length > 0) {
    const before = {
      rss: Math.min(...lookback.map((sample) => sample.rss)),
      heapTotal: Math.min(...lookback.map((sample) => sample.heapTotal)),
      heapUsed: Math.min(...lookback.map((sample) => sample.heapUsed)),
      external: Math.min(...lookback.map((sample) => sample.external)),
      arrayBuffers: Math.min(...lookback.map((sample) => sample.arrayBuffers)),
    };
    const heapIncrease = current.heapUsed - before.heapUsed;
    const rssIncrease = current.rss - before.rss;

    if (
      heapIncrease >= MEMORY_HEAP_SPIKE_MB ||
      rssIncrease >= MEMORY_RSS_SPIKE_MB
    ) {
      startMemorySpike(
        current,
        before,
        {
          heapIncreaseMB: Number(heapIncrease.toFixed(1)),
          rssIncreaseMB: Number(rssIncrease.toFixed(1)),
        },
        now
      );
    }
  }

  if (activeMemorySpike) {
    for (const key of ["rss", "heapTotal", "heapUsed", "external", "arrayBuffers"]) {
      activeMemorySpike.peakMB[key] = Math.max(
        activeMemorySpike.peakMB[key],
        current[key]
      );
    }
    updateOperationPeaks(activeMemorySpike.operationPeaks);

    const heapRecovered =
      current.heapUsed <=
      activeMemorySpike.beforeMB.heapUsed + MEMORY_SPIKE_RECOVERY_HEAP_MB;
    const rssRecovered =
      current.rss <=
      activeMemorySpike.beforeMB.rss + MEMORY_SPIKE_RECOVERY_RSS_MB;

    const requiresHeapRecovery =
      activeMemorySpike.trigger.heapIncreaseMB >= MEMORY_HEAP_SPIKE_MB;
    const requiresRssRecovery =
      activeMemorySpike.trigger.rssIncreaseMB >= MEMORY_RSS_SPIKE_MB;
    const recovered =
      (!requiresHeapRecovery || heapRecovered) &&
      (!requiresRssRecovery || rssRecovered);

    if (recovered) spikeRecoverySamples++;
    else spikeRecoverySamples = 0;

    if (spikeRecoverySamples >= 3) {
      finishMemorySpike(current, now, "recovered");
    } else if (
      now - activeMemorySpike.startedAtMs >= MEMORY_SPIKE_MAX_DURATION_MS
    ) {
      finishMemorySpike(current, now, "timeout");
    }
  }
}

function recordRssSample() {
  const now = Date.now();
  const rssBytes =
    typeof process.memoryUsage.rss === "function"
      ? process.memoryUsage.rss()
      : process.memoryUsage().rss;
  const rss = bytesToMB(rssBytes);
  rssSamples.push({ timestampMs: now, rss });
  pruneTimedSamples(rssSamples, now);
  lifetimeMemoryPeaksMB.rss = Math.max(lifetimeMemoryPeaksMB.rss, rss);
}

function activeMemorySpikeSnapshot() {
  if (!activeMemorySpike) return null;
  return {
    startedAt: activeMemorySpike.startedAt,
    durationMs: Date.now() - activeMemorySpike.startedAtMs,
    trigger: activeMemorySpike.trigger,
    beforeMB: activeMemorySpike.beforeMB,
    peakMB: activeMemorySpike.peakMB,
    operationsAtStart: activeMemorySpike.operationsAtStart,
    operationPeaks: activeMemorySpike.operationPeaks,
    garbageCollectionCountSoFar:
      gcTelemetry.lifetimeCount - activeMemorySpike.gcStartCount,
  };
}

function memoryTelemetrySnapshot() {
  return {
    sampling: {
      rssEveryMs: MEMORY_RSS_SAMPLE_MS,
      fullMemoryEveryMs: MEMORY_FULL_SAMPLE_MS,
      rollingWindowMinutes: MEMORY_WINDOW_MS / 60_000,
      retainedFullSamples: fullMemorySamples.length,
      retainedRssSamples: rssSamples.length,
    },
    spikeDetection: {
      lookbackMs: MEMORY_SPIKE_LOOKBACK_MS,
      heapIncreaseMB: MEMORY_HEAP_SPIKE_MB,
      rssIncreaseMB: MEMORY_RSS_SPIKE_MB,
      recoveryHeapAboveBaselineMB: MEMORY_SPIKE_RECOVERY_HEAP_MB,
      recoveryRssAboveBaselineMB: MEMORY_SPIKE_RECOVERY_RSS_MB,
      maximumDurationMs: MEMORY_SPIKE_MAX_DURATION_MS,
      recentSpikeLimit: MEMORY_RECENT_SPIKE_LIMIT,
    },
    peaksMB: {
      last60Seconds: memoryPeakSnapshot(60_000),
      last15Minutes: memoryPeakSnapshot(MEMORY_WINDOW_MS),
      lifetime: { ...lifetimeMemoryPeaksMB },
    },
    activeSpike: activeMemorySpikeSnapshot(),
    recentSpikes: recentMemorySpikes.slice().reverse(),
  };
}

let gcObserver = null;
if (gcTelemetry.supported) {
  try {
    gcObserver = new PerformanceObserver((list) => {
      const now = Date.now();
      for (const entry of list.getEntries()) {
        const durationMs = Number(entry.duration || 0);
        const kind = entry.detail?.kind ?? entry.kind;
        const event = {
          timestampMs: now,
          time: new Date(now).toISOString(),
          kind: gcKindName(kind),
          durationMs: Number(durationMs.toFixed(1)),
        };

        gcTelemetry.lifetimeCount++;
        gcTelemetry.lifetimeTotalMs += durationMs;
        gcTelemetry.lifetimeMaxMs = Math.max(
          gcTelemetry.lifetimeMaxMs,
          durationMs
        );

        recentGcEvents.push(event);
        pruneTimedSamples(recentGcEvents, now, 60_000);

        if (durationMs >= GC_LONG_PAUSE_MS) {
          recentLongGcPauses.push({
            time: event.time,
            kind: event.kind,
            durationMs: event.durationMs,
            memoryUsageMB: memoryUsageToMB(process.memoryUsage()),
            operations: operationSnapshot(),
          });
          while (recentLongGcPauses.length > 50) {
            recentLongGcPauses.shift();
          }
        }
      }
    });
    gcObserver.observe({ entryTypes: ["gc"] });
  } catch (error) {
    gcTelemetry.supported = false;
    console.warn("[telemetry] GC observer unavailable -", error.message);
  }
}

recordRssSample();
recordFullMemorySample();
setInterval(recordRssSample, MEMORY_RSS_SAMPLE_MS).unref();
setInterval(recordFullMemorySample, MEMORY_FULL_SAMPLE_MS).unref();

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
    if (url.protocol !== "https:" || !ALLOWED_HOSTS.has(url.hostname)) {
      return false;
    }

    if (url.hostname === "uploads.mangadex.org") {
      return /^\/covers\/[0-9a-f-]{36}\//i.test(url.pathname);
    }

    return true;
  } catch {
    return false;
  }
}

function originalMangaDexCoverUrl(value) {
  try {
    const url = new URL(value);
    if (url.hostname !== "uploads.mangadex.org") return null;
    const match = url.pathname.match(/^(\/covers\/[0-9a-f-]{36}\/[^/]+)\.(256|512)\.jpg$/i);
    if (!match) return null;
    url.pathname = match[1];
    return url.toString();
  } catch {
    return null;
  }
}

async function fetchAllowedImageResponse(imageUrl, signal) {
  let currentUrl = imageUrl;
  let thumbnailFallback = originalMangaDexCoverUrl(currentUrl);

  for (let redirectCount = 0; redirectCount <= MAX_IMAGE_REDIRECTS; redirectCount++) {
    if (!isAllowedImageUrl(currentUrl)) {
      const error = new Error("Image redirect host is not allowed");
      error.code = 400;
      throw error;
    }

    const response = await fetch(currentUrl, {
      signal,
      redirect: "manual",
      headers: { "User-Agent": IMAGE_FETCH_USER_AGENT },
    });

    if (response.status === 404 && thumbnailFallback) {
      try { await response.body?.cancel(); } catch {}
      currentUrl = thumbnailFallback;
      thumbnailFallback = null;
      continue;
    }

    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("location");
      if (!location || redirectCount >= MAX_IMAGE_REDIRECTS) {
        const error = new Error("Too many or invalid image redirects");
        error.code = 400;
        throw error;
      }
      try { await response.body?.cancel(); } catch {}
      currentUrl = new URL(location, currentUrl).toString();
      continue;
    }

    return response;
  }

  const error = new Error("Too many image redirects");
  error.code = 400;
  throw error;
}

async function readResponseBodyLimited(response, maxBytes, sizeCounter) {
  const declaredLength = Number(response.headers.get("content-length") || 0);
  if (declaredLength > maxBytes) {
    if (sizeCounter) sizeCounter();
    const error = new Error("Response too large");
    error.code = 400;
    throw error;
  }

  if (!response.body || typeof response.body.getReader !== "function") {
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length > maxBytes) {
      if (sizeCounter) sizeCounter();
      const error = new Error("Response too large");
      error.code = 400;
      throw error;
    }
    return buffer;
  }

  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = Buffer.from(value);
      total += chunk.length;
      if (total > maxBytes) {
        if (sizeCounter) sizeCounter();
        await reader.cancel("response too large");
        const error = new Error("Response too large");
        error.code = 400;
        throw error;
      }
      chunks.push(chunk);
    }
  } finally {
    reader.releaseLock();
  }

  return Buffer.concat(chunks, total);
}


// ---------- Rekognition moderation cache + bounded queue ----------
const moderationInflight = new Map();
let moderationRunning = 0;
let moderationQueued = 0;
const moderationWaiters = [];
const moderationTouchTimes = new Map();
const MODERATION_TOUCH_MIN_INTERVAL_MS = 10 * 60 * 1000;

function moderationHealthSnapshot() {
  const operational = Boolean(
    AWS_REKOGNITION_ENABLED &&
    moderationConfigured &&
    (moderationLastErrorAt === 0 || moderationLastSuccessAt >= moderationLastErrorAt)
  );

  return {
    enabled: AWS_REKOGNITION_ENABLED,
    enforcing: AWS_REKOGNITION_ENFORCE,
    publicShadowScanning: AWS_REKOGNITION_PUBLIC_SHADOW_SCAN,
    configured: moderationConfigured,
    operational,
    policyVersion: MODERATION_POLICY_VERSION,
  };
}

async function withModerationSlot(fn) {
  if (moderationQueued >= AWS_REKOGNITION_MAX_QUEUE) {
    const error = new Error("moderation queue full");
    error.code = 503;
    error.moderationUnavailable = true;
    throw error;
  }

  moderationQueued++;
  try {
    if (moderationRunning >= AWS_REKOGNITION_MAX_CONCURRENCY) {
      await new Promise((resolve) => moderationWaiters.push(resolve));
    }

    moderationRunning++;
    try {
      return await fn();
    } finally {
      moderationRunning--;
      const next = moderationWaiters.shift();
      if (next) next();
    }
  } finally {
    moderationQueued--;
  }
}

function parseJsonArray(value) {
  try {
    const parsed = JSON.parse(value || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

// ---------- Roblox moderation policy ----------
const MODERATION_POLICY_RULES = Object.freeze([
  {
    id: "explicit-sexual-activity",
    label: "Explicit Sexual Activity",
    blockAt: 70,
    uncertainAt: 50,
    priority: 100,
  },
  {
    id: "exposed-male-genitalia",
    label: "Exposed Male Genitalia",
    blockAt: 70,
    uncertainAt: 50,
    priority: 99,
  },
  {
    id: "exposed-female-genitalia",
    label: "Exposed Female Genitalia",
    blockAt: 70,
    uncertainAt: 50,
    priority: 99,
  },
  {
    id: "exposed-female-nipple",
    label: "Exposed Female Nipple",
    blockAt: 70,
    uncertainAt: 50,
    priority: 98,
  },
  {
    id: "exposed-buttocks-or-anus",
    label: "Exposed Buttocks or Anus",
    blockAt: 70,
    uncertainAt: 50,
    priority: 97,
  },
  {
    // L2 fallback for the unlikely case where AWS returns Explicit Nudity
    // without one of its more specific L3 child labels.
    id: "explicit-nudity-fallback",
    label: "Explicit Nudity",
    blockAt: 90,
    uncertainAt: 70,
    priority: 90,
  },
  {
    id: "partially-exposed-female-breast",
    label: "Partially Exposed Female Breast",
    blockAt: 80,
    uncertainAt: 60,
    priority: 80,
  },
  {
    id: "partially-exposed-buttocks",
    label: "Partially Exposed Buttocks",
    blockAt: 80,
    uncertainAt: 60,
    priority: 75,
  },
  {
    id: "implied-nudity",
    label: "Implied Nudity",
    blockAt: 80,
    uncertainAt: 60,
    priority: 70,
  },
  {
    id: "obstructed-intimate-parts",
    label: "Obstructed Intimate Parts",
    blockAt: 80,
    uncertainAt: 60,
    priority: 70,
  },
]);

function normalisePolicyLabelName(value) {
  return String(value || "").trim().toLowerCase();
}

function choosePolicyMatch(matches) {
  if (matches.length === 0) return null;
  return matches.sort(
    (a, b) =>
      b.priority - a.priority ||
      b.confidence - a.confidence ||
      a.rule.localeCompare(b.rule)
  )[0];
}

function evaluateModerationPolicy(result) {
  const labels = Array.isArray(result?.labels) ? result.labels : [];
  const labelsByName = new Map();

  for (const label of labels) {
    const name = normalisePolicyLabelName(label?.name);
    if (!name) continue;
    const confidence = Number(label?.confidence || 0);
    const previous = labelsByName.get(name);
    if (!previous || confidence > previous.confidence) {
      labelsByName.set(name, {
        name: String(label?.name || ""),
        confidence,
      });
    }
  }

  const blockMatches = [];
  const uncertainMatches = [];

  for (const rule of MODERATION_POLICY_RULES) {
    const detected = labelsByName.get(normalisePolicyLabelName(rule.label));
    if (!detected) continue;

    const match = {
      rule: rule.id,
      label: detected.name,
      confidence: Number(detected.confidence.toFixed(2)),
      priority: rule.priority,
    };

    if (detected.confidence >= rule.blockAt) {
      blockMatches.push(match);
    } else if (detected.confidence >= rule.uncertainAt) {
      uncertainMatches.push(match);
    }
  }

  const blockMatch = choosePolicyMatch(blockMatches);
  if (blockMatch) {
    return {
      policyVersion: MODERATION_POLICY_VERSION,
      policyDecision: "block",
      matchedRule: blockMatch.rule,
      matchedLabel: blockMatch.label,
      matchedConfidence: blockMatch.confidence,
    };
  }

  const uncertainMatch = choosePolicyMatch(uncertainMatches);
  if (uncertainMatch) {
    return {
      policyVersion: MODERATION_POLICY_VERSION,
      policyDecision: "uncertain",
      matchedRule: uncertainMatch.rule,
      matchedLabel: uncertainMatch.label,
      matchedConfidence: uncertainMatch.confidence,
    };
  }

  const strongestLabel = labels
    .filter((label) => label?.name)
    .sort((a, b) => Number(b?.confidence || 0) - Number(a?.confidence || 0))[0];

  return {
    policyVersion: MODERATION_POLICY_VERSION,
    policyDecision: "allow",
    matchedRule:
      labels.length > 0 ? "no-sexual-safety-rule-matched" : "no-labels",
    matchedLabel: strongestLabel ? String(strongestLabel.name) : null,
    matchedConfidence: strongestLabel
      ? Number(Number(strongestLabel.confidence || 0).toFixed(2))
      : null,
  };
}

function moderationPolicyChanged(result, policy) {
  return (
    result?.policyVersion !== policy.policyVersion ||
    result?.policyDecision !== policy.policyDecision ||
    result?.matchedRule !== policy.matchedRule ||
    result?.matchedLabel !== policy.matchedLabel ||
    Number(result?.matchedConfidence ?? -1) !==
      Number(policy.matchedConfidence ?? -1)
  );
}

function persistModerationPolicy(result, policy) {
  if (
    !db ||
    !updateModerationPolicySql ||
    !result?.imageHash ||
    !moderationPolicyChanged(result, policy)
  ) {
    return;
  }

  const now = Date.now();
  updateModerationPolicySql.run(
    policy.policyVersion,
    policy.policyDecision,
    policy.matchedRule,
    policy.matchedLabel,
    policy.matchedConfidence,
    now,
    now,
    result.imageHash
  );

  result.policyVersion = policy.policyVersion;
  result.policyDecision = policy.policyDecision;
  result.matchedRule = policy.matchedRule;
  result.matchedLabel = policy.matchedLabel;
  result.matchedConfidence = policy.matchedConfidence;
  result.policyEvaluatedAt = now;
}

function recordModerationPolicyDecision(policy) {
  stats.moderationPolicyEvaluations++;
  if (policy.policyDecision === "block") stats.moderationWouldBlock++;
  else if (policy.policyDecision === "uncertain") {
    stats.moderationWouldUncertain++;
  } else {
    stats.moderationWouldAllow++;
  }
}

function recordEnforcedModerationDecision(decision) {
  stats.moderationBlocked++;
  if (decision.policyDecision === "uncertain") {
    stats.moderationEnforcedUncertain++;
  } else {
    stats.moderationEnforcedBlock++;
  }
}

async function runModerationPolicyBackfill() {
  if (
    !db ||
    !selectModerationPolicyBackfillSql ||
    !moderationPolicyBackfillTransaction
  ) {
    return;
  }

  let updated = 0;
  try {
    while (true) {
      const rows = selectModerationPolicyBackfillSql.all(
        MODERATION_POLICY_VERSION,
        MODERATION_POLICY_BACKFILL_BATCH
      );
      if (rows.length === 0) break;

      moderationPolicyBackfillTransaction(rows, Date.now());
      updated += rows.length;
      stats.moderationPolicyBackfilled += rows.length;

      await new Promise((resolve) => setImmediate(resolve));
    }

    if (updated > 0) {
      console.log(
        `[moderation] policy ${MODERATION_POLICY_VERSION} ` +
          `backfilled ${updated} cached image decisions without AWS calls`
      );
    }
  } catch (error) {
    stats.moderationPolicyBackfillFailures++;
    console.warn("[moderation] policy backfill failed -", error.message);
  }
}
// ---------- End Roblox moderation policy ----------

function moderationRowToResult(row) {
  if (!row) return null;
  return {
    imageHash: row.imageHash,
    verdict: row.verdict,
    labels: parseJsonArray(row.labelsJson),
    moderationModelVersion: row.moderationModelVersion || null,
    contentTypes: parseJsonArray(row.contentTypesJson),
    checkedAt: Number(row.checkedAt || 0),
    policyVersion: row.policyVersion || null,
    policyDecision: row.policyDecision || null,
    matchedRule: row.matchedRule || null,
    matchedLabel: row.matchedLabel || null,
    matchedConfidence:
      row.matchedConfidence == null ? null : Number(row.matchedConfidence),
    policyEvaluatedAt: Number(row.policyEvaluatedAt || 0),
  };
}

function maybeTouchModerationCache(imageUrl, imageHash) {
  if (!db) return;
  const now = Date.now();
  const key = imageUrl ? `url:${imageUrl}` : `hash:${imageHash}`;
  const last = moderationTouchTimes.get(key) || 0;
  if (now - last < MODERATION_TOUCH_MIN_INTERVAL_MS) return;

  moderationTouchTimes.set(key, now);
  try {
    if (imageUrl) touchModerationUrlSql.run(now, imageUrl);
    if (imageHash) touchModerationResultSql.run(now, imageHash);
  } catch (error) {
    console.warn("[moderation] cache touch failed -", error.message);
  }
}

function loadModerationByUrl(imageUrl) {
  if (!db || !getModerationByUrlSql) return null;
  const row = getModerationByUrlSql.get(imageUrl);
  if (!row) return null;
  maybeTouchModerationCache(imageUrl, row.imageHash);
  return moderationRowToResult(row);
}

function loadModerationByHash(imageHash) {
  if (!db || !getModerationByHashSql) return null;
  const row = getModerationByHashSql.get(imageHash);
  if (!row) return null;
  maybeTouchModerationCache(null, imageHash);
  return moderationRowToResult(row);
}

function saveModerationUrl(imageUrl, imageHash) {
  if (!db || !putModerationUrlSql) return;
  const now = Date.now();
  putModerationUrlSql.run(imageUrl, imageHash, now, now);
}

function saveModerationResult(result) {
  if (!db || !putModerationResultSql) return;
  const now = Date.now();
  putModerationResultSql.run(
    result.imageHash,
    result.verdict,
    JSON.stringify(result.labels || []),
    result.moderationModelVersion || null,
    JSON.stringify(result.contentTypes || []),
    now,
    now
  );
}

function normaliseModerationLabels(labels) {
  return (Array.isArray(labels) ? labels : [])
    .map((label) => ({
      name: String(label?.Name || ""),
      parentName: String(label?.ParentName || ""),
      confidence: Number(Number(label?.Confidence || 0).toFixed(2)),
      taxonomyLevel: Number(label?.TaxonomyLevel || 0),
    }))
    .filter((label) => label.name)
    .sort((a, b) => b.confidence - a.confidence || a.name.localeCompare(b.name));
}

function normaliseContentTypes(contentTypes) {
  return (Array.isArray(contentTypes) ? contentTypes : [])
    .map((entry) => ({
      name: String(entry?.Name || ""),
      confidence: Number(Number(entry?.Confidence || 0).toFixed(2)),
    }))
    .filter((entry) => entry.name)
    .sort((a, b) => b.confidence - a.confidence || a.name.localeCompare(b.name));
}

function isTransientRekognitionError(error) {
  const name = String(error?.name || error?.Code || error?.code || "");
  const status = Number(error?.$metadata?.httpStatusCode || 0);
  return (
    status === 429 ||
    status >= 500 ||
    /Throttl|ProvisionedThroughput|ServiceUnavailable|InternalServer|Timeout|Networking|Abort/i.test(name)
  );
}

function isThrottlingError(error) {
  const name = String(error?.name || error?.Code || error?.code || "");
  const status = Number(error?.$metadata?.httpStatusCode || 0);
  return status === 429 || /Throttl|ProvisionedThroughput/i.test(name);
}

async function downloadAllowedImage(imageUrl) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  let imageResponse;

  beginOperation("imageDownloads");
  try {
    imageResponse = await fetchAllowedImageResponse(imageUrl, controller.signal);

    if (!imageResponse.ok) {
      const error = new Error(`Failed to download image: ${imageResponse.status}`);
      error.code = 400;
      throw error;
    }

    const contentType = imageResponse.headers.get("content-type") || "";
    if (!contentType.startsWith("image/")) {
      const error = new Error("URL did not return an image");
      error.code = 400;
      throw error;
    }

    const contentLength = Number(imageResponse.headers.get("content-length") || 0);
    if (contentLength > MAX_IMAGE_BYTES) {
      stats.sizeBlocked++;
      const error = new Error("Image too large");
      error.code = 400;
      throw error;
    }

    return await readResponseBodyLimited(
      imageResponse,
      MAX_IMAGE_BYTES,
      () => { stats.sizeBlocked++; }
    );
  } finally {
    clearTimeout(timeout);
    endOperation("imageDownloads");
  }
}

async function createModerationJpeg(imageBuffer) {
  let maxDim = AWS_REKOGNITION_MAX_DIM;
  let quality = AWS_REKOGNITION_JPEG_QUALITY;
  let output = null;

  beginOperation("moderationSharpJobs");
  try {
    for (let attempt = 0; attempt < 4; attempt++) {
      output = await sharp(imageBuffer, {
        limitInputPixels: MAX_INPUT_PIXELS,
        limitInputChannels: 4,
        sequentialRead: true,
        pages: 1,
      })
        .rotate()
        .resize({
          width: maxDim,
          height: maxDim,
          fit: "inside",
          withoutEnlargement: true,
        })
        .flatten({ background: { r: 255, g: 255, b: 255 } })
        .removeAlpha()
        .jpeg({ quality, chromaSubsampling: "4:2:0", mozjpeg: false })
        .toBuffer();

      if (output.length <= AWS_REKOGNITION_MAX_IMAGE_BYTES) return output;
      maxDim = Math.max(512, Math.floor(maxDim * 0.75));
      quality = Math.max(50, quality - 10);
    }
  } finally {
    endOperation("moderationSharpJobs");
  }

  const error = new Error("Unable to prepare image within moderation size limit");
  error.code = 400;
  throw error;
}

async function sendRekognitionRequest(jpegBuffer) {
  if (!moderationConfigured || !rekognitionClient) {
    const error = new Error("image moderation is not configured");
    error.code = 503;
    error.moderationUnavailable = true;
    throw error;
  }

  let lastError;
  for (let attempt = 0; attempt < 2; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      AWS_REKOGNITION_TIMEOUT_MS
    );
    const started = performance.now();

    stats.moderationCalls++;
    beginOperation("rekognitionRequests");
    try {
      const command = new rekognitionSdk.DetectModerationLabelsCommand({
        Image: { Bytes: jpegBuffer },
        MinConfidence: AWS_REKOGNITION_MIN_CONFIDENCE,
      });
      const response = await rekognitionClient.send(command, {
        abortSignal: controller.signal,
      });
      moderationLastSuccessAt = Date.now();
      moderationLastErrorCode = null;
      latency.moderation.record(performance.now() - started);
      return response;
    } catch (error) {
      lastError = error;
      moderationLastErrorAt = Date.now();
      moderationLastErrorCode = String(error?.name || error?.code || "error");
      latency.moderation.record(performance.now() - started);
      if (isThrottlingError(error)) stats.moderationThrottles++;

      if (attempt === 0 && isTransientRekognitionError(error)) {
        stats.moderationRetries++;
        await new Promise((resolve) => setTimeout(resolve, 200));
        continue;
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      endOperation("rekognitionRequests");
    }
  }

  throw lastError;
}

function moderationDecision(result, imageBuffer = null, source = "cache") {
  const hasLabels = result.verdict === "labels_detected";
  const policy = evaluateModerationPolicy(result);
  persistModerationPolicy(result, policy);
  recordModerationPolicyDecision(policy);

  return {
    ...result,
    ...policy,
    hasLabels,
    // This field represents the policy verdict itself. Shadow-mode callers are
    // permitted separately by applyModerationEnforcement().
    allowed: policy.policyDecision === "allow",
    imageBuffer,
    source,
  };
}

function moderationBlockedError(decision) {
  const uncertain = decision?.policyDecision === "uncertain";
  const error = new Error(
    uncertain
      ? "image unavailable pending moderation review"
      : "image blocked by moderation policy"
  );
  error.code = 403;
  error.moderationBlocked = true;
  error.policyDecision = decision?.policyDecision || "block";
  error.matchedRule = decision?.matchedRule || null;
  return error;
}

function moderationUnavailableError(error) {
  const wrapped = new Error("image moderation temporarily unavailable");
  wrapped.code = 503;
  wrapped.moderationUnavailable = true;
  wrapped.cause = error;
  return wrapped;
}

function applyModerationEnforcement(decision, effectiveEnforcement) {
  if (!effectiveEnforcement) {
    return { ...decision, allowed: true };
  }

  if (decision?.moderationUnavailable) {
    throw moderationUnavailableError(decision.moderationErrorCause);
  }

  if (decision?.policyDecision !== "allow") {
    recordEnforcedModerationDecision(decision);
    throw moderationBlockedError(decision);
  }

  return { ...decision, allowed: true };
}

async function ensureImageModerated(
  imageUrl,
  effectiveEnforcement = AWS_REKOGNITION_ENFORCE
) {
  if (!AWS_REKOGNITION_ENABLED) {
    return {
      verdict: "disabled",
      allowed: true,
      imageBuffer: null,
      source: "disabled",
    };
  }

  if (!moderationConfigured) {
    stats.moderationErrors++;
    if (effectiveEnforcement) throw moderationUnavailableError();
    return {
      verdict: "error",
      allowed: true,
      imageBuffer: null,
      source: "unconfigured",
      moderationUnavailable: true,
    };
  }

  const directHit = loadModerationByUrl(imageUrl);
  if (directHit) {
    stats.moderationCacheHits++;
    if (directHit.verdict === "no_labels") stats.moderationNoLabels++;
    else stats.moderationLabelsDetected++;

    return applyModerationEnforcement(
      moderationDecision(directHit, null, "url-cache"),
      effectiveEnforcement
    );
  }

  let promise = moderationInflight.get(imageUrl);
  if (promise) {
    stats.moderationInflightDedupes++;
  } else {
    stats.moderationCacheMisses++;
    let imageBuffer = null;

    // The shared in-flight promise computes the real moderation decision only.
    // Each waiting request applies its own enforcement mode afterwards, so a
    // Studio-enforced request can safely share work with a live shadow request.
    promise = withModerationSlot(async () => {
      // Recheck after waiting for the queue.
      const lateHit = loadModerationByUrl(imageUrl);
      if (lateHit) {
        stats.moderationCacheHits++;
        return moderationDecision(lateHit, null, "url-cache");
      }

      beginOperation("moderationDownloads");
      try {
        imageBuffer = await downloadAllowedImage(imageUrl);
      } finally {
        endOperation("moderationDownloads");
      }

      const imageHash = crypto
        .createHash("sha256")
        .update(imageBuffer)
        .digest("hex");
      saveModerationUrl(imageUrl, imageHash);

      const hashHit = loadModerationByHash(imageHash);
      if (hashHit) {
        stats.moderationCacheHits++;
        return moderationDecision(hashHit, imageBuffer, "hash-cache");
      }

      const moderationJpeg = await createModerationJpeg(imageBuffer);
      const response = await sendRekognitionRequest(moderationJpeg);
      const labels = normaliseModerationLabels(response?.ModerationLabels);
      const contentTypes = normaliseContentTypes(response?.ContentTypes);
      const result = {
        imageHash,
        verdict: labels.length > 0 ? "labels_detected" : "no_labels",
        labels,
        moderationModelVersion: response?.ModerationModelVersion || null,
        contentTypes,
        checkedAt: Date.now(),
      };
      saveModerationResult(result);

      if (result.verdict === "no_labels") stats.moderationNoLabels++;
      else stats.moderationLabelsDetected++;

      return moderationDecision(result, imageBuffer, "aws");
    })
      .catch((error) => {
        stats.moderationErrors++;
        console.warn(
          "[moderation] check failed -",
          String(error?.name || error?.code || error?.message || "error")
        );
        return {
          verdict: "error",
          allowed: true,
          imageBuffer,
          source: "error-shadow",
          moderationUnavailable: true,
          moderationErrorCause: error,
        };
      })
      .finally(() => moderationInflight.delete(imageUrl));

    moderationInflight.set(imageUrl, promise);
  }

  const decision = await promise;
  return applyModerationEnforcement(decision, effectiveEnforcement);
}

function normaliseSearchText(value) {
  return String(value || "")
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  const safe = Number.isFinite(parsed) ? parsed : fallback;
  return Math.max(minimum, Math.min(maximum, safe));
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(value || "")
  );
}

function firstLocalisedValue(value) {
  if (!value || typeof value !== "object") return null;
  return value.en || value.ja || value[Object.keys(value)[0]] || null;
}

function localisedValues(value) {
  if (!value || typeof value !== "object") return [];
  return Array.from(new Set(
    Object.values(value)
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  ));
}

function mangaDexPrimaryTitles(manga) {
  return localisedValues(manga?.attributes?.title);
}

function mangaDexAltTitles(manga) {
  const titles = [];
  for (const item of manga?.attributes?.altTitles || []) {
    titles.push(...localisedValues(item));
  }
  return Array.from(new Set(titles));
}

function mangaDexTitles(manga) {
  return Array.from(new Set([
    ...mangaDexPrimaryTitles(manga),
    ...mangaDexAltTitles(manga),
  ]));
}

function mangaDexDisplayTitle(manga, fallback = null) {
  const attributes = manga?.attributes || {};
  return (
    attributes.title?.en ||
    attributes.title?.ja ||
    firstLocalisedValue(attributes.title) ||
    fallback ||
    null
  );
}

function mangaDexCandidateSummary(item) {
  return {
    mangaDexId: item?.id || null,
    title: mangaDexDisplayTitle(item),
    anilistId: item?.attributes?.links?.al || null,
    originalLanguage: item?.attributes?.originalLanguage || null,
    contentRating: item?.attributes?.contentRating || null,
  };
}

function catalogError(message, code, details = null) {
  const error = new Error(message);
  error.code = code;
  if (details) error.details = details;
  return error;
}

function pickMangaDexMatch(
  items,
  title,
  anilistId,
  {
    allowTitleFallback = true,
    noMatchMessage = "No exact safe MangaDex match found",
    noMatchReason = "no-exact-match",
    matchedBySuffix = "",
  } = {}
) {
  const candidates = Array.isArray(items) ? items.filter(Boolean) : [];
  const wantedId = anilistId == null ? null : String(anilistId).trim();
  const wantedTitle = normaliseSearchText(title);

  if (wantedId) {
    const linked = candidates.filter(
      (item) => String(item?.attributes?.links?.al || "").trim() === wantedId
    );

    if (linked.length === 1) {
      return {
        item: linked[0],
        matchedBy: `anilist-id${matchedBySuffix}`,
      };
    }

    if (linked.length > 1) {
      const exactLinkedPrimary = linked.filter((item) =>
        mangaDexPrimaryTitles(item).some(
          (candidate) => normaliseSearchText(candidate) === wantedTitle
        )
      );
      if (exactLinkedPrimary.length === 1) {
        return {
          item: exactLinkedPrimary[0],
          matchedBy: `anilist-id+exact-primary-title${matchedBySuffix}`,
        };
      }

      stats.mangaDexAmbiguousMatches++;
      throw catalogError("MangaDex match is ambiguous for this AniList manga", 409, {
        reason: "multiple-anilist-id-matches",
        candidates: linked.slice(0, 10).map(mangaDexCandidateSummary),
      });
    }

    // With an AniList ID, never silently select a similarly titled series.
    // The resolver may retry safe+suggestive, but that retry still requires
    // the exact MangaDex links.al value to match this ID.
    if (!allowTitleFallback) {
      throw catalogError(noMatchMessage, 404, {
        reason: noMatchReason,
        requestedAniListId: wantedId,
        candidates: candidates.slice(0, 5).map(mangaDexCandidateSummary),
      });
    }
  }

  if (wantedTitle && allowTitleFallback) {
    const exactPrimary = candidates.filter((item) =>
      mangaDexPrimaryTitles(item).some(
        (candidate) => normaliseSearchText(candidate) === wantedTitle
      )
    );

    if (exactPrimary.length === 1) {
      return {
        item: exactPrimary[0],
        matchedBy: wantedId ? "exact-primary-title-fallback" : "exact-primary-title",
      };
    }

    if (exactPrimary.length > 1) {
      stats.mangaDexAmbiguousMatches++;
      throw catalogError("Multiple MangaDex series have the same primary title", 409, {
        reason: "multiple-exact-primary-title-matches",
        candidates: exactPrimary.slice(0, 10).map(mangaDexCandidateSummary),
      });
    }

    const exactAlt = candidates.filter((item) =>
      mangaDexAltTitles(item).some(
        (candidate) => normaliseSearchText(candidate) === wantedTitle
      )
    );

    if (exactAlt.length > 0) {
      stats.mangaDexAmbiguousMatches++;
      throw catalogError(
        "Only alternate-title MangaDex matches were found; provide the AniList ID to select safely",
        409,
        {
          reason: "alternate-title-only",
          candidates: exactAlt.slice(0, 10).map(mangaDexCandidateSummary),
        }
      );
    }
  }

  throw catalogError(noMatchMessage, 404, {
    reason: noMatchReason,
    candidates: candidates.slice(0, 5).map(mangaDexCandidateSummary),
  });
}

function buildMangaDexCoverUrl(mangaId, fileName, variant = MANGADEX_COVER_VARIANT) {
  const safeFileName = encodeURIComponent(String(fileName || ""));
  const base = `${MANGADEX_COVER_BASE}/${mangaId}/${safeFileName}`;
  if (!variant || variant === "original") return base;
  return `${base}.${variant}.jpg`;
}

async function fetchJsonLimited(url, headers = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CATALOG_FETCH_TIMEOUT_MS);
  beginOperation("mangaDexRequests");
  stats.mangaDexRequests++;

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": MANGADEX_USER_AGENT,
        ...headers,
      },
    });

    if (!response.ok) {
      const error = new Error(`MangaDex request failed: ${response.status}`);
      error.code = response.status >= 500 ? 503 : 400;
      throw error;
    }

    const buffer = await readResponseBodyLimited(
      response,
      CATALOG_MAX_RESPONSE_BYTES,
      null
    );
    stats.mangaDexResponseBytes += buffer.length;
    return JSON.parse(buffer.toString("utf8"));
  } catch (error) {
    stats.mangaDexFailures++;
    if (error?.name === "AbortError") {
      const timeoutError = new Error("MangaDex request timed out");
      timeoutError.code = 503;
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timeout);
    endOperation("mangaDexRequests");
  }
}

async function cachedCatalogValue(key, loader) {
  const fresh = catalogCache.get(key, false);
  if (fresh) {
    stats.catalogCacheHits++;
    return { value: fresh.value, stale: false };
  }

  stats.catalogCacheMisses++;
  const existing = catalogInflight.get(key);
  if (existing) {
    stats.catalogInflightDedupes++;
    return existing;
  }

  const promise = (async () => {
    try {
      const value = await loader();
      catalogCache.set(key, value);
      return { value, stale: false };
    } catch (error) {
      const stale = catalogCache.get(key, true);
      if (stale) {
        stats.catalogStaleFallbacks++;
        return { value: stale.value, stale: true };
      }
      throw error;
    } finally {
      catalogInflight.delete(key);
    }
  })();

  catalogInflight.set(key, promise);
  return promise;
}

async function resolveMangaDexManga({ title, anilistId, mangaDexId }) {
  if (mangaDexId) {
    return {
      id: mangaDexId,
      title: title || null,
      originalLanguage: null,
      contentRating: null,
      matchedBy: "provided-id",
    };
  }

  const normalisedTitle = normaliseSearchText(title);
  const cacheKey = `mangadex:match:v2:${anilistId || "none"}:${normalisedTitle}`;
  const resolved = await cachedCatalogValue(cacheKey, async () => {
    const buildSearchUrl = (
      ratings,
      offset = 0,
      limit = MANGADEX_MATCH_LIMIT
    ) => {
      const url = new URL("/manga", MANGADEX_API_BASE);
      url.searchParams.set("title", title);
      url.searchParams.set("limit", String(limit));
      url.searchParams.set("offset", String(offset));
      for (const rating of ratings) {
        url.searchParams.append("contentRating[]", rating);
      }
      url.searchParams.set("order[relevance]", "desc");
      return url;
    };

    // Pass 1: safe-only. Title-only requests can match only an exact primary
    // title in this safe result set.
    const safePayload = await fetchJsonLimited(
      buildSearchUrl(["safe"]).toString()
    );

    let match;
    if (anilistId) {
      try {
        match = pickMangaDexMatch(
          safePayload?.data || [],
          title,
          anilistId,
          {
            allowTitleFallback: false,
            noMatchMessage: "No exact safe MangaDex AniList match found",
            noMatchReason: "no-safe-anilist-id-match",
          }
        );
      } catch (error) {
        if (error?.code !== 404 || error?.details?.reason !== "no-safe-anilist-id-match") {
          throw error;
        }

        // Pass 2: safe+suggestive, but only an exact links.al match is allowed.
        // A title match alone can never unlock a suggestive MangaDex entry.
        stats.mangaDexSuggestiveFallbackRequests++;
        const expandedPayload = await fetchJsonLimited(
          buildSearchUrl(["safe", "suggestive"]).toString()
        );
        let expandedItems = Array.isArray(expandedPayload?.data)
          ? expandedPayload.data
          : [];

        // MangaDex relevance ordering can place the exact series below the
        // first page. Scan at most 100 lightweight metadata records, in two
        // bounded requests, rather than increasing one response/body buffer.
        const alreadyHasExactId = expandedItems.some(
          (item) =>
            String(item?.attributes?.links?.al || "").trim() ===
            String(anilistId).trim()
        );
        const expandedTotal = Number(expandedPayload?.total || expandedItems.length);
        if (
          !alreadyHasExactId &&
          expandedItems.length < expandedTotal &&
          expandedItems.length < MANGADEX_MATCH_SCAN_MAX
        ) {
          const remaining = MANGADEX_MATCH_SCAN_MAX - expandedItems.length;
          const secondLimit = Math.min(MANGADEX_MATCH_LIMIT, remaining);
          if (secondLimit > 0) {
            const secondPayload = await fetchJsonLimited(
              buildSearchUrl(
                ["safe", "suggestive"],
                expandedItems.length,
                secondLimit
              ).toString()
            );
            expandedItems = expandedItems.concat(secondPayload?.data || []);
          }
        }

        match = pickMangaDexMatch(
          expandedItems,
          title,
          anilistId,
          {
            allowTitleFallback: false,
            noMatchMessage: "No exact MangaDex AniList match found",
            noMatchReason: "no-safe-or-suggestive-anilist-id-match",
            matchedBySuffix: "-suggestive-fallback",
          }
        );
        stats.mangaDexSuggestiveFallbackMatches++;
      }
    } else {
      match = pickMangaDexMatch(
        safePayload?.data || [],
        title,
        null,
        {
          allowTitleFallback: true,
          noMatchMessage: "No exact safe MangaDex title match found",
          noMatchReason: "no-exact-safe-title-match",
        }
      );
    }

    const selected = match.item;
    return {
      id: selected.id,
      title: mangaDexDisplayTitle(selected, title),
      originalLanguage: selected?.attributes?.originalLanguage || null,
      contentRating: selected?.attributes?.contentRating || null,
      matchedBy: match.matchedBy,
    };
  });

  return { ...resolved.value, stale: resolved.stale };
}

function normaliseVolumeLabel(value) {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text || text.toLowerCase() === "none") return null;
  const numeric = Number(text);
  if (Number.isFinite(numeric)) return String(numeric);
  return text;
}

function localePreferenceOrder(originalLanguage) {
  const values = [
    String(originalLanguage || "").toLowerCase(),
    ...MANGADEX_PREFERRED_COVER_LOCALES,
    "ja",
    "en",
  ].filter(Boolean);
  return Array.from(new Set(values));
}

function localeRank(locale, originalLanguage) {
  const value = String(locale || "").toLowerCase();
  const base = value.split("-")[0];
  const preferences = localePreferenceOrder(originalLanguage);
  for (let index = 0; index < preferences.length; index++) {
    const preferred = preferences[index];
    if (value === preferred) return index * 2;
    if (base && base === preferred.split("-")[0]) return index * 2 + 1;
  }
  return 10_000;
}

function compareCoverChoice(a, b, originalLanguage) {
  const localeDifference =
    localeRank(a.locale, originalLanguage) - localeRank(b.locale, originalLanguage);
  if (localeDifference !== 0) return localeDifference;

  const versionDifference = Number(b.version || 0) - Number(a.version || 0);
  if (versionDifference !== 0) return versionDifference;

  const updatedDifference =
    Date.parse(b.updatedAt || 0) - Date.parse(a.updatedAt || 0);
  if (Number.isFinite(updatedDifference) && updatedDifference !== 0) {
    return updatedDifference;
  }

  return String(a.id || "").localeCompare(String(b.id || ""));
}

function compareVolumeLabels(a, b) {
  const aNumber = Number(a.volume);
  const bNumber = Number(b.volume);
  const aNumeric = a.volume != null && Number.isFinite(aNumber);
  const bNumeric = b.volume != null && Number.isFinite(bNumber);

  if (aNumeric && bNumeric && aNumber !== bNumber) return aNumber - bNumber;
  if (aNumeric !== bNumeric) return aNumeric ? -1 : 1;
  if (a.volume == null && b.volume != null) return 1;
  if (a.volume != null && b.volume == null) return -1;
  return String(a.volume || "").localeCompare(String(b.volume || ""), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

function deduplicateMangaDexCovers(rawCovers, mangaId, originalLanguage) {
  const groups = new Map();
  for (const cover of rawCovers) {
    const volume = normaliseVolumeLabel(cover.volume);
    const key = volume == null ? "__unnumbered__" : `volume:${volume}`;
    const current = groups.get(key) || [];
    current.push({ ...cover, volume });
    groups.set(key, current);
  }

  const covers = [];
  for (const variants of groups.values()) {
    variants.sort((a, b) => compareCoverChoice(a, b, originalLanguage));
    const chosen = variants[0];
    covers.push({
      id: chosen.id,
      source: "mangadex",
      type: "manga-volume",
      volume: chosen.volume,
      locale: chosen.locale || null,
      imageUrl: buildMangaDexCoverUrl(mangaId, chosen.fileName),
      originalImageUrl: buildMangaDexCoverUrl(mangaId, chosen.fileName, "original"),
      availableVariantCount: variants.length,
      availableLocales: Array.from(new Set(
        variants.map((item) => item.locale).filter(Boolean)
      )),
    });
  }

  covers.sort(compareVolumeLabels);
  return covers;
}

async function fetchMangaDexCoverCatalog({ mangaId, originalLanguage }) {
  const localeKey = localePreferenceOrder(originalLanguage).join(",");
  const cacheKey = `mangadex:covers:v2:${mangaId}:${localeKey}`;

  return cachedCatalogValue(cacheKey, async () => {
    const rawCovers = [];
    let offset = 0;
    let reportedTotal = null;
    let truncated = false;

    while (offset < MANGADEX_COVER_MAX_RECORDS) {
      const limit = Math.min(
        MANGADEX_COVER_FETCH_LIMIT,
        MANGADEX_COVER_MAX_RECORDS - offset
      );
      const url = new URL("/cover", MANGADEX_API_BASE);
      url.searchParams.append("manga[]", mangaId);
      url.searchParams.set("limit", String(limit));
      url.searchParams.set("offset", String(offset));
      url.searchParams.set("order[volume]", "asc");

      const payload = await fetchJsonLimited(url.toString());
      const data = Array.isArray(payload?.data) ? payload.data : [];
      reportedTotal = Number(payload?.total ?? data.length);

      for (const item of data) {
        const attributes = item?.attributes || {};
        const fileName = String(attributes.fileName || "").trim();
        if (!fileName || !item?.id) continue;
        rawCovers.push({
          id: item.id,
          volume: attributes.volume == null ? null : String(attributes.volume),
          locale: attributes.locale || null,
          fileName,
          version: Number(attributes.version || 0),
          createdAt: attributes.createdAt || null,
          updatedAt: attributes.updatedAt || null,
        });
      }

      stats.mangaDexCoverRecordsFetched += data.length;
      offset += data.length;
      if (data.length === 0 || data.length < limit || offset >= reportedTotal) break;
    }

    if (reportedTotal != null && offset < reportedTotal) {
      truncated = true;
      stats.mangaDexCoverCatalogTruncated++;
    }

    const covers = deduplicateMangaDexCovers(
      rawCovers,
      mangaId,
      originalLanguage
    );
    const duplicatesRemoved = Math.max(0, rawCovers.length - covers.length);
    stats.mangaDexCoverDuplicatesRemoved += duplicatesRemoved;

    return {
      covers,
      total: covers.length,
      rawTotal: reportedTotal ?? rawCovers.length,
      fetchedRaw: rawCovers.length,
      duplicatesRemoved,
      truncated,
    };
  });
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
    const rawRgb = withSyncOperation("zstdDecompressions", () =>
      Buffer.from(zstdApi.decompress(entry.pixelsZstd))
    );
    validateRawLength(entry, rawRgb);
    return rawRgb;
  }

  const error = new Error("Pixel cache entry has no decodable payload");
  error.code = 500;
  throw error;
}

function materializeV1Pixels(entry) {
  return withSyncOperation("v1PixelArrays", () => {
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
  });
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
  const compressed = withSyncOperation("zstdCompressions", () =>
    zstdApi.compress(rawRgb, ZSTD_LEVEL)
  );
  entry.pixelsZstd = Buffer.from(compressed);

  saveDiskZstd(key, entry.pixelsZstd);

  if (db) entry.pixelsGz = null;
  entry.pixels = null;
  entry.rawRgb = null;

  stats.zstdPayloadCompressions++;
  return entry.pixelsZstd;
}

function sendV1Response(res, entry) {
  return withSyncOperation("v1Responses", () => {
    stats.formatV1Responses++;
    return res.json({
      width: entry.width,
      height: entry.height,
      pixels: materializeV1Pixels(entry),
    });
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
    message: "Paint Any Anime backend is running",
    apiVersion: API_VERSION,
    pixelFormats: {
      default: 1,
      supported: hasZstd() ? [1, 2] : [1],
      format2Available: hasZstd(),
    },
    catalog: {
      mangaDexEnabled: ENABLE_MANGADEX,
      endpoint: "/catalog/manga-covers",
    },
    moderation: moderationHealthSnapshot(),
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    format2Available: hasZstd(),
    mangaDexEnabled: ENABLE_MANGADEX,
    moderation: moderationHealthSnapshot(),
  });
});

app.get("/stats", (req, res) => {
  const diskRow = db ? countSql.get() : { n: 0 };
  const moderationRow = db && countModerationResultsSql
    ? countModerationResultsSql.get()
    : { n: 0 };
  const policyDecisionRows = db && countModerationPolicySql
    ? countModerationPolicySql.all(MODERATION_POLICY_VERSION)
    : [];
  const policyCachedDecisions = {
    allow: 0,
    block: 0,
    uncertain: 0,
  };
  for (const row of policyDecisionRows) {
    if (row.policyDecision in policyCachedDecisions) {
      policyCachedDecisions[row.policyDecision] = Number(row.n || 0);
    }
  }
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
      catalogPerMinute: CATALOG_RATE_LIMIT_PER_MIN,
      catalogBucketsTracked: catalogBuckets.size,
    },
    sharp: {
      cache: sharp.cache(),
      concurrency: sharp.concurrency(),
      counters: sharp.counters(),
      maxInputPixels: MAX_INPUT_PIXELS,
    },
    moderation: {
      ...moderationHealthSnapshot(),
      region: AWS_REGION,
      minConfidence: AWS_REKOGNITION_MIN_CONFIDENCE,
      maxConcurrency: AWS_REKOGNITION_MAX_CONCURRENCY,
      maxQueue: AWS_REKOGNITION_MAX_QUEUE,
      running: moderationRunning,
      queued: moderationQueued,
      inflight: moderationInflight.size,
      cachedImages: Number(moderationRow.n || 0),
      policyCachedDecisions,
      lastSuccessAt: moderationLastSuccessAt
        ? new Date(moderationLastSuccessAt).toISOString()
        : null,
      lastErrorAt: moderationLastErrorAt
        ? new Date(moderationLastErrorAt).toISOString()
        : null,
      lastErrorCode: moderationLastErrorCode,
    },
    catalog: {
      mangaDexEnabled: ENABLE_MANGADEX,
      cacheItems: catalogCache.size(),
      cacheBytes: catalogCache.bytes,
      cacheLimits: {
        items: CATALOG_CACHE_MAX_ITEMS,
        bytes: CATALOG_CACHE_MAX_BYTES,
        ttlMs: CATALOG_CACHE_TTL_MS,
        staleMs: CATALOG_CACHE_STALE_MS,
      },
      inFlight: catalogInflight.size,
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
    memoryTelemetry: memoryTelemetrySnapshot(),
    garbageCollection: garbageCollectionSnapshot(),
    operations: {
      active: operationSnapshot(),
      lifetimePeak: {
        ...operationTelemetry.lifetimePeak,
        activePixelizeResponses: maxActivePixelizeResponses,
        activeV2Streams: maxActiveV2Streams,
      },
    },
    counters: { ...stats },
  });
});

app.get("/catalog/manga-covers", async (req, res) => {
  const ip = req.ip || req.connection?.remoteAddress || "unknown";
  if (!catalogRateLimit(ip)) {
    stats.catalogRateLimited++;
    return res.status(429).json({ error: "catalog rate limited" });
  }

  stats.catalogRequests++;
  beginOperation("catalogRequests");

  try {
    if (!ENABLE_MANGADEX) {
      return res.status(503).json({
        error: "MangaDex integration is disabled",
        reason:
          "Enable only after confirming compliance with MangaDex acceptable-use terms.",
      });
    }

    const title = String(req.query.title || "").trim().slice(0, 160);
    const anilistId = String(req.query.anilistId || "").trim() || null;
    const mangaDexId = String(req.query.mangaDexId || "").trim() || null;
    const mainCoverUrl = String(req.query.mainCoverUrl || "").trim() || null;
    const mainTitle = String(req.query.mainTitle || title || "Manga").trim().slice(0, 160);
    const page = boundedInteger(req.query.page, 1, 1, 10_000);
    const perPage = boundedInteger(req.query.perPage, 4, 1, 12);
    const includeMain = String(req.query.includeMain || "true") !== "false";
    const hasMain = includeMain && Boolean(mainCoverUrl);

    if (!mangaDexId && !title) {
      return res.status(400).json({ error: "title or mangaDexId is required" });
    }
    if (mangaDexId && !isUuid(mangaDexId)) {
      return res.status(400).json({ error: "mangaDexId must be a UUID" });
    }
    if (anilistId && !/^\d+$/.test(anilistId)) {
      return res.status(400).json({ error: "anilistId must be numeric" });
    }
    if (mainCoverUrl && !isAllowedImageUrl(mainCoverUrl)) {
      return res.status(400).json({ error: "mainCoverUrl host is not allowed" });
    }

    const manga = await resolveMangaDexManga({ title, anilistId, mangaDexId });
    const mainSlots = hasMain ? 1 : 0;
    const firstCombinedIndex = (page - 1) * perPage;
    const items = [];

    if (hasMain && firstCombinedIndex === 0 && mainCoverUrl) {
      items.push({
        id: anilistId ? `anilist:${anilistId}` : `anilist:${normaliseSearchText(mainTitle)}`,
        source: "anilist",
        type: "manga-main",
        title: `${mainTitle} — Main Cover`,
        volume: null,
        imageUrl: mainCoverUrl,
      });
    }

    const coverOffset = Math.max(0, firstCombinedIndex - mainSlots);
    const coverLimit = Math.max(0, perPage - items.length);
    const coverCatalog = await fetchMangaDexCoverCatalog({
      mangaId: manga.id,
      originalLanguage: manga.originalLanguage,
    });
    if (coverLimit > 0) {
      const visibleCovers = coverCatalog.value.covers.slice(
        coverOffset,
        coverOffset + coverLimit
      );
      for (const cover of visibleCovers) {
        const label = cover.volume ? `Vol. ${cover.volume}` : "Cover";
        items.push({
          ...cover,
          title: `${mainTitle || manga.title || "Manga"} — ${label}`,
        });
      }
    }

    const total = coverCatalog.value.total + (hasMain ? 1 : 0);
    const totalPages = Math.max(1, Math.ceil(total / perPage));

    return res.json({
      mode: "manga",
      page,
      perPage,
      total,
      totalPages,
      hasNextPage: page < totalPages,
      hasMain,
      items,
      manga: {
        anilistId,
        mangaDexId: manga.id,
        title: manga.title || mainTitle,
        originalLanguage: manga.originalLanguage || null,
        matchedBy: manga.matchedBy,
        contentRating: manga.contentRating || null,
      },
      coverCatalog: {
        rawTotal: coverCatalog.value.rawTotal,
        fetchedRaw: coverCatalog.value.fetchedRaw,
        deduplicatedTotal: coverCatalog.value.total,
        duplicatesRemoved: coverCatalog.value.duplicatesRemoved,
        truncated: coverCatalog.value.truncated,
      },
      stale: Boolean(manga.stale || coverCatalog.stale),
      attribution: {
        mangaDex: "MangaDex",
        anilist: mainCoverUrl ? "AniList" : null,
      },
    });
  } catch (error) {
    stats.catalogFailures++;
    const status = Number(error?.code || 500);
    if (status >= 400 && status < 600) {
      const payload = { error: String(error.message || error) };
      if (error?.details) payload.details = error.details;
      return res.status(status).json(payload);
    }
    console.error("[catalog/manga-covers]", error);
    return res.status(500).json({ error: "catalog request failed" });
  } finally {
    endOperation("catalogRequests");
  }
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

    // Moderation is intentionally checked before every pixel cache layer.
    // This prevents artwork cached before moderation was introduced from
    // bypassing enforcement. A first-time check returns the downloaded source
    // buffer so the generation path can reuse it without a second download.
    // The effective mode is request-scoped: global enforcement applies to all
    // callers, while a valid secret header can enforce only a Studio request.
    const effectiveModerationEnforcement =
      effectiveModerationEnforcementForRequest(req);
    const moderationRequired = shouldModerateRequest(req);
    let moderationResult;
    if (moderationRequired) {
      moderationResult = await ensureImageModerated(
        imageUrl,
        effectiveModerationEnforcement
      );
    } else {
      stats.moderationSkippedPublicShadow++;
      moderationResult = {
        verdict: "skipped",
        allowed: true,
        imageBuffer: null,
        source: "public-shadow-disabled",
      };
    }
    let moderatedImageBuffer = moderationResult.imageBuffer || null;

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

        const generationOperation = `${selectedMem.kind}Generations`;
        beginOperation(generationOperation);
        try {
          let imageBuffer = moderatedImageBuffer;
          moderatedImageBuffer = null;
          if (!imageBuffer) {
            imageBuffer = await downloadAllowedImage(imageUrl);
          }

          let raw;
          beginOperation("sharpJobs");
          try {
            raw = await sharp(imageBuffer, {
              limitInputPixels: MAX_INPUT_PIXELS,
              limitInputChannels: 4,
              sequentialRead: true,
              pages: 1,
            })
              .resize(w, h, {
                fit: resizeFit,
                position: "center",
                background: { r: 255, g: 255, b: 255, alpha: 1 },
              })
              .flatten({ background: { r: 255, g: 255, b: 255 } })
              .removeAlpha()
              .raw()
              .toBuffer({ resolveWithObject: true });
          } finally {
            endOperation("sharpJobs");
          }
          imageBuffer = null;

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
              withSyncOperation("zstdCompressions", () =>
                zstdApi.compress(rawRgb, ZSTD_LEVEL)
              )
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

          // Never retain uncompressed RGB in the long-lived LRU. For v2 with
          // SQLite, keep only Zstd; for v1 keep only the compact gzip payload.
          generatedEntry.rawRgb = null;
          generatedEntry.pixels = null;
          if (formatVersion === 2 && db) {
            generatedEntry.pixelsGz = null;
          }

          memCache.set(key, generatedEntry, cacheTtl);
          stats.totalPixelsProcessed +=
            generatedEntry.width * generatedEntry.height;

          return { entry: generatedEntry, source: "generated" };
        } finally {
          endOperation(generationOperation);
        }
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

    if (error && error.code === 403 && error.moderationBlocked) {
      return res.status(403).json({
        error:
          error.policyDecision === "uncertain"
            ? "image unavailable pending moderation review"
            : "image blocked by moderation policy",
      });
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
    console.log(`Pixelize backend v${API_VERSION} on :${PORT}`);
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
      `  rateLimit=${RATE_LIMIT_PER_MIN}/min catalogRate=${CATALOG_RATE_LIMIT_PER_MIN}/min ` +
        `MAX_DIM=${MAX_DIM} MAX_INPUT_PIXELS=${MAX_INPUT_PIXELS}`
    );
    console.log(
      `  sharp cache=${SHARP_CACHE_MEMORY_MB}MB/${SHARP_CACHE_ITEMS} items ` +
        `concurrency=${SHARP_CONCURRENCY}`
    );
    console.log(
      `  MangaDex=${ENABLE_MANGADEX ? "enabled" : "disabled"} ` +
        `catalogCache=${CATALOG_CACHE_MAX_ITEMS} items/${Math.round(CATALOG_CACHE_MAX_BYTES / 1024 / 1024)}MB`
    );
    console.log(
      `  Rekognition enabled=${AWS_REKOGNITION_ENABLED} ` +
        `enforcing=${AWS_REKOGNITION_ENFORCE} ` +
        `publicShadowScan=${AWS_REKOGNITION_PUBLIC_SHADOW_SCAN} ` +
        `configured=${moderationConfigured} ` +
        `region=${AWS_REGION} concurrency=${AWS_REKOGNITION_MAX_CONCURRENCY} ` +
        `policy=${MODERATION_POLICY_VERSION}`
    );
    console.log(
      `  memoryTelemetry rss=${MEMORY_RSS_SAMPLE_MS}ms ` +
        `full=${MEMORY_FULL_SAMPLE_MS}ms ` +
        `spikeHeap=${MEMORY_HEAP_SPIKE_MB}MB ` +
        `spikeRss=${MEMORY_RSS_SPIKE_MB}MB`
    );

    setImmediate(() => {
      runModerationPolicyBackfill().catch((error) => {
        stats.moderationPolicyBackfillFailures++;
        console.warn(
          "[moderation] policy backfill startup failure -",
          error.message
        );
      });
    });
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
      if (rekognitionClient && typeof rekognitionClient.destroy === "function") {
        rekognitionClient.destroy();
      }
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
