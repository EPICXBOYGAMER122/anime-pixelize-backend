const express = require("express");
const sharp = require("sharp");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3000;

// Only allow anime image hosts you expect.
// AniList cover URLs usually use s4.anilist.co.
// Jikan/MAL images usually use cdn.myanimelist.net.
const ALLOWED_HOSTS = new Set([
  "s4.anilist.co",
  "cdn.myanimelist.net"
]);

// Simple memory cache so the same cover does not get pixelized repeatedly.
const cache = new Map();
const MAX_CACHE_ITEMS = 100;

function isAllowedImageUrl(imageUrl) {
  try {
    const url = new URL(imageUrl);
    return url.protocol === "https:" && ALLOWED_HOSTS.has(url.hostname);
  } catch {
    return false;
  }
}

function setCache(key, value) {
  if (cache.size >= MAX_CACHE_ITEMS) {
    const firstKey = cache.keys().next().value;
    cache.delete(firstKey);
  }
  cache.set(key, value);
}

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    message: "Anime pixelize backend is running"
  });
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.post("/pixelize", async (req, res) => {
  try {
    const { imageUrl, width, height, fit } = req.body;

    if (!imageUrl || typeof imageUrl !== "string") {
      return res.status(400).json({ error: "Missing imageUrl" });
    }

    if (!isAllowedImageUrl(imageUrl)) {
      return res.status(400).json({
        error: "Image URL host is not allowed",
        allowedHosts: Array.from(ALLOWED_HOSTS)
      });
    }

    const w = Math.max(8, Math.min(256, Number(width) || 120));
    const h = Math.max(8, Math.min(256, Number(height) || 120));

    // "cover" fills the whole square but can crop the anime poster.
    // "contain" shows the full poster but can create side borders.
    const resizeFit = fit === "contain" ? "contain" : "cover";

    const cacheKey = `${imageUrl}|${w}|${h}|${resizeFit}`;
    if (cache.has(cacheKey)) {
      return res.json(cache.get(cacheKey));
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    const imageResponse = await fetch(imageUrl, {
      signal: controller.signal,
      headers: {
        "User-Agent": "AnimePixelizeBackend/1.0"
      }
    });

    clearTimeout(timeout);

    if (!imageResponse.ok) {
      return res.status(400).json({
        error: `Failed to download image: ${imageResponse.status}`
      });
    }

    const contentType = imageResponse.headers.get("content-type") || "";
    if (!contentType.startsWith("image/")) {
      return res.status(400).json({
        error: "URL did not return an image"
      });
    }

    const contentLength = Number(imageResponse.headers.get("content-length") || 0);
    if (contentLength > 5 * 1024 * 1024) {
      return res.status(400).json({
        error: "Image too large"
      });
    }

    const buffer = Buffer.from(await imageResponse.arrayBuffer());

    const raw = await sharp(buffer)
      .resize(w, h, {
        fit: resizeFit,
        position: "center",
        background: { r: 255, g: 255, b: 255, alpha: 1 }
      })
      .flatten({ background: { r: 255, g: 255, b: 255 } })
      .raw()
      .toBuffer({ resolveWithObject: true });

    const { data, info } = raw;
    const pixels = [];

    for (let i = 0; i < info.width * info.height; i++) {
      const offset = i * info.channels;
      pixels.push([
        data[offset],
        data[offset + 1],
        data[offset + 2]
      ]);
    }

    const result = {
      width: info.width,
      height: info.height,
      pixels
    };

    setCache(cacheKey, result);
    res.json(result);

  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: String(error.message || error)
    });
  }
});

app.listen(PORT, () => {
  console.log(`Pixelize backend running on port ${PORT}`);
});