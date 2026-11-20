const express = require("express");
const path = require("path");
const axios = require("axios");
const cheerio = require("cheerio");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3000;
const ACCESS_KEY = process.env.ACCESS_KEY || ""; // optional: protects /api/chat
const MODELS = (process.env.MODELS || "deepseek/deepseek-v4-flash").split(",").map((m) => m.trim());
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT || "30", 10); // requests per minute per IP

const UA = "Mozilla/5.0 (Linux; Android 15; Pixel 9) Chrome/150";
const SITE = "https://deep-seek.ai";

// ---------- scraper: session (cookies + csrf), cached for 5 minutes ----------
let session = null;

async function getSession(force = false) {
  if (!force && session && Date.now() - session.time < 5 * 60 * 1000) return session;

  const html = await axios.get(`${SITE}/chat`, { headers: { "User-Agent": UA }, timeout: 30000 });

  const cookies = {};
  (html.headers["set-cookie"] || []).forEach((c) => {
    const [k, v] = c.split(";")[0].split("=");
    if (k && v) cookies[k.trim()] = v.trim();
  });

  const $ = cheerio.load(html.data);
  let csrf = $('meta[name="csrf-token"]').attr("content") || cookies["XSRF-TOKEN"];
  if (!csrf) {
    const script = $("script").toArray().find((s) => ($(s).html() || "").includes("csrfToken"));
    csrf = script ? $(script).html().match(/csrfToken["']?\s*[:=]\s*["']([^"']+)["']/)?.[1] : null;
  }
  if (!csrf) throw new Error("No CSRF token");

  const cookieStr = Object.entries(cookies).map(([k, v]) => `${k}=${v}`).join("; ");
  session = { csrf, cookieStr, time: Date.now() };
  return session;
}

// sends messages to deep-seek.ai, calls onDelta(text) for every piece, resolves with full answer
async function askDeepSeek(messages, model, onDelta, signal, retry = true) {
  const s = await getSession(!retry);

  let res;
  try {
    res = await axios({
      method: "POST",
      url: `${SITE}/api/chat`,
      headers: {
        "User-Agent": UA,
        "Content-Type": "application/json",
        Cookie: s.cookieStr,
        "X-CSRF-Token": s.csrf,
        Origin: SITE,
        Referer: `${SITE}/chat`,
      },
      data: { model, messages },
      responseType: "stream",
      timeout: 120000,
      signal,
    });
  } catch (err) {
    // session probably expired -> refresh once and retry
    if (retry && [401, 403, 419].includes(err.response?.status)) {
      session = null;
      return askDeepSeek(messages, model, onDelta, signal, false);
    }
    throw err;
  }

  let answer = "", buffer = "";
  return new Promise((resolve, reject) => {
    res.data.on("data", (chunk) => {
      buffer += chunk.toString();
      const parts = buffer.split("\n\n");
      buffer = parts.pop() || "";
      for (const p of parts) {
        for (const line of p.split("\n")) {
          if (!line.startsWith("data: ")) continue;
          const json = line.slice(6).trim();
          if (json === "[DONE]") continue;
          try {
            const d = JSON.parse(json);
            const text = d.choices?.[0]?.delta?.content;
            if (text) {
              answer += text;
              if (onDelta) onDelta(text);
            }
          } catch (_) {}
        }
      }
    });
    res.data.on("end", () => resolve(answer));
    res.data.on("error", reject);
  });
}

// ---------- tiny in-memory rate limiter ----------
const hits = new Map();
function rateLimit(req, res, next) {
  const ip = req.headers["x-forwarded-for"]?.split(",")[0].trim() || req.ip;
  const now = Date.now();
  const list = (hits.get(ip) || []).filter((t) => now - t < 60000);
  if (list.length >= RATE_LIMIT) {
    return res.status(429).json({ success: false, error: "Too many requests. Try again in a minute." });
  }
  list.push(now);
  hits.set(ip, list);
  next();
}
setInterval(() => {
  const now = Date.now();
  for (const [ip, list] of hits) {
    const fresh = list.filter((t) => now - t < 60000);
    fresh.length ? hits.set(ip, fresh) : hits.delete(ip);
  }
}, 60000).unref();

// ---------- access key check ----------
function auth(req, res, next) {
  if (!ACCESS_KEY) return next();
  const header = req.headers["authorization"] || "";
  const key = req.headers["x-api-key"] || (header.startsWith("Bearer ") ? header.slice(7) : "");
  if (key !== ACCESS_KEY) {
    return res.status(401).json({ success: false, error: "Invalid or missing API key." });
  }
  next();
}

// ---------- routes ----------
app.get("/", (_req, res) => res.sendFile(path.join(__dirname, "index.html")));

app.get("/api/health", (_req, res) => res.json({ status: "ok", time: new Date().toISOString() }));

app.get("/api/config", (_req, res) =>
  res.json({ models: MODELS, authRequired: Boolean(ACCESS_KEY), configured: true })
);

app.post("/api/chat", rateLimit, auth, async (req, res) => {
  const { message, messages, system, model, stream } = req.body || {};

  let msgs = [];
  if (system) msgs.push({ role: "system", content: String(system) });
  if (Array.isArray(messages) && messages.length) msgs = msgs.concat(messages);
  else if (message) msgs.push({ role: "user", content: String(message) });

  if (!msgs.some((m) => m.role === "user")) {
    return res.status(400).json({ success: false, error: "Provide `message` or `messages`." });
  }

  const chosenModel = MODELS.includes(model) ? model : MODELS[0];
  const controller = new AbortController();
  res.on("close", () => controller.abort());
  const started = Date.now();

  try {
    // streaming: re-emit pieces as OpenAI-style SSE chunks
    if (stream) {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      await askDeepSeek(
        msgs,
        chosenModel,
        (text) => res.write(`data: ${JSON.stringify({ model: chosenModel, choices: [{ delta: { content: text } }] })}\n\n`),
        controller.signal
      );
      res.write("data: [DONE]\n\n");
      return res.end();
    }

    // normal JSON response
    const reply = await askDeepSeek(msgs, chosenModel, null, controller.signal);
    res.json({
      success: true,
      model: chosenModel,
      reply,
      latency_ms: Date.now() - started,
      raw: { model: chosenModel, messages: msgs, reply },
    });
  } catch (err) {
    if (controller.signal.aborted) return;
    console.error(err.message);
    if (!res.headersSent) res.status(502).json({ success: false, error: "Scrape failed: " + err.message });
    else res.end();
  }
});

app.use((_req, res) => res.status(404).json({ success: false, error: "Not found" }));

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
