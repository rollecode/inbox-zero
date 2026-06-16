// srh-shim.mjs - minimal Upstash-Redis-HTTP shim over local Redis (zero deps).
// Replaces the hiett/serverless-redis-http Docker container.
// Speaks the Upstash REST protocol (POST / , /pipeline , /multi-exec) and
// proxies to a plain Redis on TCP via a tiny RESP2 client.
import net from "node:net";
import http from "node:http";

const REDIS_HOST = process.env.SRH_REDIS_HOST || "127.0.0.1";
const REDIS_PORT = Number(process.env.SRH_REDIS_PORT || 6379);
const PORT = Number(process.env.SRH_PORT || 8079);
const TOKEN = process.env.SRH_TOKEN || "dev_token";

let sock = null;
let buf = Buffer.alloc(0);
const queue = [];

function connect() {
  sock = net.connect(REDIS_PORT, REDIS_HOST);
  sock.setNoDelay(true);
  sock.on("connect", () => console.log(`[srh] connected to redis ${REDIS_HOST}:${REDIS_PORT}`));
  sock.on("data", (chunk) => { buf = Buffer.concat([buf, chunk]); drain(); });
  sock.on("error", (err) => console.error("[srh] redis socket error:", err.message));
  sock.on("close", () => {
    while (queue.length) queue.shift().reject(new Error("redis connection closed"));
    buf = Buffer.alloc(0);
    setTimeout(connect, 500);
  });
}

function drain() {
  while (queue.length) {
    const parsed = parseReply(buf, 0);
    if (!parsed) break;
    const [value, consumed] = parsed;
    buf = buf.subarray(consumed);
    queue.shift().resolve(value);
  }
}

function parseReply(b, off) {
  if (off >= b.length) return null;
  const type = String.fromCharCode(b[off]);
  const lineEnd = findCRLF(b, off + 1);
  if (lineEnd === -1) return null;
  const line = b.toString("utf8", off + 1, lineEnd);
  const next = lineEnd + 2;
  switch (type) {
    case "+": return [line, next];
    case "-": return [{ __err: line }, next];
    case ":": return [Number(line), next];
    case "$": {
      const len = Number(line);
      if (len === -1) return [null, next];
      const end = next + len;
      if (end + 2 > b.length) return null;
      return [b.toString("utf8", next, end), end + 2];
    }
    case "*": {
      const count = Number(line);
      if (count === -1) return [null, next];
      const arr = [];
      let cur = next;
      for (let i = 0; i < count; i++) {
        const r = parseReply(b, cur);
        if (!r) return null;
        arr.push(r[0]);
        cur = r[1];
      }
      return [arr, cur];
    }
    default: return [line, next];
  }
}

function findCRLF(b, from) {
  for (let i = from; i < b.length - 1; i++) {
    if (b[i] === 13 && b[i + 1] === 10) return i;
  }
  return -1;
}

function sendCommand(args) {
  return new Promise((resolve, reject) => {
    if (!sock || sock.destroyed) return reject(new Error("redis not connected"));
    const parts = [Buffer.from(`*${args.length}\r\n`)];
    for (const a of args) {
      const s = a === null || a === undefined ? "" : String(a);
      parts.push(Buffer.from(`$${Buffer.byteLength(s)}\r\n`), Buffer.from(s), Buffer.from("\r\n"));
    }
    queue.push({ resolve, reject });
    sock.write(Buffer.concat(parts));
  });
}

function encodeResult(value, base64) {
  if (value === null) return null;
  if (typeof value === "number") return value;
  if (typeof value === "string") return base64 ? Buffer.from(value, "utf8").toString("base64") : value;
  if (Array.isArray(value)) return value.map((v) => encodeResult(v, base64));
  return value;
}

async function runOne(cmd, base64) {
  if (!Array.isArray(cmd) || cmd.length === 0) return { error: "empty command" };
  const reply = await sendCommand(cmd);
  if (reply && typeof reply === "object" && "__err" in reply) return { error: reply.__err };
  return { result: encodeResult(reply, base64) };
}

const server = http.createServer((req, res) => {
  const enc = (req.headers["upstash-encoding"] || "").toString().toLowerCase();
  const base64 = enc === "base64";
  const token = (req.headers["authorization"] || "").toString().replace(/^Bearer\s+/i, "");
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", async () => {
    if (TOKEN && token !== TOKEN) {
      res.writeHead(401, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "Unauthorized" }));
    }
    const path = req.url.split("?")[0];
    let parsed = null;
    try { parsed = body ? JSON.parse(body) : null; } catch { parsed = null; }
    try {
      if (path === "/pipeline" || path === "/multi-exec") {
        const commands = Array.isArray(parsed) ? parsed : [];
        const results = [];
        for (const cmd of commands) results.push(await runOne(cmd, base64));
        res.writeHead(200, { "content-type": "application/json" });
        return res.end(JSON.stringify(results));
      }
      let cmd;
      if (Array.isArray(parsed)) {
        cmd = parsed;
      } else {
        cmd = path.split("/").filter(Boolean).map(decodeURIComponent);
        if (parsed !== null) cmd.push(typeof parsed === "string" ? parsed : JSON.stringify(parsed));
      }
      const out = await runOne(cmd, base64);
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify(out));
    } catch (e) {
      res.writeHead(200, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: String((e && e.message) || e) }));
    }
  });
});

connect();
server.listen(PORT, "127.0.0.1", () => console.log(`[srh] Upstash-HTTP shim listening on http://127.0.0.1:${PORT}`));
