// Annotation stub (section 5). Single process, single server worker: counters and
// the failure schedule are per-process state.
import http from "node:http";
import { createHash } from "node:crypto";

type Config = {
  latency_mode: "fixed" | "jitter";
  latency_ms: number;
  latency_jitter_min_ms: number;
  latency_jitter_max_ms: number;
  latency_seed: number;
  failure_every_n: number;
  failure_status: number;
  in_flight_capacity: number;
};

const DEFAULTS: Config = {
  latency_mode: "fixed",
  latency_ms: 150,
  latency_jitter_min_ms: 50,
  latency_jitter_max_ms: 250,
  latency_seed: 20260803,
  failure_every_n: 7,
  failure_status: 500,
  in_flight_capacity: 2,
};

function fromEnv(): Config {
  const c = { ...DEFAULTS };
  for (const k of Object.keys(DEFAULTS) as (keyof Config)[]) {
    const v = process.env[`STUB_${k.toUpperCase()}`];
    if (v === undefined) continue;
    (c as Record<string, unknown>)[k] = k === "latency_mode" ? v : Number(v);
  }
  return c;
}

export function createStub(config: Config = fromEnv()) {
  let cfg = { ...config };
  let billed = 0,
    serverErrors = 0,
    overCapacity = 0,
    inFlight = 0,
    maxInFlight = 0,
    latencyStep = 0;

  const latency = (): number => {
    if (cfg.latency_mode !== "jitter") return cfg.latency_ms;
    // deterministic jitter sequence: replays identically after /v1/reset
    const h = createHash("sha256").update(`${cfg.latency_seed}:${latencyStep++}`).digest();
    const span = Math.max(0, cfg.latency_jitter_max_ms - cfg.latency_jitter_min_ms);
    return cfg.latency_jitter_min_ms + (h.readUInt32BE(0) % (span + 1));
  };

  const stats = () => ({
    billed_calls: billed,
    current_in_flight: inFlight,
    max_in_flight: maxInFlight,
    server_error_calls: serverErrors,
    over_capacity_calls: overCapacity,
    config: cfg,
  });

  const send = (res: http.ServerResponse, status: number, body: unknown) => {
    const b = Buffer.from(JSON.stringify(body));
    res.writeHead(status, { "content-type": "application/json", "content-length": b.length });
    res.end(b);
  };

  const readBody = (req: http.IncomingMessage) =>
    new Promise<string>((resolve, reject) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      req.on("error", reject);
    });

  const server = http.createServer(async (req, res) => {
    const url = (req.url ?? "/").split("?")[0];

    if (req.method === "GET" && url === "/healthz") return send(res, 200, { status: "ok" });
    if (req.method === "GET" && url === "/v1/stats") return send(res, 200, stats());

    if (req.method === "POST" && url === "/v1/reset") {
      const raw = await readBody(req);
      if (raw.trim()) {
        let patch: Record<string, unknown>;
        try {
          patch = JSON.parse(raw);
        } catch {
          return send(res, 400, { error: { code: "invalid_request", message: "body is not JSON" } });
        }
        for (const [k, v] of Object.entries(patch)) {
          if (!(k in DEFAULTS)) return send(res, 400, { error: { code: "invalid_request", message: `unknown field ${k}` } });
          (cfg as Record<string, unknown>)[k] = k === "latency_mode" ? v : Number(v);
        }
      }
      billed = serverErrors = overCapacity = maxInFlight = latencyStep = 0;
      return send(res, 200, { status: "reset", config: cfg });
    }

    if (req.method === "POST" && url === "/v1/annotate") {
      const raw = await readBody(req);
      let body: Record<string, unknown>;
      try {
        body = JSON.parse(raw);
      } catch {
        return send(res, 400, { error: { code: "invalid_request", message: "body is not JSON" } });
      }
      const keys = Object.keys(body ?? {});
      if (keys.length !== 1 || keys[0] !== "content_b64" || typeof body.content_b64 !== "string")
        return send(res, 400, { error: { code: "invalid_request", message: "content_b64 is the only accepted field" } });

      // billed before the outcome is chosen (EXT-REQ-4)
      billed += 1;
      const myBilled = billed;

      if (inFlight >= cfg.in_flight_capacity) {
        overCapacity += 1;
        return send(res, 429, { error: { code: "over_capacity", message: "in-flight limit reached" } });
      }

      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      const wait = latency();
      const fail = cfg.failure_every_n > 0 && myBilled % cfg.failure_every_n === 0;
      let released = false;
      const release = () => {
        if (!released) {
          released = true;
          inFlight -= 1;
        }
      };
      res.on("close", release); // caller disconnect frees the slot

      await new Promise((r) => setTimeout(r, wait));
      if (res.destroyed) return release();

      if (fail) {
        serverErrors += 1;
        release();
        return send(res, cfg.failure_status, { error: { code: "server_error", message: "failure schedule fired" } });
      }

      const bytes = Buffer.from(body.content_b64 as string, "base64");
      const sha = createHash("sha256").update(bytes).digest("hex");
      release();
      return send(res, 200, {
        annotation: {
          sha256: sha,
          // deterministic function of the request bytes alone (EXT-REQ-5)
          label: ["document", "record", "table", "image", "note"][bytes.length ? bytes[0] % 5 : 0],
          confidence: Number((((parseInt(sha.slice(0, 4), 16) % 1000) / 1000) * 0.5 + 0.5).toFixed(3)),
          tokens: Math.min(4096, bytes.length),
        },
        meta: { billed_call: myBilled, latency_ms: wait },
      });
    }

    return send(res, 404, { error: { code: "not_found", message: url } });
  });

  return server;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const i = process.argv.indexOf("--port");
  const port = i >= 0 ? Number(process.argv[i + 1]) : Number(process.env.PORT ?? 8080);
  createStub().listen(port, () => console.log(`stub listening on ${port}`));
}
