// Corpus generator (section 3). Deterministic: (seed, size) fully determines file
// bytes; tenant only enters the manifest.
import { createHash } from "node:crypto";
import { deflateSync } from "node:zlib";
import fs from "node:fs";
import path from "node:path";

export type FileEntry = {
  order: number;
  path: string;
  extension: string;
  bytes: number;
  sha256: string;
  role: "normal" | "duplicate" | "edge_case";
  duplicate_of: string | null;
  edge_case: string | null;
  expected_outcome: "annotated" | "empty_content" | "decode_failed";
  expects_annotation: boolean;
};

export type Manifest = {
  corpus_id: string;
  arguments: { seed: number; size: number; tenant: string };
  totals: Record<string, number>;
  edge_cases: { path: string; edge_case: string; expected_outcome: string }[];
  files: FileEntry[];
  digest: string;
};

const EXTS = ["txt", "json", "csv", "png"];
const EMPTY_IDX = 7; // 0-based; both < 50 so every allowed size has them
const DECODE_IDX = 13;
const WORDS = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa".split(" ");

function rng(seed: number, size: number, index: number): () => number {
  const h = createHash("sha256").update(`${seed}:${size}:${index}`).digest();
  let s = h.readUInt32BE(0) || 1;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const crcTable = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c;
  }
  return t;
})();

function crc32(buf: Buffer): number {
  let c = -1;
  for (const b of buf) c = crcTable[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
}

function pngChunk(type: string, data: Buffer): Buffer {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, "ascii"), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}

function makePng(r: () => number): Buffer {
  const w = 8 + Math.floor(r() * 9);
  const h = 8 + Math.floor(r() * 9);
  const raw = Buffer.alloc(h * (1 + w * 3));
  for (let y = 0; y < h; y++) {
    const off = y * (1 + w * 3);
    raw[off] = 0; // filter: none
    for (let x = 0; x < w; x++) {
      raw[off + 1 + x * 3] = Math.floor(r() * 256);
      raw[off + 2 + x * 3] = Math.floor(r() * 256);
      raw[off + 3 + x * 3] = Math.floor(r() * 256);
    }
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(w, 0);
  ihdr.writeUInt32BE(h, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 2; // truecolour
  // level 0 (stored) keeps the deflate stream identical across zlib builds
  const idat = deflateSync(raw, { level: 0 });
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk("IHDR", ihdr),
    pngChunk("IDAT", idat),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

function words(r: () => number, n: number): string {
  return Array.from({ length: n }, () => WORDS[Math.floor(r() * WORDS.length)]).join(" ");
}

function makeContent(ext: string, index: number, seed: number, size: number): Buffer {
  const r = rng(seed, size, index);
  if (ext === "png") return makePng(r);
  if (ext === "json") {
    const obj = {
      id: index,
      title: words(r, 4),
      tags: Array.from({ length: 1 + Math.floor(r() * 4) }, () => words(r, 1)),
      score: Math.floor(r() * 1000),
      body: words(r, 20 + Math.floor(r() * 30)),
    };
    return Buffer.from(JSON.stringify(obj, null, 2) + "\n", "utf8");
  }
  if (ext === "csv") {
    const rows = 3 + Math.floor(r() * 12);
    const lines = ["id,name,value"];
    for (let i = 0; i < rows; i++) lines.push(`${i},${words(r, 1)},${Math.floor(r() * 500)}`);
    return Buffer.from(lines.join("\n") + "\n", "utf8");
  }
  const paras = 2 + Math.floor(r() * 5);
  return Buffer.from(
    Array.from({ length: paras }, () => words(r, 20 + Math.floor(r() * 40))).join("\n\n") + "\n",
    "utf8",
  );
}

function canonical(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    return `{${Object.keys(o)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonical(o[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(v ?? null);
}

/** Builds manifest + file bytes in memory. Pure function of the arguments. */
export function buildCorpus(seed: number, size: number, tenant: string) {
  const files: FileEntry[] = [];
  const bytesByPath = new Map<string, Buffer>();
  const pathByIndex = new Map<number, string>();

  for (let i = 0; i < size; i++) {
    let ext = EXTS[i % EXTS.length];
    let role: FileEntry["role"] = "normal";
    let edge: string | null = null;
    let rel: string;
    let data: Buffer;
    let dupOf: string | null = null;

    if (i === EMPTY_IDX) {
      ext = "txt";
      edge = "empty_content";
      role = "edge_case";
      rel = "edge/empty.txt";
      data = Buffer.alloc(0);
    } else if (i === DECODE_IDX) {
      ext = "json";
      edge = "decode_failed";
      role = "edge_case";
      rel = "edge/malformed.json";
      data = makePng(rng(seed, size, i)); // PNG bytes behind a .json extension
    } else if (i >= 15 && i % 10 === 5) {
      // ~1 in 10 items is a byte-for-byte duplicate of an earlier item, same extension
      const src = i - 4;
      const srcPath = pathByIndex.get(src)!;
      ext = path.extname(srcPath).slice(1);
      role = "duplicate";
      dupOf = srcPath;
      data = bytesByPath.get(srcPath)!;
      rel =
        i % 20 === 5
          ? `dupes/${path.basename(srcPath)}` // same name, different directory
          : `dupes/copy_${String(i + 1).padStart(4, "0")}.${ext}`; // different name
    } else {
      rel = `${ext}/item_${String(i + 1).padStart(4, "0")}.${ext}`;
      data = makeContent(ext, i, seed, size);
    }

    bytesByPath.set(rel, data);
    pathByIndex.set(i, rel);
    const expected: FileEntry["expected_outcome"] =
      edge === "empty_content" ? "empty_content" : edge === "decode_failed" ? "decode_failed" : "annotated";
    files.push({
      order: i + 1,
      path: rel,
      extension: ext,
      bytes: data.length,
      sha256: createHash("sha256").update(data).digest("hex"),
      role,
      duplicate_of: dupOf,
      edge_case: edge,
      expected_outcome: expected,
      expects_annotation: edge === null,
    });
  }

  const totals: Record<string, number> = { items: size };
  for (const f of files) {
    totals[`ext_${f.extension}`] = (totals[`ext_${f.extension}`] ?? 0) + 1;
    totals[`role_${f.role}`] = (totals[`role_${f.role}`] ?? 0) + 1;
  }

  const base = {
    corpus_id: createHash("sha256").update(`${seed}:${size}:${tenant}`).digest("hex").slice(0, 16),
    arguments: { seed, size, tenant },
    totals,
    edge_cases: files
      .filter((f) => f.edge_case)
      .map((f) => ({ path: f.path, edge_case: f.edge_case!, expected_outcome: f.expected_outcome })),
    files,
  };
  const manifest: Manifest = { ...base, digest: createHash("sha256").update(canonical(base)).digest("hex") };
  return { manifest, bytesByPath };
}

export function writeCorpus(dir: string, seed: number, size: number, tenant: string, force: boolean) {
  if (fs.existsSync(dir) && fs.readdirSync(dir).length > 0) {
    if (!force) throw new Error(`${dir} is not empty (use --force)`);
    fs.rmSync(dir, { recursive: true });
  }
  const { manifest, bytesByPath } = buildCorpus(seed, size, tenant);
  for (const [rel, data] of bytesByPath) {
    const abs = path.join(dir, "files", rel);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, data);
  }
  fs.writeFileSync(path.join(dir, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
  return manifest;
}

/** Re-derives the corpus from the manifest's own arguments and diffs every byte. */
export function verifyCorpus(dir: string) {
  const manifest: Manifest = JSON.parse(fs.readFileSync(path.join(dir, "manifest.json"), "utf8"));
  const { seed, size, tenant } = manifest.arguments;
  const { manifest: expected, bytesByPath } = buildCorpus(seed, size, tenant);
  const problems: string[] = [];
  if (expected.digest !== manifest.digest) problems.push("manifest digest differs");
  const onDisk = new Set<string>();
  const walk = (d: string, prefix = "") => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      if (e.isDirectory()) walk(path.join(d, e.name), `${prefix}${e.name}/`);
      else onDisk.add(prefix + e.name);
    }
  };
  walk(path.join(dir, "files"));
  for (const [rel, want] of bytesByPath) {
    onDisk.delete(rel);
    const abs = path.join(dir, "files", rel);
    if (!fs.existsSync(abs)) problems.push(`missing ${rel}`);
    else if (!fs.readFileSync(abs).equals(want)) problems.push(`bytes differ ${rel}`);
  }
  for (const extra of onDisk) problems.push(`unexpected file ${extra}`);
  return { ok: problems.length === 0, problems, corpus_id: manifest.corpus_id, checked: bytesByPath.size };
}
