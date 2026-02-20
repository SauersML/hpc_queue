interface Env {
  HPC_QUEUE: Queue;
  API_KEY: string;
  R2_ACCESS_KEY_ID?: string;
  R2_SECRET_ACCESS_KEY?: string;
  R2_BUCKET?: string;
  R2_ACCOUNT_ID?: string;
  R2_REGION?: string;
}

type JobRequest = {
  input: Record<string, unknown>;
  metadata?: Record<string, unknown>;
};

type JobMessage = {
  job_id: string;
  input: Record<string, unknown>;
  created_at: string;
  metadata?: Record<string, unknown>;
};

type PresignRequest = {
  object_key: string;
  expires_seconds?: number;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isQueueRateLimitError(err: unknown): boolean {
  const text = String(err);
  return text.includes("Too Many Requests") || text.includes("429");
}

async function enqueueWithRetry(queue: Queue, job: JobMessage, maxAttempts = 5): Promise<void> {
  let lastError: unknown = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await queue.send(job);
      return;
    } catch (err) {
      lastError = err;
      if (!isQueueRateLimitError(err) || attempt >= maxAttempts) {
        throw err;
      }
      const delayMs = Math.min(2000, 100 * (2 ** (attempt - 1))) + Math.floor(Math.random() * 100);
      await sleep(delayMs);
    }
  }
  throw lastError instanceof Error ? lastError : new Error("enqueue_failed_unknown");
}

function serializeError(err: unknown): Record<string, unknown> {
  if (err instanceof Error) {
    const out: Record<string, unknown> = {
      name: err.name,
      message: err.message,
      stack: err.stack,
    };
    const maybe = err as Error & { cause?: unknown; code?: unknown };
    if (maybe.cause !== undefined) out.cause = String(maybe.cause);
    if (maybe.code !== undefined) out.code = String(maybe.code);
    return out;
  }
  return { value: String(err) };
}

function requireObjectKey(url: URL): string | null {
  const key = (url.searchParams.get("object_key") ?? "").trim();
  if (!key || key.includes("..")) return null;
  return key;
}

const ADJECTIVES = [
  "adaptive", "aerobic", "agile", "alkaline", "amber", "ancient", "aquatic", "arctic", "axial",
  "basal", "bioactive", "biogenic", "biologic", "biotic", "bold", "brisk", "calm", "carbonic",
  "cellular", "central", "chloric", "chromatic", "ciliary", "cobalt", "cognitive", "colonial",
  "complex", "cortical", "crimson", "critical", "crosslinked", "curious", "cyclic", "cytogenic",
  "daring", "deep", "dendritic", "diffusive", "diploid", "dynamic", "eager", "ecologic", "electric",
  "embryonic", "enzymatic", "epigenetic", "eukaryotic", "evolved", "fertile", "fierce", "filamentous",
  "final", "floral", "fluidic", "fungal", "genelevel", "gentle", "genetic", "genomic", "golden",
  "granular", "grand", "growthlinked", "haploid", "healthy", "helical", "heritable", "hidden", "hosted",
  "hybrid", "hydrophilic", "icy", "immune", "induced", "inertial", "ionic", "jovial", "keen", "kinetic",
  "labile", "leafy", "lentic", "lively", "lunar", "lytic", "magic", "mellow", "membranous", "metabolic",
  "microbial", "mitotic", "misty", "mobile", "modern", "modular", "molecular", "motile", "multicellular",
  "mutable", "myogenic", "native", "neural", "nimble", "noble", "nuclear", "nutritive", "oceanic", "opal",
  "organic", "osmotic", "ovarian", "pearl", "pelagic", "phasic", "photonic", "physiologic", "pluripotent",
  "polar", "polygenic", "primal", "primary", "prokaryotic", "proteomic", "quick", "radial", "rapid",
  "reactive", "regenerative", "reliable", "reproductive", "resilient", "ribosomal", "robust", "rooted",
  "royal", "secreted", "sensory", "serial", "silent", "silver", "solar", "somatic", "spatial", "speciated",
  "stable", "stellar", "stochastic", "stromal", "structural", "swift", "symbiotic", "synaptic", "systemic",
  "tidy", "trophic", "urban", "vascular", "velvet", "viable", "vivid", "wild", "wise", "young", "zesty",
];

const NOUNS = [
  "allele", "aminoacid", "amphibian", "anchor", "apex", "archaea", "archive", "artery", "atlas", "aurora",
  "axon", "bacterium", "beacon", "beetle", "biome", "biopsy", "branch", "bridge", "cambium", "canyon",
  "capsid", "cascade", "cell", "cellwall", "centromere", "chloroplast", "chromosome", "citadel", "clade",
  "cloud", "cluster", "codon", "colony", "comet", "compass", "complex", "cortex", "cosmos", "crater",
  "crest", "culture", "cytoplasm", "delta", "dendrite", "domain", "ecotone", "ecosystem", "ember",
  "embryo", "enzyme", "epitope", "estuary", "exon", "falcon", "fiber", "filament", "fjord", "flora", "flux",
  "forest", "forge", "frontier", "fungus", "galaxy", "gamete", "gene", "genome", "genotype", "germline",
  "gland", "habitat", "harbor", "helios", "helix", "horizon", "hormone", "host", "hypha", "immunome",
  "intron", "island", "junction", "jungle", "kernel", "kinase", "lagoon", "lantern", "leaf", "ligand",
  "lineage", "locus", "matrix", "meadow", "membrane", "meteor", "microbe", "mitochondrion", "module",
  "molecule", "moss", "mutation", "mycelium", "nebula", "nervenet", "niche", "nucleus", "nucleotide",
  "oasis", "ocean", "oncogene", "operon", "orbit", "organelle", "origin", "pathway", "petiole", "phage",
  "phenotype", "phloem", "photon", "phylogeny", "planet", "plasmid", "platelet", "portal", "prairie",
  "primordium", "protein", "protist", "pulse", "quartz", "radar", "receptor", "reef", "relay", "retina",
  "ribosome", "rocket", "savanna", "scaffold", "signal", "soma", "species", "spectrum", "spindle",
  "spore", "stamen", "stemcell", "stroma", "summit", "synapse", "taxonomy", "thunder", "tissue", "trait",
  "transcript", "tropism", "tubule", "uplink", "vector", "vein", "vesicle", "vessel", "vortex", "willow",
  "xylem", "zenith", "zygote",
];

function randomItem(items: readonly string[]): string {
  const idx = crypto.getRandomValues(new Uint32Array(1))[0] % items.length;
  return items[idx]!;
}

function shortJobId(): string {
  const adjective = randomItem(ADJECTIVES);
  const noun = randomItem(NOUNS);
  const altNoun = randomItem(NOUNS);
  const pattern = crypto.getRandomValues(new Uint32Array(1))[0] % 3;
  const bytes = new Uint8Array(3);
  crypto.getRandomValues(bytes);
  const suffix = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  if (pattern === 0) return `${adjective}-${noun}-${suffix}`;
  if (pattern === 1) return `${noun}-${adjective}-${suffix}`;
  return `${noun}-${altNoun}-${suffix}`;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function authorize(request: Request, env: Env): boolean {
  return request.headers.get("x-api-key") === env.API_KEY;
}

function toHex(bytes: ArrayBuffer): string {
  return Array.from(new Uint8Array(bytes))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function encodeRFC3986(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function isoDate(now: Date): { amzDate: string; dateScope: string } {
  const y = now.getUTCFullYear().toString().padStart(4, "0");
  const m = (now.getUTCMonth() + 1).toString().padStart(2, "0");
  const d = now.getUTCDate().toString().padStart(2, "0");
  const hh = now.getUTCHours().toString().padStart(2, "0");
  const mm = now.getUTCMinutes().toString().padStart(2, "0");
  const ss = now.getUTCSeconds().toString().padStart(2, "0");
  return {
    amzDate: `${y}${m}${d}T${hh}${mm}${ss}Z`,
    dateScope: `${y}${m}${d}`,
  };
}

async function sha256Hex(value: string): Promise<string> {
  const data = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return toHex(digest);
}

async function hmacSha256Raw(key: Uint8Array, value: string): Promise<ArrayBuffer> {
  const rawKey = new Uint8Array(key).buffer as ArrayBuffer;
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    rawKey,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(value));
}

async function deriveSigningKey(secret: string, dateScope: string, region: string, service: string): Promise<ArrayBuffer> {
  const kDate = await hmacSha256Raw(new TextEncoder().encode(`AWS4${secret}`), dateScope);
  const kRegion = await hmacSha256Raw(new Uint8Array(kDate), region);
  const kService = await hmacSha256Raw(new Uint8Array(kRegion), service);
  return hmacSha256Raw(new Uint8Array(kService), "aws4_request");
}

async function presignR2Url(env: Env, method: "PUT" | "GET" | "DELETE", objectKey: string, expiresSeconds: number): Promise<string> {
  const accessKeyId = (env.R2_ACCESS_KEY_ID ?? "").trim();
  const secretAccessKey = (env.R2_SECRET_ACCESS_KEY ?? "").trim();
  if (!accessKeyId || !secretAccessKey) {
    throw new Error("worker_missing_r2_secrets");
  }
  const accountId = (env.R2_ACCOUNT_ID ?? "").trim() || "59908b351c3a3321ff84dd2d78bf0b42";
  const bucket = (env.R2_BUCKET ?? "").trim() || "hpc-queue-grab";
  const region = (env.R2_REGION ?? "").trim() || "auto";
  const service = "s3";
  const host = `${accountId}.r2.cloudflarestorage.com`;
  const keyPath = objectKey
    .split("/")
    .map((part) => encodeRFC3986(part))
    .join("/");
  const canonicalUri = `/${encodeRFC3986(bucket)}/${keyPath}`;

  const { amzDate, dateScope } = isoDate(new Date());
  const credentialScope = `${dateScope}/${region}/${service}/aws4_request`;
  const credential = `${accessKeyId}/${credentialScope}`;
  const expires = Math.max(1, Math.min(604800, Math.floor(expiresSeconds)));

  const qp = [
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", credential],
    ["X-Amz-Date", amzDate],
    ["X-Amz-Expires", String(expires)],
    ["X-Amz-SignedHeaders", "host"],
  ];
  qp.sort((a, b) => (a[0] === b[0] ? a[1].localeCompare(b[1]) : a[0].localeCompare(b[0])));
  const canonicalQueryString = qp.map(([k, v]) => `${encodeRFC3986(k)}=${encodeRFC3986(v)}`).join("&");

  const canonicalRequest = [
    method,
    canonicalUri,
    canonicalQueryString,
    `host:${host}\n`,
    "host",
    "UNSIGNED-PAYLOAD",
  ].join("\n");
  const canonicalRequestHash = await sha256Hex(canonicalRequest);
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    canonicalRequestHash,
  ].join("\n");
  const signingKey = await deriveSigningKey(secretAccessKey, dateScope, region, service);
  const signature = toHex(await hmacSha256Raw(new Uint8Array(signingKey), stringToSign));

  const finalQuery = `${canonicalQueryString}&X-Amz-Signature=${signature}`;
  return `https://${host}${canonicalUri}?${finalQuery}`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!authorize(request, env)) {
      return jsonResponse({ error: "unauthorized" }, 401);
    }

    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return jsonResponse({ ok: true, service: "hpc-queue-producer" });
    }

    if (request.method === "POST" && url.pathname === "/jobs") {
      let payload: JobRequest;
      try {
        payload = (await request.json()) as JobRequest;
      } catch {
        return jsonResponse({ error: "invalid_json" }, 400);
      }

      if (!env.HPC_QUEUE || typeof env.HPC_QUEUE.send !== "function") {
        return jsonResponse({ error: "queue_binding_missing", detail: "HPC_QUEUE is not configured" }, 500);
      }

      const job: JobMessage = {
        job_id: shortJobId(),
        input: payload.input ?? {},
        created_at: new Date().toISOString(),
        metadata: payload.metadata,
      };

      try {
        await enqueueWithRetry(env.HPC_QUEUE, job);
      } catch (err) {
        console.error("enqueue_failed", JSON.stringify(serializeError(err)));
        if (isQueueRateLimitError(err)) {
          return new Response(JSON.stringify({ error: "enqueue_rate_limited", detail: String(err) }), {
            status: 429,
            headers: {
              "content-type": "application/json",
              "retry-after": "2",
            },
          });
        }
        return jsonResponse({ error: "enqueue_failed", detail: String(err) }, 500);
      }

      return jsonResponse(
        {
          status: "queued",
          job_id: job.job_id,
          queue: "hpc-jobs",
        },
        202,
      );
    }

    if (request.method === "POST" && url.pathname === "/grab/presign") {
      let payload: PresignRequest;
      try {
        payload = (await request.json()) as PresignRequest;
      } catch {
        return jsonResponse({ error: "invalid_json" }, 400);
      }

      const objectKey = String(payload.object_key ?? "").trim();
      if (!objectKey) {
        return jsonResponse({ error: "object_key_required" }, 400);
      }
      if (objectKey.includes("..")) {
        return jsonResponse({ error: "invalid_object_key" }, 400);
      }
      const expires = Math.max(60, Math.min(3600, Number(payload.expires_seconds ?? 1800)));

      try {
        const putUrl = await presignR2Url(env, "PUT", objectKey, expires);
        const getUrl = await presignR2Url(env, "GET", objectKey, expires);
        const deleteUrl = await presignR2Url(env, "DELETE", objectKey, expires);
        return jsonResponse({
          object_key: objectKey,
          expires_seconds: expires,
          urls: {
            put: putUrl,
            get: getUrl,
            delete: deleteUrl,
          },
        });
      } catch (err) {
        return jsonResponse({ error: "presign_failed", detail: String(err) }, 500);
      }
    }

    if (request.method === "POST" && url.pathname === "/grab/upload") {
      const objectKey = requireObjectKey(url);
      if (!objectKey) {
        return jsonResponse({ error: "invalid_object_key" }, 400);
      }
      try {
        const putUrl = await presignR2Url(env, "PUT", objectKey, 1800);
        const upstream = await fetch(putUrl, {
          method: "PUT",
          body: request.body,
        });
        if (!upstream.ok) {
          const detail = await upstream.text();
          return jsonResponse({ error: "upload_failed", status: upstream.status, detail }, 502);
        }
        return jsonResponse({ ok: true, object_key: objectKey });
      } catch (err) {
        return jsonResponse({ error: "upload_failed", detail: String(err) }, 500);
      }
    }

    if (request.method === "GET" && url.pathname === "/grab/download") {
      const objectKey = requireObjectKey(url);
      if (!objectKey) {
        return jsonResponse({ error: "invalid_object_key" }, 400);
      }
      try {
        const getUrl = await presignR2Url(env, "GET", objectKey, 1800);
        const upstream = await fetch(getUrl, { method: "GET" });
        if (!upstream.ok) {
          const detail = await upstream.text();
          return jsonResponse({ error: "download_failed", status: upstream.status, detail }, 502);
        }
        return new Response(upstream.body, {
          status: 200,
          headers: {
            "content-type": upstream.headers.get("content-type") ?? "application/octet-stream",
            "cache-control": "no-store",
          },
        });
      } catch (err) {
        return jsonResponse({ error: "download_failed", detail: String(err) }, 500);
      }
    }

    if (request.method === "POST" && url.pathname === "/grab/delete") {
      const objectKey = requireObjectKey(url);
      if (!objectKey) {
        return jsonResponse({ error: "invalid_object_key" }, 400);
      }
      try {
        const deleteUrl = await presignR2Url(env, "DELETE", objectKey, 1800);
        const upstream = await fetch(deleteUrl, { method: "DELETE" });
        if (!upstream.ok) {
          const detail = await upstream.text();
          return jsonResponse({ error: "delete_failed", status: upstream.status, detail }, 502);
        }
        return jsonResponse({ ok: true, object_key: objectKey });
      } catch (err) {
        return jsonResponse({ error: "delete_failed", detail: String(err) }, 500);
      }
    }

    return jsonResponse({ error: "not_found" }, 404);
  },
} satisfies ExportedHandler<Env>;
