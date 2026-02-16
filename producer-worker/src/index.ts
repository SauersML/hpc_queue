interface Env {
  HPC_QUEUE: Queue;
  API_KEY: string;
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

const ADJECTIVES = [
  "agile", "amber", "ancient", "arctic", "bold", "brisk", "calm", "cobalt", "crimson",
  "curious", "daring", "deep", "eager", "electric", "fierce", "final", "gentle", "golden",
  "grand", "hidden", "icy", "jovial", "keen", "lively", "lunar", "magic", "mellow", "midnight",
  "misty", "modern", "nimble", "noble", "opal", "pearl", "polar", "primal", "quick", "rapid",
  "royal", "silent", "silver", "solar", "stellar", "swift", "tidy", "urban", "velvet", "vivid",
  "wild", "wise", "young", "zesty",
];

const NOUNS = [
  "anchor", "apex", "archive", "arrow", "atlas", "aurora", "beacon", "binary", "bridge", "canyon",
  "cascade", "cinder", "citadel", "cloud", "comet", "compass", "cosmos", "crater", "crest", "delta",
  "domain", "ember", "engine", "falcon", "fjord", "forest", "forge", "frontier", "galaxy", "harbor",
  "helios", "horizon", "island", "jungle", "kernel", "lagoon", "lantern", "matrix", "meadow", "meteor",
  "nebula", "nexus", "oak", "oasis", "ocean", "orbit", "origin", "phoenix", "planet", "portal",
  "prairie", "quartz", "radar", "reef", "relay", "rocket", "signal", "summit", "thunder", "uplink",
  "valley", "vector", "vortex", "willow", "zenith",
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

      const job: JobMessage = {
        job_id: shortJobId(),
        input: payload.input ?? {},
        created_at: new Date().toISOString(),
        metadata: payload.metadata,
      };

      await env.HPC_QUEUE.send(job);

      return jsonResponse(
        {
          status: "queued",
          job_id: job.job_id,
          queue: "hpc-jobs",
        },
        202,
      );
    }

    return jsonResponse({ error: "not_found" }, 404);
  },
} satisfies ExportedHandler<Env>;
