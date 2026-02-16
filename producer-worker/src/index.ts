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
        job_id: crypto.randomUUID(),
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
