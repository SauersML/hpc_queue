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
