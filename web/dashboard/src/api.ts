import type {
  EvidenceSummary,
  Overview,
  RejectionsResponse,
  ReplayRequest,
  ReplayResponse,
  SourcesResponse,
  TenantSeriesResponse,
} from "./types";

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8081";
const INGEST_API_BASE_URL =
  import.meta.env.VITE_INGEST_API_BASE_URL ?? "http://localhost:8080";
const API_BEARER_TOKEN =
  import.meta.env.VITE_API_BEARER_TOKEN?.trim() ?? "";

function buildHeaders(): HeadersInit {
  if (!API_BEARER_TOKEN) {
    return {};
  }
  return {
    Authorization: `Bearer ${API_BEARER_TOKEN}`,
  };
}

async function fetchJSON<T>(path: string): Promise<T> {
  return requestJSON<T>(`${API_BASE_URL}${path}`, {
    headers: buildHeaders(),
  });
}

async function requestJSON<T>(url: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(buildHeaders());
  if (init.headers) {
    new Headers(init.headers).forEach((value, key) => headers.set(key, value));
  }
  const response = await fetch(url, {
    ...init,
    headers,
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const payload = (await response.json()) as { error?: string };
      if (payload.error) {
        detail = `${detail}: ${payload.error}`;
      }
    } catch {
      // Keep the HTTP status as the useful fallback.
    }
    throw new Error(detail);
  }
  return (await response.json()) as T;
}

function asArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function normalizeOverview(payload: Overview): Overview {
  return {
    ...payload,
    generated_at: asString(payload?.generated_at),
    accepted_total: asNumber(payload?.accepted_total),
    rejected_total: asNumber(payload?.rejected_total),
    processed_total: asNumber(payload?.processed_total),
    duplicate_total: asNumber(payload?.duplicate_total),
    dead_letter_total: asNumber(payload?.dead_letter_total),
    consumer_lag: asNumber(payload?.consumer_lag),
    processor_instances: asNumber(payload?.processor_instances),
    processor_active_partitions: asNumber(payload?.processor_active_partitions),
    processor_inflight_messages: asNumber(payload?.processor_inflight_messages),
    events_per_second_last_minute: asNumber(payload?.events_per_second_last_minute),
    error_rate_last_minute: asNumber(payload?.error_rate_last_minute),
    processing_p50_ms: asNumber(payload?.processing_p50_ms),
    processing_p95_ms: asNumber(payload?.processing_p95_ms),
    processing_p99_ms: asNumber(payload?.processing_p99_ms),
    ingest_last_seen_at: payload?.ingest_last_seen_at ? asString(payload.ingest_last_seen_at) : undefined,
    processor_last_seen_at: payload?.processor_last_seen_at ? asString(payload.processor_last_seen_at) : undefined,
    recent_rejections: asArray<Overview["recent_rejections"][number]>(payload?.recent_rejections).map((item) => ({
      ...item,
      id: asNumber(item?.id),
      reason: asString(item?.reason, "unknown"),
      tenant_id: item?.tenant_id ? asString(item.tenant_id) : undefined,
      source_id: item?.source_id ? asString(item.source_id) : undefined,
      created_at: asString(item?.created_at),
    })),
  };
}

export async function fetchOverview() {
  const payload = await fetchJSON<Overview>("/api/v1/metrics/overview");
  return normalizeOverview(payload);
}

export async function fetchTenantSeries(tenantId: string, window = "15m") {
  const payload = await fetchJSON<TenantSeriesResponse>(
    `/api/v1/metrics/tenants/${encodeURIComponent(tenantId)}?window=${encodeURIComponent(window)}`,
  );
  return {
    tenant_id: asString(payload?.tenant_id, tenantId),
    window: asString(payload?.window, window),
    series: asArray<TenantSeriesResponse["series"][number]>(payload?.series).map((item) => ({
      bucket_start: asString(item?.bucket_start),
      events_count: asNumber(item?.events_count),
      ok_count: asNumber(item?.ok_count),
      warn_count: asNumber(item?.warn_count),
      error_count: asNumber(item?.error_count),
      average_value: asNumber(item?.average_value),
    })),
  };
}

export async function fetchTopSources(tenantId: string, limit = 8) {
  const query = tenantId
    ? `?tenantId=${encodeURIComponent(tenantId)}&limit=${limit}`
    : `?limit=${limit}`;
  const payload = await fetchJSON<SourcesResponse>(`/api/v1/metrics/sources/top${query}`);
  return {
    tenant_id: asString(payload?.tenant_id, tenantId),
    sources: asArray<SourcesResponse["sources"][number]>(payload?.sources).map((item) => ({
      tenant_id: asString(item?.tenant_id),
      source_id: asString(item?.source_id, "unknown"),
      events: asNumber(item?.events),
      last_event_at: asString(item?.last_event_at),
    })),
  };
}

export async function fetchRejections(limit = 10) {
  const payload = await fetchJSON<RejectionsResponse>(`/api/v1/metrics/rejections?limit=${limit}`);
  return {
    rejections: asArray<RejectionsResponse["rejections"][number]>(payload?.rejections).map((item) => ({
      id: asNumber(item?.id),
      reason: asString(item?.reason, "unknown"),
      tenant_id: item?.tenant_id ? asString(item.tenant_id) : undefined,
      source_id: item?.source_id ? asString(item.source_id) : undefined,
      created_at: asString(item?.created_at),
    })),
  };
}

function normalizeReplayResponse(payload: ReplayResponse): ReplayResponse {
  const replay = payload?.replay ?? {
    start_date: "",
    end_date: "",
    tenant_id: "",
    files_read: 0,
    scanned: 0,
    skipped: 0,
    replayed: 0,
    completed_at: "",
  };
  return {
    status: asString(payload?.status, "unknown"),
    replay: {
      start_date: asString(replay.start_date),
      end_date: asString(replay.end_date),
      tenant_id: asString(replay.tenant_id),
      files_read: asNumber(replay.files_read),
      scanned: asNumber(replay.scanned),
      skipped: asNumber(replay.skipped),
      replayed: asNumber(replay.replayed),
      completed_at: asString(replay.completed_at),
    },
  };
}

export async function replayArchive(request: ReplayRequest) {
  const payload = await requestJSON<ReplayResponse>(
    `${INGEST_API_BASE_URL}/api/v1/admin/replay`,
    {
      method: "POST",
      headers: {
        ...buildHeaders(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    },
  );
  return normalizeReplayResponse(payload);
}

function normalizeEvidenceSummary(payload: EvidenceSummary): EvidenceSummary {
  const benchmark = payload?.benchmark
    ? {
        artifact: asString(payload.benchmark.artifact),
        started_at_utc: asString(payload.benchmark.started_at_utc),
        completed_at_utc: asString(payload.benchmark.completed_at_utc),
        target_eps: asNumber(payload.benchmark.target_eps),
        accepted_eps: asNumber(payload.benchmark.accepted_eps),
        processed_eps: asNumber(payload.benchmark.processed_eps),
        query_p95_ms: asNumber(payload.benchmark.query_p95_ms),
        peak_lag: asNumber(payload.benchmark.peak_lag),
        producer_count: asNumber(payload.benchmark.producer_count, 1),
        processor_replicas: asNumber(payload.benchmark.processor_replicas),
        summary: asString(payload.benchmark.summary),
        gaps: asArray<string>(payload.benchmark.gaps).map((gap) => asString(gap)),
      }
    : null;

  return {
    schema_version: asNumber(payload?.schema_version, 1),
    generated_at: asString(payload?.generated_at),
    status: asString(payload?.status, "missing"),
    artifact_root: asString(payload?.artifact_root),
    benchmark,
    failure_drills: asArray<EvidenceSummary["failure_drills"][number]>(
      payload?.failure_drills,
    ).map((drill) => ({
      scenario_id: asString(drill?.scenario_id),
      title: asString(drill?.title, "Unknown drill"),
      status: asString(drill?.status, "missing"),
      artifact: asString(drill?.artifact),
      started_at_utc: asString(drill?.started_at_utc),
      completed_at_utc: asString(drill?.completed_at_utc),
      result: asString(drill?.result),
      operator_note: asString(drill?.operator_note),
      remaining_gap: asString(drill?.remaining_gap),
      metrics: asArray<EvidenceSummary["failure_drills"][number]["metrics"][number]>(
        drill?.metrics,
      ).map((metric) => ({
        label: asString(metric?.label),
        value: asString(metric?.value),
        unit: metric?.unit ? asString(metric.unit) : undefined,
        tone: metric?.tone ? asString(metric.tone) : undefined,
      })),
    })),
  };
}

export async function fetchEvidenceSummary() {
  const payload = await fetchJSON<EvidenceSummary>("/api/v1/evidence/latest");
  return normalizeEvidenceSummary(payload);
}
