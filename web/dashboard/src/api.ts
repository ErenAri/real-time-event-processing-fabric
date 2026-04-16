import type {
  Overview,
  RejectionsResponse,
  SourcesResponse,
  TenantSeriesResponse,
} from "./types";

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8081";
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
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: buildHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
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
