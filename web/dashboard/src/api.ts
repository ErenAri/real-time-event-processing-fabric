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

export function fetchOverview() {
  return fetchJSON<Overview>("/api/v1/metrics/overview");
}

export function fetchTenantSeries(tenantId: string, window = "15m") {
  return fetchJSON<TenantSeriesResponse>(
    `/api/v1/metrics/tenants/${encodeURIComponent(tenantId)}?window=${encodeURIComponent(window)}`,
  );
}

export function fetchTopSources(tenantId: string, limit = 8) {
  const query = tenantId
    ? `?tenantId=${encodeURIComponent(tenantId)}&limit=${limit}`
    : `?limit=${limit}`;
  return fetchJSON<SourcesResponse>(`/api/v1/metrics/sources/top${query}`);
}

export function fetchRejections(limit = 10) {
  return fetchJSON<RejectionsResponse>(`/api/v1/metrics/rejections?limit=${limit}`);
}
