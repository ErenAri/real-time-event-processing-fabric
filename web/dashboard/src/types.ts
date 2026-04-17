export interface RecentRejection {
  id: number;
  reason: string;
  tenant_id?: string;
  source_id?: string;
  created_at: string;
}

export interface Overview {
  generated_at: string;
  accepted_total: number;
  rejected_total: number;
  processed_total: number;
  duplicate_total: number;
  dead_letter_total: number;
  consumer_lag: number;
  processor_instances: number;
  processor_active_partitions: number;
  processor_inflight_messages: number;
  events_per_second_last_minute: number;
  error_rate_last_minute: number;
  processing_p50_ms: number;
  processing_p95_ms: number;
  processing_p99_ms: number;
  ingest_last_seen_at?: string;
  processor_last_seen_at?: string;
  recent_rejections: RecentRejection[];
}

export interface TenantBucket {
  bucket_start: string;
  events_count: number;
  ok_count: number;
  warn_count: number;
  error_count: number;
  average_value: number;
}

export interface TenantSeriesResponse {
  tenant_id: string;
  window: string;
  series: TenantBucket[];
}

export interface SourceMetric {
  tenant_id: string;
  source_id: string;
  events: number;
  last_event_at: string;
}

export interface SourcesResponse {
  tenant_id: string;
  sources: SourceMetric[];
}

export interface RejectionsResponse {
  rejections: RecentRejection[];
}

export interface ReplayRequest {
  start_date: string;
  end_date?: string;
  tenant_id: string;
  limit: number;
}

export interface ReplayResult {
  start_date: string;
  end_date: string;
  tenant_id: string;
  files_read: number;
  scanned: number;
  skipped: number;
  replayed: number;
  completed_at: string;
}

export interface ReplayResponse {
  status: string;
  replay: ReplayResult;
}

export interface EvidenceMetric {
  label: string;
  value: string;
  unit?: string;
  tone?: string;
}

export interface BenchmarkEvidence {
  artifact: string;
  started_at_utc: string;
  completed_at_utc: string;
  target_eps: number;
  accepted_eps: number;
  processed_eps: number;
  query_p95_ms: number;
  peak_lag: number;
  producer_count: number;
  processor_replicas: number;
  summary: string;
  gaps: string[];
}

export interface FailureDrillEvidence {
  scenario_id: string;
  title: string;
  status: string;
  artifact: string;
  started_at_utc: string;
  completed_at_utc: string;
  result: string;
  operator_note: string;
  remaining_gap: string;
  metrics: EvidenceMetric[];
}

export interface EvidenceSummary {
  schema_version: number;
  generated_at: string;
  status: string;
  artifact_root: string;
  benchmark?: BenchmarkEvidence | null;
  failure_drills: FailureDrillEvidence[];
}
