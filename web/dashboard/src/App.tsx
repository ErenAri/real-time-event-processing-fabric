import { startTransition, useEffect, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  fetchOverview,
  fetchRejections,
  fetchTenantSeries,
  fetchTopSources,
} from "./api";
import type {
  Overview,
  RecentRejection,
  SourceMetric,
  TenantBucket,
} from "./types";

const pollIntervalMs = 2000;
const emptyOverview: Overview = {
  generated_at: "",
  accepted_total: 0,
  rejected_total: 0,
  processed_total: 0,
  duplicate_total: 0,
  dead_letter_total: 0,
  consumer_lag: 0,
  processor_instances: 0,
  processor_active_partitions: 0,
  processor_inflight_messages: 0,
  events_per_second_last_minute: 0,
  error_rate_last_minute: 0,
  processing_p50_ms: 0,
  processing_p95_ms: 0,
  processing_p99_ms: 0,
  recent_rejections: [],
};

function formatNumber(value: number, digits = 0) {
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

function formatPercent(value: number) {
  return `${formatNumber(value * 100, 2)}%`;
}

function formatTime(value?: string) {
  if (!value) {
    return "No signal yet";
  }
  return new Date(value).toLocaleTimeString();
}

export default function App() {
  const [tenantId, setTenantId] = useState("tenant_01");
  const [overview, setOverview] = useState<Overview>(emptyOverview);
  const [series, setSeries] = useState<TenantBucket[]>([]);
  const [sources, setSources] = useState<SourceMetric[]>([]);
  const [rejections, setRejections] = useState<RecentRejection[]>([]);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  useEffect(() => {
    let active = true;

    const sync = async () => {
      try {
        const [overviewResponse, seriesResponse, sourcesResponse, rejectionResponse] =
          await Promise.all([
            fetchOverview(),
            fetchTenantSeries(tenantId),
            fetchTopSources(tenantId),
            fetchRejections(),
          ]);

        if (!active) {
          return;
        }

        startTransition(() => {
          setOverview(overviewResponse);
          setSeries(seriesResponse.series);
          setSources(sourcesResponse.sources);
          setRejections(rejectionResponse.rejections);
          setError("");
          setLastUpdated(new Date());
        });
      } catch (requestError) {
        if (!active) {
          return;
        }

        const message =
          requestError instanceof Error
            ? requestError.message
            : "Unknown dashboard sync error";
        startTransition(() => {
          setError(message);
        });
      }
    };

    sync();
    const intervalId = window.setInterval(sync, pollIntervalMs);
    return () => {
      active = false;
      window.clearInterval(intervalId);
    };
  }, [tenantId]);

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">PulseStream / MVP control plane</p>
          <h1>Real-time event analytics with measurable backpressure, lag, and recovery signals.</h1>
          <p className="hero-text">
            The dashboard polls the query service every {pollIntervalMs / 1000}s and surfaces
            the current ingest rate, processor health, rejection stream, and tenant-scoped hot
            views.
          </p>
        </div>
        <div className="hero-meta">
          <label className="field">
            <span>Tenant focus</span>
            <input
              value={tenantId}
              onChange={(event) => setTenantId(event.target.value)}
              placeholder="tenant_01"
            />
          </label>
          <div className="meta-card">
            <span>Last refresh</span>
            <strong>{lastUpdated ? lastUpdated.toLocaleTimeString() : "Pending first poll"}</strong>
          </div>
          <div className="meta-card">
            <span>API status</span>
            <strong>{error ? "Degraded" : "Healthy"}</strong>
          </div>
        </div>
      </section>

      {error ? (
        <section className="banner error-banner">
          <strong>Dashboard sync failed.</strong> Query service response: {error}
        </section>
      ) : null}

      <section className="stats-grid">
        <MetricCard
          label="Accepted"
          value={formatNumber(overview.accepted_total)}
          detail={`Rejected ${formatNumber(overview.rejected_total)}`}
        />
        <MetricCard
          label="Processed"
          value={formatNumber(overview.processed_total)}
          detail={`Duplicates ${formatNumber(overview.duplicate_total)} / Dead-lettered ${formatNumber(overview.dead_letter_total)}`}
        />
        <MetricCard
          label="Throughput"
          value={`${formatNumber(overview.events_per_second_last_minute, 1)} eps`}
          detail={`Error rate ${formatPercent(overview.error_rate_last_minute)} / Replicas ${formatNumber(overview.processor_instances)}`}
        />
        <MetricCard
          label="Consumer lag"
          value={formatNumber(overview.consumer_lag)}
          detail={`P95 ${formatNumber(overview.processing_p95_ms, 2)} ms / In-flight ${formatNumber(overview.processor_inflight_messages)}`}
        />
      </section>

      <section className="panels-grid">
        <article className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Tenant throughput</p>
              <h2>{tenantId} over the last 15 minutes</h2>
            </div>
            <p className="panel-caption">10-second buckets from the Postgres hot view.</p>
          </div>
          <div className="chart-surface">
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={series}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.18)" />
                <XAxis
                  dataKey="bucket_start"
                  tickFormatter={(value) => new Date(value).toLocaleTimeString([], { minute: "2-digit", second: "2-digit" })}
                  stroke="#9fb9c6"
                />
                <YAxis stroke="#9fb9c6" />
                <Tooltip
                  labelFormatter={(value) => new Date(value).toLocaleString()}
                  contentStyle={{ background: "#08232f", border: "1px solid #19465a", borderRadius: 16 }}
                />
                <Line type="monotone" dataKey="events_count" stroke="#60d6ca" strokeWidth={3} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Latency envelope</p>
              <h2>Processor timing</h2>
            </div>
            <p className="panel-caption">Derived from the stream processor quantile window.</p>
          </div>
          <dl className="latency-list">
            <div>
              <dt>P50</dt>
              <dd>{formatNumber(overview.processing_p50_ms, 2)} ms</dd>
            </div>
            <div>
              <dt>P95</dt>
              <dd>{formatNumber(overview.processing_p95_ms, 2)} ms</dd>
            </div>
            <div>
              <dt>P99</dt>
              <dd>{formatNumber(overview.processing_p99_ms, 2)} ms</dd>
            </div>
            <div>
              <dt>Processor replicas</dt>
              <dd>{formatNumber(overview.processor_instances)}</dd>
            </div>
            <div>
              <dt>Dead-lettered</dt>
              <dd>{formatNumber(overview.dead_letter_total)}</dd>
            </div>
            <div>
              <dt>Active partitions</dt>
              <dd>{formatNumber(overview.processor_active_partitions)}</dd>
            </div>
            <div>
              <dt>Ingest last seen</dt>
              <dd>{formatTime(overview.ingest_last_seen_at)}</dd>
            </div>
            <div>
              <dt>Processor last seen</dt>
              <dd>{formatTime(overview.processor_last_seen_at)}</dd>
            </div>
          </dl>
        </article>

        <article className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Status mix</p>
              <h2>Signal quality</h2>
            </div>
            <p className="panel-caption">Warn and error counts highlight noisy tenants fast.</p>
          </div>
          <div className="chart-surface compact">
            <ResponsiveContainer width="100%" height={240}>
              <AreaChart data={series}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.18)" />
                <XAxis hide dataKey="bucket_start" />
                <YAxis stroke="#9fb9c6" />
                <Tooltip
                  labelFormatter={(value) => new Date(value).toLocaleString()}
                  contentStyle={{ background: "#08232f", border: "1px solid #19465a", borderRadius: 16 }}
                />
                <Area type="monotone" dataKey="ok_count" stackId="1" stroke="#60d6ca" fill="#60d6ca" fillOpacity={0.65} />
                <Area type="monotone" dataKey="warn_count" stackId="1" stroke="#ffb85c" fill="#ffb85c" fillOpacity={0.75} />
                <Area type="monotone" dataKey="error_count" stackId="1" stroke="#ff6b5b" fill="#ff6b5b" fillOpacity={0.8} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel panel-wide">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Top sources</p>
              <h2>Most active sensors for {tenantId}</h2>
            </div>
            <p className="panel-caption">Ordered by lifetime events in the hot store.</p>
          </div>
          <div className="chart-surface">
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={sources} layout="vertical" margin={{ left: 12, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.18)" />
                <XAxis type="number" stroke="#9fb9c6" />
                <YAxis type="category" dataKey="source_id" width={96} stroke="#9fb9c6" />
                <Tooltip
                  contentStyle={{ background: "#08232f", border: "1px solid #19465a", borderRadius: 16 }}
                />
                <Bar dataKey="events" radius={[0, 12, 12, 0]}>
                  {sources.map((source, index) => (
                    <Cell
                      key={source.source_id}
                      fill={index % 2 === 0 ? "#60d6ca" : "#1da0a9"}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Rejections</p>
              <h2>Latest ingest failures</h2>
            </div>
            <p className="panel-caption">Malformed payloads and publish failures land here.</p>
          </div>
          <div className="timeline">
            {rejections.length === 0 ? (
              <p className="empty-state">No rejection activity yet.</p>
            ) : (
              rejections.map((rejection) => (
                <article key={rejection.id} className="timeline-row">
                  <div>
                    <strong>{rejection.reason}</strong>
                    <p>
                      {rejection.tenant_id || "unknown tenant"} / {rejection.source_id || "unknown source"}
                    </p>
                  </div>
                  <time>{new Date(rejection.created_at).toLocaleTimeString()}</time>
                </article>
              ))
            )}
          </div>
        </article>
      </section>
    </main>
  );
}

function MetricCard(props: { label: string; value: string; detail: string }) {
  return (
    <article className="metric-card">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
      <p>{props.detail}</p>
    </article>
  );
}
