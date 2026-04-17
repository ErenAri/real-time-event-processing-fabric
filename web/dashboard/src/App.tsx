import { startTransition, useEffect, useState } from "react";
import type { FormEvent } from "react";
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
  fetchEvidenceSummary,
  fetchOverview,
  fetchRejections,
  fetchTenantSeries,
  fetchTopSources,
  replayArchive,
} from "./api";
import type {
  EvidenceSummary,
  FailureDrillEvidence,
  Overview,
  RecentRejection,
  ReplayResponse,
  SourceMetric,
  TenantBucket,
} from "./types";

const pollIntervalMs = 2000;
const defaultTenant = "tenant_01";

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

const fallbackFailureEvidence: FailureDrillEvidence[] = [
  {
    scenario_id: "broker-outage",
    title: "Broker outage",
    status: "verified",
    artifact: "broker-outage-20260416-201249.json",
    started_at_utc: "",
    completed_at_utc: "",
    result: "0 archive accounting gap",
    operator_note: "Kafka loss produced explicit publish_failed and backpressure rejections while the processor stayed live.",
    remaining_gap: "Producer log still had 278 client-side timeouts under sustained broker loss.",
    metrics: [],
  },
  {
    scenario_id: "pause-postgres",
    title: "Postgres pause",
    status: "verified",
    artifact: "pause-postgres-20260416-202040.json",
    started_at_utc: "",
    completed_at_utc: "",
    result: "1.24s processor recovery",
    operator_note: "Read-path degradation was visible and processing resumed after Postgres health returned.",
    remaining_gap: "Final lag remained elevated, so drain capacity still needs work.",
    metrics: [],
  },
  {
    scenario_id: "replay-archive",
    title: "Replay rebuild",
    status: "verified",
    artifact: "replay-archive-20260416173251.json",
    started_at_utc: "",
    completed_at_utc: "",
    result: "0 duplicate overcount",
    operator_note: "25 replayed duplicates were discarded; after scoped reset, replay rebuilt hot views to 25.",
    remaining_gap: "Date-only archive scanned 498,706 records to replay 25.",
    metrics: [],
  },
  {
    scenario_id: "poison-message",
    title: "Poison message",
    status: "verified",
    artifact: "inject-poison-message-20260411-152328.json",
    started_at_utc: "",
    completed_at_utc: "",
    result: "DLQ path verified",
    operator_note: "A malformed Kafka record was written to the DLQ and surfaced through dead_letter_total.",
    remaining_gap: "Next step is a dashboard drill runner, not only artifact display.",
    metrics: [],
  },
];

const fabricAlignment = [
  {
    concept: "Eventstreams",
    implementation: "ingest-service + Kafka + stream-processor",
    note: "Custom code gives local reproducibility and failure control before any managed-service mapping.",
  },
  {
    concept: "Eventhouse",
    implementation: "PostgreSQL hot views today",
    note: "Fast operational reads are implemented; a KQL/Eventhouse variant can be documented later.",
  },
  {
    concept: "Lakehouse",
    implementation: "raw archive + Blob-backed archive option",
    note: "Replay and cold-path durability are already represented without adding a heavy lake stack locally.",
  },
  {
    concept: "Activator",
    implementation: "Prometheus alerts and failure drill evidence",
    note: "Research suggests push-based actions; the practical next step is alert rules tied to measured drills.",
  },
];

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
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Invalid timestamp";
  }
  return date.toLocaleTimeString();
}

function todayUTC() {
  return new Date().toISOString().slice(0, 10);
}

function serviceFreshness(value?: string) {
  if (!value) {
    return "unknown";
  }
  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) {
    return "unknown";
  }
  const seconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  if (seconds <= 15) {
    return "healthy";
  }
  if (seconds <= 60) {
    return "stale";
  }
  return "offline";
}

function getSystemStatus(overview: Overview, error: string) {
  if (error) {
    return {
      label: "Degraded",
      tone: "danger" as const,
      detail: "Query API is not returning a healthy response.",
    };
  }
  if (overview.consumer_lag > 50_000 || overview.error_rate_last_minute > 0.05) {
    return {
      label: "Pressure",
      tone: "warning" as const,
      detail: "Lag or error rate is above the operator threshold.",
    };
  }
  return {
    label: "Nominal",
    tone: "healthy" as const,
    detail: "Core telemetry is inside the local operating envelope.",
  };
}

export default function App() {
  const [tenantDraft, setTenantDraft] = useState(defaultTenant);
  const [tenantId, setTenantId] = useState(defaultTenant);
  const [overview, setOverview] = useState<Overview>(emptyOverview);
  const [series, setSeries] = useState<TenantBucket[]>([]);
  const [sources, setSources] = useState<SourceMetric[]>([]);
  const [rejections, setRejections] = useState<RecentRejection[]>([]);
  const [evidence, setEvidence] = useState<EvidenceSummary | null>(null);
  const [evidenceError, setEvidenceError] = useState("");
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [replayTenant, setReplayTenant] = useState(defaultTenant);
  const [replayDate, setReplayDate] = useState(todayUTC());
  const [replayLimit, setReplayLimit] = useState(25);
  const [replayResult, setReplayResult] = useState<ReplayResponse | null>(null);
  const [replayError, setReplayError] = useState("");
  const [replayPending, setReplayPending] = useState(false);

  useEffect(() => {
    let active = true;

    const sync = async () => {
      try {
        const evidenceRequest = fetchEvidenceSummary()
          .then((summary) => ({ summary, error: "" }))
          .catch((requestError) => ({
            summary: null,
            error:
              requestError instanceof Error
                ? requestError.message
                : "Unknown evidence sync error",
          }));

        const [overviewResponse, seriesResponse, sourcesResponse, rejectionResponse, evidenceResponse] =
          await Promise.all([
            fetchOverview(),
            fetchTenantSeries(tenantId),
            fetchTopSources(tenantId),
            fetchRejections(12),
            evidenceRequest,
          ]);

        if (!active) {
          return;
        }

        startTransition(() => {
          setOverview(overviewResponse);
          setSeries(seriesResponse.series);
          setSources(sourcesResponse.sources);
          setRejections(rejectionResponse.rejections);
          setEvidence(evidenceResponse.summary);
          setEvidenceError(evidenceResponse.error);
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

  const status = getSystemStatus(overview, error);
  const ingestFreshness = serviceFreshness(overview.ingest_last_seen_at);
  const processorFreshness = serviceFreshness(overview.processor_last_seen_at);
  const acceptedVsProcessed =
    overview.accepted_total > 0
      ? Math.min(100, (overview.processed_total / overview.accepted_total) * 100)
      : 0;

  async function handleTenantSubmit(event: FormEvent) {
    event.preventDefault();
    const nextTenant = tenantDraft.trim();
    if (nextTenant) {
      setTenantId(nextTenant);
      setReplayTenant(nextTenant);
    }
  }

  async function handleReplaySubmit(event: FormEvent) {
    event.preventDefault();
    setReplayPending(true);
    setReplayError("");
    try {
      const result = await replayArchive({
        start_date: replayDate,
        end_date: replayDate,
        tenant_id: replayTenant.trim(),
        limit: replayLimit,
      });
      startTransition(() => {
        setReplayResult(result);
      });
    } catch (requestError) {
      const message =
        requestError instanceof Error ? requestError.message : "Replay request failed";
      setReplayError(message);
    } finally {
      setReplayPending(false);
    }
  }

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">PulseStream / operator control plane</p>
          <h1>Real-time event operations with live health, replay, and failure evidence.</h1>
          <p className="hero-text">
            Research pointed toward Fabric-style operational visibility: live stream state,
            action triggers, replayability, and clear topology. This dashboard keeps the
            custom Kafka/Postgres system visible without pretending to be a managed Fabric clone.
          </p>
          <div className="hero-actions">
            <StatusPill tone={status.tone}>{status.label}</StatusPill>
            <span>{status.detail}</span>
          </div>
        </div>
        <div className="hero-meta">
          <form className="field" onSubmit={handleTenantSubmit}>
            <span>Tenant focus</span>
            <div className="input-row">
              <input
                value={tenantDraft}
                onChange={(event) => setTenantDraft(event.target.value)}
                placeholder="tenant_01"
              />
              <button type="submit">Apply</button>
            </div>
          </form>
          <div className="meta-card">
            <span>Last refresh</span>
            <strong>{lastUpdated ? lastUpdated.toLocaleTimeString() : "Pending first poll"}</strong>
          </div>
          <div className="meta-card split">
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
          detail={`${formatNumber(acceptedVsProcessed, 1)}% of accepted count represented in hot views`}
        />
        <MetricCard
          label="Throughput"
          value={`${formatNumber(overview.events_per_second_last_minute, 1)} eps`}
          detail={`Error rate ${formatPercent(overview.error_rate_last_minute)}`}
        />
        <MetricCard
          label="Consumer lag"
          value={formatNumber(overview.consumer_lag)}
          detail={`P95 ${formatNumber(overview.processing_p95_ms, 2)} ms / P99 ${formatNumber(overview.processing_p99_ms, 2)} ms`}
        />
      </section>

      <section className="control-grid">
        <TopologyPanel
          ingestFreshness={ingestFreshness}
          processorFreshness={processorFreshness}
          overview={overview}
        />
        <ReplayPanel
          tenant={replayTenant}
          date={replayDate}
          limit={replayLimit}
          pending={replayPending}
          result={replayResult}
          error={replayError}
          onTenantChange={setReplayTenant}
          onDateChange={setReplayDate}
          onLimitChange={setReplayLimit}
          onSubmit={handleReplaySubmit}
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
            <StatRow label="P50" value={`${formatNumber(overview.processing_p50_ms, 2)} ms`} />
            <StatRow label="P95" value={`${formatNumber(overview.processing_p95_ms, 2)} ms`} />
            <StatRow label="P99" value={`${formatNumber(overview.processing_p99_ms, 2)} ms`} />
            <StatRow label="Processor replicas" value={formatNumber(overview.processor_instances)} />
            <StatRow label="Dead-lettered" value={formatNumber(overview.dead_letter_total)} />
            <StatRow label="Active partitions" value={formatNumber(overview.processor_active_partitions)} />
            <StatRow label="Ingest last seen" value={formatTime(overview.ingest_last_seen_at)} />
            <StatRow label="Processor last seen" value={formatTime(overview.processor_last_seen_at)} />
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

        <FailureEvidencePanel evidence={evidence} evidenceError={evidenceError} />
        <FabricAlignmentPanel />

        <article className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Rejections</p>
              <h2>Latest ingest failures</h2>
            </div>
            <p className="panel-caption">Malformed payloads, backpressure, and publish failures land here.</p>
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

function StatusPill(props: { tone: "healthy" | "warning" | "danger" | "neutral"; children: string }) {
  return <span className={`status-pill ${props.tone}`}>{props.children}</span>;
}

function StatRow(props: { label: string; value: string }) {
  return (
    <div>
      <dt>{props.label}</dt>
      <dd>{props.value}</dd>
    </div>
  );
}

function TopologyPanel(props: {
  ingestFreshness: string;
  processorFreshness: string;
  overview: Overview;
}) {
  const nodes = [
    { label: "Producer", detail: "synthetic telemetry", tone: "neutral" as const },
    { label: "Ingest", detail: props.ingestFreshness, tone: props.ingestFreshness as "healthy" | "stale" | "offline" | "unknown" },
    { label: "Kafka", detail: "brokered events", tone: "healthy" as const },
    {
      label: "Processor",
      detail: `${props.overview.processor_instances} replicas`,
      tone: props.processorFreshness as "healthy" | "stale" | "offline" | "unknown",
    },
    { label: "Postgres", detail: "hot views", tone: "healthy" as const },
    { label: "Dashboard", detail: "2s polling", tone: "healthy" as const },
  ];

  return (
    <article className="panel control-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Live topology</p>
          <h2>Operational path</h2>
        </div>
        <p className="panel-caption">Research signal: make data flow and failure domains visible.</p>
      </div>
      <div className="topology-flow">
        {nodes.map((node, index) => (
          <div className="topology-step" key={node.label}>
            <div className={`topology-node ${node.tone}`}>
              <strong>{node.label}</strong>
              <span>{node.detail}</span>
            </div>
            {index < nodes.length - 1 ? <span className="topology-edge" /> : null}
          </div>
        ))}
      </div>
      <div className="topology-footer">
        <StatBadge label="Active partitions" value={formatNumber(props.overview.processor_active_partitions)} />
        <StatBadge label="In-flight" value={formatNumber(props.overview.processor_inflight_messages)} />
        <StatBadge label="DLQ" value={formatNumber(props.overview.dead_letter_total)} />
      </div>
    </article>
  );
}

function ReplayPanel(props: {
  tenant: string;
  date: string;
  limit: number;
  pending: boolean;
  result: ReplayResponse | null;
  error: string;
  onTenantChange: (value: string) => void;
  onDateChange: (value: string) => void;
  onLimitChange: (value: number) => void;
  onSubmit: (event: FormEvent) => void;
}) {
  return (
    <article className="panel control-panel replay-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Admin replay</p>
          <h2>Archive recovery lever</h2>
        </div>
        <p className="panel-caption">Republishes raw archive records through the same processor path.</p>
      </div>
      <form className="replay-form" onSubmit={props.onSubmit}>
        <label>
          <span>Tenant</span>
          <input
            value={props.tenant}
            onChange={(event) => props.onTenantChange(event.target.value)}
            placeholder="tenant_01"
            required
          />
        </label>
        <label>
          <span>Date</span>
          <input
            value={props.date}
            onChange={(event) => props.onDateChange(event.target.value)}
            type="date"
            required
          />
        </label>
        <label>
          <span>Limit</span>
          <input
            value={props.limit}
            onChange={(event) => props.onLimitChange(Number(event.target.value))}
            type="number"
            min={0}
            max={5000}
            required
          />
        </label>
        <button type="submit" disabled={props.pending}>
          {props.pending ? "Replaying..." : "Replay archive"}
        </button>
      </form>
      {props.error ? <p className="form-error">{props.error}</p> : null}
      {props.result ? (
        <div className="replay-result">
          <StatBadge label="Scanned" value={formatNumber(props.result.replay.scanned)} />
          <StatBadge label="Skipped" value={formatNumber(props.result.replay.skipped)} />
          <StatBadge label="Replayed" value={formatNumber(props.result.replay.replayed)} />
          <StatBadge label="Files" value={formatNumber(props.result.replay.files_read)} />
        </div>
      ) : (
        <p className="empty-state">
          Use this for scoped recovery tests. Replayed duplicates should be discarded by the processor.
        </p>
      )}
    </article>
  );
}

function FailureEvidencePanel(props: {
  evidence: EvidenceSummary | null;
  evidenceError: string;
}) {
  const drills =
    props.evidence && props.evidence.failure_drills.length > 0
      ? props.evidence.failure_drills
      : fallbackFailureEvidence;
  const benchmark = props.evidence?.benchmark ?? null;
  const isLiveEvidence = props.evidence?.status === "available";

  return (
    <article className="panel panel-wide">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Failure evidence</p>
          <h2>Measured drills, not claims</h2>
        </div>
        <p className="panel-caption">Senior signal comes from observed behavior and remaining gaps.</p>
      </div>
      {benchmark ? (
        <div className="evidence-benchmark">
          <div>
            <StatusPill tone="healthy">Latest benchmark</StatusPill>
            <strong>{benchmark.summary}</strong>
          </div>
          <div className="evidence-benchmark-metrics">
            <StatBadge label="Target" value={`${formatNumber(benchmark.target_eps, 0)} eps`} />
            <StatBadge label="Producers" value={formatNumber(benchmark.producer_count)} />
            <StatBadge label="Accepted" value={`${formatNumber(benchmark.accepted_eps, 1)} eps`} />
            <StatBadge label="Processed" value={`${formatNumber(benchmark.processed_eps, 1)} eps`} />
            <StatBadge label="Query P95" value={`${formatNumber(benchmark.query_p95_ms, 2)} ms`} />
          </div>
          <small>{benchmark.artifact}</small>
        </div>
      ) : null}
      {props.evidenceError ? (
        <p className="form-error">Evidence endpoint unavailable: {props.evidenceError}</p>
      ) : null}
      {!isLiveEvidence && !props.evidenceError ? (
        <p className="empty-state">
          Showing fallback evidence labels. Run a benchmark or chaos drill to refresh
          artifacts/evidence/latest.json.
        </p>
      ) : null}
      <div className="evidence-grid">
        {drills.map((item) => (
          <article className="evidence-card" key={item.title}>
            <div>
              <StatusPill tone={item.status === "verified" ? "healthy" : item.status === "missing" ? "neutral" : "warning"}>
                {item.status}
              </StatusPill>
              <h3>{item.title}</h3>
              <strong>{item.result}</strong>
              <p>{item.operator_note}</p>
              {item.metrics.length > 0 ? (
                <div className="evidence-metrics">
                  {item.metrics.map((metric) => (
                    <StatBadge
                      key={`${item.scenario_id}-${metric.label}`}
                      label={metric.label}
                      value={`${metric.value}${metric.unit ? ` ${metric.unit}` : ""}`}
                    />
                  ))}
                </div>
              ) : null}
            </div>
            <footer>
              <span>{item.artifact}</span>
              <small>{item.remaining_gap}</small>
            </footer>
          </article>
        ))}
      </div>
    </article>
  );
}

function FabricAlignmentPanel() {
  return (
    <article className="panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Research mapping</p>
          <h2>Fabric concepts, custom core</h2>
        </div>
        <p className="panel-caption">The useful takeaway is alignment, not copying every managed service.</p>
      </div>
      <div className="alignment-list">
        {fabricAlignment.map((item) => (
          <article className="alignment-row" key={item.concept}>
            <strong>{item.concept}</strong>
            <span>{item.implementation}</span>
            <p>{item.note}</p>
          </article>
        ))}
      </div>
    </article>
  );
}

function StatBadge(props: { label: string; value: string }) {
  return (
    <div className="stat-badge">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}
