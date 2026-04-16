package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pulsestream/internal/auth"
	"pulsestream/internal/platform"
	"pulsestream/internal/store"
)

type MetricsReader interface {
	GetOverview(ctx context.Context) (store.Overview, error)
	GetTenantSeries(ctx context.Context, tenantID string, window time.Duration) ([]store.TenantBucket, error)
	GetTopSources(ctx context.Context, tenantID string, limit int) ([]store.SourceMetric, error)
	RecentRejections(ctx context.Context, limit int) ([]store.RecentRejection, error)
}

type QueryHandler struct {
	logger        *slog.Logger
	reader        MetricsReader
	verifier      *auth.Verifier
	startedAt     time.Time
	requestsTotal atomic.Int64

	queryLatency prometheus.Histogram
	requests     prometheus.Counter
}

func NewQueryHandler(logger *slog.Logger, reader MetricsReader, verifier *auth.Verifier, registry *prometheus.Registry) *QueryHandler {
	handler := &QueryHandler{
		logger:    logger,
		reader:    reader,
		verifier:  verifier,
		startedAt: time.Now().UTC(),
		queryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_query_request_duration_seconds",
			Help:    "Duration of query-service API requests.",
			Buckets: prometheus.DefBuckets,
		}),
		requests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_query_requests_total",
			Help: "Total query-service requests.",
		}),
	}
	registry.MustRegister(handler.queryLatency, handler.requests)
	return handler
}

func (h *QueryHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/metrics/overview", h.wrap(h.handleOverview))
	mux.HandleFunc("/api/v1/metrics/tenants/", h.wrap(h.handleTenantSeries))
	mux.HandleFunc("/api/v1/metrics/sources/top", h.wrap(h.handleTopSources))
	mux.HandleFunc("/api/v1/metrics/rejections", h.wrap(h.handleRejections))
}

func (h *QueryHandler) Snapshot() store.QueryState {
	return store.QueryState{
		RequestCount:  h.requestsTotal.Load(),
		LastSeenAt:    time.Now().UTC(),
		UptimeSeconds: int64(time.Since(h.startedAt).Seconds()),
	}
}

func (h *QueryHandler) wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			h.queryLatency.Observe(time.Since(start).Seconds())
		}()
		h.requests.Inc()
		h.requestsTotal.Add(1)
		next(w, r)
	}
}

func (h *QueryHandler) handleOverview(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticateRequest(w, r)
	if !ok {
		return
	}

	overview, err := h.reader.GetOverview(auth.ContextWithPrincipal(r.Context(), principal))
	if err != nil {
		h.logger.Error("overview_query_failed", "error", err)
		platform.WriteError(w, http.StatusInternalServerError, "failed to load overview")
		return
	}
	overview.RecentRejections = ensureRecentRejections(overview.RecentRejections)
	platform.WriteJSON(w, http.StatusOK, overview)
}

func (h *QueryHandler) handleTenantSeries(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticateRequest(w, r)
	if !ok {
		return
	}

	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/metrics/tenants/")
	if tenantID == "" {
		platform.WriteError(w, http.StatusBadRequest, "tenant id is required")
		return
	}
	if principal.Role == auth.RoleTenantUser && tenantID != principal.TenantID {
		platform.WriteError(w, http.StatusForbidden, "token tenant does not match requested tenant")
		return
	}

	window := 15 * time.Minute
	if raw := r.URL.Query().Get("window"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			platform.WriteError(w, http.StatusBadRequest, "invalid window")
			return
		}
		window = parsed
	}

	series, err := h.reader.GetTenantSeries(auth.ContextWithPrincipal(r.Context(), principal), tenantID, window)
	if err != nil {
		h.logger.Error("tenant_series_query_failed", "error", err, "tenant_id", tenantID)
		platform.WriteError(w, http.StatusInternalServerError, "failed to load tenant series")
		return
	}
	platform.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"window":    window.String(),
		"series":    ensureTenantSeries(series),
	})
}

func (h *QueryHandler) handleTopSources(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticateRequest(w, r)
	if !ok {
		return
	}

	tenantID := r.URL.Query().Get("tenantId")
	if principal.Role == auth.RoleTenantUser {
		switch {
		case tenantID == "":
			tenantID = principal.TenantID
		case tenantID != principal.TenantID:
			platform.WriteError(w, http.StatusForbidden, "token tenant does not match requested tenant")
			return
		}
	}
	limit := 10
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			platform.WriteError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}

	sources, err := h.reader.GetTopSources(auth.ContextWithPrincipal(r.Context(), principal), tenantID, limit)
	if err != nil {
		h.logger.Error("top_sources_query_failed", "error", err)
		platform.WriteError(w, http.StatusInternalServerError, "failed to load top sources")
		return
	}
	platform.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"sources":   ensureTopSources(sources),
	})
}

func (h *QueryHandler) handleRejections(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticateRequest(w, r)
	if !ok {
		return
	}

	limit := 10
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			platform.WriteError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}

	rejections, err := h.reader.RecentRejections(auth.ContextWithPrincipal(r.Context(), principal), limit)
	if err != nil {
		h.logger.Error("rejections_query_failed", "error", err)
		platform.WriteError(w, http.StatusInternalServerError, "failed to load recent rejections")
		return
	}
	platform.WriteJSON(w, http.StatusOK, map[string]any{"rejections": ensureRecentRejections(rejections)})
}

func (h *QueryHandler) authenticateRequest(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	if h.verifier == nil {
		return auth.Principal{Role: auth.RoleAdmin, Subject: "auth-disabled"}, true
	}

	principal, err := h.verifier.ParseRequest(r)
	if err != nil {
		platform.WriteError(w, http.StatusUnauthorized, "bearer token is required")
		return auth.Principal{}, false
	}
	return principal, true
}

func ensureTenantSeries(series []store.TenantBucket) []store.TenantBucket {
	if series == nil {
		return []store.TenantBucket{}
	}
	return series
}

func ensureTopSources(sources []store.SourceMetric) []store.SourceMetric {
	if sources == nil {
		return []store.SourceMetric{}
	}
	return sources
}

func ensureRecentRejections(rejections []store.RecentRejection) []store.RecentRejection {
	if rejections == nil {
		return []store.RecentRejection{}
	}
	return rejections
}
