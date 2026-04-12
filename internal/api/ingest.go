package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	eventarchive "pulsestream/internal/archive"
	"pulsestream/internal/auth"
	"pulsestream/internal/events"
	"pulsestream/internal/platform"
	"pulsestream/internal/store"
)

type RejectionRecorder interface {
	RecordRejection(ctx context.Context, record store.RejectionRecord) error
}

type RawArchiver interface {
	Archive(ctx context.Context, event events.TelemetryEvent, rawPayload []byte) error
}

type RawReplayer interface {
	Replay(
		ctx context.Context,
		filter eventarchive.ReplayFilter,
		publish func(context.Context, events.TelemetryEvent) error,
	) (eventarchive.ReplayResult, error)
}

type IngestHandler struct {
	logger         *slog.Logger
	publisher      platform.EventPublisher
	rejectionStore RejectionRecorder
	archiver       RawArchiver
	replayer       RawReplayer
	verifier       *auth.Verifier
	adminToken     string
	startedAt      time.Time

	acceptedTotal atomic.Int64
	rejectedTotal atomic.Int64
	inFlightTotal atomic.Int64

	inFlightLimit chan struct{}

	requestDuration  prometheus.Histogram
	publishDuration  prometheus.Histogram
	archiveDuration  prometheus.Histogram
	acceptedCounter  prometheus.Counter
	archivedCounter  prometheus.Counter
	replayCounter    prometheus.Counter
	rejectedCounters *prometheus.CounterVec
	inFlightGauge    prometheus.Gauge
}

func NewIngestHandler(
	logger *slog.Logger,
	publisher platform.EventPublisher,
	rejectionStore RejectionRecorder,
	archiver RawArchiver,
	replayer RawReplayer,
	verifier *auth.Verifier,
	adminToken string,
	registry *prometheus.Registry,
) *IngestHandler {
	handler := &IngestHandler{
		logger:         logger,
		publisher:      publisher,
		rejectionStore: rejectionStore,
		archiver:       archiver,
		replayer:       replayer,
		verifier:       verifier,
		adminToken:     strings.TrimSpace(adminToken),
		startedAt:      time.Now().UTC(),
		requestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_ingest_request_duration_seconds",
			Help:    "Duration of ingest requests.",
			Buckets: prometheus.DefBuckets,
		}),
		publishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_ingest_publish_duration_seconds",
			Help:    "Kafka publish duration for accepted events.",
			Buckets: prometheus.DefBuckets,
		}),
		archiveDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_ingest_archive_duration_seconds",
			Help:    "Duration of raw archive writes for valid events.",
			Buckets: prometheus.DefBuckets,
		}),
		acceptedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_accepted_total",
			Help: "Number of accepted events.",
		}),
		archivedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_archived_total",
			Help: "Number of valid events written to the raw archive.",
		}),
		replayCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_replayed_total",
			Help: "Number of archived events replayed through the admin endpoint.",
		}),
		inFlightGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsestream_ingest_inflight_requests",
			Help: "Number of ingest requests currently in archive/publish processing.",
		}),
		rejectedCounters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulsestream_ingest_rejected_total",
			Help: "Number of rejected events by reason.",
		}, []string{"reason"}),
	}

	registry.MustRegister(
		handler.requestDuration,
		handler.publishDuration,
		handler.archiveDuration,
		handler.acceptedCounter,
		handler.archivedCounter,
		handler.replayCounter,
		handler.inFlightGauge,
		handler.rejectedCounters,
	)

	return handler
}

func (h *IngestHandler) SetMaxInFlight(limit int) {
	if limit <= 0 {
		h.inFlightLimit = nil
		return
	}
	h.inFlightLimit = make(chan struct{}, limit)
}

func (h *IngestHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/events", h.handleEvents)
	mux.HandleFunc("/api/v1/admin/replay", h.handleReplay)
}

func (h *IngestHandler) Snapshot() store.IngestState {
	return store.IngestState{
		AcceptedTotal: h.acceptedTotal.Load(),
		RejectedTotal: h.rejectedTotal.Load(),
		LastSeenAt:    time.Now().UTC(),
		UptimeSeconds: int64(time.Since(h.startedAt).Seconds()),
	}
}

func (h *IngestHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		h.requestDuration.Observe(time.Since(start).Seconds())
	}()

	if r.Method != http.MethodPost {
		platform.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		h.reject(r.Context(), body, "", "", "read_body_failed")
		platform.WriteError(w, http.StatusBadRequest, "unable to read request body")
		return
	}

	event, err := events.DecodeTelemetryEvent(body)
	if err != nil {
		h.reject(r.Context(), body, "", "", "decode_failed")
		platform.WriteError(w, http.StatusBadRequest, "invalid event payload")
		return
	}

	principal, ok := h.authorizeEventIngest(w, r, event.TenantID)
	if !ok {
		return
	}

	if err := event.Validate(); err != nil {
		h.reject(r.Context(), body, event.TenantID, event.SourceID, "validation_failed")
		platform.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := auth.ContextWithPrincipal(r.Context(), principal)
	if !h.tryAcquireInFlight() {
		h.reject(ctx, body, event.TenantID, event.SourceID, "backpressure")
		platform.WriteError(w, http.StatusServiceUnavailable, "ingest service is overloaded")
		return
	}
	defer h.releaseInFlight()

	if h.archiver != nil {
		archiveStart := time.Now()
		if err := h.archiver.Archive(ctx, event, body); err != nil {
			h.reject(ctx, body, event.TenantID, event.SourceID, "archive_failed")
			h.logger.Error("archive_event_failed", "error", err, "tenant_id", event.TenantID, "source_id", event.SourceID)
			platform.WriteError(w, http.StatusBadGateway, "failed to archive event")
			return
		}
		h.archiveDuration.Observe(time.Since(archiveStart).Seconds())
		h.archivedCounter.Inc()
	}

	publishStart := time.Now()
	if err := h.publisher.PublishEvent(ctx, event); err != nil {
		h.reject(ctx, body, event.TenantID, event.SourceID, "publish_failed")
		h.logger.Error("publish_event_failed", "error", err, "tenant_id", event.TenantID, "source_id", event.SourceID)
		platform.WriteError(w, http.StatusBadGateway, "failed to publish event")
		return
	}
	h.publishDuration.Observe(time.Since(publishStart).Seconds())
	h.acceptedCounter.Inc()
	h.acceptedTotal.Add(1)

	platform.WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":    "accepted",
		"event_id":  event.EventID,
		"tenant_id": event.TenantID,
	})
}

func (h *IngestHandler) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		platform.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.replayer == nil || h.publisher == nil {
		platform.WriteError(w, http.StatusServiceUnavailable, "replay is not configured")
		return
	}
	principal, ok := h.authorizeReplay(w, r)
	if !ok {
		return
	}

	var request struct {
		StartDate string `json:"start_date"`
		EndDate   string `json:"end_date"`
		TenantID  string `json:"tenant_id"`
		Limit     int    `json:"limit"`
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		platform.WriteError(w, http.StatusBadRequest, "invalid replay request")
		return
	}

	startDate, err := eventarchive.ParseDate(request.StartDate)
	if err != nil {
		platform.WriteError(w, http.StatusBadRequest, "start_date must use YYYY-MM-DD")
		return
	}

	endDate := startDate
	if strings.TrimSpace(request.EndDate) != "" {
		endDate, err = eventarchive.ParseDate(request.EndDate)
		if err != nil {
			platform.WriteError(w, http.StatusBadRequest, "end_date must use YYYY-MM-DD")
			return
		}
	}
	if request.Limit < 0 {
		platform.WriteError(w, http.StatusBadRequest, "limit must be zero or greater")
		return
	}

	filter := eventarchive.ReplayFilter{
		StartDate: startDate,
		EndDate:   endDate,
		TenantID:  strings.TrimSpace(request.TenantID),
		Limit:     request.Limit,
	}
	ctx := auth.ContextWithPrincipal(r.Context(), principal)
	result, err := h.replayer.Replay(ctx, filter, h.publisher.PublishEvent)
	if err != nil {
		h.logger.Error("replay_archive_failed", "error", err, "tenant_id", filter.TenantID)
		platform.WriteError(w, http.StatusBadGateway, "failed to replay archived events")
		return
	}

	h.replayCounter.Add(float64(result.Replayed))
	h.logger.Info(
		"archive_replay_completed",
		"start_date", result.StartDate.Format("2006-01-02"),
		"end_date", result.EndDate.Format("2006-01-02"),
		"tenant_id", result.TenantID,
		"replayed", result.Replayed,
		"scanned", result.Scanned,
	)

	platform.WriteJSON(w, http.StatusOK, map[string]any{
		"status": "completed",
		"replay": result,
	})
}

func (h *IngestHandler) reject(ctx context.Context, payload []byte, tenantID string, sourceID string, reason string) {
	h.rejectedCounters.WithLabelValues(reason).Inc()
	h.rejectedTotal.Add(1)
	if h.rejectionStore == nil {
		return
	}

	if !json.Valid(payload) {
		payload = []byte(`{"raw":"payload_not_valid_json"}`)
	}

	record := store.RejectionRecord{
		Reason:   reason,
		TenantID: tenantID,
		SourceID: sourceID,
		Payload:  payload,
	}
	if err := h.rejectionStore.RecordRejection(ctx, record); err != nil && !errors.Is(err, context.Canceled) {
		h.logger.Error("record_rejection_failed", "error", err, "reason", reason)
	}
}

func (h *IngestHandler) authorizeEventIngest(w http.ResponseWriter, r *http.Request, tenantID string) (auth.Principal, bool) {
	principal, err := h.authenticateRequest(r)
	if err != nil {
		platform.WriteError(w, http.StatusUnauthorized, "bearer token is required")
		return auth.Principal{}, false
	}

	switch principal.Role {
	case auth.RoleAdmin:
		return principal, true
	case auth.RoleTenantUser:
		if strings.TrimSpace(tenantID) == "" || principal.TenantID != strings.TrimSpace(tenantID) {
			platform.WriteError(w, http.StatusForbidden, "token tenant does not match event tenant")
			return auth.Principal{}, false
		}
		return principal, true
	default:
		platform.WriteError(w, http.StatusForbidden, "role is not allowed to publish events")
		return auth.Principal{}, false
	}
}

func (h *IngestHandler) authorizeReplay(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	if h.verifier != nil {
		principal, err := h.verifier.ParseRequest(r)
		if err == nil && principal.Role == auth.RoleAdmin {
			return principal, true
		}
	}

	if h.authorizeAdminToken(r) {
		return auth.Principal{Role: auth.RoleAdmin, Subject: "legacy-admin-token"}, true
	}

	platform.WriteError(w, http.StatusUnauthorized, "admin token is required")
	return auth.Principal{}, false
}

func (h *IngestHandler) authenticateRequest(r *http.Request) (auth.Principal, error) {
	if h.verifier == nil {
		return auth.Principal{Role: auth.RoleAdmin, Subject: "auth-disabled"}, nil
	}
	return h.verifier.ParseRequest(r)
}

func (h *IngestHandler) authorizeAdminToken(r *http.Request) bool {
	if h.adminToken == "" {
		return false
	}

	token := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
	if token == "" {
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(authorization) >= 7 && strings.EqualFold(authorization[:7], "Bearer ") {
			token = strings.TrimSpace(authorization[7:])
		}
	}

	return subtle.ConstantTimeCompare([]byte(token), []byte(h.adminToken)) == 1
}

func (h *IngestHandler) tryAcquireInFlight() bool {
	if h.inFlightLimit != nil {
		select {
		case h.inFlightLimit <- struct{}{}:
		default:
			return false
		}
	}

	total := h.inFlightTotal.Add(1)
	h.inFlightGauge.Set(float64(total))
	return true
}

func (h *IngestHandler) releaseInFlight() {
	if h.inFlightLimit != nil {
		select {
		case <-h.inFlightLimit:
		default:
		}
	}

	total := h.inFlightTotal.Add(-1)
	if total < 0 {
		total = 0
		h.inFlightTotal.Store(0)
	}
	h.inFlightGauge.Set(float64(total))
}
