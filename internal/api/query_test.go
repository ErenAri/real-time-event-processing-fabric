package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pulsestream/internal/auth"
	"pulsestream/internal/evidence"
	"pulsestream/internal/platform"
	"pulsestream/internal/store"
)

type fakeMetricsReader struct {
	overview         store.Overview
	tenantSeries     []store.TenantBucket
	windows          []store.WindowBucket
	topSources       []store.SourceMetric
	rejections       []store.RecentRejection
	lastTenantID     string
	lastWindowTenant string
	lastTopTenant    string
}

func (f *fakeMetricsReader) GetOverview(context.Context) (store.Overview, error) {
	return f.overview, nil
}

func (f *fakeMetricsReader) GetTenantSeries(_ context.Context, tenantID string, _ time.Duration) ([]store.TenantBucket, error) {
	f.lastTenantID = tenantID
	return f.tenantSeries, nil
}

func (f *fakeMetricsReader) GetEventWindows(_ context.Context, tenantID string, _ string, _ time.Duration, _ time.Duration) ([]store.WindowBucket, error) {
	f.lastWindowTenant = tenantID
	return f.windows, nil
}

func (f *fakeMetricsReader) GetTopSources(_ context.Context, tenantID string, _ int) ([]store.SourceMetric, error) {
	f.lastTopTenant = tenantID
	return f.topSources, nil
}

func (f *fakeMetricsReader) RecentRejections(context.Context, int) ([]store.RecentRejection, error) {
	return f.rejections, nil
}

func TestQueryHandlerRejectsMissingBearerToken(t *testing.T) {
	verifier, err := auth.NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	handler := NewQueryHandler(platform.NewLogger("test"), &fakeMetricsReader{}, verifier, prometheus.NewRegistry())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/overview", nil)
	rec := httptest.NewRecorder()

	handler.handleOverview(rec, request)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestQueryHandlerRejectsTenantMismatch(t *testing.T) {
	verifier, err := auth.NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	token, err := auth.SignHS256("secret-value", auth.NewClaims(
		auth.RoleTenantUser,
		"tenant_01",
		"user-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	handler := NewQueryHandler(platform.NewLogger("test"), &fakeMetricsReader{}, verifier, prometheus.NewRegistry())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/tenants/tenant_02?window=15m", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.handleTenantSeries(rec, request)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestQueryHandlerDefaultsTopSourcesToTokenTenant(t *testing.T) {
	verifier, err := auth.NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	token, err := auth.SignHS256("secret-value", auth.NewClaims(
		auth.RoleTenantUser,
		"tenant_01",
		"user-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	reader := &fakeMetricsReader{
		topSources: []store.SourceMetric{{TenantID: "tenant_01", SourceID: "sensor_1", Events: 10}},
	}
	handler := NewQueryHandler(platform.NewLogger("test"), reader, verifier, prometheus.NewRegistry())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/sources/top?limit=5", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.handleTopSources(rec, request)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if reader.lastTopTenant != "tenant_01" {
		t.Fatalf("expected tenant_01, got %q", reader.lastTopTenant)
	}
}

func TestQueryHandlerEmitsEmptyArraysInsteadOfNull(t *testing.T) {
	handler := NewQueryHandler(platform.NewLogger("test"), &fakeMetricsReader{
		overview: store.Overview{},
	}, nil, prometheus.NewRegistry())

	overviewReq := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/overview", nil)
	overviewRec := httptest.NewRecorder()
	handler.handleOverview(overviewRec, overviewReq)
	if overviewRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from overview, got %d", overviewRec.Code)
	}
	var overviewBody map[string]any
	if err := json.Unmarshal(overviewRec.Body.Bytes(), &overviewBody); err != nil {
		t.Fatalf("unmarshal overview body: %v", err)
	}
	if value, ok := overviewBody["recent_rejections"].([]any); !ok || value == nil {
		t.Fatalf("expected recent_rejections to be an array, got %#v", overviewBody["recent_rejections"])
	}

	seriesReq := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/tenants/tenant_01?window=15m", nil)
	seriesRec := httptest.NewRecorder()
	handler.handleTenantSeries(seriesRec, seriesReq)
	if seriesRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from tenant series, got %d", seriesRec.Code)
	}
	var seriesBody map[string]any
	if err := json.Unmarshal(seriesRec.Body.Bytes(), &seriesBody); err != nil {
		t.Fatalf("unmarshal series body: %v", err)
	}
	if value, ok := seriesBody["series"].([]any); !ok || value == nil {
		t.Fatalf("expected series to be an array, got %#v", seriesBody["series"])
	}

	sourcesReq := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/sources/top?limit=5", nil)
	sourcesRec := httptest.NewRecorder()
	handler.handleTopSources(sourcesRec, sourcesReq)
	if sourcesRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from top sources, got %d", sourcesRec.Code)
	}
	var sourcesBody map[string]any
	if err := json.Unmarshal(sourcesRec.Body.Bytes(), &sourcesBody); err != nil {
		t.Fatalf("unmarshal sources body: %v", err)
	}
	if value, ok := sourcesBody["sources"].([]any); !ok || value == nil {
		t.Fatalf("expected sources to be an array, got %#v", sourcesBody["sources"])
	}

	rejectionsReq := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/rejections?limit=5", nil)
	rejectionsRec := httptest.NewRecorder()
	handler.handleRejections(rejectionsRec, rejectionsReq)
	if rejectionsRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from rejections, got %d", rejectionsRec.Code)
	}
	var rejectionsBody map[string]any
	if err := json.Unmarshal(rejectionsRec.Body.Bytes(), &rejectionsBody); err != nil {
		t.Fatalf("unmarshal rejections body: %v", err)
	}
	if value, ok := rejectionsBody["rejections"].([]any); !ok || value == nil {
		t.Fatalf("expected rejections to be an array, got %#v", rejectionsBody["rejections"])
	}
}

func TestQueryHandlerLoadsEvidenceSummary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "latest.json")
	t.Setenv("EVIDENCE_REPORT_PATH", path)

	payload := `{
		"schema_version": 1,
		"generated_at": "2026-04-16T18:00:00Z",
		"status": "available",
		"artifact_root": "artifacts",
		"benchmark": {
			"artifact": "artifacts/benchmarks/benchmark.json",
			"started_at_utc": "2026-04-16T17:59:00Z",
			"completed_at_utc": "2026-04-16T18:00:00Z",
			"target_eps": 5000,
			"accepted_eps": 4800,
			"processed_eps": 4700,
			"query_p95_ms": 12.5,
			"peak_lag": 100,
			"processor_replicas": 3,
			"summary": "test benchmark",
			"gaps": []
		},
		"failure_drills": []
	}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write evidence summary: %v", err)
	}

	handler := NewQueryHandler(platform.NewLogger("test"), &fakeMetricsReader{}, nil, prometheus.NewRegistry())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/evidence/latest", nil)
	rec := httptest.NewRecorder()

	handler.handleEvidenceLatest(rec, request)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body evidence.Summary
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal evidence body: %v", err)
	}
	if body.Status != "available" {
		t.Fatalf("expected available status, got %q", body.Status)
	}
	if body.Benchmark == nil || body.Benchmark.AcceptedEPS != 4800 {
		t.Fatalf("expected benchmark evidence, got %#v", body.Benchmark)
	}
}

func TestQueryHandlerEvidenceRequiresAdmin(t *testing.T) {
	verifier, err := auth.NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	token, err := auth.SignHS256("secret-value", auth.NewClaims(
		auth.RoleTenantUser,
		"tenant_01",
		"user-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	handler := NewQueryHandler(platform.NewLogger("test"), &fakeMetricsReader{}, verifier, prometheus.NewRegistry())
	request := httptest.NewRequest(http.MethodGet, "/api/v1/evidence/latest", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.handleEvidenceLatest(rec, request)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}
