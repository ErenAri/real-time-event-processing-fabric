package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pulsestream/internal/auth"
	"pulsestream/internal/platform"
	"pulsestream/internal/store"
)

type fakeMetricsReader struct {
	overview      store.Overview
	tenantSeries  []store.TenantBucket
	topSources    []store.SourceMetric
	rejections    []store.RecentRejection
	lastTenantID  string
	lastTopTenant string
}

func (f *fakeMetricsReader) GetOverview(context.Context) (store.Overview, error) {
	return f.overview, nil
}

func (f *fakeMetricsReader) GetTenantSeries(_ context.Context, tenantID string, _ time.Duration) ([]store.TenantBucket, error) {
	f.lastTenantID = tenantID
	return f.tenantSeries, nil
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
