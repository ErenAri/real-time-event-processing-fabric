package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestVerifierParsesTenantToken(t *testing.T) {
	verifier, err := NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	token, err := SignHS256("secret-value", NewClaims(
		RoleTenantUser,
		"tenant_01",
		"user-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/overview", nil)
	request.Header.Set("Authorization", "Bearer "+token)

	principal, err := verifier.ParseRequest(request)
	if err != nil {
		t.Fatalf("parse request: %v", err)
	}
	if principal.Role != RoleTenantUser {
		t.Fatalf("expected tenant_user role, got %q", principal.Role)
	}
	if principal.TenantID != "tenant_01" {
		t.Fatalf("expected tenant_01, got %q", principal.TenantID)
	}
}

func TestSignHS256RejectsTenantTokenWithoutTenantID(t *testing.T) {
	_, err := SignHS256("secret-value", NewClaims(
		RoleTenantUser,
		"",
		"user-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err == nil {
		t.Fatal("expected tenant token without tenant_id to fail")
	}
}

func TestBearerTokenParsesAuthorizationHeader(t *testing.T) {
	token := BearerToken("Bearer abc.def.ghi")
	if token != "abc.def.ghi" {
		t.Fatalf("expected token to be extracted, got %q", token)
	}
}
