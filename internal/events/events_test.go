package events

import (
	"testing"
	"time"
)

func TestTelemetryEventValidate(t *testing.T) {
	event := TelemetryEvent{
		SchemaVersion: CurrentSchemaVersion,
		EventID:       "evt-1",
		TenantID:      "tenant_1",
		SourceID:      "sensor_1",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         73.4,
		Status:        StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	}

	if err := event.Validate(); err != nil {
		t.Fatalf("expected valid event, got %v", err)
	}
}

func TestTelemetryEventValidateRejectsMissingFields(t *testing.T) {
	event := TelemetryEvent{Status: StatusOK}
	if err := event.Validate(); err == nil {
		t.Fatal("expected validation error for missing required fields")
	}
}

func TestDecodeTelemetryEventRejectsUnknownFields(t *testing.T) {
	_, err := DecodeTelemetryEvent([]byte(`{
		"schema_version":1,
		"event_id":"evt-1",
		"tenant_id":"tenant_1",
		"source_id":"sensor_1",
		"event_type":"telemetry",
		"timestamp":"2026-04-10T12:00:00Z",
		"value":12.2,
		"status":"ok",
		"region":"eu-west",
		"sequence":1,
		"unknown":"boom"
	}`))
	if err == nil {
		t.Fatal("expected decode error for unknown field")
	}
}

func TestTelemetryEventValidateRejectsWrongSchemaVersion(t *testing.T) {
	event := TelemetryEvent{
		SchemaVersion: 99,
		EventID:       "evt-1",
		TenantID:      "tenant_1",
		SourceID:      "sensor_1",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         12.3,
		Status:        StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	}

	if err := event.Validate(); err == nil {
		t.Fatal("expected schema version validation error")
	}
}
