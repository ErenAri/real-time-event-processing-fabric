package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Status string

const (
	CurrentSchemaVersion = 1

	StatusOK    Status = "ok"
	StatusWarn  Status = "warn"
	StatusError Status = "error"
)

var validStatuses = map[Status]struct{}{
	StatusOK:    {},
	StatusWarn:  {},
	StatusError: {},
}

type TelemetryEvent struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	TenantID      string    `json:"tenant_id"`
	SourceID      string    `json:"source_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	Value         float64   `json:"value"`
	Status        Status    `json:"status"`
	Region        string    `json:"region"`
	Sequence      int64     `json:"sequence"`
}

func (e *TelemetryEvent) Validate() error {
	if e.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("schema_version must be %d", CurrentSchemaVersion)
	}
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event_id is required")
	}
	if strings.TrimSpace(e.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(e.SourceID) == "" {
		return errors.New("source_id is required")
	}
	if strings.TrimSpace(e.EventType) == "" {
		return errors.New("event_type is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	if _, ok := validStatuses[e.Status]; !ok {
		return fmt.Errorf("status must be one of ok, warn, error")
	}
	if e.Sequence <= 0 {
		return errors.New("sequence must be greater than zero")
	}

	e.Timestamp = e.Timestamp.UTC()
	return nil
}

func (e TelemetryEvent) PartitionKey() string {
	return fmt.Sprintf("%s:%s", e.TenantID, e.SourceID)
}

func DecodeTelemetryEvent(payload []byte) (TelemetryEvent, error) {
	var event TelemetryEvent
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return TelemetryEvent{}, fmt.Errorf("decode event: %w", err)
	}

	event.Timestamp = event.Timestamp.UTC()
	return event, nil
}

func EncodeTelemetryEvent(event TelemetryEvent) ([]byte, error) {
	if event.SchemaVersion == 0 {
		event.SchemaVersion = CurrentSchemaVersion
	}
	return json.Marshal(event)
}
