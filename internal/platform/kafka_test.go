package platform

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"

	"pulsestream/internal/deadletter"
	"pulsestream/internal/events"
)

type fakeKafkaWriter struct {
	messages []kafka.Message
}

func (f *fakeKafkaWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	f.messages = append(f.messages, messages...)
	return nil
}

func (f *fakeKafkaWriter) Close() error {
	return nil
}

func TestKafkaPublisherInjectsTraceContextHeaders(t *testing.T) {
	restore := installKafkaTestTelemetry(t)
	defer restore()

	writer := &fakeKafkaWriter{}
	publisher := &KafkaPublisher{
		writer:  writer,
		topic:   "pulsestream.events",
		brokers: []string{"kafka:9092"},
	}

	ctx, span := otel.Tracer("test").Start(context.Background(), "root")
	defer span.End()

	err := publisher.PublishEvent(ctx, events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-1",
		TenantID:      "tenant_01",
		SourceID:      "sensor_01",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         12.5,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	})
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(writer.messages))
	}
	if findHeaderValue(writer.messages[0].Headers, "traceparent") == "" {
		t.Fatal("expected traceparent header to be present on published message")
	}
}

func TestKafkaDeadLetterPublisherInjectsTraceContextHeaders(t *testing.T) {
	restore := installKafkaTestTelemetry(t)
	defer restore()

	writer := &fakeKafkaWriter{}
	publisher := &KafkaDeadLetterPublisher{
		writer:  writer,
		topic:   "pulsestream.events.dlq",
		brokers: []string{"kafka:9092"},
	}

	ctx, span := otel.Tracer("test").Start(context.Background(), "root")
	defer span.End()

	err := publisher.PublishDeadLetter(ctx, deadletter.Record{
		FailedAt:      time.Now().UTC(),
		Reason:        "decode_failed",
		SourceTopic:   "pulsestream.events",
		SourceOffset:  42,
		SourceKey:     "tenant_01:sensor_01",
		PayloadBase64: "e30=",
	})
	if err != nil {
		t.Fatalf("publish dead-letter record: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected one published message, got %d", len(writer.messages))
	}
	if findHeaderValue(writer.messages[0].Headers, "traceparent") == "" {
		t.Fatal("expected traceparent header to be present on dead-letter message")
	}
	if findHeaderValue(writer.messages[0].Headers, "dead-letter-reason") != "decode_failed" {
		t.Fatalf("expected dead-letter-reason header, got %q", findHeaderValue(writer.messages[0].Headers, "dead-letter-reason"))
	}
}

func installKafkaTestTelemetry(t *testing.T) func() {
	t.Helper()

	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	provider := tracesdk.NewTracerProvider()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	}
}

func findHeaderValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
