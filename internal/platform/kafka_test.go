package platform

import (
	"context"
	"strings"
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

func TestLoadKafkaConnectionConfigDefaultsToPlaintext(t *testing.T) {
	config, err := LoadKafkaConnectionConfigFromEnv()
	if err != nil {
		t.Fatalf("load kafka config: %v", err)
	}

	if config.SecurityProtocol != KafkaSecurityProtocolPlaintext {
		t.Fatalf("expected plaintext security protocol, got %q", config.SecurityProtocol)
	}
	if !config.AllowAutoTopicCreation {
		t.Fatal("expected auto topic creation to default to true")
	}

	reader, err := config.NewReader("pulsestream.events", "group-a", []string{"localhost:9092"})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer reader.Close()
}

func TestLoadKafkaConnectionConfigSupportsEventHubsSASLSSL(t *testing.T) {
	t.Setenv("KAFKA_SECURITY_PROTOCOL", KafkaSecurityProtocolSASLSSL)
	t.Setenv("KAFKA_SASL_MECHANISM", KafkaSASLMechanismPlain)
	t.Setenv("KAFKA_SASL_USERNAME", "$ConnectionString")
	t.Setenv("KAFKA_SASL_PASSWORD", "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=secret")
	t.Setenv("KAFKA_TLS_SERVER_NAME", "namespace.servicebus.windows.net")
	t.Setenv("KAFKA_ALLOW_AUTO_TOPIC_CREATION", "false")

	config, err := LoadKafkaConnectionConfigFromEnv()
	if err != nil {
		t.Fatalf("load kafka config: %v", err)
	}

	if config.SecurityProtocol != KafkaSecurityProtocolSASLSSL {
		t.Fatalf("expected SASL_SSL security protocol, got %q", config.SecurityProtocol)
	}
	if config.AllowAutoTopicCreation {
		t.Fatal("expected auto topic creation to be disabled")
	}

	writer, err := config.newWriter([]string{"namespace.servicebus.windows.net:9093"}, "pulsestream.events", KafkaPublisherConfig{})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer writer.Close()

	if !writer.AllowAutoTopicCreation {
		// value should track config; this branch is only to force the next assertion block.
	} else {
		t.Fatal("expected writer auto topic creation to be disabled")
	}

	transport, ok := writer.Transport.(*kafka.Transport)
	if !ok {
		t.Fatalf("expected kafka transport, got %T", writer.Transport)
	}
	if transport.TLS == nil {
		t.Fatal("expected TLS config to be set")
	}
	if transport.SASL == nil {
		t.Fatal("expected SASL mechanism to be set")
	}
}

func TestNewWriterAppliesOperationalTimeoutsAndRetries(t *testing.T) {
	config := KafkaConnectionConfig{
		SecurityProtocol:       KafkaSecurityProtocolPlaintext,
		AllowAutoTopicCreation: true,
	}

	writer, err := config.newWriter([]string{"localhost:9092"}, "pulsestream.events", KafkaPublisherConfig{
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxAttempts:  2,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer writer.Close()

	if writer.ReadTimeout != 2*time.Second {
		t.Fatalf("expected read timeout to be applied, got %s", writer.ReadTimeout)
	}
	if writer.WriteTimeout != 3*time.Second {
		t.Fatalf("expected write timeout to be applied, got %s", writer.WriteTimeout)
	}
	if writer.MaxAttempts != 2 {
		t.Fatalf("expected max attempts to be applied, got %d", writer.MaxAttempts)
	}
}

func TestLoadKafkaConnectionConfigRejectsInvalidSASLCombination(t *testing.T) {
	t.Setenv("KAFKA_SECURITY_PROTOCOL", KafkaSecurityProtocolSASLSSL)
	t.Setenv("KAFKA_SASL_MECHANISM", KafkaSASLMechanismPlain)
	t.Setenv("KAFKA_SASL_USERNAME", "$ConnectionString")

	_, err := LoadKafkaConnectionConfigFromEnv()
	if err == nil {
		t.Fatal("expected configuration error for missing SASL password")
	}
	if !strings.Contains(err.Error(), "KAFKA_SASL_PASSWORD") {
		t.Fatalf("expected SASL password error, got %v", err)
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
