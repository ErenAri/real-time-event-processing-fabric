package telemetry

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaHeaderPropagationRoundTrip(t *testing.T) {
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
	defer func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	}()

	member, err := baggage.NewMember("tenant", "tenant_01")
	if err != nil {
		t.Fatalf("create baggage member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("create baggage: %v", err)
	}

	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx, span := otel.Tracer("test").Start(ctx, "root")
	defer span.End()

	message := kafka.Message{}
	InjectKafkaHeaders(ctx, &message.Headers)

	if getKafkaHeaderValue(message.Headers, "traceparent") == "" {
		t.Fatal("expected traceparent header to be injected")
	}
	if getKafkaHeaderValue(message.Headers, "baggage") == "" {
		t.Fatal("expected baggage header to be injected")
	}

	extracted := ExtractKafkaContext(context.Background(), message)
	spanContext := trace.SpanContextFromContext(extracted)
	if !spanContext.IsValid() {
		t.Fatal("expected extracted span context to be valid")
	}
	if !spanContext.IsRemote() {
		t.Fatal("expected extracted span context to be marked remote")
	}
	if spanContext.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("trace id mismatch: got %s want %s", spanContext.TraceID(), span.SpanContext().TraceID())
	}

	extractedBag := baggage.FromContext(extracted)
	member = extractedBag.Member("tenant")
	if member.Value() != "tenant_01" {
		t.Fatalf("baggage mismatch: got %q want %q", member.Value(), "tenant_01")
	}
}

func getKafkaHeaderValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
