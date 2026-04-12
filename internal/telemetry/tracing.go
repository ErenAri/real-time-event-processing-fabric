package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

func Configure(ctx context.Context, serviceName string, logger *slog.Logger) (func(context.Context) error, error) {
	exporterName := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")))
	if exporterName == "" {
		exporterName = "none"
	}

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	if exporterName == "none" {
		return func(context.Context) error { return nil }, nil
	}
	if exporterName != "stdout" {
		return nil, fmt.Errorf("unsupported OTEL_TRACES_EXPORTER %q", exporterName)
	}

	exporter, err := stdouttrace.New(stdouttrace.WithoutTimestamps())
	if err != nil {
		return nil, fmt.Errorf("create stdout trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	sampleRatio := 1.0
	if raw := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLE_RATIO")); raw != "" {
		sampleRatio, err = strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("parse OTEL_TRACES_SAMPLE_RATIO: %w", err)
		}
		if sampleRatio < 0 || sampleRatio > 1 {
			return nil, fmt.Errorf("OTEL_TRACES_SAMPLE_RATIO must be between 0 and 1")
		}
	}

	provider := tracesdk.NewTracerProvider(
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(sampleRatio)),
		tracesdk.WithBatcher(exporter),
		tracesdk.WithResource(res),
	)

	otel.SetTracerProvider(provider)
	logger.Info("otel_configured", "exporter", "stdout", "sample_ratio", sampleRatio)

	return provider.Shutdown, nil
}

func InstrumentHTTP(operation string, next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, operation)
}
