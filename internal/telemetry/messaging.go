package telemetry

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const messagingTracerName = "pulsestream/messaging"

func InjectKafkaHeaders(ctx context.Context, headers *[]kafka.Header) {
	if headers == nil {
		return
	}

	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: headers})
}

func ExtractKafkaContext(ctx context.Context, message kafka.Message) context.Context {
	headers := append([]kafka.Header(nil), message.Headers...)
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &headers})
}

func StartKafkaProducerSpan(
	ctx context.Context,
	topic string,
	key []byte,
	bodySize int,
	messageID string,
	brokers []string,
) (context.Context, trace.Span) {
	return otel.Tracer(messagingTracerName).Start(
		ctx,
		spanName("send", topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(kafkaAttributes(topic, key, bodySize, messageID, brokers)...),
		trace.WithAttributes(
			attribute.String("messaging.operation.name", "send"),
			attribute.String("messaging.operation.type", "send"),
		),
	)
}

func StartKafkaProcessSpan(
	ctx context.Context,
	message kafka.Message,
	consumerGroup string,
	brokers []string,
) (context.Context, trace.Span) {
	parentCtx := ExtractKafkaContext(ctx, message)
	attrs := kafkaAttributes(message.Topic, message.Key, len(message.Value), "", brokers)
	attrs = append(
		attrs,
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.destination.partition.id", strconv.Itoa(message.Partition)),
		attribute.Int64("messaging.kafka.offset", message.Offset),
	)
	if strings.TrimSpace(consumerGroup) != "" {
		attrs = append(attrs, attribute.String("messaging.consumer.group.name", strings.TrimSpace(consumerGroup)))
	}

	return otel.Tracer(messagingTracerName).Start(
		parentCtx,
		spanName("process", message.Topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
}

func StartKafkaCommitSpan(
	ctx context.Context,
	message kafka.Message,
	consumerGroup string,
	brokers []string,
) (context.Context, trace.Span) {
	attrs := kafkaAttributes(message.Topic, message.Key, len(message.Value), "", brokers)
	attrs = append(
		attrs,
		attribute.String("messaging.operation.name", "commit"),
		attribute.String("messaging.operation.type", "settle"),
		attribute.String("messaging.destination.partition.id", strconv.Itoa(message.Partition)),
		attribute.Int64("messaging.kafka.offset", message.Offset),
	)
	if strings.TrimSpace(consumerGroup) != "" {
		attrs = append(attrs, attribute.String("messaging.consumer.group.name", strings.TrimSpace(consumerGroup)))
	}

	return otel.Tracer(messagingTracerName).Start(
		ctx,
		spanName("commit", message.Topic),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

func SetKafkaMessageID(span trace.Span, messageID string) {
	if span == nil || strings.TrimSpace(messageID) == "" {
		return
	}

	span.SetAttributes(attribute.String("messaging.message.id", strings.TrimSpace(messageID)))
}

func RecordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}

	span.RecordError(err)
	span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
}

func kafkaAttributes(topic string, key []byte, bodySize int, messageID string, brokers []string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
	}
	if strings.TrimSpace(topic) != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", strings.TrimSpace(topic)))
	}
	if len(key) > 0 {
		attrs = append(attrs, attribute.String("messaging.kafka.message.key", string(key)))
	}
	if bodySize >= 0 {
		attrs = append(attrs, attribute.Int("messaging.message.body.size", bodySize))
	}
	if strings.TrimSpace(messageID) != "" {
		attrs = append(attrs, attribute.String("messaging.message.id", strings.TrimSpace(messageID)))
	}
	if serverAddress, serverPort, ok := firstBrokerAttributes(brokers); ok {
		attrs = append(attrs, attribute.String("server.address", serverAddress))
		if serverPort > 0 {
			attrs = append(attrs, attribute.Int("server.port", serverPort))
		}
		attrs = append(attrs, attribute.String("peer.service", serverAddress))
	}
	return attrs
}

func firstBrokerAttributes(brokers []string) (string, int, bool) {
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}

		host, portText, err := net.SplitHostPort(broker)
		if err != nil {
			return broker, 0, true
		}

		port, err := strconv.Atoi(portText)
		if err != nil {
			return host, 0, true
		}
		return host, port, true
	}

	return "", 0, false
}

func spanName(operation string, topic string) string {
	operation = strings.TrimSpace(operation)
	topic = strings.TrimSpace(topic)
	if operation == "" {
		operation = "messaging"
	}
	if topic == "" {
		return operation + " kafka"
	}
	return operation + " " + topic
}

type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}

	for _, header := range *c.headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key string, value string) {
	if c.headers == nil {
		return
	}

	for i, header := range *c.headers {
		if strings.EqualFold(header.Key, key) {
			(*c.headers)[i].Key = strings.ToLower(strings.TrimSpace(key))
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}

	*c.headers = append(*c.headers, kafka.Header{
		Key:   strings.ToLower(strings.TrimSpace(key)),
		Value: []byte(value),
	})
}

func (c kafkaHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}

	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}
