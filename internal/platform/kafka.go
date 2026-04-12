package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"pulsestream/internal/deadletter"
	"pulsestream/internal/events"
	"pulsestream/internal/telemetry"
)

type EventPublisher interface {
	PublishEvent(ctx context.Context, event events.TelemetryEvent) error
	Close() error
}

type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, record deadletter.Record) error
	Close() error
}

type kafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	writer  kafkaMessageWriter
	topic   string
	brokers []string
}

type KafkaDeadLetterPublisher struct {
	writer  kafkaMessageWriter
	topic   string
	brokers []string
}

type KafkaPublisherConfig struct {
	BatchTimeout    time.Duration
	BatchSize       int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxAttempts     int
	WriteBackoffMin time.Duration
	WriteBackoffMax time.Duration
}

func NewKafkaPublisher(brokers []string, topic string, config KafkaPublisherConfig, connectionConfig KafkaConnectionConfig) (*KafkaPublisher, error) {
	writer, err := connectionConfig.newWriter(brokers, topic, config)
	if err != nil {
		return nil, err
	}

	return &KafkaPublisher{
		writer:  writer,
		topic:   topic,
		brokers: append([]string(nil), brokers...),
	}, nil
}

func NewKafkaDeadLetterPublisher(brokers []string, topic string, config KafkaPublisherConfig, connectionConfig KafkaConnectionConfig) (*KafkaDeadLetterPublisher, error) {
	writer, err := connectionConfig.newWriter(brokers, topic, config)
	if err != nil {
		return nil, err
	}

	return &KafkaDeadLetterPublisher{
		writer:  writer,
		topic:   topic,
		brokers: append([]string(nil), brokers...),
	}, nil
}

func (p *KafkaPublisher) PublishEvent(ctx context.Context, event events.TelemetryEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	ctx, span := telemetry.StartKafkaProducerSpan(ctx, p.topic, []byte(event.PartitionKey()), len(payload), event.EventID, p.brokers)
	defer span.End()

	message := kafka.Message{
		Key:   []byte(event.PartitionKey()),
		Value: payload,
		Time:  event.Timestamp,
	}
	telemetry.InjectKafkaHeaders(ctx, &message.Headers)
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		telemetry.RecordSpanError(span, err)
		return fmt.Errorf("publish to kafka: %w", err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

func (p *KafkaDeadLetterPublisher) PublishDeadLetter(ctx context.Context, record deadletter.Record) error {
	payload, err := deadletter.Encode(record)
	if err != nil {
		return fmt.Errorf("marshal dead-letter record: %w", err)
	}

	ctx, span := telemetry.StartKafkaProducerSpan(ctx, p.topic, []byte(record.SourceKey), len(payload), record.EventID, p.brokers)
	defer span.End()

	message := kafka.Message{
		Key:   []byte(record.SourceKey),
		Value: payload,
		Time:  record.FailedAt,
		Headers: []kafka.Header{
			{Key: "dead-letter-reason", Value: []byte(record.Reason)},
		},
	}
	telemetry.InjectKafkaHeaders(ctx, &message.Headers)
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		telemetry.RecordSpanError(span, err)
		return fmt.Errorf("publish dead-letter message: %w", err)
	}
	return nil
}

func (p *KafkaDeadLetterPublisher) Close() error {
	return p.writer.Close()
}

func NewKafkaReader(brokers []string, topic string, groupID string, connectionConfig KafkaConnectionConfig) (*kafka.Reader, error) {
	return connectionConfig.NewReader(topic, groupID, brokers)
}
