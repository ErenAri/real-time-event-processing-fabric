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
	BatchTimeout time.Duration
	BatchSize    int
}

func NewKafkaPublisher(brokers []string, topic string, config KafkaPublisherConfig) *KafkaPublisher {
	return &KafkaPublisher{
		writer:  newKafkaWriter(brokers, topic, config),
		topic:   topic,
		brokers: append([]string(nil), brokers...),
	}
}

func NewKafkaDeadLetterPublisher(brokers []string, topic string, config KafkaPublisherConfig) *KafkaDeadLetterPublisher {
	return &KafkaDeadLetterPublisher{
		writer:  newKafkaWriter(brokers, topic, config),
		topic:   topic,
		brokers: append([]string(nil), brokers...),
	}
}

func newKafkaWriter(brokers []string, topic string, config KafkaPublisherConfig) kafkaMessageWriter {
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 5 * time.Millisecond
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 256
	}

	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		RequiredAcks:           kafka.RequireOne,
		BatchTimeout:           config.BatchTimeout,
		BatchSize:              config.BatchSize,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.Hash{},
	}
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

func NewKafkaReader(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
}
