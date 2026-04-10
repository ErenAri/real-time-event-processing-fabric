package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"pulsestream/internal/events"
)

type EventPublisher interface {
	PublishEvent(ctx context.Context, event events.TelemetryEvent) error
	Close() error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

type KafkaPublisherConfig struct {
	BatchTimeout time.Duration
	BatchSize    int
}

func NewKafkaPublisher(brokers []string, topic string, config KafkaPublisherConfig) *KafkaPublisher {
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 5 * time.Millisecond
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 256
	}

	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			RequiredAcks:           kafka.RequireOne,
			BatchTimeout:           config.BatchTimeout,
			BatchSize:              config.BatchSize,
			AllowAutoTopicCreation: true,
			Balancer:               &kafka.Hash{},
		},
	}
}

func (p *KafkaPublisher) PublishEvent(ctx context.Context, event events.TelemetryEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(event.PartitionKey()),
		Value: payload,
		Time:  event.Timestamp,
	}
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish to kafka: %w", err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
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
