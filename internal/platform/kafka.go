package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"

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

	batchRequests chan kafkaPublishRequest
	batchSize     int
	batchInterval time.Duration
	writeTimeout  time.Duration
	batchMu       sync.RWMutex
	closed        bool
	batchWG       sync.WaitGroup
	closeOnce     sync.Once
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
	EnableBatcher   bool
	QueueCapacity   int
	FlushInterval   time.Duration
}

type kafkaPublishRequest struct {
	ctx    context.Context
	event  events.TelemetryEvent
	result chan error
}

func NewKafkaPublisher(brokers []string, topic string, config KafkaPublisherConfig, connectionConfig KafkaConnectionConfig) (*KafkaPublisher, error) {
	writer, err := connectionConfig.newWriter(brokers, topic, config)
	if err != nil {
		return nil, err
	}

	publisher := &KafkaPublisher{
		writer:       writer,
		topic:        topic,
		brokers:      append([]string(nil), brokers...),
		writeTimeout: config.WriteTimeout,
	}
	if config.EnableBatcher {
		publisher.startBatcher(config)
	}
	return publisher, nil
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
	if p.batchRequests != nil {
		return p.publishEventBatched(ctx, event)
	}
	return p.writeEvent(ctx, event)
}

func (p *KafkaPublisher) PublishEvents(ctx context.Context, eventList []events.TelemetryEvent) error {
	if len(eventList) == 0 {
		return nil
	}
	if len(eventList) == 1 {
		return p.PublishEvent(ctx, eventList[0])
	}
	return p.writeEvents(ctx, eventList)
}

func (p *KafkaPublisher) writeEvent(ctx context.Context, event events.TelemetryEvent) error {
	return p.writeEvents(ctx, []events.TelemetryEvent{event})
}

func (p *KafkaPublisher) writeEvents(ctx context.Context, eventList []events.TelemetryEvent) error {
	if len(eventList) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(eventList))
	spans := make([]trace.Span, 0, len(eventList))
	for _, event := range eventList {
		payload, err := json.Marshal(event)
		if err != nil {
			for _, span := range spans {
				telemetry.RecordSpanError(span, err)
				span.End()
			}
			return fmt.Errorf("marshal event: %w", err)
		}

		spanCtx, span := telemetry.StartKafkaProducerSpan(ctx, p.topic, []byte(event.PartitionKey()), len(payload), event.EventID, p.brokers)
		message := kafka.Message{
			Key:   []byte(event.PartitionKey()),
			Value: payload,
			Time:  event.Timestamp,
		}
		telemetry.InjectKafkaHeaders(spanCtx, &message.Headers)
		messages = append(messages, message)
		spans = append(spans, span)
	}

	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		err = fmt.Errorf("publish to kafka: %w", err)
	}
	for _, span := range spans {
		if err != nil {
			telemetry.RecordSpanError(span, err)
		}
		span.End()
	}
	return err
}

func (p *KafkaPublisher) startBatcher(config KafkaPublisherConfig) {
	if config.QueueCapacity <= 0 {
		config.QueueCapacity = 10000
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = config.BatchTimeout
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 5 * time.Millisecond
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 256
	}
	if p.writeTimeout <= 0 {
		p.writeTimeout = 5 * time.Second
	}

	p.batchRequests = make(chan kafkaPublishRequest, config.QueueCapacity)
	p.batchSize = config.BatchSize
	p.batchInterval = config.FlushInterval
	p.batchWG.Add(1)
	go p.runBatcher()
}

func (p *KafkaPublisher) publishEventBatched(ctx context.Context, event events.TelemetryEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	request := kafkaPublishRequest{
		ctx:    ctx,
		event:  event,
		result: make(chan error, 1),
	}
	p.batchMu.RLock()
	if p.closed {
		p.batchMu.RUnlock()
		return fmt.Errorf("kafka publisher closed")
	}
	select {
	case p.batchRequests <- request:
		p.batchMu.RUnlock()
	case <-ctx.Done():
		p.batchMu.RUnlock()
		return ctx.Err()
	}

	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *KafkaPublisher) runBatcher() {
	defer p.batchWG.Done()

	ticker := time.NewTicker(p.batchInterval)
	defer ticker.Stop()

	batch := make([]kafkaPublishRequest, 0, p.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.flushPublishBatch(batch)
		batch = batch[:0]
	}

	for {
		select {
		case request, ok := <-p.batchRequests:
			if !ok {
				flush()
				return
			}
			batch = append(batch, request)
			if len(batch) >= p.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *KafkaPublisher) flushPublishBatch(requests []kafkaPublishRequest) {
	messages := make([]kafka.Message, 0, len(requests))
	requestIndexes := make([]int, 0, len(requests))
	spans := make([]trace.Span, 0, len(requests))
	for index, request := range requests {
		payload, err := json.Marshal(request.event)
		if err != nil {
			request.result <- fmt.Errorf("marshal event: %w", err)
			continue
		}

		ctx, span := telemetry.StartKafkaProducerSpan(
			request.ctx,
			p.topic,
			[]byte(request.event.PartitionKey()),
			len(payload),
			request.event.EventID,
			p.brokers,
		)
		message := kafka.Message{
			Key:   []byte(request.event.PartitionKey()),
			Value: payload,
			Time:  request.event.Timestamp,
		}
		telemetry.InjectKafkaHeaders(ctx, &message.Headers)

		messages = append(messages, message)
		requestIndexes = append(requestIndexes, index)
		spans = append(spans, span)
	}

	if len(messages) == 0 {
		return
	}

	writeCtx, cancel := context.WithTimeout(context.Background(), p.writeTimeout)
	err := p.writer.WriteMessages(writeCtx, messages...)
	cancel()
	if err != nil {
		err = fmt.Errorf("publish kafka batch: %w", err)
	}
	for i, requestIndex := range requestIndexes {
		span := spans[i]
		if err != nil {
			telemetry.RecordSpanError(span, err)
		}
		span.End()
		requests[requestIndex].result <- err
	}
}

func (p *KafkaPublisher) Close() error {
	p.closeOnce.Do(func() {
		if p.batchRequests != nil {
			p.batchMu.Lock()
			p.closed = true
			close(p.batchRequests)
			p.batchMu.Unlock()
			p.batchWG.Wait()
		}
	})
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
