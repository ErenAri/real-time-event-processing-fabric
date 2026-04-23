package platform

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fakeKafkaSeedConn struct {
	controller kafka.Broker
	err        error
	closed     bool
}

func (f *fakeKafkaSeedConn) Controller() (kafka.Broker, error) {
	if f.err != nil {
		return kafka.Broker{}, f.err
	}
	return f.controller, nil
}

func (f *fakeKafkaSeedConn) Close() error {
	f.closed = true
	return nil
}

type fakeKafkaControllerConn struct {
	configs []kafka.TopicConfig
	err     error
	closed  bool
}

func (f *fakeKafkaControllerConn) CreateTopics(configs ...kafka.TopicConfig) error {
	if f.err != nil {
		return f.err
	}
	f.configs = append(f.configs, configs...)
	return nil
}

func (f *fakeKafkaControllerConn) Close() error {
	f.closed = true
	return nil
}

func TestEnsureKafkaTopicsUsesControllerAndDeduplicatesTopics(t *testing.T) {
	seedConn := &fakeKafkaSeedConn{
		controller: kafka.Broker{Host: "controller.kafka", Port: 9093},
	}
	controllerConn := &fakeKafkaControllerConn{}

	var seedAddress string
	var controllerAddress string
	err := ensureKafkaTopicsWithDialer(
		context.Background(),
		[]string{"kafka-1:9092", "kafka-2:9092"},
		[]KafkaTopicSpec{
			{Name: "pulsestream.events", NumPartitions: 12, ReplicationFactor: 1},
			{Name: "pulsestream.events.dlq", NumPartitions: 12, ReplicationFactor: 1},
			{Name: "pulsestream.events", NumPartitions: 6, ReplicationFactor: 3},
			{Name: "   "},
		},
		func(_ context.Context, network string, address string) (kafkaSeedConn, error) {
			if network != "tcp" {
				t.Fatalf("expected tcp network, got %q", network)
			}
			seedAddress = address
			return seedConn, nil
		},
		func(_ context.Context, network string, address string) (kafkaControllerConn, error) {
			if network != "tcp" {
				t.Fatalf("expected tcp network, got %q", network)
			}
			controllerAddress = address
			return controllerConn, nil
		},
	)
	if err != nil {
		t.Fatalf("ensure kafka topics: %v", err)
	}

	if seedAddress != "kafka-1:9092" {
		t.Fatalf("expected first broker to be used as seed, got %q", seedAddress)
	}
	if controllerAddress != "controller.kafka:9093" {
		t.Fatalf("expected controller address, got %q", controllerAddress)
	}
	if !seedConn.closed {
		t.Fatal("expected seed connection to be closed")
	}
	if !controllerConn.closed {
		t.Fatal("expected controller connection to be closed")
	}
	if len(controllerConn.configs) != 2 {
		t.Fatalf("expected 2 unique topic configs, got %d", len(controllerConn.configs))
	}
	if controllerConn.configs[0].Topic != "pulsestream.events" {
		t.Fatalf("unexpected first topic %q", controllerConn.configs[0].Topic)
	}
	if controllerConn.configs[1].Topic != "pulsestream.events.dlq" {
		t.Fatalf("unexpected second topic %q", controllerConn.configs[1].Topic)
	}
}

func TestEnsureKafkaTopicsDefaultsInvalidTopicSizing(t *testing.T) {
	configs := buildKafkaTopicConfigs([]KafkaTopicSpec{
		{Name: "pulsestream.events", NumPartitions: 0, ReplicationFactor: 0},
	})
	if len(configs) != 1 {
		t.Fatalf("expected one config, got %d", len(configs))
	}
	if configs[0].NumPartitions != 1 {
		t.Fatalf("expected partitions to default to 1, got %d", configs[0].NumPartitions)
	}
	if configs[0].ReplicationFactor != 1 {
		t.Fatalf("expected replication factor to default to 1, got %d", configs[0].ReplicationFactor)
	}
}

func TestEnsureKafkaTopicsReturnsControllerCreateError(t *testing.T) {
	expectedErr := errors.New("create failed")
	seedConn := &fakeKafkaSeedConn{
		controller: kafka.Broker{Host: "controller.kafka", Port: 9093},
	}
	controllerConn := &fakeKafkaControllerConn{err: expectedErr}

	err := ensureKafkaTopicsWithDialer(
		context.Background(),
		[]string{"kafka-1:9092"},
		[]KafkaTopicSpec{{Name: "pulsestream.events", NumPartitions: 12, ReplicationFactor: 1}},
		func(_ context.Context, _, _ string) (kafkaSeedConn, error) {
			return seedConn, nil
		},
		func(_ context.Context, _, _ string) (kafkaControllerConn, error) {
			return controllerConn, nil
		},
	)
	if err == nil {
		t.Fatal("expected create topics error")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped create error, got %v", err)
	}
}
