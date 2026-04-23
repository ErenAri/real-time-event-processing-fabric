package platform

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

type KafkaTopicSpec struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

type kafkaSeedConn interface {
	Controller() (kafka.Broker, error)
	Close() error
}

type kafkaControllerConn interface {
	CreateTopics(...kafka.TopicConfig) error
	Close() error
}

type kafkaSeedDialFunc func(context.Context, string, string) (kafkaSeedConn, error)
type kafkaControllerDialFunc func(context.Context, string, string) (kafkaControllerConn, error)

func EnsureKafkaTopics(ctx context.Context, brokers []string, topics []KafkaTopicSpec, connectionConfig KafkaConnectionConfig) error {
	dialer, err := connectionConfig.newDialer()
	if err != nil {
		return err
	}

	return ensureKafkaTopicsWithDialer(
		ctx,
		brokers,
		topics,
		func(ctx context.Context, network string, address string) (kafkaSeedConn, error) {
			return dialer.DialContext(ctx, network, address)
		},
		func(ctx context.Context, network string, address string) (kafkaControllerConn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	)
}

func ensureKafkaTopicsWithDialer(
	ctx context.Context,
	brokers []string,
	topics []KafkaTopicSpec,
	dialSeed kafkaSeedDialFunc,
	dialController kafkaControllerDialFunc,
) error {
	configs := buildKafkaTopicConfigs(topics)
	if len(configs) == 0 {
		return nil
	}
	if len(brokers) == 0 {
		return fmt.Errorf("at least one kafka broker is required")
	}

	seedConn, err := dialSeed(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka seed broker %s: %w", brokers[0], err)
	}
	defer seedConn.Close()

	controller, err := seedConn.Controller()
	if err != nil {
		return fmt.Errorf("resolve kafka controller: %w", err)
	}

	controllerAddress := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialController(ctx, "tcp", controllerAddress)
	if err != nil {
		return fmt.Errorf("dial kafka controller %s: %w", controllerAddress, err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("create kafka topics: %w", err)
	}

	return nil
}

func buildKafkaTopicConfigs(topics []KafkaTopicSpec) []kafka.TopicConfig {
	configs := make([]kafka.TopicConfig, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))

	for _, topic := range topics {
		name := strings.TrimSpace(topic.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		partitions := topic.NumPartitions
		if partitions <= 0 {
			partitions = 1
		}
		replicationFactor := topic.ReplicationFactor
		if replicationFactor <= 0 {
			replicationFactor = 1
		}

		configs = append(configs, kafka.TopicConfig{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		})
	}

	return configs
}
