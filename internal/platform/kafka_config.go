package platform

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	KafkaSecurityProtocolPlaintext     = "PLAINTEXT"
	KafkaSecurityProtocolSSL           = "SSL"
	KafkaSecurityProtocolSASLSSL       = "SASL_SSL"
	KafkaSecurityProtocolSASLPlaintext = "SASL_PLAINTEXT"

	KafkaSASLMechanismPlain = "PLAIN"
)

type KafkaConnectionConfig struct {
	ClientID               string
	SecurityProtocol       string
	SASLMechanism          string
	SASLUsername           string
	SASLPassword           string
	TLSServerName          string
	TLSInsecureSkipVerify  bool
	AllowAutoTopicCreation bool
}

func LoadKafkaConnectionConfigFromEnv() (KafkaConnectionConfig, error) {
	securityProtocol := strings.ToUpper(EnvString("KAFKA_SECURITY_PROTOCOL", KafkaSecurityProtocolPlaintext))
	saslMechanism := strings.ToUpper(EnvString("KAFKA_SASL_MECHANISM", ""))
	tlsInsecureSkipVerify, err := EnvBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false)
	if err != nil {
		return KafkaConnectionConfig{}, err
	}
	allowAutoTopicCreation, err := EnvBool("KAFKA_ALLOW_AUTO_TOPIC_CREATION", true)
	if err != nil {
		return KafkaConnectionConfig{}, err
	}

	config := KafkaConnectionConfig{
		ClientID:               EnvString("KAFKA_CLIENT_ID", ""),
		SecurityProtocol:       securityProtocol,
		SASLMechanism:          saslMechanism,
		SASLUsername:           EnvString("KAFKA_SASL_USERNAME", ""),
		SASLPassword:           EnvString("KAFKA_SASL_PASSWORD", ""),
		TLSServerName:          EnvString("KAFKA_TLS_SERVER_NAME", ""),
		TLSInsecureSkipVerify:  tlsInsecureSkipVerify,
		AllowAutoTopicCreation: allowAutoTopicCreation,
	}
	if err := config.Validate(); err != nil {
		return KafkaConnectionConfig{}, err
	}
	return config, nil
}

func (c KafkaConnectionConfig) Validate() error {
	switch c.SecurityProtocol {
	case KafkaSecurityProtocolPlaintext, KafkaSecurityProtocolSSL, KafkaSecurityProtocolSASLSSL, KafkaSecurityProtocolSASLPlaintext:
	default:
		return fmt.Errorf("unsupported KAFKA_SECURITY_PROTOCOL %q", c.SecurityProtocol)
	}

	if !c.usesSASL() {
		if strings.TrimSpace(c.SASLMechanism) != "" || strings.TrimSpace(c.SASLUsername) != "" || strings.TrimSpace(c.SASLPassword) != "" {
			return fmt.Errorf("KAFKA_SASL_* settings require KAFKA_SECURITY_PROTOCOL to use SASL")
		}
		return nil
	}

	switch c.SASLMechanism {
	case KafkaSASLMechanismPlain:
		if strings.TrimSpace(c.SASLUsername) == "" {
			return fmt.Errorf("KAFKA_SASL_USERNAME is required when SASL is enabled")
		}
		if strings.TrimSpace(c.SASLPassword) == "" {
			return fmt.Errorf("KAFKA_SASL_PASSWORD is required when SASL is enabled")
		}
	case "":
		return fmt.Errorf("KAFKA_SASL_MECHANISM is required when SASL is enabled")
	default:
		return fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", c.SASLMechanism)
	}

	return nil
}

func (c KafkaConnectionConfig) NewReader(topic string, groupID string, brokers []string) (*kafka.Reader, error) {
	dialer, err := c.newDialer()
	if err != nil {
		return nil, err
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		Dialer:         dialer,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	}), nil
}

func (c KafkaConnectionConfig) newWriter(brokers []string, topic string, config KafkaPublisherConfig) (*kafka.Writer, error) {
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 5 * time.Millisecond
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 256
	}

	transport, err := c.newTransport()
	if err != nil {
		return nil, err
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		RequiredAcks:           kafka.RequireOne,
		BatchTimeout:           config.BatchTimeout,
		BatchSize:              config.BatchSize,
		AllowAutoTopicCreation: c.AllowAutoTopicCreation,
		Balancer:               &kafka.Hash{},
		Transport:              transport,
	}
	if config.ReadTimeout > 0 {
		writer.ReadTimeout = config.ReadTimeout
	}
	if config.WriteTimeout > 0 {
		writer.WriteTimeout = config.WriteTimeout
	}
	if config.MaxAttempts > 0 {
		writer.MaxAttempts = config.MaxAttempts
	}
	if config.WriteBackoffMin > 0 {
		writer.WriteBackoffMin = config.WriteBackoffMin
	}
	if config.WriteBackoffMax > 0 {
		writer.WriteBackoffMax = config.WriteBackoffMax
	}
	return writer, nil
}

func (c KafkaConnectionConfig) newDialer() (*kafka.Dialer, error) {
	mechanism, err := c.newSASLMechanism()
	if err != nil {
		return nil, err
	}

	return &kafka.Dialer{
		ClientID:      c.ClientID,
		Timeout:       10 * time.Second,
		DualStack:     true,
		KeepAlive:     30 * time.Second,
		TLS:           c.newTLSConfig(),
		SASLMechanism: mechanism,
	}, nil
}

func (c KafkaConnectionConfig) newTransport() (*kafka.Transport, error) {
	mechanism, err := c.newSASLMechanism()
	if err != nil {
		return nil, err
	}

	return &kafka.Transport{
		ClientID: c.ClientID,
		TLS:      c.newTLSConfig(),
		SASL:     mechanism,
	}, nil
}

func (c KafkaConnectionConfig) newTLSConfig() *tls.Config {
	if !c.usesTLS() {
		return nil
	}

	return &tls.Config{
		ServerName:         strings.TrimSpace(c.TLSServerName),
		InsecureSkipVerify: c.TLSInsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
}

func (c KafkaConnectionConfig) newSASLMechanism() (sasl.Mechanism, error) {
	if !c.usesSASL() {
		return nil, nil
	}

	switch c.SASLMechanism {
	case KafkaSASLMechanismPlain:
		return plain.Mechanism{
			Username: c.SASLUsername,
			Password: c.SASLPassword,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", c.SASLMechanism)
	}
}

func (c KafkaConnectionConfig) usesTLS() bool {
	switch c.SecurityProtocol {
	case KafkaSecurityProtocolSSL, KafkaSecurityProtocolSASLSSL:
		return true
	default:
		return false
	}
}

func (c KafkaConnectionConfig) usesSASL() bool {
	switch c.SecurityProtocol {
	case KafkaSecurityProtocolSASLSSL, KafkaSecurityProtocolSASLPlaintext:
		return true
	default:
		return false
	}
}
