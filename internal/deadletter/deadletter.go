package deadletter

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Header struct {
	Key         string `json:"key"`
	ValueBase64 string `json:"value_base64"`
}

type Record struct {
	FailedAt        time.Time `json:"failed_at"`
	Reason          string    `json:"reason"`
	Error           string    `json:"error"`
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int       `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
	SourceKey       string    `json:"source_key,omitempty"`
	ConsumerGroup   string    `json:"consumer_group,omitempty"`
	EventID         string    `json:"event_id,omitempty"`
	TenantID        string    `json:"tenant_id,omitempty"`
	SourceID        string    `json:"source_id,omitempty"`
	PayloadBase64   string    `json:"payload_base64"`
	Headers         []Header  `json:"headers,omitempty"`
}

func NewRecord(
	message kafka.Message,
	consumerGroup string,
	reason string,
	err error,
	eventID string,
	tenantID string,
	sourceID string,
) Record {
	record := Record{
		FailedAt:        time.Now().UTC(),
		Reason:          strings.TrimSpace(reason),
		SourceTopic:     message.Topic,
		SourcePartition: message.Partition,
		SourceOffset:    message.Offset,
		SourceKey:       string(message.Key),
		ConsumerGroup:   strings.TrimSpace(consumerGroup),
		EventID:         strings.TrimSpace(eventID),
		TenantID:        strings.TrimSpace(tenantID),
		SourceID:        strings.TrimSpace(sourceID),
		PayloadBase64:   base64.StdEncoding.EncodeToString(message.Value),
	}
	if err != nil {
		record.Error = err.Error()
	}
	for _, header := range message.Headers {
		record.Headers = append(record.Headers, Header{
			Key:         header.Key,
			ValueBase64: base64.StdEncoding.EncodeToString(header.Value),
		})
	}
	return record
}

func Encode(record Record) ([]byte, error) {
	return json.Marshal(record)
}
