package store

import (
	"testing"
	"time"
)

func TestAggregateIngestStates(t *testing.T) {
	firstSeen := time.Date(2026, 4, 10, 18, 0, 0, 0, time.UTC)
	secondSeen := firstSeen.Add(5 * time.Second)

	aggregate := aggregateIngestStates([]stateEnvelope{
		{
			InstanceID: "ingest-1",
			IngestState: &IngestState{
				AcceptedTotal: 120,
				RejectedTotal: 7,
				LastSeenAt:    firstSeen,
			},
		},
		{
			InstanceID: "ingest-2",
			IngestState: &IngestState{
				AcceptedTotal: 80,
				RejectedTotal: 3,
				LastSeenAt:    secondSeen,
			},
		},
	})

	if aggregate.AcceptedTotal != 200 {
		t.Fatalf("accepted total mismatch: got %d want %d", aggregate.AcceptedTotal, 200)
	}
	if aggregate.RejectedTotal != 10 {
		t.Fatalf("rejected total mismatch: got %d want %d", aggregate.RejectedTotal, 10)
	}
	if !aggregate.LastSeenAt.Equal(secondSeen) {
		t.Fatalf("latest ingest heartbeat mismatch: got %v want %v", aggregate.LastSeenAt, secondSeen)
	}
}

func TestAggregateProcessorStates(t *testing.T) {
	firstSeen := time.Date(2026, 4, 10, 18, 0, 0, 0, time.UTC)
	secondSeen := firstSeen.Add(5 * time.Second)

	aggregate := aggregateProcessorStates([]stateEnvelope{
		{
			InstanceID: "processor-1",
			Processor: &ProcessorState{
				DuplicateTotal:   11,
				DeadLetterTotal:  2,
				ConsumerLag:      350,
				ActivePartitions: 2,
				InFlightMessages: 40,
				LastSeenAt:       firstSeen,
				ProcessingP50MS:  4.2,
				ProcessingP95MS:  18.4,
				ProcessingP99MS:  25.7,
			},
		},
		{
			InstanceID: "processor-2",
			Processor: &ProcessorState{
				DuplicateTotal:   5,
				DeadLetterTotal:  1,
				ConsumerLag:      125,
				ActivePartitions: 1,
				InFlightMessages: 22,
				LastSeenAt:       secondSeen,
				ProcessingP50MS:  5.1,
				ProcessingP95MS:  14.9,
				ProcessingP99MS:  30.3,
			},
		},
	})

	if aggregate.InstanceCount != 2 {
		t.Fatalf("instance count mismatch: got %d want %d", aggregate.InstanceCount, 2)
	}
	if aggregate.DuplicateTotal != 16 {
		t.Fatalf("duplicate total mismatch: got %d want %d", aggregate.DuplicateTotal, 16)
	}
	if aggregate.DeadLetterTotal != 3 {
		t.Fatalf("dead-letter total mismatch: got %d want %d", aggregate.DeadLetterTotal, 3)
	}
	if aggregate.ConsumerLag != 475 {
		t.Fatalf("consumer lag mismatch: got %d want %d", aggregate.ConsumerLag, 475)
	}
	if aggregate.ActivePartitions != 3 {
		t.Fatalf("active partitions mismatch: got %d want %d", aggregate.ActivePartitions, 3)
	}
	if aggregate.InFlightMessages != 62 {
		t.Fatalf("in-flight messages mismatch: got %d want %d", aggregate.InFlightMessages, 62)
	}
	if aggregate.LastSeenAt == nil || !aggregate.LastSeenAt.Equal(secondSeen) {
		t.Fatalf("latest processor heartbeat mismatch: got %v want %v", aggregate.LastSeenAt, secondSeen)
	}
	if aggregate.ProcessingP50MS != 5.1 {
		t.Fatalf("p50 mismatch: got %.1f want %.1f", aggregate.ProcessingP50MS, 5.1)
	}
	if aggregate.ProcessingP95MS != 18.4 {
		t.Fatalf("p95 mismatch: got %.1f want %.1f", aggregate.ProcessingP95MS, 18.4)
	}
	if aggregate.ProcessingP99MS != 30.3 {
		t.Fatalf("p99 mismatch: got %.1f want %.1f", aggregate.ProcessingP99MS, 30.3)
	}
}
