package store

import (
	"sort"
	"strconv"
	"time"
)

func aggregateIngestStates(states []stateEnvelope) IngestState {
	var aggregate IngestState
	for _, state := range states {
		if state.IngestState == nil {
			continue
		}

		aggregate.AcceptedTotal += state.IngestState.AcceptedTotal
		aggregate.RejectedTotal += state.IngestState.RejectedTotal
		if aggregate.LastSeenAt.IsZero() || state.IngestState.LastSeenAt.After(aggregate.LastSeenAt) {
			aggregate.LastSeenAt = state.IngestState.LastSeenAt
		}
	}
	return aggregate
}

type processorAggregate struct {
	DuplicateTotal   int64
	LateEventTotal   int64
	DeadLetterTotal  int64
	ConsumerLag      int64
	InstanceCount    int
	ActivePartitions int64
	InFlightMessages int64
	LastSeenAt       *time.Time
	ProcessingP50MS  float64
	ProcessingP95MS  float64
	ProcessingP99MS  float64
	BatchSizeP95     float64
	BatchFlushP95MS  float64
	Partitions       []PartitionState
}

func aggregateProcessorStates(states []stateEnvelope) processorAggregate {
	var aggregate processorAggregate
	partitionByOwner := map[string]PartitionState{}
	for _, state := range states {
		if state.Processor == nil {
			continue
		}

		aggregate.InstanceCount++
		aggregate.DuplicateTotal += state.Processor.DuplicateTotal
		aggregate.LateEventTotal += state.Processor.LateEventTotal
		aggregate.DeadLetterTotal += state.Processor.DeadLetterTotal
		aggregate.ConsumerLag += state.Processor.ConsumerLag
		aggregate.ActivePartitions += state.Processor.ActivePartitions
		aggregate.InFlightMessages += state.Processor.InFlightMessages
		if state.Processor.ProcessingP50MS > aggregate.ProcessingP50MS {
			aggregate.ProcessingP50MS = state.Processor.ProcessingP50MS
		}
		if state.Processor.ProcessingP95MS > aggregate.ProcessingP95MS {
			aggregate.ProcessingP95MS = state.Processor.ProcessingP95MS
		}
		if state.Processor.ProcessingP99MS > aggregate.ProcessingP99MS {
			aggregate.ProcessingP99MS = state.Processor.ProcessingP99MS
		}
		if state.Processor.BatchSizeP95 > aggregate.BatchSizeP95 {
			aggregate.BatchSizeP95 = state.Processor.BatchSizeP95
		}
		if state.Processor.BatchFlushP95MS > aggregate.BatchFlushP95MS {
			aggregate.BatchFlushP95MS = state.Processor.BatchFlushP95MS
		}
		for _, partition := range state.Processor.Partitions {
			partition.OwnerInstanceID = state.InstanceID
			key := state.InstanceID + ":" + strconv.Itoa(partition.Partition)
			partitionByOwner[key] = partition
		}
		if aggregate.LastSeenAt == nil || state.Processor.LastSeenAt.After(*aggregate.LastSeenAt) {
			lastSeenAt := state.Processor.LastSeenAt
			aggregate.LastSeenAt = &lastSeenAt
		}
	}
	for _, partition := range partitionByOwner {
		aggregate.Partitions = append(aggregate.Partitions, partition)
	}
	sort.Slice(aggregate.Partitions, func(i, j int) bool {
		if aggregate.Partitions[i].Partition == aggregate.Partitions[j].Partition {
			return aggregate.Partitions[i].OwnerInstanceID < aggregate.Partitions[j].OwnerInstanceID
		}
		return aggregate.Partitions[i].Partition < aggregate.Partitions[j].Partition
	})
	return aggregate
}
