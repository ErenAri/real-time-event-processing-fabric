package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"
)

type Summary struct {
	SchemaVersion int            `json:"schema_version"`
	GeneratedAt   time.Time      `json:"generated_at"`
	Status        string         `json:"status"`
	ArtifactRoot  string         `json:"artifact_root"`
	Benchmark     *Benchmark     `json:"benchmark,omitempty"`
	FailureDrills []FailureDrill `json:"failure_drills"`
}

type Benchmark struct {
	Artifact             string   `json:"artifact"`
	StartedAt            string   `json:"started_at_utc"`
	CompletedAt          string   `json:"completed_at_utc"`
	TargetEPS            float64  `json:"target_eps"`
	AcceptedEPS          float64  `json:"accepted_eps"`
	ProcessedEPS         float64  `json:"processed_eps"`
	QueryP95MS           float64  `json:"query_p95_ms"`
	PeakLag              int64    `json:"peak_lag"`
	PostLoadDrainSeconds float64  `json:"post_load_drain_seconds"`
	ProducerCount        int      `json:"producer_count"`
	Replicas             int      `json:"processor_replicas"`
	BatchSize            int      `json:"batch_size"`
	Summary              string   `json:"summary"`
	Gaps                 []string `json:"gaps"`
	Gates                []Gate   `json:"gates"`
}

type Gate struct {
	Name     string  `json:"name"`
	Status   string  `json:"status"`
	Target   float64 `json:"target"`
	Observed float64 `json:"observed"`
	Unit     string  `json:"unit,omitempty"`
}

type FailureDrill struct {
	ScenarioID   string   `json:"scenario_id"`
	Title        string   `json:"title"`
	Status       string   `json:"status"`
	Artifact     string   `json:"artifact"`
	StartedAt    string   `json:"started_at_utc"`
	CompletedAt  string   `json:"completed_at_utc"`
	Result       string   `json:"result"`
	OperatorNote string   `json:"operator_note"`
	RemainingGap string   `json:"remaining_gap"`
	Metrics      []Metric `json:"metrics"`
}

type Metric struct {
	Label string `json:"label"`
	Value string `json:"value"`
	Unit  string `json:"unit,omitempty"`
	Tone  string `json:"tone,omitempty"`
}

func DefaultSummary(path string) Summary {
	return Summary{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC(),
		Status:        "missing",
		ArtifactRoot:  path,
		FailureDrills: []FailureDrill{},
	}
}

func LoadSummary(path string) (Summary, error) {
	path = strings.TrimSpace(path)
	summary := DefaultSummary(path)
	if path == "" {
		return summary, nil
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return summary, nil
	}
	if err != nil {
		return summary, err
	}
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	if len(bytes.TrimSpace(data)) == 0 {
		return summary, nil
	}

	if err := json.Unmarshal(data, &summary); err != nil {
		return Summary{}, err
	}
	if summary.SchemaVersion == 0 {
		summary.SchemaVersion = 1
	}
	if summary.GeneratedAt.IsZero() {
		summary.GeneratedAt = time.Now().UTC()
	}
	if summary.Status == "" {
		summary.Status = "available"
	}
	if summary.ArtifactRoot == "" {
		summary.ArtifactRoot = path
	}
	if summary.FailureDrills == nil {
		summary.FailureDrills = []FailureDrill{}
	}
	if summary.Benchmark != nil && summary.Benchmark.Gaps == nil {
		summary.Benchmark.Gaps = []string{}
	}
	if summary.Benchmark != nil && summary.Benchmark.Gates == nil {
		summary.Benchmark.Gates = []Gate{}
	}
	for index := range summary.FailureDrills {
		if summary.FailureDrills[index].Metrics == nil {
			summary.FailureDrills[index].Metrics = []Metric{}
		}
	}
	return summary, nil
}
