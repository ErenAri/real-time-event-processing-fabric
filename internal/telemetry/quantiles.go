package telemetry

import (
	"cmp"
	"slices"
	"sync"
	"time"
)

type Quantiles struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
}

type QuantileWindow struct {
	mu     sync.Mutex
	values []float64
	maxLen int
}

func NewQuantileWindow(maxLen int) *QuantileWindow {
	return &QuantileWindow{maxLen: maxLen}
}

func (w *QuantileWindow) Observe(duration time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.values = append(w.values, float64(duration.Milliseconds()))
	if len(w.values) > w.maxLen {
		w.values = w.values[len(w.values)-w.maxLen:]
	}
}

func (w *QuantileWindow) Snapshot() Quantiles {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.values) == 0 {
		return Quantiles{}
	}

	sorted := slices.Clone(w.values)
	slices.SortFunc(sorted, func(a, b float64) int {
		return cmp.Compare(a, b)
	})

	return Quantiles{
		P50: percentile(sorted, 0.50),
		P95: percentile(sorted, 0.95),
		P99: percentile(sorted, 0.99),
	}
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * pct)
	return values[index]
}
