package telemetry

import (
	"testing"
	"time"
)

func TestQuantileWindowSnapshot(t *testing.T) {
	window := NewQuantileWindow(10)
	window.Observe(10 * time.Millisecond)
	window.Observe(20 * time.Millisecond)
	window.Observe(30 * time.Millisecond)
	window.Observe(40 * time.Millisecond)

	snapshot := window.Snapshot()
	if snapshot.P50 <= 0 || snapshot.P95 <= 0 || snapshot.P99 <= 0 {
		t.Fatalf("expected non-zero quantiles, got %+v", snapshot)
	}
}
