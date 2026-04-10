package platform

import (
	"context"
	"log/slog"
	"time"
)

func RunPeriodic(ctx context.Context, every time.Duration, logger *slog.Logger, task string, fn func(context.Context) error) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				logger.Error("periodic_task_failed", "task", task, "error", err)
			}
		}
	}
}
