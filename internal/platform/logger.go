package platform

import (
	"log/slog"
	"os"
)

func NewLogger(service string) *slog.Logger {
	return slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	).With("service", service)
}
