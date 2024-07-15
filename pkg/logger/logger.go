package logger

import (
	"log"
	"log/slog"
	"os"
)

func NewLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening log file: %v", err)
		return nil
	}

	logger := slog.New(slog.NewTextHandler(file, opts))

	return logger
}
