package telemetry_test

import (
	"testing"

	"github.com/acreage/okestra/telemetry"
)

func TestLogLevels(t *testing.T) {
	tests := []struct {
		name    string
		level   telemetry.LogLevel
		message string
	}{
		{
			name:    "Debug level logging",
			level:   telemetry.DEBUG,
			message: "2023-10-10 15:30:45 [DEBUG] Hello, world!",
		},
		{
			name:    "Info level logging",
			level:   telemetry.INFO,
			message: "2023-10-10 15:30:45 [INFO] Hello, world!",
		},
		{
			name:    "Warning level logging",
			level:   telemetry.WARN,
			message: "2023-10-10 15:30:45 [WARN] Hello, world!",
		},
		{
			name:    "Error level logging",
			level:   telemetry.ERROR,
			message: "2023-10-10 15:30:45 [ERROR] Hello, world!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call Log function
			telemetry.Log(tt.level, tt.message)
		})
	}
}
