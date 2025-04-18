package telemetry_test

import (
	"testing"
	"time"

	"github.com/acreage/okestra/telemetry"
	"github.com/stretchr/testify/assert"
)

// test the telemetry log handler
func TestTelemetryTriggerEvent(t *testing.T) {
	// start telemetry server
	server := telemetry.StartTelemetryServer()
	handler := telemetry.NewLogHandler("log-test")
	server.AddHandler(handler)

	// measurements
	measurements := map[string]interface{}{
		"startedAt": time.Now().UnixNano() / int64(time.Millisecond),
	}

	// metadata
	metadata := map[string]interface{}{
		"node":      "test",
		"user":      "test",
		"requestId": 123432,
		"message":   "user created successfully.",
	}

	server.TriggerEvent("okestra.cg.data.request", measurements, metadata)
}

func TestTelemetryExecuteEvent(t *testing.T) {
	server := telemetry.StartTelemetryServer()
	handler := telemetry.NewLogHandler("log-test")
	server.AddHandler(handler)

	server.Execute("okestra.cg.data.request", func() (map[string]interface{}, map[string]interface{}, error) {
		// we will do something here
		// probably call the logger
		measurements := map[string]interface{}{
			"startedAt": time.Now().UnixNano() / int64(time.Millisecond),
		}

		// metadata
		metadata := map[string]interface{}{
			"node":      "test",
			"user":      "test",
			"requestId": 123432,
			"message":   "user created successfully.",
		}
		server.TriggerEvent("okestra.cg.data.request", measurements, metadata)
		return measurements, metadata, nil
	})
	// okay, we should receive 4 logs
}

func TestTelemetryExecuteEventWithException(t *testing.T) {
	server := telemetry.StartTelemetryServer()
	handler := telemetry.NewLogHandler("log-test")
	server.AddHandler(handler)

	assert.Panics(t, func() {
		server.Execute("okestra.cg.data.request", func() (map[string]interface{}, map[string]interface{}, error) {
			panic("test")
		})
	})
}
