package telemetry

import "log"

type LogLevel string

// LogLevels
const (
	DEBUG LogLevel = "DEBUG"
	INFO  LogLevel = "INFO"
	WARN  LogLevel = "WARNING"
	ERROR LogLevel = "ERROR"
)

// manages console based logs
// handles coloring mainly
func Log(level LogLevel, message string) {
	// log the event
	var colorStart, colorEnd string
	switch level {
	case DEBUG:
		colorStart = "\033[92m" // Light green for debug
	case INFO:
		colorStart = "\033[34m" // Blue
	case WARN:
		colorStart = "\033[33m" // Yellow
	case ERROR:
		colorStart = "\033[31m" // Red
	default:
		colorStart = "\033[0m" // Default
	}

	colorEnd = "\033[0m"
	log.SetPrefix(colorStart)
	defer log.SetPrefix(colorEnd)

	log.Printf("%s", message)
}
