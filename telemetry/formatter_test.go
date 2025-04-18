package telemetry_test

import (
	"testing"
	"time"

	"github.com/acreage/okestra/telemetry"
	"github.com/stretchr/testify/assert"
)

func TestFormatString_Default(t *testing.T) {
	// Using CompileString with no pattern functions should produce default (zero) values.
	f := telemetry.NewFormatter()
	f.CompileString("$date $time [$level] $message")
	customTime := time.Date(2023, 10, 10, 15, 30, 45, 0, time.UTC)
	output := f.FormatString(
		telemetry.SetDate(customTime),
		telemetry.SetTime(customTime),
		telemetry.SetLevel(telemetry.INFO),
		telemetry.SetMessage("Hello, world!"),
	)

	expected := "2023-10-10 15:30:45 [INFO] Hello, world!"
	assert.Equal(t, expected, output)
}

func TestFormatString_UnknownToken(t *testing.T) {
	// Unknown tokens should be replaced with empty strings.
	f := telemetry.NewFormatter()
	f.CompileString("Hello $unknown!")
	output := f.FormatString()
	expected := "Hello !"
	assert.Equal(t, expected, output)
}

func TestFormatString_MixedLiterals(t *testing.T) {
	// Test that literal text and token replacements intermix correctly.
	f := telemetry.NewFormatter()
	f.CompileString("Start-$date-End")
	customTime := time.Date(2023, 10, 10, 15, 30, 45, 0, time.UTC)
	output := f.FormatString(telemetry.SetDate(customTime))
	expected := "Start-2023-10-10-End"
	assert.Equal(t, expected, output)
}

func TestFormatString_CompileJson(t *testing.T) {
	// Test the CompileJson method with an array of literal and token strings.
	f := telemetry.NewFormatter()
	pattern := []string{"Date: ", "$date", ", Time: ", "$time", ", Level: ", "$level", ", Message: ", "$message"}
	f.CompileJson(pattern)
	output := f.FormatString()
	expected := "Date: 0001-01-01, Time: 00:00:00, Level: , Message: "
	if output != expected {
		t.Errorf("TestFormatString_CompileJson: expected %q, got %q", expected, output)
	}
}

func TestFormatString_WithSetters(t *testing.T) {
	// This test assumes that the telemetry package provides setter functions
	// that correctly update the internal PatternMetadata.
	f := telemetry.NewFormatter()
	f.CompileString("$date $time [$level] $message")

	// Define a specific time; the expected date and time strings will be derived from this.
	customTime := time.Date(2023, 10, 10, 15, 30, 45, 0, time.UTC)

	// Use setter functions to modify the pattern metadata.
	output := f.FormatString(
		telemetry.SetDate(customTime),
		telemetry.SetTime(customTime),
		telemetry.SetLevel("INFO"),
		telemetry.SetMessage("Hello, world!"),
	)
	// Expected output uses the provided values.
	expected := "2023-10-10 15:30:45 [INFO] Hello, world!"
	if output != expected {
		t.Errorf("TestFormatString_WithSetters: expected %q, got %q", expected, output)
	}
}
