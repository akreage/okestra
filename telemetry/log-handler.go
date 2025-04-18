package telemetry

import (
	"strings"
	"time"
)

type FieldKind string

const (
	FieldKindMeasurement FieldKind = "measurement"
	FieldKindMetadata    FieldKind = "metadata"
)

type EventKind string

const (
	EventKindStart EventKind = "start"
	EventKindStop  EventKind = "stop"
	EventKindError EventKind = "exception"
	EventKindBase  EventKind = ""
)

type FieldConverter func(interface{}) interface{}

// log Handler configuration
type LogHandlerConfig struct {
	// event kind
	EventKind EventKind
	// log level
	LogLevel LogLevel
	// format
	LogFormat string
	// fields to omit
	// kind is measurement / metadata
	OmitFields []struct {
		// field kind
		Kind FieldKind
		Name string
	}
	// field converters
	// kind is measurement / metadata
	FieldConverters map[string]struct {
		// field kind
		Kind FieldKind
		// field converter
		Converter FieldConverter
	}
}

// LogHandler is a telemetry handler that logs events
type LogHandler struct {
	id string
}

// NewLogHandler creates a new LogHandler with the given id
func NewLogHandler(id string) TelemetryHandlerInterface {
	return &LogHandler{id: id}
}

// Id returns the unique id of the handler
func (h *LogHandler) Id() string {
	return h.id
}

// AttachHandlers attaches event handlers to the LogHandler
func (h *LogHandler) AttachHandlers() (*[]TelemetryEventDefinition, error) {
	// Define the events this handler will handle
	events := []TelemetryEventDefinition{
		{
			Name:   []string{"okestra", "cg", "data", "request"},
			Config: defaultLogHandlerConfig(),
		},
	}
	return &events, nil
}

// HandleEvent handles the telemetry event by logging it
func (h *LogHandler) HandleEvent(event TelemetryEvent, measurements map[string]interface{}, metadata map[string]interface{}, c *TelemetryEventDefinition) error {
	config := getEventKindConfig(event, c)
	if config == nil {
		// no config found, return
		return nil
	}
	// update measurements
	measurements = updateFields(config, FieldKindMeasurement, measurements)
	// update metadata
	metadata = updateFields(config, FieldKindMetadata, metadata)
	// merge the two maps
	merged := mergeMaps(measurements, metadata)
	delete(merged, "message")
	delete(merged, "node")

	// call formatter and execute a final format
	f := NewFormatter()
	f.CompileString(getFormat(config))

	// only pass node if it exists in metadata
	var node string = "localhost"
	var message string = ""
	if n, ok := metadata["node"]; ok {
		node = n.(string)
	}
	if m, ok := metadata["message"]; ok {
		message = m.(string)
	}

	formmatedString := f.FormatString(
		SetDate(time.Now()),
		SetTime(time.Now()),
		SetLevel(config.LogLevel),
		SetEvent(event),
		SetNode(node),
		SetMetadata(merged),
		SetMessage(message),
	)

	Log(config.LogLevel, formmatedString+"\n")
	return nil
}

func getFormat(config *LogHandlerConfig) string {
	if config.LogFormat == "" {
		return "$date $time [$level] $event - $metadata $message"
	}

	return config.LogFormat
}

func updateFields(config *LogHandlerConfig, kind FieldKind, fields map[string]interface{}) map[string]interface{} {
	// convert fields
	for k, v := range fields {
		if converter, ok := config.FieldConverters[k]; ok && converter.Kind == kind {
			fields[k] = converter.Converter(v)
		}
	}
	// omit fields
	for _, omit := range config.OmitFields {
		if omit.Kind == kind {
			delete(fields, omit.Name)
		}
	}
	return fields
}

// mergeMaps combines two maps into one, with values from the second map overwriting those from the first in case of key collisions.
func mergeMaps(map1, map2 map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})

	// Copy all entries from the first map
	for k, v := range map1 {
		merged[k] = v
	}

	// Copy all entries from the second map, overwriting duplicates
	for k, v := range map2 {
		merged[k] = v
	}

	return merged
}

// return the right config for the event kind
// this is used to get the right config for the event kind
// it will match the event kind with the name of the event
// and return the config for the event kind
func getEventKindConfig(event TelemetryEvent, c *TelemetryEventDefinition) *LogHandlerConfig {
	// split the event into parts
	eventParts := strings.Split(string(event), ".")

	// run through all the configs, compare the event kind
	// and return config for matching event kind
	// if no match, return nil
	for _, config := range c.Config.([]LogHandlerConfig) {
		// get the last part of the event for the TelemetryEventDefinition
		// if base, we don't change the name
		name := c.Name
		if string(config.EventKind) != "" {
			name = append(c.Name, string(config.EventKind))
		}

		// check if the event parts match the name
		if len(eventParts) != len(name) {
			continue
		}

		match := true
		for i, part := range eventParts {
			if part != name[i] {
				match = false
				break
			}
		}
		if match {
			return &config
		}
	}

	return nil
}

// returns the default log handler configs
// this can be reused for new events when registering
func defaultLogHandlerConfig() []LogHandlerConfig {
	return []LogHandlerConfig{
		{
			EventKind: EventKindBase,
			LogLevel:  INFO,
			LogFormat: "$date $time [$level] $event - metadata:$metadata - message:$message",
		},
		{
			EventKind: EventKindStart,
			LogLevel:  INFO,
			LogFormat: "$date $time [$level] $event - metadata:$metadata - message:$message",
		},
		{
			EventKind: EventKindStop,
			LogLevel:  INFO,
			LogFormat: "$date $time [$level] $event - metadata:$metadata - message:$message",
		},
		{
			EventKind: EventKindError,
			LogLevel:  ERROR,
			LogFormat: "$date $time [$level] $event - metadata:$metadata - message:$message",
		},
	}
}
