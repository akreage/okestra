package telemetry

// telemetry callback func
type TelemetryCallbackFunc func() (map[string]interface{}, map[string]interface{}, error)

// new telemetry events are defined here with configurations
type TelemetryEventDefinition struct {
	// event name
	Name []string
	// event configuration
	Config interface{}
}

// telemetry event types
type TelemetryEvent string

// any package/service that wants to receive telemetry data should implement this interface
// and register it with the telemetry server
type TelemetryHandlerInterface interface {
	// returns a unique id for the handler
	Id() string
	// attach one or more handlers
	AttachHandlers() (*[]TelemetryEventDefinition, error)
	// called by telemetry Service to handle an event
	HandleEvent(event TelemetryEvent, measurements map[string]interface{}, metadata map[string]interface{}, config *TelemetryEventDefinition) error
}

// telemetry interface
type TelemetryInterface interface {
	// add a new telemetry Handler
	AddHandler(TelemetryHandlerInterface) error
	// remove an existing telemtry handler
	RemoveHandler(TelemetryHandlerInterface) error
	// Execute a single telemetry event
	TriggerEvent(event TelemetryEvent, measurements map[string]interface{}, metadata map[string]interface{}) error
	// Engulf telemetry in a function to measure start and end time
	Execute(TelemetryEvent, TelemetryCallbackFunc) error
}
