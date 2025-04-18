package telemetry

// registration of telemetry events

// supervisor events
const (
	SupervisorStateEvent      TelemetryEvent = "okestra.supervisor.state"
	SupervisorStateErrorEvent TelemetryEvent = "okestra.supervisor.state.error"
	SupervisorEvent           TelemetryEvent = "okestra.supervisor"
)
