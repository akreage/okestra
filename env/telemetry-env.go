package env

// telemetry environment variables def
type TelemetryEnv struct {
	// use goroutines to handle events
	TelemetryUseGoroutines bool `env:"TELEMETRY_USE_GOROUTINES,default=false"`
	// goroutines pool size
	TelemetryGoroutinesPoolSize int `env:"TELEMETRY_GOROUTINE_POOL_SIZE,default=10"`
	// routine buffer size
	TelemetryBufferSize int `env:"TELEMETRY_BUFFER_SIZE,default=10"`
}
