package env

// supervisor environment variables def
type SupervisorEnv struct {
	SupervisorMaxRetries  int    `env:"SUPERVISOR_MAX_RETRIES,default=3"`
	SupervisorRetryPeriod int    `env:"SUPERVISOR_RETRY_PERIOD,default=1000"`
	SupervisorGlobalLock  bool   `env:"SUPERVISOR_GLOBAL_LOCK,default=false"`
	SupervisorMaxChildren int    `env:"SUPERVISOR_MAX_CHILDREN,default=-1"`
	SupervisorIsLocal     bool   `env:"SUPERVISOR_IS_LOCAL,default=true"`
	SupervisorNodeName    string `env:"SUPERVISOR_NODE_NAME,default=localhost"`
}
