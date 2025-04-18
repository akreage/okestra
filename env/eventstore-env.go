package env

type EventStoreEnv struct {
	// out of order time
	OutOfOrderTime int `env:"EVENTSTORE_OUT_OF_ORDER_TIME,default=10"`
	// delete streams interval
	DeleteStreamsInterval int `env:"EVENTSTORE_DELETE_STREAMS_INTERVAL,default=1"`
	// delete streams ttl
	DeleteStreamsTTL int `env:"EVENTSTORE_DELETE_STREAMS_TTL,default=10"`
}
