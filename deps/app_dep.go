package deps

import (
	"sync"

	"github.com/acreage/okestra/eventstore"
	"github.com/acreage/okestra/telemetry"
)

// func for options addition
type AppDependencyOptionFunc func(*AppDependency)

// dependency for tasks executing in a task supervisor

type AppDependency struct {
	Telemetry     telemetry.TelemetryInterface
	Eventsourcing eventstore.EventStoreInterface
	Stream        *eventstore.Stream
}

var (
	taskDep  *AppDependency
	initOnce sync.Once
)

// func for options addition
func NewAppDependency(opts ...AppDependencyOptionFunc) *AppDependency {
	if taskDep != nil {
		return taskDep
	}

	initOnce.Do(func() {
		taskDep = &AppDependency{}
		for _, opt := range opts {
			opt(taskDep)
		}
	})

	return taskDep
}

// func for options addition
func WithTelemetry(telemetry telemetry.TelemetryInterface) AppDependencyOptionFunc {
	return func(td *AppDependency) {
		td.Telemetry = telemetry
	}
}

// func for options addition
func WithEventsourcing(eventsourcing eventstore.EventStoreInterface) AppDependencyOptionFunc {
	return func(td *AppDependency) {
		td.Eventsourcing = eventsourcing
	}
}

// func for options addition
func WithStream(stream *eventstore.Stream) AppDependencyOptionFunc {
	return func(td *AppDependency) {
		td.Stream = stream
	}
}

// create a copy of the task dependency for new tasks accepting a task func
func (td *AppDependency) Copy(opts ...AppDependencyOptionFunc) *AppDependency {
	tdCopy := &AppDependency{
		Telemetry:     td.Telemetry,
		Eventsourcing: td.Eventsourcing,
		Stream:        td.Stream,
	}

	for _, opt := range opts {
		opt(tdCopy)
	}

	return tdCopy
}
