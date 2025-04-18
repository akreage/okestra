package telemetry

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/acreage/okestra/env"
	"github.com/alitto/pond"
	"github.com/sethvargo/go-envconfig"
)

// telemetry definition
type Telemetry struct {
	// registered telemetry handlers
	Handlers map[string]TelemetryHandlerInterface
	// worker pool
	*pond.WorkerPool
	// telemetry environment variables
	*env.TelemetryEnv
	// add lock
	lock sync.RWMutex
}

// start a telemetry server
func StartTelemetryServer() TelemetryInterface {
	t := &Telemetry{
		Handlers:     make(map[string]TelemetryHandlerInterface),
		TelemetryEnv: getEnvVars(),
		lock:         sync.RWMutex{},
	}

	if t.TelemetryUseGoroutines {
		// start a worker pool
		t.WorkerPool = pond.New(t.TelemetryGoroutinesPoolSize, t.TelemetryBufferSize, pond.Strategy(pond.Lazy()))
	}
	return t
}

// add a new telemetry Handler
func (t *Telemetry) AddHandler(handler TelemetryHandlerInterface) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.Handlers[handler.Id()] = handler
	return nil
}

// remove an existing telemtry handler
func (t *Telemetry) RemoveHandler(handler TelemetryHandlerInterface) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.Handlers, handler.Id())
	return nil
}

// Execute a single telemetry event
func (t *Telemetry) TriggerEvent(event TelemetryEvent, measurements map[string]interface{}, metadata map[string]interface{}) error {
	t.lock.RLock()
	defer t.lock.RUnlock()
	// fetch all handlers and call HandleEvent
	for _, handler := range t.Handlers {
		// get all event handler definitions
		events, _ := handler.AttachHandlers()
		for _, eventDef := range *events {
			// check if the event is handled by this handler
			if eventMatches(eventDef.Name, event) {
				t.executeTelemetryEvent(func() {
					handler.HandleEvent(event, measurements, metadata, &eventDef)
				})
			}
		}
	}
	return nil
}

// execute the callback and measure the time taken to start and finish
func (t *Telemetry) Execute(event TelemetryEvent, callback TelemetryCallbackFunc) error {
	t.lock.RLock()
	defer t.lock.RUnlock()
	// pass recovery code here
	defer func() {
		if r := recover(); r != nil {
			// log the error
			errorTime := time.Now().UnixNano() / int64(time.Millisecond)
			measurements := map[string]interface{}{
				"error":      r,
				"errorTime":  errorTime,
				"stackTrace": string(debug.Stack()),
			}
			t.TriggerEvent(TelemetryEvent(event+".exception"), measurements, map[string]interface{}{})

			// repopagate panic
			panic(r)
		}
	}()
	// start the event
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	t.TriggerEvent(event+".start", map[string]interface{}{
		"startTime": startTime,
	}, nil)
	// execute the callback
	measurements, metadata, err := callback()
	// stop the event
	stopTime := time.Now().UnixNano() / int64(time.Millisecond)
	// get the difference between start and stop time
	duration := stopTime - startTime
	measurements["duration"] = duration
	measurements["startTime"] = startTime
	measurements["stopTime"] = stopTime

	// trigger the event
	t.TriggerEvent(event+".stop", measurements, metadata)
	return err
}

// maybe run telemetry concurrently
func (t *Telemetry) executeTelemetryEvent(eventFunc func()) {
	if t.TelemetryUseGoroutines {
		t.WorkerPool.Submit(eventFunc)
	} else {
		eventFunc()
	}
}

// convert the telemetry event definition to a string with 3 variants
func eventMatches(matchingEvent []string, event TelemetryEvent) bool {
	matchedName := strings.Join(matchingEvent, ".")
	// check if any variation of name matches the event
	events := []string{matchedName, matchedName + ".start", matchedName + ".stop", matchedName + ".exception"}
	// slices contains check
	return slices.Contains(events, string(event))
}

// fetch telemetry environment variables
func getEnvVars() *env.TelemetryEnv {
	var jwtConfigs env.TelemetryEnv
	ctx := context.Background()
	err := envconfig.Process(ctx, &jwtConfigs)
	if err != nil {
		// print error and fail with fmt
		fmt.Printf("%s [telemetry] unable to load environment variables: %v\n", time.Now().Format(time.RFC3339), err)
		log.Fatal(err)
	}
	return &jwtConfigs
}
