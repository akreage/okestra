package eventstore

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/acreage/okestra/env"
	"github.com/google/uuid"
)

type StreamSubscriber struct {
	currentPosition int
	handler         SubscriptionHandler
	timer           *time.Timer
	oop             []*EventOOP // when the event is received out of order, we store them until the correct order is received
}

// event out of position management
type EventOOP struct {
	eventID  uuid.UUID
	position int
}

// subscription manager ensures
// 1. events are sent out to subscribers
// 2. events are received in order
// 3. events are not lost
// 4. events are not duplicated
// 5. if an event in a wrong position is received, it will store until it receives the correct event.
// in order to main the order
// 6. a timer is used to flush out of order events. This will send out the events in order.
type SubscriptionManager struct {
	Name           string
	Streams        map[uuid.UUID]*StreamSubscriber
	OutOfOrderTime time.Duration
}

// TODO: implement memory pressure management
// TODO: implement event store persistence

type EventStoreManager struct {
	// context
	ctx context.Context
	// array of streams
	streams []*Stream
	// event streams
	streamEvents []*StreamEvent
	// array of subscriptions manager
	subscriptions []*SubscriptionManager
	// array of events
	events []*Event
	// environment variables
	*env.EventStoreEnv
	// map of streams to events
	lock sync.RWMutex
}

// create a new event store manager
func NewEventStoreManager() *EventStoreManager {
	return &EventStoreManager{
		EventStoreEnv: getEnvVars(),
		streams:       make([]*Stream, 0),
		streamEvents:  make([]*StreamEvent, 0),
		subscriptions: make([]*SubscriptionManager, 0),
		events:        make([]*Event, 0),
		lock:          sync.RWMutex{},
		ctx:           context.Background(),
	}
}

// this holds the event state in memory
// you may need to persist to a database if you want to keep the events after the server terminates
// but generally, you can use memory storage
// also, to avoid data bloat, setting a reasonable ttl for the events is a good idea
func (esm *EventStoreManager) NewStream(streamID uuid.UUID) error {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	// check if the stream already exists
	_, err := esm.GetStream(streamID)
	if err == nil {
		// stream already exists
		return &EventStoreError{
			Op:  "NewStream",
			Err: fmt.Errorf("stream already exists"),
			Key: streamID.String(),
		}
	}

	// create a stream
	stream := &Stream{
		ID:        streamID,
		CreatedAt: time.Now(),
		Position:  0,
	}

	esm.streams = append(esm.streams, stream)
	return nil
}

// append events to a stream
func (esm *EventStoreManager) AppendToStream(streamID uuid.UUID, startPosition int, events []*Event) error {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	// check if the stream exists
	stream, err := esm.GetStream(streamID)
	if err != nil {
		return err
	}

	// check if the start position is valid
	if stream.Position != startPosition {
		return &EventStoreError{
			Op:  "appendToStream",
			Err: fmt.Errorf("invalid position"),
			Key: streamID.String(),
		}
	}

	// add to events
	startPosition = stream.Position
	for _, event := range events {
		esm.events = append(esm.events, event)
		streamEvent := &StreamEvent{
			StreamID: streamID,
			EventID:  event.ID,
			Position: startPosition,
		}
		esm.streamEvents = append(esm.streamEvents, streamEvent)
		// publish the event to subscribers
		esm.Publish(esm.ctx, stream, streamEvent, event)
		startPosition++
	}

	// update the stream position
	stream.Position += len(events)

	return nil
}

// read events from a stream
func (esm *EventStoreManager) ReadStream(streamID uuid.UUID, startPosition int, endPosition int) ([]*Event, error) {
	esm.lock.RLock()
	defer esm.lock.RUnlock()

	// check if the stream exists
	_, err := esm.GetStream(streamID)
	if err != nil {
		return nil, err
	}

	// read the events
	events := []*Event{}
	for _, streamEvent := range esm.streamEvents {
		if streamEvent.StreamID == streamID && streamEvent.Position >= startPosition && streamEvent.Position < endPosition {
			eventIndex := slices.IndexFunc(esm.events, func(e *Event) bool {
				return e.ID == streamEvent.EventID
			})
			events = append(events, esm.events[eventIndex])
		}
	}

	return events, nil
}

// get the position of the stream
func (esm *EventStoreManager) GetStreamPosition(streamID uuid.UUID) (int, error) {
	esm.lock.RLock()
	defer esm.lock.RUnlock()

	// check if the stream exists
	stream, err := esm.GetStream(streamID)
	if err != nil {
		return 0, err
	}

	return stream.Position, nil
}

// get the stream
func (esm *EventStoreManager) ReadAllStreams(streamID uuid.UUID) ([]*Event, error) {
	esm.lock.RLock()
	defer esm.lock.RUnlock()

	// check if the stream exists
	_, err := esm.GetStream(streamID)
	if err != nil {
		return nil, err
	}

	// read the events
	events := make([]*Event, 0)
	for _, streamEvent := range esm.streamEvents {
		if streamEvent.StreamID == streamID {
			events = append(events, esm.events[streamEvent.Position])
		}
	}

	return events, nil
}

// delete streams
func (esm *EventStoreManager) DeleteStreams(ctx context.Context) error {
	started := make(chan bool)
	go func() {
		// signal goroutine started
		started <- true

		// Create a ticker that ticks at a fixed interval (e.g., every minute).
		ticker := time.NewTicker(time.Duration(esm.DeleteStreamsInterval) * time.Second)
		// defer ticker stop
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// find streams that have expired
				// and delete them
				for _, stream := range esm.streams {
					if stream.CreatedAt.Add(time.Duration(esm.DeleteStreamsTTL) * time.Second).Before(time.Now()) {
						esm.DeleteStream(stream.ID)
					}
				}
			}
		}
	}()

	// wait for the channel to start
	<-started
	return nil
}

// delete a stream
func (esm *EventStoreManager) DeleteStream(streamID uuid.UUID) error {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	// check if the stream exists
	_, err := esm.GetStream(streamID)
	if err != nil {
		return err
	}

	// get stream events
	streamEvents := make([]*StreamEvent, 0)
	for _, streamEvent := range esm.streamEvents {
		if streamEvent.StreamID == streamID {
			streamEvents = append(streamEvents, streamEvent)
		}
	}

	// delete the stream events
	esm.streamEvents = slices.DeleteFunc(esm.streamEvents, func(e *StreamEvent) bool {
		return e.StreamID == streamID
	})

	// delete the events
	for _, streamEvent := range streamEvents {
		esm.events = slices.DeleteFunc(esm.events, func(e *Event) bool {
			return e.ID == streamEvent.EventID
		})
	}

	// delete the stream
	esm.streams = slices.DeleteFunc(esm.streams, func(s *Stream) bool {
		return s.ID == streamID
	})

	// delete stream from subscriptions
	esm.deleteStreamFromSubscriptions(streamID)

	return nil
}

// check if a stream exists
func (esm *EventStoreManager) GetStream(streamID uuid.UUID) (*Stream, error) {
	for _, stream := range esm.streams {
		if stream.ID == streamID {
			return stream, nil
		}
	}
	return nil, &EventStoreError{
		Op:  "GetStream",
		Err: fmt.Errorf("stream not found"),
		Key: streamID.String(),
	}
}

// func for adding events to the event store
// mostly for testing purposes
func (esm *EventStoreManager) AddEvent(event *Event) {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	esm.events = append(esm.events, event)
}

// get an event
func (esm *EventStoreManager) getEvent(eventID uuid.UUID) (*Event, error) {
	for _, event := range esm.events {
		if event.ID == eventID {
			return event, nil
		}
	}
	return nil, &EventStoreError{
		Op:  "GetEvent",
		Err: fmt.Errorf("event not found"),
		Key: eventID.String(),
	}
}
