package eventstore

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// subscription handler
type SubscriptionHandler func(ctx context.Context, event *Event) error

// stream structure
type Stream struct {
	ID        uuid.UUID
	CreatedAt time.Time
	Position  int
}

// stream events
type StreamEvent struct {
	StreamID uuid.UUID
	EventID  uuid.UUID
	Position int
}

// event structure
type Event struct {
	ID        uuid.UUID
	Timestamp time.Time
	Event     string
	RequestID uuid.UUID
	Data      []byte
	Metadata  map[string]interface{} // orchastration Id, workflow Id, Activity Id, etc
}

// subscription event (subscribes to a stream)
type Subscription struct {
	Name     string    // name of the subscription
	StreamID uuid.UUID // stream id to subscribe to
	Handler  SubscriptionHandler
}

// subscription interface
type SubscriptionInterface interface {
	Subscribe(subscription *Subscription) error
	Unsubscribe(name string) error
	UnsubscribeStream(name string, streamID uuid.UUID) error
	Publish(ctx context.Context, stream *Stream, streamEvent *StreamEvent, event *Event) error
}

// eventstore interface
type EventStoreInterface interface {
	NewStream(streamID uuid.UUID, ttl time.Duration) error
	AppendToStream(streamID uuid.UUID, startPosition int, events []*Event) error
	ReadStream(streamID uuid.UUID, startPosition int, endPosition int) ([]*Event, error)
	GetStreamPosition(streamID uuid.UUID) (int, error)
	ReadAllStreams(streamID uuid.UUID) ([]Event, error)
	DeleteStreams() error
	DeleteStream(streamID uuid.UUID) error
}

// event manager interface
type EventInterface interface {
	AppendEvent(event Event)
	GetEvents() []Event
	GetEvent(id uuid.UUID) (Event, error)
	DeleteEvent(id uuid.UUID) error // unconventional, but to keep the events contained, best older onces are deleted
}

// event store error
type EventStoreError struct {
	Op  string
	Err error
	Key string
}

// error implementation
func (e *EventStoreError) Error() string {
	return fmt.Sprintf("%s: %s", e.Op, e.Err.Error())
}
