package eventstore

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// create a new event
func NewEvent(event string, data []byte, metadata map[string]interface{}, timestamp time.Time) *Event {
	return &Event{
		ID:        uuid.New(),
		Timestamp: timestamp,
		Event:     event,
		Data:      data,
		Metadata:  metadata,
	}
}

// append event to the event manager
func (em *EventStoreManager) AppendEvent(event *Event) {
	em.lock.Lock()
	defer em.lock.Unlock()
	em.events = append(em.events, event)
}

// get events
func (em *EventStoreManager) GetEvents() []*Event {
	em.lock.RLock()
	defer em.lock.RUnlock()
	return em.events
}

// get event by id
func (em *EventStoreManager) GetEvent(id uuid.UUID) (*Event, error) {
	em.lock.RLock()
	defer em.lock.RUnlock()
	for _, event := range em.events {
		if event.ID == id {
			return event, nil
		}
	}
	return nil, &EventStoreError{
		Op:  "GetEvent",
		Err: errors.New("event not found"),
		Key: id.String(),
	}
}

// delete event by id
func (em *EventStoreManager) DeleteEvent(id uuid.UUID) error {
	em.lock.Lock()
	defer em.lock.Unlock()
	for i, event := range em.events {
		if event.ID == id {
			em.events = append(em.events[:i], em.events[i+1:]...)
			return nil
		}
	}
	return &EventStoreError{
		Op:  "DeleteEvent",
		Err: errors.New("event not found"),
		Key: id.String(),
	}
}
