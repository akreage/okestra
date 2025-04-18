package eventstore_test

import (
	"testing"
	"time"

	"github.com/acreage/okestra/eventstore"
	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {

	// create a new event
	t.Run("CreateNewEvent", func(t *testing.T) {
		data := []byte(`{"key1": "value1", "key2": 123}`)
		metadata := map[string]interface{}{"orchastrator": "test-orchastrator", "workflow": "test-workflow", "activity": "test-activity"}
		ev := "eventTested"
		timestamp := time.Now()
		event := eventstore.NewEvent(ev, data, metadata, timestamp)

		assert.NotNil(t, event)
		assert.Equal(t, "eventTested", event.Event)
		assert.Equal(t, data, event.Data)
		assert.NotEmpty(t, event.ID)
		assert.NotZero(t, event.Timestamp)
	})

	t.Run("AppendEvent&GetEvents", func(t *testing.T) {
		// create a new event
		event := createNewEvent()

		// event manager
		manager := eventstore.NewEventStoreManager()

		// create a new event manager
		assert.Equal(t, 0, len(manager.GetEvents()))
		manager.AppendEvent(event)
		assert.Equal(t, 1, len(manager.GetEvents()))

		// get events
		evt := manager.GetEvents()[0]
		assert.Equal(t, event.ID, evt.ID)
	})

	t.Run("DeleteEvent", func(t *testing.T) {
		// create a new event
		event := createNewEvent()

		// event manager
		manager := eventstore.NewEventStoreManager()
		manager.AppendEvent(event)

		// delete event
		assert.Equal(t, 1, len(manager.GetEvents()))
		manager.DeleteEvent(event.ID)
		assert.Equal(t, 0, len(manager.GetEvents()))
	})

	t.Run("GetEvent - Event not found", func(t *testing.T) {
		// create a new event
		event := createNewEvent()

		// event manager
		manager := eventstore.NewEventStoreManager()
		evt, err := manager.GetEvent(event.ID)
		assert.Error(t, err)
		assert.Nil(t, evt)
	})

	t.Run("DeleteEvent - Event not found", func(t *testing.T) {
		// create a new event
		event := createNewEvent()

		// event manager
		manager := eventstore.NewEventStoreManager()
		err := manager.DeleteEvent(event.ID)
		assert.Error(t, err)
	})
}

// helpers
// create a new event
func createNewEvent() *eventstore.Event {
	data := []byte(`{"key1": "value1", "key2": 123}`)
	metadata := map[string]interface{}{"orchastrator": "test-orchastrator", "workflow": "test-workflow", "activity": "test-activity"}
	ev := "eventTested"
	timestamp := time.Now()
	return eventstore.NewEvent(ev, data, metadata, timestamp)
}
