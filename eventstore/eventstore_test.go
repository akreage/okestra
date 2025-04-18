package eventstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/acreage/okestra/eventstore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEventStore(t *testing.T) {

	t.Run("ok: GetEvent - event exists", func(t *testing.T) {
		manager := eventstore.NewEventStoreManager()
		event := createNewEvent()
		manager.AddEvent(event)
		_, err := manager.GetEvent(event.ID)
		assert.Nil(t, err)
	})

	t.Run("error: GetEvent - event does not exist", func(t *testing.T) {
		eventID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		_, err := manager.GetEvent(eventID)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "GetEvent", esErr.Op)
		assert.Equal(t, eventID.String(), esErr.Key)
		assert.Equal(t, "event not found", esErr.Err.Error())
	})

	t.Run("error: ReadStreamPosition - stream does not exist", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		_, err := manager.GetStreamPosition(streamID)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "GetStream", esErr.Op)
		assert.Equal(t, streamID.String(), esErr.Key)
		assert.Equal(t, "stream not found", esErr.Err.Error())
	})

	t.Run("ok: AppendToStream - stream already exists", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		err := manager.NewStream(streamID)
		assert.Nil(t, err)

		// get stream position
		pos, err := manager.GetStreamPosition(streamID)
		assert.Nil(t, err)
		assert.Equal(t, 0, pos)

		// append event
		err = manager.AppendToStream(streamID, pos, []*eventstore.Event{createNewEvent()})
		assert.Nil(t, err)

		// get stream position
		pos, err = manager.GetStreamPosition(streamID)
		assert.Nil(t, err)
		assert.Equal(t, 1, pos)
	})

	t.Run("error: AppendToStream - stream does not exist", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()

		// append to stream error
		err := manager.AppendToStream(streamID, 0, []*eventstore.Event{createNewEvent()})
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "GetStream", esErr.Op)
		assert.Equal(t, streamID.String(), esErr.Key)
		assert.Equal(t, "stream not found", esErr.Err.Error())
	})

	t.Run("error: AppendToStream - invalid position", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		err := manager.NewStream(streamID)
		assert.Nil(t, err)

		// use an out of position to update stream
		err = manager.AppendToStream(streamID, 1, []*eventstore.Event{createNewEvent()})
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "appendToStream", esErr.Op)
		assert.Equal(t, streamID.String(), esErr.Key)
		assert.Equal(t, "invalid position", esErr.Err.Error())
	})

	t.Run("ok: ReadStream - stream exists", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		err := manager.NewStream(streamID)
		assert.Nil(t, err)

		// append event
		err = manager.AppendToStream(streamID, 0, []*eventstore.Event{createNewEvent(), createNewEvent(), createNewEvent()})
		assert.Nil(t, err)

		// read stream
		events, err := manager.ReadStream(streamID, 0, 1)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(events))
	})

	t.Run("error: ReadStream - stream does not exist", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		_, err := manager.ReadStream(streamID, 0, 1)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "GetStream", esErr.Op)
		assert.Equal(t, streamID.String(), esErr.Key)
		assert.Equal(t, "stream not found", esErr.Err.Error())
	})

	t.Run("ok: ReadAllStreams- stream exists", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		err := manager.NewStream(streamID)
		assert.Nil(t, err)

		// append event
		err = manager.AppendToStream(streamID, 0, []*eventstore.Event{createNewEvent(), createNewEvent(), createNewEvent()})
		assert.Nil(t, err)

		// read stream
		events, err := manager.ReadAllStreams(streamID)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(events))

		// get position
		pos, err := manager.GetStreamPosition(streamID)
		assert.Nil(t, err)
		assert.Equal(t, 3, pos)

		// insert into stream
		err = manager.AppendToStream(streamID, pos, []*eventstore.Event{createNewEvent(), createNewEvent(), createNewEvent()})
		assert.Nil(t, err)

		// read stream
		events, err = manager.ReadAllStreams(streamID)
		assert.Nil(t, err)
		assert.Equal(t, 6, len(events))
	})

	t.Run("ok: DeleteStream - stream exists", func(t *testing.T) {
		// create a channel to wait for stream to be deleted
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		err := manager.NewStream(streamID)
		assert.Nil(t, err)

		// set delete streams interval to 1 second
		manager.DeleteStreamsInterval = 1
		manager.DeleteStreamsTTL = 1

		// create streams and add events for a second
		manager.AppendToStream(streamID, 0, []*eventstore.Event{createNewEvent(), createNewEvent(), createNewEvent()})

		// wait for stream to be deleted
		// create to context
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
		defer cancel()
		manager.DeleteStreams(ctx)

		// check for stream, should find the stream and its events
		events, err := manager.ReadAllStreams(streamID)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(events))

		// do same after 2 seconds
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			events, err := manager.ReadAllStreams(streamID)
			assert.Nil(t, events)
			assert.Error(t, err)
			break
		}
	})

	t.Run("error: DeleteStream - stream does not exist", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		err := manager.DeleteStream(streamID)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "GetStream", esErr.Op)
		assert.Equal(t, streamID.String(), esErr.Key)
		assert.Equal(t, "stream not found", esErr.Err.Error())
	})
}
