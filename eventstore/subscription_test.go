package eventstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/acreage/okestra/eventstore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type TestHandler struct {
	events []*eventstore.Event
}

func (h *TestHandler) Handle(ctx context.Context, event *eventstore.Event) error {
	h.events = append(h.events, event)
	return nil
}

func TestSubscription(t *testing.T) {
	// create a new subscription
	t.Run("ok: Subscribe - subscription already exists - and new subscription created", func(t *testing.T) {
		streamID := uuid.New()
		handler := &TestHandler{}
		subscription := handler.createSubscription(streamID)

		// create event store manager
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		// return subcription already exists error
		err := manager.Subscribe(subscription)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "subscription", esErr.Op)
		assert.Equal(t, subscription.StreamID.String(), esErr.Key)
		assert.Equal(t, "stream already exists", esErr.Err.Error())

		// create a new subscription
		// and pass to the manager, should not return error
		newStreamID := uuid.New()
		subscription = handler.createSubscription(newStreamID)
		manager.NewStream(newStreamID)
		err = manager.Subscribe(subscription)
		assert.NoError(t, err)
	})

	t.Run("error: Subscribe - stream does not exist", func(t *testing.T) {
		streamID := uuid.New()
		handler := &TestHandler{}
		subscription := handler.createSubscription(streamID)
		manager := eventstore.NewEventStoreManager()
		err := manager.Subscribe(subscription)
		assert.Error(t, err)
	})

	t.Run("ok: Unsubscribe - unsubscribe from existing subscription", func(t *testing.T) {
		streamID := uuid.New()
		handler := &TestHandler{}
		subscription := handler.createSubscription(streamID)
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		// ensures there are no streams
		manager.Subscribe(subscription)
		manager.DeleteStream(streamID)

		err := manager.Unsubscribe(subscription.Name)
		assert.NoError(t, err)
	})

	t.Run("error: Unsubscribe - non existing subscription", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		err := manager.Unsubscribe("testSubscriber")
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "getSubscriptionManager", esErr.Op)
		assert.Equal(t, "testSubscriber", esErr.Key)
		assert.Equal(t, "subscription not found", esErr.Err.Error())
	})

	t.Run("ok: UnsubscribeStream - unsubscribe from existing stream", func(t *testing.T) {
		streamID1 := uuid.New()
		streamID2 := uuid.New()

		handler1 := &TestHandler{}
		handler2 := &TestHandler{}
		sub1 := handler1.createSubscription(streamID1)
		sub2 := handler2.createSubscription(streamID2)
		sub2.Name = "testSubscriber2"

		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID1)
		manager.NewStream(streamID2)
		manager.Subscribe(sub1)
		manager.Subscribe(sub2)

		// create subscriptions
		err := manager.UnsubscribeStream(sub1.Name, streamID1)
		assert.NoError(t, err)
	})

	t.Run("error: UnsubscribeStream - non existing subscription manager", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		err := manager.UnsubscribeStream("testSubscriber", streamID)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "getSubscriptionManager", esErr.Op)
		assert.Equal(t, "testSubscriber", esErr.Key)
		assert.Equal(t, "subscription not found", esErr.Err.Error())
	})

	t.Run("error: UnsubscribeStream - non existing stream", func(t *testing.T) {
		streamID := uuid.New()
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		handler := &TestHandler{}
		subscription := eventstore.NewSubscription("testSubscriber", streamID, handler.Handle)
		manager.Subscribe(subscription)
		unknownStreamID := uuid.New()
		err := manager.UnsubscribeStream("testSubscriber", unknownStreamID)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "UnsubscribeStream", esErr.Op)
		assert.Equal(t, unknownStreamID.String(), esErr.Key)
		assert.Equal(t, "subscription manager not found", esErr.Err.Error())
	})

	t.Run("error: Unsubscribe - subscription has streams", func(t *testing.T) {
		streamID := uuid.New()
		handler := &TestHandler{}
		subscription := handler.createSubscription(streamID)
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		// ensures there are no streams
		manager.Subscribe(subscription)

		err := manager.Unsubscribe(subscription.Name)
		assert.Error(t, err)
		// convert error to eventstore error
		esErr, ok := err.(*eventstore.EventStoreError)
		assert.True(t, ok)
		assert.Equal(t, "unsubscribe", esErr.Op)
		assert.Equal(t, subscription.Name, esErr.Key)
		assert.Equal(t, "subscription is still waiting for streams", esErr.Err.Error())
	})

	t.Run("ok: AppendToStream - get event on handler list", func(t *testing.T) {
		manager := eventstore.NewEventStoreManager()
		handler := &TestHandler{}
		streamID := createAndSubscribeToManager(manager, handler)

		// create event
		err := manager.AppendToStream(streamID, 0, []*eventstore.Event{createNewEvent(), createNewEvent()})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(handler.events))
	})

	t.Run("ok: NewSubscription", func(t *testing.T) {
		streamID := uuid.New()
		handler := &TestHandler{}
		subscription := handler.createSubscription(streamID)

		// create event store manager
		// create stream
		// subscribe to stream
		manager := eventstore.NewEventStoreManager()
		manager.NewStream(streamID)
		err := manager.Subscribe(subscription)
		// returns subscription already exists error
		assert.Error(t, err)

		// create event
		err = manager.AppendToStream(streamID, 0, []*eventstore.Event{createNewEvent()})
		assert.NoError(t, err)

		// get stream position and append new event
		position, err := manager.GetStreamPosition(streamID)
		assert.NoError(t, err)
		assert.Equal(t, 1, position)
		err = manager.AppendToStream(streamID, position, []*eventstore.Event{createNewEvent()})
		assert.NoError(t, err)

		// check if the event is handled
		assert.Equal(t, 2, len(handler.events))
	})

	t.Run("ok: Publish - out of order then right order", func(t *testing.T) {
		manager := eventstore.NewEventStoreManager()
		// set out of order to 5secs
		// we don't expect this to be called
		manager.OutOfOrderTime = 5
		handler := &TestHandler{}
		streamID := createAndSubscribeToManager(manager, handler)
		manager.NewStream(streamID)

		// get stream and send
		stream, _ := manager.GetStream(streamID)
		ctx, event, streamEvent := createEventAndStreamEvent(streamID, 0)
		manager.AddEvent(event)
		manager.Publish(ctx, stream, streamEvent, event)
		// validate handler has 1 event
		assert.Equal(t, 1, len(handler.events))
		// send out of order
		ctx, event1, streamEvent1 := createEventAndStreamEvent(streamID, 2)
		manager.AddEvent(event1)
		manager.Publish(ctx, stream, streamEvent1, event1)
		// validate handler has 1 event
		assert.Equal(t, 1, len(handler.events))
		// send in order
		ctx, event2, streamEvent2 := createEventAndStreamEvent(streamID, 1)
		manager.AddEvent(event2)
		manager.Publish(ctx, stream, streamEvent2, event2)
		// assert handler has 3 events
		assert.Equal(t, 3, len(handler.events))

		// ensure events are in order
		assert.Equal(t, event.ID, handler.events[0].ID)
		assert.Equal(t, event2.ID, handler.events[1].ID)
		assert.Equal(t, event1.ID, handler.events[2].ID)
	})

	t.Run("error: Publish - out of order then right order - event not found", func(t *testing.T) {
		manager := eventstore.NewEventStoreManager()
		// set out of order to 5secs
		// we don't expect this to be called
		manager.OutOfOrderTime = 5
		handler := &TestHandler{}
		streamID := createAndSubscribeToManager(manager, handler)
		manager.NewStream(streamID)

		// get stream and send
		stream, _ := manager.GetStream(streamID)
		ctx, event, streamEvent := createEventAndStreamEvent(streamID, 0)
		manager.Publish(ctx, stream, streamEvent, event)
		// validate handler has 1 event
		assert.Equal(t, 1, len(handler.events))
		// send out of order
		ctx, event1, streamEvent1 := createEventAndStreamEvent(streamID, 2)
		manager.Publish(ctx, stream, streamEvent1, event1)
		// validate handler has 1 event
		assert.Equal(t, 1, len(handler.events))
		// send in order
		ctx, event2, streamEvent2 := createEventAndStreamEvent(streamID, 1)
		manager.Publish(ctx, stream, streamEvent2, event2)

		// assert length equal 2
		assert.Equal(t, 2, len(handler.events))
		// ensure events are in order
		assert.Equal(t, event.ID, handler.events[0].ID)
		assert.Equal(t, event2.ID, handler.events[1].ID)
	})

	t.Run("ok: AppendToStream - out of order event", func(t *testing.T) {
		manager := eventstore.NewEventStoreManager()
		// set out of order time to 500ms
		manager.OutOfOrderTime = 1
		handler := &TestHandler{}
		streamID := createAndSubscribeToManager(manager, handler)
		manager.NewStream(streamID)

		// get stream
		stream, _ := manager.GetStream(streamID)
		ctx, event, streamEvent := createEventAndStreamEvent(streamID, 0)

		// create event
		err := manager.Publish(ctx, stream, streamEvent, event)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(handler.events))

		// create out of order event
		ctx, event, streamEvent = createEventAndStreamEvent(streamID, 2)
		err = manager.Publish(ctx, stream, streamEvent, event)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(handler.events))

		// wait for out of order time
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		timeout := time.After(3 * time.Second)
		for {
			select {
			case <-ticker.C:
				if len(handler.events) == 2 {
					assert.Equal(t, 2, len(handler.events))
					return
				}
			case <-timeout:
				assert.Fail(t, "out of order event not handled")
				return
			}
		}
	})
}

func (h *TestHandler) createSubscription(streamID uuid.UUID) *eventstore.Subscription {
	return eventstore.NewSubscription("testSubscriber", streamID, h.Handle)
}

func createAndSubscribeToManager(manager *eventstore.EventStoreManager, handler *TestHandler) uuid.UUID {
	streamID := uuid.New()
	subscription := handler.createSubscription(streamID)
	// register stream
	manager.NewStream(streamID)
	// subscribe to stream
	manager.Subscribe(subscription)
	return streamID
}

func createEventAndStreamEvent(streamID uuid.UUID, position int) (context.Context, *eventstore.Event, *eventstore.StreamEvent) {
	event := createNewEvent()
	streamEvent := &eventstore.StreamEvent{
		StreamID: streamID,
		EventID:  event.ID,
		Position: position,
	}

	// create context
	ctx := context.Background()

	// return event and stream event
	return ctx, event, streamEvent
}
