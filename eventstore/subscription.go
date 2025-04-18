package eventstore

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/acreage/okestra/env"
	"github.com/google/uuid"
	"github.com/sethvargo/go-envconfig"
)

// create a new subscription
func NewSubscription(name string, streamID uuid.UUID, handler SubscriptionHandler) *Subscription {
	return &Subscription{
		Name:     name,
		StreamID: streamID,
		Handler:  handler,
	}
}

// create a new subscription manager
// with a default stream
// the outo of order time is the time to wait for the correct event order
// after which send any cached events to the handler with the correct order
func NewSubscriptionManager(sub *Subscription, outOfOrderTime time.Duration) *SubscriptionManager {
	// create a new subscription manager
	sbManager := &SubscriptionManager{
		Name:           sub.Name,
		Streams:        make(map[uuid.UUID]*StreamSubscriber),
		OutOfOrderTime: outOfOrderTime,
	}

	// create a new stream subscriber
	streamSubscription := StreamSubscriber{
		currentPosition: 0,
		handler:         sub.Handler,
		oop:             []*EventOOP{},
	}

	// add stream subscriber to subscription manager
	sbManager.Streams[sub.StreamID] = &streamSubscription

	return sbManager
}

// subscribe to a stream
func (esm *EventStoreManager) Subscribe(subscription *Subscription) error {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	// ensure stream exists
	if _, err := esm.GetStream(subscription.StreamID); err != nil {
		return err
	}

	// get the subscription manager
	subManager, err := esm.getSubscriptionManager(subscription.Name)
	if err != nil {
		// subscription name does not exist,
		// create a new subscription manager with given name
		subManager = NewSubscriptionManager(subscription, time.Duration(esm.OutOfOrderTime*int(time.Second)))
		esm.subscriptions = append(esm.subscriptions, subManager)
	}

	// ensure stream does not exist already
	if _, ok := subManager.Streams[subscription.StreamID]; ok {
		return &EventStoreError{
			Op:  "subscription",
			Key: subscription.StreamID.String(),
			Err: fmt.Errorf("stream already exists"),
		}
	}

	// add stream subscription to manager
	streamSubscription := &StreamSubscriber{
		currentPosition: 0,
		handler:         subscription.Handler,
		oop:             []*EventOOP{},
	}

	subManager.Streams[subscription.StreamID] = streamSubscription

	return nil
}

// unsubscribe from a stream
func (esm *EventStoreManager) UnsubscribeStream(subscriberNamer string, streamID uuid.UUID) error {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	// get the subscription manager
	subManager, err := esm.getSubscriptionManager(subscriberNamer)
	if err != nil {
		return err
	}

	// ensure stream exists
	if _, ok := subManager.Streams[streamID]; !ok {
		return &EventStoreError{
			Op:  "UnsubscribeStream",
			Key: streamID.String(),
			Err: fmt.Errorf("subscription manager not found"),
		}
	}

	// delete stream from subscription manager
	delete(subManager.Streams, streamID)

	return nil
}

// unsubscribe a subscription
func (esm *EventStoreManager) Unsubscribe(subscriberNamer string) error {
	esm.lock.Lock()
	defer esm.lock.Unlock()

	// get the subscription manager
	subManager, err := esm.getSubscriptionManager(subscriberNamer)
	if err != nil {
		return err
	}

	// check if subscription streams is empty
	// otherwise, return error as subscriber is still waiting for streams
	if len(subManager.Streams) > 0 {
		return &EventStoreError{
			Op:  "unsubscribe",
			Key: subscriberNamer,
			Err: fmt.Errorf("subscription is still waiting for streams"),
		}
	}

	// remove subscription manager
	esm.subscriptions = slices.DeleteFunc(esm.subscriptions, func(sub *SubscriptionManager) bool {
		return sub.Name == subscriberNamer
	})

	return nil
}

// publish a stream event to subscribers
func (esm *EventStoreManager) Publish(ctx context.Context, stream *Stream, streamEvent *StreamEvent, event *Event) error {
	// return all subscribers that have subscribed to stream id
	subscriberManagers := esm.subscriberManagersByStreamID(stream.ID)

	for _, subManager := range subscriberManagers {
		streamSubscriber := subManager.Streams[stream.ID]

		// if the incoming event position equals current position,
		// execute the handler
		if streamSubscriber.currentPosition == streamEvent.Position {
			streamSubscriber.handler(ctx, event)
			// increase the current position
			streamSubscriber.currentPosition++

			// maybe execute out of order events
			esm.maybeExecuteOutOfOrderEvents(ctx, streamSubscriber)
			// maybe stop oop timer
			esm.maybeStopOopTimer(streamSubscriber)
			return nil
		}

		if streamEvent.Position < streamSubscriber.currentPosition {
			// if the incoming event position is less than current position,
			// send the event to the handler
			// this is probably due to a duplicate or event being sent after out of position wait time had expired
			streamSubscriber.handler(ctx, event)
			return nil
		}

		if streamEvent.Position > streamSubscriber.currentPosition {
			// append to oop event list
			streamSubscriber.oop = append(streamSubscriber.oop, &EventOOP{
				eventID:  event.ID,
				position: streamEvent.Position,
			})

			// if the timer is nil, set a new timer
			if streamSubscriber.timer == nil {
				// schedule out of order timer
				streamSubscriber.timer = time.AfterFunc(
					time.Duration(esm.OutOfOrderTime)*time.Second,
					esm.executeOutOfOrder(ctx, subManager, stream),
				)
			}

			// sort the oops by position
			slices.SortFunc(streamSubscriber.oop, func(a, b *EventOOP) int {
				return a.position - b.position
			})

			return nil
		}
	}

	return nil
}

// maybe execute out of order events
func (esm *EventStoreManager) maybeExecuteOutOfOrderEvents(ctx context.Context, streamSubscriber *StreamSubscriber) {
	// register sent out event ids
	sentOutEventIDs := []uuid.UUID{}

	// if oop is empty, stop timer
	if len(streamSubscriber.oop) == 0 {
		esm.maybeStopOopTimer(streamSubscriber)
		return
	}

	// run oop execution
	for _, oop := range streamSubscriber.oop {
		if streamSubscriber.currentPosition == oop.position {
			event, err := esm.getEvent(oop.eventID)
			if err != nil {
				// TODO: handle error with telemetry
				log.Printf("error getting event: %v", err)
				continue
			}
			streamSubscriber.handler(ctx, event)
			sentOutEventIDs = append(sentOutEventIDs, oop.eventID)
			streamSubscriber.currentPosition++
		}
	}

	// remove sent out event ids from oop
	streamSubscriber.oop = slices.DeleteFunc(streamSubscriber.oop, func(oop *EventOOP) bool {
		return slices.Contains(sentOutEventIDs, oop.eventID)
	})
}

func (esm *EventStoreManager) maybeStopOopTimer(streamSubscriber *StreamSubscriber) {
	if streamSubscriber.timer != nil && len(streamSubscriber.oop) == 0 {
		streamSubscriber.timer.Stop()
		streamSubscriber.timer = nil
	}
}

// execute out of order events cannot sit for too long
// so at some point we will send them out to the publisher's handler out of position
// we set a timer that flushes out the events here
// execute out of order timer
func (esm *EventStoreManager) executeOutOfOrder(ctx context.Context, manager *SubscriptionManager, stream *Stream) func() {
	return func() {
		// acquire lock
		esm.lock.Lock()
		defer esm.lock.Unlock()

		// get stream subscriber
		streamSubscriber := manager.Streams[stream.ID]

		if len(streamSubscriber.oop) == 0 {
			// reset timer
			streamSubscriber.timer = nil
			return
		}

		// execute out of order events
		// Find the highest position processed
		// update current position
		highestPos := 0
		for _, oop := range streamSubscriber.oop {
			// get event
			event, _ := esm.getEvent(oop.eventID)
			// execute handler
			streamSubscriber.handler(ctx, event)
			// update highest position
			if oop.position > highestPos {
				highestPos = oop.position
			}
		}

		// update current position if higher than currentPosition
		if highestPos > streamSubscriber.currentPosition {
			streamSubscriber.currentPosition = highestPos
		}

		// clear oop
		streamSubscriber.oop = []*EventOOP{}

		// reset timer
		streamSubscriber.timer = nil
	}
}

// get subscription by name
func (esm *EventStoreManager) getSubscriptionManager(name string) (*SubscriptionManager, error) {
	for _, sub := range esm.subscriptions {
		if sub.Name == name {
			return sub, nil
		}
	}

	return nil, &EventStoreError{
		Op:  "getSubscriptionManager",
		Key: name,
		Err: fmt.Errorf("subscription not found"),
	}
}

func (esm *EventStoreManager) subscriberManagersByStreamID(streamID uuid.UUID) []*SubscriptionManager {
	subscriberManagers := []*SubscriptionManager{}
	for _, subManager := range esm.subscriptions {
		if _, ok := subManager.Streams[streamID]; ok {
			subscriberManagers = append(subscriberManagers, subManager)
		}
	}

	return subscriberManagers
}

// delete stream from subscriptions
func (esm *EventStoreManager) deleteStreamFromSubscriptions(streamID uuid.UUID) {
	for _, subManager := range esm.subscriptions {
		delete(subManager.Streams, streamID)
	}
}

// fetch eventstore environment variables
func getEnvVars() *env.EventStoreEnv {
	var jwtConfigs env.EventStoreEnv
	ctx := context.Background()
	err := envconfig.Process(ctx, &jwtConfigs)
	if err != nil {
		// print error and fail with fmt
		log.Fatal(err)
	}
	return &jwtConfigs
}
