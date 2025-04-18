package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/acreage/okestra/env"
	"github.com/acreage/okestra/pg"
	"github.com/acreage/okestra/telemetry"
)

type Supervisor struct {
	env.SupervisorEnv
	telemetry.TelemetryInterface
	children       map[string]*ChildSpec
	supervisorName string
	isMaster       bool
	spec           SupervisorSpec
	ID             string
	status         Status
	pg             pg.PGInterface[SupervisorInterface, SupervisableInterface]
	ctx            context.Context
	stats          *SupervisorStats
	mu             sync.RWMutex
	wg             sync.WaitGroup
	statusChan     chan Status
}

// creates a new supervisor
func NewSupervisor(
	ctx context.Context,
	spec SupervisorSpec,
	env env.SupervisorEnv,
	pg pg.PGInterface[SupervisorInterface, SupervisableInterface],
	tel telemetry.TelemetryInterface,
) *Supervisor {
	return &Supervisor{
		supervisorName:     spec.Key,
		children:           make(map[string]*ChildSpec),
		spec:               spec,
		status:             StatusRunning,
		ctx:                ctx,
		mu:                 sync.RWMutex{},
		wg:                 sync.WaitGroup{},
		ID:                 spec.Key,
		SupervisorEnv:      env,
		pg:                 pg,
		stats:              &SupervisorStats{},
		TelemetryInterface: tel,
		statusChan:         nil,
		isMaster:           true,
	}
}

// starts the supervisor itself
func (s *Supervisor) Start(supervisorName string) (string, <-chan Status, error) {
	// if supervisor is not set, defaults to master
	if supervisorName != "" {
		s.supervisorName = supervisorName
		s.isMaster = false
		s.statusChan = make(chan Status)
	}

	// check if supervisor is already running
	if s.status != StatusRunning {
		return "", nil, &SupervisorError{
			Op:  "Start",
			Err: errors.New("supervisor is not running, status is " + string(s.status)),
			Key: s.ID,
		}
	}

	// register as group owner
	err := s.pg.RegisterGroup(s)
	if err != nil {
		// stop the supervisor
		// TODO: implement stop as a supervisable interface
		s.status = StatusFailed
		return "", nil, &SupervisorError{
			Op:  "Start",
			Err: err,
			Key: s.ID,
		}
	}

	// start a goroutine to monitor context cancellation
	go func() {
		<-s.ctx.Done()
		s.Stop()
	}()

	return s.ID, s.statusChan, nil
}

// for pg group owner interface
func (s *Supervisor) Key() string {
	return s.ID
}

// is local currently defaults true
func (s *Supervisor) IsLocal() bool {
	return s.SupervisorIsLocal
}

func (s *Supervisor) NodeName() string {
	return s.SupervisorNodeName
}

func (s *Supervisor) Stop() error {
	// leave the group
	err := s.pg.RemoveGroup(s)
	if err != nil {
		return err
	}

	// stop all children
	s.wg.Wait()

	// set read lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// set status to stopped
	s.status = StatusStopped
	// remove the supervisor's children
	s.children = make(map[string]*ChildSpec)

	return nil
}

// handle signals
func (s *Supervisor) HandleSignal(source string, signal pg.PGSignal) error {
	// lock the supervisor
	s.mu.Lock()

	// if not is master, and its request to stop, then stop the supervisor
	if !s.isMaster && signal.Reason == pg.PGSignalKillReasonMemberKilled {
		// unlock
		s.mu.Unlock()
		s.Stop()
		return nil
	}

	// find the child by id
	child, ok := s.children[source]
	if !ok {
		s.mu.Unlock()
		// TODO: force deletion grom children list with pg
		return &SupervisorError{
			Op:  "HandleSignal",
			Err: errors.New("child not found"),
			Key: s.ID,
		}
	}

	// if signal is a join, check if the child is to be monitored
	if signal.Signal == pg.PGSignalCreateKind {
		if s.spec.MonitorChildren {
			// TODO: we can implement a fine grain monitoring strategy here
			// for now, we just monitor all children
			go s.pg.Monitor(s, source)
		}
		s.mu.Unlock()
		return nil
	}

	// undertand the supervisor's restart strategy
	// find the reason for the signal
	// find the child by id
	// supervisors are stateless, hence does not keep state of children call
	err := s.identifyRestartStrategy(child, signal)
	s.mu.Unlock()
	return err
}

func (s *Supervisor) ChildrenStats() (*map[string]*ChildSpec, error) {
	return &s.children, nil
}

func (s *Supervisor) SupervisedStats() (*SupervisorStats, error) {
	stats := s.stats
	stats.Status = s.status
	return stats, nil
}

func (s *Supervisor) ChildStats(key string) (*ChildSpec, error) {
	// lock the supervisor
	s.mu.RLock()
	defer s.mu.RUnlock()

	// find the child by id
	child, ok := s.children[key]
	if !ok {
		return nil, &SupervisorError{
			Op:  "ChildStats",
			Err: errors.New("child not found"),
			Key: s.ID,
		}
	}
	return child, nil
}

func (s *Supervisor) UpdateStatus(status Status) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status = status
}

func (s *Supervisor) StopChild(key string) error {
	// Lock to prevent race conditions
	s.mu.Lock()
	defer s.mu.Unlock()

	// If key is specified, only stop that child
	child, ok := s.children[key]
	if !ok {
		return &SupervisorError{
			Op:  "StopChild",
			Err: errors.New("child not found"),
			Key: s.ID,
		}
	}

	// Stop the child
	child.sub.Stop()
	// Remove from children map to prevent it from being monitored
	s.RemoveChild(key)
	return nil
}

// start a child
func (s *Supervisor) StartChild(child SupervisableInterface) error {
	// locking mechanism here
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if child already exists
	if _, ok := s.children[child.Key()]; ok {
		return &SupervisorError{
			Op:  "StartChild",
			Err: errors.New("child already exists"),
			Key: s.ID,
		}
	}

	// if reached max children, return an error
	if len(s.children) >= s.spec.MaxChildren {
		return &SupervisorError{
			Op:  "StartChild",
			Err: errors.New("max children reached"),
			Key: s.ID,
		}
	}

	return s.doStartChild(child)
}

// start a child
func (s *Supervisor) doStartChild(child SupervisableInterface) error {
	// create a wait group for the
	s.wg.Add(1)
	// increment the starting stats count
	s.stats.Starting++

	// if the supe
	if !s.IsLocal() {
		s.stats.Starting--
		return &SupervisorError{
			Op:  "StartChild",
			Err: errors.New("supervisor is not local"),
			Key: s.ID,
		}
	}

	go func() {
		// always call done on the wait group
		defer s.wg.Done()

		// Recover from panics
		defer func() {
			if r := recover(); r != nil {
				s.stats.Failures++
				s.pg.Leave(s.ID, child.Key(), pg.PGSignal{
					RecipientKey: s.ID,
					Signal:       pg.PGSignalTypePanic,
					Reason:       pg.PGSignalKillReasonFatalError,
					Mode:         pg.PGSignalSendModeUnicast,
					Source:       pg.PGSignalSourceMember,
					SourceKey:    child.Key(),
				})
			}
		}()

		// start the child wrapped in a telemetry event
		measurements := map[string]interface{}{}
		metadata := map[string]interface{}{
			"child":      child.Key(),
			"supervisor": s.ID,
		}

		// status channel
		var statusChan <-chan Status

		// execute within telemetry context
		// this helps to capture telemetry events
		err := s.TelemetryInterface.Execute(telemetry.SupervisorEvent, func() (map[string]interface{}, map[string]interface{}, error) {
			// start the child
			childID, status, err := child.Start(s.ID)
			statusChan = status
			if err != nil {
				return measurements, metadata, err
			}

			// update the child spec
			s.addOrUpdateChildSpec(childID, child)

			// update the stats

			s.stats.Running++
			s.stats.Starting--

			// join supervisor group
			err = s.pg.Join(s.ID, child)
			if err != nil {
				return measurements, metadata, err
			}

			return measurements, metadata, nil
		})

		if err != nil {
			return
		}

		// Keep monitoring the child status until a terminal status is received
		// or the context is canceled
		for status := range statusChan {
			// update the metadata
			metadata["status"] = status

			// Update the child status in the map
			if childSpec, found := s.children[child.Key()]; found {
				childSpec.Status = status
			}

			// Handle terminal statuses
			if status == StatusDone {
				// leave the group
				s.pg.Leave(s.ID, child.Key(), pg.PGSignal{
					RecipientKey: s.ID,
					Signal:       pg.PGSignalTypeNormal,
					Reason:       pg.PGSignalKillReasonLeave,
					Mode:         pg.PGSignalSendModeUnicast,
					Source:       pg.PGSignalSourceMember,
					SourceKey:    child.Key(),
				})

				err := createError("runningChild", errors.New("child done"), child.Key())
				s.publishErrToTelemetry(err, metadata)
				return
			}

			if status != StatusRunning && status != StatusStarting {
				err := createError(string(status), fmt.Errorf("child %s", status), child.Key())
				s.publishErrToTelemetry(err, metadata)
				panic(err)
			}

			// Add a check to exit if the child has been removed
			if _, exists := s.children[child.Key()]; !exists {
				// send a request to stop the child
				child.Stop()
				err := createError("runningChild", errors.New("child under supervision, has been removed"), child.Key())
				s.publishErrToTelemetry(err, metadata)
				panic(err)
			}
		}

		// if channel is closed, stop the child
		err = createError("runningChild", errors.New("child stopped"), child.Key())
		s.publishErrToTelemetry(err, metadata)
	}()

	return nil
}

func (s *Supervisor) addOrUpdateChildSpec(childID string, child SupervisableInterface) {
	// lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// get child spec else create a new one
	childSpec, ok := s.children[childID]
	if !ok {
		// update the child spec
		s.children[childID] = &ChildSpec{
			FailureCount: 0,
			RetryCount:   0,
			LastFailure:  time.Time{},
			LastRetry:    time.Time{},
			Status:       StatusRunning,
			StartedAt:    time.Now(),
			sub:          child,
		}
	} else {
		// reassign the child spec's sub
		childSpec.Status = StatusRunning
		childSpec.StartedAt = time.Now()
		childSpec.LastRetry = time.Now()
	}
}

// identify the restart strategy
func (s *Supervisor) identifyRestartStrategy(child *ChildSpec, signal pg.PGSignal) error {
	// identify the restart strategy
	// if transient or permanent and reason for failure is not a crash,
	// check child spec, if max retries isn't reached in the given period, restart the child
	// if max retries is reached, return an error
	// if always, always restart if the max retries constraints aren't met
	// if never, do nothing
	switch s.spec.RestartStrategy {
	case SupervisorRestartAlways:
		return s.tryRestartChild(child)
	case SupervisorRestartTransient:
		// check if the failure was
		// restart the child
		if signal.Reason == pg.PGSignalKillReasonLeave {
			s.RemoveChild(child.sub.Key())
			// do not restart. completed task
			return nil
		}
		return s.tryRestartChild(child)
	default:
		s.RemoveChild(child.sub.Key())
		// no action required
		return nil
	}
}

func (s *Supervisor) tryRestartChild(child *ChildSpec) error {
	if s.hasMaxedOutRetries(child) {
		s.stats.Failures++
		s.stats.Completed++

		// remove the child
		s.RemoveChild(child.sub.Key())

		// take the appropriate action
		s.takeFailureAction()

		return &SupervisorError{
			Op:  "IdentifyRestartStrategy",
			Err: errors.New("max retries reached within set period"),
			Key: s.ID,
		}
	}

	// update and restart the child
	s.updateChildSpec(child)
	return s.doStartChild(child.sub)
}

// has retries been met
func (s *Supervisor) hasMaxedOutRetries(child *ChildSpec) bool {
	// get the last failure time
	// if no last failure, return false
	if child.LastFailure.IsZero() {
		return false
	}

	// get time since last failure in milliseconds
	timeSinceFailure := time.Since(child.LastFailure).Milliseconds()

	// check if we've exceeded retry period (in ms) and reset count
	if timeSinceFailure > int64(s.spec.RetryPeriod) {
		child.FailureCount = 0
		child.RetryCount = 0
		return false
	}

	// we are within the retry period, check if the child retries exceeds the max retries
	if child.RetryCount >= s.spec.MaxRetries {
		return true
	}

	return false
}

// updates the status of a child in the supervisor
func (s *Supervisor) RemoveChild(name string) {
	// ensure the child exists first
	if _, ok := s.children[name]; !ok {
		return
	}

	// remove the child
	s.stats.Running--
	s.stats.Completed++
	delete(s.children, name)
}

// update the child spec data
func (s *Supervisor) updateChildSpec(child *ChildSpec) {
	child.LastFailure = time.Now()
	child.FailureCount++
	child.RetryCount++
	child.Status = StatusFailed
}

func createError(op string, err error, key string) *SupervisorError {
	return &SupervisorError{
		Op:  op,
		Err: err,
		Key: key,
	}
}

func (s *Supervisor) publishErrToTelemetry(err error, metadata map[string]interface{}) {
	metadata["error"] = err
	s.TelemetryInterface.TriggerEvent(
		telemetry.SupervisorStateErrorEvent,
		map[string]interface{}{
			"occurredAt": time.Now().UnixMilli(),
		},
		metadata,
	)
}

// if a child fails, take the appropriate action
func (s *Supervisor) takeFailureAction() {
	if s.spec.FailStrategy == SupervisorFailOneStrategy {
		return
	}

	// stop the supervisor
	go s.Stop()
}
