package supervisor_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/acreage/okestra/env"
	"github.com/acreage/okestra/mocks"
	"github.com/acreage/okestra/pg"
	"github.com/acreage/okestra/supervisor"
	"github.com/acreage/okestra/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	supervisorSpec = supervisor.SupervisorSpec{
		Name:            "test-supervisor",
		Key:             "test-supervisor",
		FailStrategy:    supervisor.SupervisorFailOneStrategy,
		RestartStrategy: supervisor.SupervisorRestartTransient,
		Type:            supervisor.SubSupervisorType,
		MaxRetries:      3,
		RetryPeriod:     500,
		GlobalLock:      false,
		MaxChildren:     3,
		MonitorChildren: true,
	}
)

// create a worker to be supervised
type worker struct {
	ctx context.Context
	// name
	name string
	// supervisor
	supervisor string
	// actions are registered here
	actions []string
	status  chan supervisor.Status
}

func (w *worker) Start(supName string) (string, <-chan supervisor.Status, error) {
	w.supervisor = supName
	w.actions = append(w.actions, "start")
	if w.status == nil {
		w.status = make(chan supervisor.Status)
	}
	return w.name, w.status, nil
}

func (w *worker) Stop() error {
	w.actions = append(w.actions, "stop")
	return nil
}

func (w *worker) Context() context.Context {
	return w.ctx
}

func (w *worker) SetContext(ctx context.Context) {
	w.ctx = ctx
}

func (w *worker) HandleSignal(source string, signal pg.PGSignal) error {
	w.actions = append(w.actions, "handleSignal")
	close(w.status)
	return nil
}

func (w *worker) UpdateStatus(status supervisor.Status) {
	// send status to the status channel
	w.actions = append(w.actions, string(status))
	w.status <- status
}

func (w *worker) Key() string {
	return w.name
}

func TestChildStats(t *testing.T) {
	t.Run("ok: get child stats", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}

		// start supervisor
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		stats, err := sup.ChildStats(child.Key())
		assert.NoError(t, err)
		assert.Equal(t, supervisor.StatusRunning, stats.Status)
	})

	t.Run("error: child not found", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		stats, err := sup.ChildStats("worker-supervisor")
		assert.Error(t, err)
		assert.Nil(t, stats)
	})
}

func TestStopChild(t *testing.T) {
	t.Run("ok: stop child", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		err = sup.StopChild(child.Key())
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		stats, err := sup.ChildStats(child.Key())
		assert.Error(t, err)
		assert.Nil(t, stats)
	})

	t.Run("error: child not found", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		err := sup.StopChild("worker-supervisor")
		assert.Error(t, err)
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "StopChild", supErr.Op)
		assert.Equal(t, "child not found", supErr.Err.Error())
	})
}

// test teh starting of a supervisor
func TestSupervisorStart(t *testing.T) {
	t.Run("ok: start supervisor", func(t *testing.T) {
		// create a supervisor
		pgMock := new(mocks.PGMock)
		pgMock.On("RegisterGroup", mock.Anything).Return(nil)

		supervisor := createSupervisor(pgMock)
		// start the supervisor
		name, _, err := supervisor.Start("")
		assert.NoError(t, err)
		assert.NotEmpty(t, name)
		assert.Equal(t, "test-supervisor", name)
	})

	t.Run("error: supervisor status is not running", func(t *testing.T) {
		// create a supervisor
		pgMock := new(mocks.PGMock)
		pgMock.On("RegisterGroup", mock.Anything).Return(nil)

		// create a supervisor
		sup := createSupervisor(pgMock)
		sup.UpdateStatus(supervisor.StatusDone)

		_, _, err := sup.Start("")
		assert.Error(t, err)
		// verify error is supervisor error
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "Start", supErr.Op)
		assert.Equal(t, "supervisor is not running, status is done", supErr.Err.Error())
	})

	t.Run("error: unable to start supervisor, registering pg group failed", func(t *testing.T) {
		// create a supervisor
		pgMock := new(mocks.PGMock)
		sup := createSupervisor(pgMock)
		// error msg

		pgErr := &pg.PGError{
			Op:  "RegisterGroup",
			Err: fmt.Errorf("failed to register pg group"),
			Key: "test-supervisor",
		}

		// mock the pg group
		pgMock.On("RegisterGroup", sup).Return(pgErr)

		// start the supervisor
		name, _, err := sup.Start("")
		assert.Error(t, err)
		assert.Empty(t, name)
	})
}

// key tests
func TestKey(t *testing.T) {
	t.Run("ok: get key", func(t *testing.T) {
		// create a supervisor
		sup := unmockedSupervisor()
		assert.Equal(t, "test-supervisor", sup.Key())
	})
}

func TestIsLocal(t *testing.T) {
	t.Run("ok: is local", func(t *testing.T) {
		// create a supervisor
		sup := unmockedSupervisor()
		sup.SupervisorIsLocal = true
		assert.True(t, sup.IsLocal())
	})
}

func TestNodeName(t *testing.T) {
	t.Run("ok: get node name", func(t *testing.T) {
		// create a supervisor
		sup := unmockedSupervisor()
		sup.SupervisorNodeName = "test-node"
		assert.Equal(t, "test-node", sup.NodeName())
	})
}

// a supervisor to start a child
func TestStartChild(t *testing.T) {
	t.Run("ok: start child and return done", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}

		// start supervisor
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for a second
		time.Sleep(100 * time.Millisecond)

		// update status to done
		spec, err := sup.ChildStats(child.Key())
		assert.Equal(t, supervisor.StatusRunning, spec.Status)
		assert.NotNil(t, spec)
		assert.NoError(t, err)

		stats, err := sup.SupervisedStats()
		assert.NoError(t, err)
		children, err := sup.ChildrenStats()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(*children))
		assert.Equal(t, 1, stats.Running)
		assert.Equal(t, 0, stats.Completed)
		assert.Equal(t, 0, stats.Failures)
		assert.Equal(t, 0, stats.Starting)

		child.UpdateStatus(supervisor.StatusDone)
		assert.Equal(t, child.actions[0], "start")
		assert.Equal(t, child.actions[len(child.actions)-1], "done")
		time.Sleep(100 * time.Millisecond)
		stats, err = sup.SupervisedStats()
		assert.NoError(t, err)
		children, err = sup.ChildrenStats()
		assert.NoError(t, err)
		assert.Equal(t, 0, len(*children))
		assert.Equal(t, 0, stats.Running)
		assert.Equal(t, 1, stats.Completed)
		assert.Equal(t, 0, stats.Failures)
		assert.Equal(t, 0, stats.Starting)
	})

	t.Run("error: child already exists", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(200 * time.Millisecond)
		// start the child again
		err = sup.StartChild(child)
		assert.Error(t, err)
		// verify error is supervisor error
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "StartChild", supErr.Op)
		assert.Equal(t, "child already exists", supErr.Err.Error())
	})

	t.Run("error: max children reached", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		workers := make([]*worker, 4)
		for i := 0; i < 4; i++ {
			workers[i] = &worker{name: fmt.Sprintf("worker-%d", i)}
		}

		err := sup.StartChild(workers[0])
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)
		err = sup.StartChild(workers[1])
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)
		err = sup.StartChild(workers[2])
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)
		err = sup.StartChild(workers[3])
		assert.Error(t, err)
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "StartChild", supErr.Op)
		assert.Equal(t, "max children reached", supErr.Err.Error())
	})

	t.Run("ok: child is failed", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)
		child.UpdateStatus(supervisor.StatusFailed)

		// sleep for 100ms
		// check if child was recreated
		// then delete again
		time.Sleep(100 * time.Millisecond)
		childStats, err := sup.ChildStats(child.Key())
		assert.NoError(t, err)
		assert.Equal(t, supervisor.StatusRunning, childStats.Status)

		// delete the child
		// err = sup.StopChild(child.Key())
		// assert.NoError(t, err)
	})

	t.Run("ok: supervisor is not local", func(t *testing.T) {
		// create a supervisor
		sup := unmockedSupervisor()
		sup.SupervisorIsLocal = false
		assert.False(t, sup.IsLocal())

		// start the supervisor
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.Error(t, err)

		// verify error is supervisor error
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "StartChild", supErr.Op)
		assert.Equal(t, "supervisor is not local", supErr.Err.Error())
	})

	// test with context timeout
	t.Run("error: context timeout", func(t *testing.T) {
		// create a supervisor
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		sup := supervisor.NewSupervisor(
			ctx,
			supervisorSpec,
			env.SupervisorEnv{
				SupervisorIsLocal:     true,
				SupervisorMaxChildren: 3,
			},
			pg.NewProcessGroup[supervisor.SupervisorInterface, supervisor.SupervisableInterface](),
			telemetry.StartTelemetryServer(),
		)

		_, _, err := sup.Start("")
		assert.NoError(t, err)

		// start a child
		child := &worker{name: "worker-supervisor"}
		err = sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(500 * time.Millisecond)

		// get supervisor stats
		stats, err := sup.SupervisedStats()
		assert.NoError(t, err)
		assert.Equal(t, supervisor.StatusStopped, stats.Status)
	})

	t.Run("ok: no restarts for transient strategy - child left normally", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// worker sends done signal
		child.UpdateStatus(supervisor.StatusDone)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// get child from supervisor should return nil
		childStats, err := sup.ChildStats(child.Key())
		assert.Error(t, err)
		assert.Nil(t, childStats)

		// get specific error
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "ChildStats", supErr.Op)
		assert.Equal(t, "child not found", supErr.Err.Error())
	})

	t.Run("ok: set as never restart strategy", func(t *testing.T) {
		// create a supervisor
		spec := supervisorSpec
		spec.RestartStrategy = supervisor.SupervisorRestartNever
		_, sup := startSupervisorForTestWithSpec(t, spec)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// worker sends done signal
		child.UpdateStatus(supervisor.StatusFailed)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// get child from supervisor should return nil
		childStats, err := sup.ChildStats(child.Key())
		assert.Error(t, err)
		assert.Nil(t, childStats)

		// get specific error
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "ChildStats", supErr.Op)
		assert.Equal(t, "child not found", supErr.Err.Error())
	})

	t.Run("ok: set as always restart strategy", func(t *testing.T) {
		// create a supervisor
		spec := supervisorSpec
		spec.RestartStrategy = supervisor.SupervisorRestartAlways
		_, sup := startSupervisorForTestWithSpec(t, spec)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// worker sends done signal
		child.UpdateStatus(supervisor.StatusFailed)

		// sleep for 100ms
		time.Sleep(200 * time.Millisecond)

		// get child from supervisor should return nil
		childStats, err := sup.ChildStats(child.Key())
		assert.NoError(t, err)
		assert.Equal(t, supervisor.StatusRunning, childStats.Status)
		assert.Equal(t, 1, childStats.RetryCount)
		assert.Equal(t, 1, childStats.FailureCount)
	})

	t.Run("error: restarts strategy fails after max retries reached within set period", func(t *testing.T) {
		// create a supervisor
		spec := supervisorSpec
		spec.RestartStrategy = supervisor.SupervisorRestartTransient
		spec.MaxRetries = 3       // retry 3 times
		spec.RetryPeriod = 600000 // within 10 minutes

		_, sup := startSupervisorForTestWithSpec(t, spec)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// iterate over 3 times and fail
		for i := 1; i <= 3; i++ {
			child.UpdateStatus(supervisor.StatusFailed)
			time.Sleep(100 * time.Millisecond)
			// get the stats
			stats, _ := sup.ChildStats(child.Key())
			assert.Equal(t, supervisor.StatusRunning, stats.Status)
			assert.Equal(t, i, stats.RetryCount)
			assert.Equal(t, i, stats.FailureCount)
		}

		// fail again
		child.UpdateStatus(supervisor.StatusFailed)

		// sleep for 2 seconds
		time.Sleep(100 * time.Millisecond)

		// get child from supervisor should return nil
		childStats, err := sup.ChildStats(child.Key())
		assert.Error(t, err)
		assert.Nil(t, childStats)

		// get specific error
		var supErr *supervisor.SupervisorError
		assert.ErrorAs(t, err, &supErr)
		assert.Equal(t, "ChildStats", supErr.Op)
		assert.Equal(t, "child not found", supErr.Err.Error())
	})

	t.Run("ok: max time is never reached", func(t *testing.T) {
		// create a spec
		// create a supervisor
		spec := supervisorSpec
		spec.RestartStrategy = supervisor.SupervisorRestartTransient
		spec.MaxRetries = 3  // retry 3 times
		spec.RetryPeriod = 1 // within 1 ms

		_, sup := startSupervisorForTestWithSpec(t, spec)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// fail the child 5 times
		for i := 1; i <= 3; i++ {
			child.UpdateStatus(supervisor.StatusFailed)
			time.Sleep(100 * time.Millisecond)
			stats, _ := sup.ChildStats(child.Key())
			assert.Equal(t, supervisor.StatusRunning, stats.Status)
			assert.Equal(t, 1, stats.RetryCount)
			assert.Equal(t, 1, stats.FailureCount)
		}

		// get child from supervisor should return nil
		childStats, err := sup.ChildStats(child.Key())
		assert.NoError(t, err)
		assert.Equal(t, supervisor.StatusRunning, childStats.Status)
	})

	t.Run("error: child is removed from supervision and sends a running signal - child is sent a stop message", func(t *testing.T) {
		// create a supervisor
		_, sup := startSupervisorForTest(t)
		child := &worker{name: "worker-supervisor"}
		err := sup.StartChild(child)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// remove the child
		sup.RemoveChild(child.Key())

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// get child from supervisor should return nil
		childStats, err := sup.ChildStats(child.Key())
		assert.Error(t, err)
		assert.Nil(t, childStats)

		// worker sends a running signal
		child.UpdateStatus(supervisor.StatusRunning)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// get child from supervisor should return nil
		childStats, err = sup.ChildStats(child.Key())
		assert.Error(t, err)
		assert.Nil(t, childStats)
	})
}

func TestSupervisorSupervision(t *testing.T) {
	t.Run("ok: create another supervisor to be supervised", func(t *testing.T) {
		pg := pg.NewProcessGroup[supervisor.SupervisorInterface, supervisor.SupervisableInterface]()
		tel := telemetry.StartTelemetryServer()

		masterSpec := supervisorSpec
		masterSpec.Key = "master-supervisor"
		masterSpec.Name = "master-supervisor"
		masterSup := supervisor.NewSupervisor(
			context.Background(),
			masterSpec,
			env.SupervisorEnv{
				SupervisorIsLocal:     true,
				SupervisorMaxChildren: 3,
			},
			pg,
			tel,
		)
		name, statusChan, err := masterSup.Start("")
		assert.NoError(t, err)
		assert.Nil(t, statusChan)
		assert.Equal(t, "master-supervisor", name)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// create a child supervisor
		childSupSpec := supervisorSpec
		childSupSpec.Key = "child-supervisor"
		childSupSpec.Name = "child-supervisor"

		childSup := supervisor.NewSupervisor(
			context.Background(),
			childSupSpec,
			env.SupervisorEnv{
				SupervisorIsLocal:     true,
				SupervisorMaxChildren: 3,
			},
			pg,
			tel,
		)

		err = masterSup.StartChild(childSup)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// create a worker
		worker := &worker{name: "worker-supervisor"}
		err = childSup.StartChild(worker)
		assert.NoError(t, err)

		// sleep for 100ms
		time.Sleep(100 * time.Millisecond)

		// get master supervisor stats
		stats, err := masterSup.SupervisedStats()
		assert.NoError(t, err)
		assert.Equal(t, 1, stats.Running)
		assert.Equal(t, 0, stats.Completed)
		assert.Equal(t, 0, stats.Failures)
		assert.Equal(t, 0, stats.Starting)

		// get child supervisor stats
		childStats, err := childSup.SupervisedStats()
		assert.NoError(t, err)
		assert.Equal(t, 1, childStats.Running)
		assert.Equal(t, 0, childStats.Completed)
		assert.Equal(t, 0, childStats.Failures)
		assert.Equal(t, 0, childStats.Starting)

	})
}

func createSupervisor(pgMock *mocks.PGMock) *supervisor.Supervisor {
	// create a pg mock
	return supervisor.NewSupervisor(
		context.Background(),
		supervisorSpec,
		env.SupervisorEnv{
			SupervisorIsLocal: true,
		},
		pgMock,
		telemetry.StartTelemetryServer(),
	)
}

func startSupervisorForTestWithSpec(t *testing.T, spec supervisor.SupervisorSpec) (name string, sup *supervisor.Supervisor) {
	// create a supervisor
	sup = supervisor.NewSupervisor(
		context.Background(),
		spec,
		env.SupervisorEnv{
			SupervisorIsLocal:     true,
			SupervisorMaxChildren: 3,
		},
		pg.NewProcessGroup[supervisor.SupervisorInterface, supervisor.SupervisableInterface](),
		telemetry.StartTelemetryServer(),
	)

	name, _, err := sup.Start("")
	assert.NoError(t, err)
	return
}

func startSupervisorForTest(t *testing.T) (name string, sup *supervisor.Supervisor) {
	// create a supervisor
	sup = unmockedSupervisor()
	name, _, err := sup.Start("")
	assert.NoError(t, err)
	return
}

func unmockedSupervisor() *supervisor.Supervisor {
	// add a background
	return supervisor.NewSupervisor(
		context.Background(),
		supervisorSpec,
		env.SupervisorEnv{
			SupervisorIsLocal:     true,
			SupervisorMaxChildren: 3,
		},
		pg.NewProcessGroup[supervisor.SupervisorInterface, supervisor.SupervisableInterface](),
		telemetry.StartTelemetryServer(),
	)
}
