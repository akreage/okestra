package supervisor

import (
	"fmt"
	"time"

	"github.com/acreage/okestra/pg"
)

// supervisor fail strategy
type SupervisorFailStrategy string

// restart strategy
type SupervisorRestartStrategy string

// supervisor type
type SupervisorType string

// supervisor status
type Status string

// supervisor stats
type SupervisorStats struct {
	Running   int
	Completed int
	Failures  int
	Starting  int
	Status    Status
}

// supervisor children stats
type ChildSpec struct {
	FailureCount int
	RetryCount   int
	LastFailure  time.Time
	LastRetry    time.Time
	StartedAt    time.Time
	Status       Status
	sub          SupervisableInterface
}

const (
	// if a child process fails, the supervisor will try to restart only that failed child
	// this only works if the restart strategy is `permanent` or `transient`that fails due to a crash
	SupervisorFailOneStrategy SupervisorFailStrategy = "failOne" // if one fails, fail that worker only
	SupervisorFailAllStrategy SupervisorFailStrategy = "failAll" // if one fails, fail all

	// restart strategy
	// if a child process fails, always restart it
	SupervisorRestartAlways SupervisorRestartStrategy = "always"
	// if a child process fails, restart it only if it fails due to a crash
	SupervisorRestartTransient SupervisorRestartStrategy = "transient"
	// if a child process fails, do not restart it
	SupervisorRestartNever SupervisorRestartStrategy = "never"

	// supervisor type

	// supervisor supervises another supervisor
	SubSupervisorType SupervisorType = "supervisor"
	// supervisor supervises a worker
	TaskSupervisorType SupervisorType = "task"

	// supervisor status
	StatusRunning  Status = "running"
	StatusStopped  Status = "stopped"
	StatusFailed   Status = "failed" // should panic in this scenario
	StatusDone     Status = "done"
	StatusStarting Status = "starting"
)

// supervisor error
type SupervisorError struct {
	Op  string // the operation that caused the error
	Err error  // the original error
	Key string // related error key
}

// supervisor interface form process owners
// task managers register with supervisors and their lifecycle is managed by supervisors
type SupervisorInterface interface {
	// start a child
	StartChild(sub SupervisableInterface) error
	// stop a child
	StopChild(key string) error
	// get supervisor stats
	SupervisedStats() (*SupervisorStats, error)
	// get supervisor children stats
	ChildrenStats() (*map[string]*ChildSpec, error)
	// get an individual child stats
	ChildStats(key string) (*ChildSpec, error)
	// update supervisor status
	UpdateStatus(status Status)
	// inherited pg interface
	pg.PGGroupOwner[SupervisorInterface]
}

// supervisable interface says a process/channel can be supervised
type SupervisableInterface interface {
	Start(supervisor string) (string, <-chan Status, error)
	Stop() error
	// inherited pg member interface
	pg.PGMember[SupervisableInterface]
}

// specifications used to start a supervisor
type SupervisorSpec struct {
	Name            string // name of the supervisor. Should be unique
	Key             string // key of the supervisor for registring to pgs and others. Should be unique
	FailStrategy    SupervisorFailStrategy
	RestartStrategy SupervisorRestartStrategy
	Type            SupervisorType
	MaxRetries      int
	RetryPeriod     int
	GlobalLock      bool // if true, there will be a single instance of the supervisor name acrose all processes
	MaxChildren     int  // if max children is -1, then there is no limit
	MonitorChildren bool // if true, the supervisor will monitor its children
}

func (e *SupervisorError) Error() string {
	return fmt.Sprintf("supervisor error: %s", e.Err)
}
