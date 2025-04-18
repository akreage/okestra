package activity

import (
	"fmt"

	"github.com/acreage/okestra/supervisor"
	"github.com/google/uuid"
)

// an activity is a set of one or more tasks that are executed in a specific order
// this creates a task graph

// task function type
type TaskFunc func() error

const (
	ActivityStatusOnHold    = "onHold"
	ActivityStatusRunning   = "running"
	ActivityStatusCompleted = "completed"
	ActivityStatusFailed    = "failed"
)

// TaskParameterDefinition defines the expected parameters
type TaskParameterDefinition struct {
	Name         string
	Type         string // e.g., "string", "int", "float", etc.
	Required     bool
	DefaultValue interface{}
	Validation   func(interface{}) error // optional validation function
}

// task data struct
type TaskData struct {
	TaskId uuid.UUID
	Error  error
	Data   interface{}
}

// task funcs struct
// TaskDefinition defines a reusable task type
type TaskDefinition struct {
	FuncId     uuid.UUID
	TaskFunc   TaskFunc
	Parameters map[string]TaskParameterDefinition // Parameter schema
}

// task struct
type Task struct {
	Id          uuid.UUID
	Name        string
	Description string
	Definition  *TaskDefinition
	Args        map[string]interface{}
}

// task interface
type TaskInterface interface {
	Add(task *Task) error
	Delete(task *Task) error
	List() []*Task
	Get(taskId uuid.UUID) (*Task, error)
	GetByName(name string) (*Task, error)
	Count() int
	ValidateTaskArgs(task *Task) error
}

// pipeline runner interface
type PipelineRunnerInterface interface {
	StartSupervisor(spec *supervisor.SupervisorSpec) error
	StopSupervisor() error
	RunPipeline(activity *ActivityManager) error
}

// task definition interface
type TaskDefinitionInterface interface {
	Add(definition *TaskDefinition) error
	Delete(definition *TaskDefinition) error
	List() []*TaskDefinition
	Get(funcId uuid.UUID) (*TaskDefinition, error)
	GetByName(name string) (*TaskDefinition, error)
	Count() int
}

// Serializer interface for saving and loading activities
type Serializer interface {
	Save() ([]byte, error)
	Load(data []byte) error
}

// TODO: add a serializer to save the activity to file and load later

// activity interface
type ActivityInterface interface {
	AddTask(task *Task) error
	RemoveTask(task *Task) error
	AddEdge(fromTask *Task, toTask *Task) error
	RemoveEdge(fromTask *Task, toTask *Task) error
	GetGraph() *ActivityGraph
	ListTasks() []*Task
}

// activity graph interface
type ActivityGraphInterface interface {
	AddNode(taskId string) error
	AddEdge(fromTaskId string, toTaskId string) error
	RemoveNode(taskId string) error
	RemoveEdge(fromTaskId string, toTaskId string) error
	HasCycle() bool
	TopologicalSort() ([]string, error)
}

// activity error
type ActivityError struct {
	Op  string // the operation that caused the error
	Err error  // the original error
	Key string // related error key
}

// error interface
func (e *ActivityError) Error() string {
	return fmt.Sprintf("%s: %s", e.Op, e.Err.Error())
}
