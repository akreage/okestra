package activity

import (
	"github.com/google/uuid"
)

// activity manager
type ActivityManager struct {
	Name        string
	Id          uuid.UUID
	Description string
	Status      string
	Graph       ActivityGraphInterface
	Tasks       TaskInterface
}

// activity manager
func NewActivityManager(name string, description string) *ActivityManager {
	return &ActivityManager{
		Name:        name,
		Id:          uuid.New(),
		Description: description,
		Status:      ActivityStatusOnHold,
		Tasks:       NewTaskManager(),
		Graph:       NewActivityGraph(),
	}
}

// add task
func (a *ActivityManager) AddTask(task *Task) error {
	err := a.Tasks.Add(task)
	if err != nil {
		return err
	}

	if err := a.Graph.AddNode(task.Id.String()); err != nil {
		// remove task
		a.Tasks.Delete(task)
		return err
	}

	return nil
}

// remove task
func (a *ActivityManager) RemoveTask(task *Task) error {
	// remove task from graph
	err := a.Tasks.Delete(task)
	if err != nil {
		return err
	}

	// remove task from graph
	return a.Graph.RemoveNode(task.Id.String())
}

// add edge
func (a *ActivityManager) AddEdge(fromTask *Task, toTask *Task) error {
	return a.Graph.AddEdge(fromTask.Id.String(), toTask.Id.String())
}

// remove edge
func (a *ActivityManager) RemoveEdge(fromTask *Task, toTask *Task) error {
	return a.Graph.RemoveEdge(fromTask.Id.String(), toTask.Id.String())
}

func (a *ActivityManager) ListTasks() []*Task {
	return a.Tasks.List()
}

func (a *ActivityManager) GetGraph() *ActivityGraph {
	return a.Graph.(*ActivityGraph)
}
