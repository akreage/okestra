package activity

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

// TaskManager implements the TaskInterface
type TaskManager struct {
	tasks map[uuid.UUID]*Task
	mu    sync.RWMutex
}

// NewTaskManager creates a new TaskManager
func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[uuid.UUID]*Task),
	}
}

// Add adds a task to the manager
func (m *TaskManager) Add(task *Task) error {
	id, err := isTaskNilOrIdNil(task)
	if err != nil {
		return err
	}

	task.Id = id

	// Validate task has a definition
	if task.Definition == nil {
		return &ActivityError{
			Op:  "Add",
			Err: errors.New("task must have a definition"),
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if task with this ID already exists
	if _, exists := m.tasks[task.Id]; exists {
		return &ActivityError{
			Op:  "Add",
			Err: errors.New("task with this ID already exists"),
			Key: task.Id.String(),
		}
	}

	m.tasks[task.Id] = task
	return nil
}

// Delete removes a task from the manager
func (m *TaskManager) Delete(task *Task) error {
	if task == nil {
		return &ActivityError{
			Op:  "Delete",
			Err: errors.New("cannot delete nil task"),
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tasks[task.Id]; !exists {
		return &ActivityError{
			Op:  "Delete",
			Err: errors.New("task not found"),
			Key: task.Id.String(),
		}
	}

	delete(m.tasks, task.Id)
	return nil
}

// List returns all tasks
func (m *TaskManager) List() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks := make([]*Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

// Get retrieves a task by ID
func (m *TaskManager) Get(taskId uuid.UUID) (*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.tasks[taskId]
	if !exists {
		return nil, &ActivityError{
			Op:  "Get",
			Err: errors.New("task not found"),
			Key: taskId.String(),
		}
	}

	return task, nil
}

// GetByName retrieves a task by name
func (m *TaskManager) GetByName(name string) (*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, task := range m.tasks {
		if task.Name == name {
			return task, nil
		}
	}

	return nil, &ActivityError{
		Op:  "GetByName",
		Err: errors.New("task not found"),
		Key: name,
	}
}

// Count returns the number of tasks
func (m *TaskManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.tasks)
}

// ValidateTaskArgs validates that the task arguments match the parameter definitions
func (m *TaskManager) ValidateTaskArgs(task *Task) error {
	if task == nil {
		return &ActivityError{
			Op:  "ValidateTaskArgs",
			Err: errors.New("cannot validate nil task"),
		}
	}

	if task.Definition == nil {
		return &ActivityError{
			Op:  "ValidateTaskArgs",
			Err: errors.New("task has no definition"),
			Key: task.Id.String(),
		}
	}

	// Check for required parameters
	for paramName, paramDef := range task.Definition.Parameters {
		if paramDef.Required {
			if _, exists := task.Args[paramName]; !exists {
				return &ActivityError{
					Op:  "ValidateTaskArgs",
					Err: errors.New("missing required parameter: " + paramName),
					Key: task.Id.String(),
				}
			}
		}
	}

	// Validate parameters with validation functions
	for paramName, paramValue := range task.Args {
		paramDef, exists := task.Definition.Parameters[paramName]
		if !exists {
			return &ActivityError{
				Op:  "ValidateTaskArgs",
				Err: errors.New("unexpected parameter: " + paramName),
				Key: task.Id.String(),
			}
		}

		if paramDef.Validation != nil {
			if err := paramDef.Validation(paramValue); err != nil {
				return &ActivityError{
					Op:  "ValidateTaskArgs",
					Err: errors.New("validation failed for parameter " + paramName + ": " + err.Error()),
					Key: task.Id.String(),
				}
			}
		}
	}

	return nil
}

// helpers

// is task nil or task
func isTaskNilOrIdNil(task *Task) (uuid.UUID, error) {
	if task == nil {
		return uuid.Nil, &ActivityError{
			Op:  "isTaskNilOrIdNil",
			Err: errors.New("cannot add nil task"),
		}
	}

	if task.Id == uuid.Nil {
		return uuid.New(), nil
	}

	return task.Id, nil
}
