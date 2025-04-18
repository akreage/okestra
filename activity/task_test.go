package activity

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func createSampleTaskDefinition() *TaskDefinition {
	return &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: func() error { return nil },
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:     "param1",
				Type:     "string",
				Required: true,
			},
			"param2": {
				Name:         "param2",
				Type:         "int",
				Required:     false,
				DefaultValue: 0,
			},
		},
	}
}

func TestNewTaskManager(t *testing.T) {
	manager := NewTaskManager()

	assert.NotNil(t, manager)
	assert.Empty(t, manager.tasks)
	assert.Equal(t, 0, manager.Count())
}

func TestAddTask(t *testing.T) {
	manager := NewTaskManager()
	definition := createSampleTaskDefinition()

	// Create a simple task
	task := &Task{
		Id:          uuid.New(),
		Name:        "Test Task",
		Description: "A test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value1",
			"param2": 42,
		},
	}

	// Test adding a task
	err := manager.Add(task)
	assert.NoError(t, err)
	assert.Equal(t, 1, manager.Count())

	// Test adding a duplicate task
	err = manager.Add(task)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Add", actErr.Op)
	assert.Equal(t, "task with this ID already exists", actErr.Err.Error())
	assert.Equal(t, task.Id.String(), actErr.Key)

	// Test adding nil task
	err = manager.Add(nil)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Add", actErr.Op)
	assert.Equal(t, "cannot add nil task", actErr.Err.Error())

	// Test adding task with auto-generated ID
	task2 := &Task{
		Name:        "Test Task 2",
		Description: "Another test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value2",
		},
	}

	err = manager.Add(task2)
	assert.NoError(t, err)
	assert.Equal(t, 2, manager.Count())
	assert.NotEqual(t, uuid.Nil, task2.Id)

	// Test adding task without definition
	task3 := &Task{
		Name:        "Test Task 3",
		Description: "A task without definition",
	}

	err = manager.Add(task3)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Add", actErr.Op)
	assert.Equal(t, "task must have a definition", actErr.Err.Error())
}

func TestDeleteTask(t *testing.T) {
	manager := NewTaskManager()
	definition := createSampleTaskDefinition()

	// Create and add a task
	task := &Task{
		Id:          uuid.New(),
		Name:        "Test Task",
		Description: "A test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value1",
		},
	}

	_ = manager.Add(task)

	// Test deleting the task
	err := manager.Delete(task)
	assert.NoError(t, err)
	assert.Equal(t, 0, manager.Count())

	// Test deleting a non-existent task
	err = manager.Delete(task)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Delete", actErr.Op)
	assert.Equal(t, "task not found", actErr.Err.Error())
	assert.Equal(t, task.Id.String(), actErr.Key)

	// Test deleting nil task
	err = manager.Delete(nil)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Delete", actErr.Op)
	assert.Equal(t, "cannot delete nil task", actErr.Err.Error())
}

func TestListTasks(t *testing.T) {
	manager := NewTaskManager()
	definition := createSampleTaskDefinition()

	// Create and add multiple tasks
	task1 := &Task{
		Id:          uuid.New(),
		Name:        "Test Task 1",
		Description: "First test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value1",
		},
	}

	task2 := &Task{
		Id:          uuid.New(),
		Name:        "Test Task 2",
		Description: "Second test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value2",
		},
	}

	_ = manager.Add(task1)
	_ = manager.Add(task2)

	// Test listing tasks
	tasks := manager.List()
	assert.Equal(t, 2, len(tasks))
	assert.Contains(t, tasks, task1)
	assert.Contains(t, tasks, task2)
}

func TestGetTask(t *testing.T) {
	manager := NewTaskManager()
	definition := createSampleTaskDefinition()

	// Create and add a task
	task := &Task{
		Id:          uuid.New(),
		Name:        "Test Task",
		Description: "A test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value1",
		},
	}

	_ = manager.Add(task)

	// Test getting the task
	retrievedTask, err := manager.Get(task.Id)
	assert.NoError(t, err)
	assert.Equal(t, task, retrievedTask)

	// Test getting a non-existent task
	nonExistentId := uuid.New()
	_, err = manager.Get(nonExistentId)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Get", actErr.Op)
	assert.Equal(t, "task not found", actErr.Err.Error())
	assert.Equal(t, nonExistentId.String(), actErr.Key)
}

func TestGetTaskByName(t *testing.T) {
	manager := NewTaskManager()
	definition := createSampleTaskDefinition()

	// Create and add a task
	task := &Task{
		Id:          uuid.New(),
		Name:        "Test Task",
		Description: "A test task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "value1",
		},
	}

	_ = manager.Add(task)

	// Test getting the task by name
	retrievedTask, err := manager.GetByName("Test Task")
	assert.NoError(t, err)
	assert.Equal(t, task, retrievedTask)

	// Test getting a non-existent task
	_, err = manager.GetByName("Non-existent Task")
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "GetByName", actErr.Op)
	assert.Equal(t, "task not found", actErr.Err.Error())
	assert.Equal(t, "Non-existent Task", actErr.Key)
}

func TestValidateTaskArgs(t *testing.T) {
	manager := NewTaskManager()

	// Create a task definition with validation
	validationFunc := func(value interface{}) error {
		strValue, ok := value.(string)
		if !ok {
			return errors.New("value must be a string")
		}

		if len(strValue) < 3 {
			return errors.New("string must be at least 3 characters")
		}

		return nil
	}

	definition := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: func() error { return nil },
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:       "param1",
				Type:       "string",
				Required:   true,
				Validation: validationFunc,
			},
			"param2": {
				Name:     "param2",
				Type:     "int",
				Required: false,
			},
		},
	}

	// Test valid task
	task := &Task{
		Id:          uuid.New(),
		Name:        "Valid Task",
		Description: "A valid task",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "valid value",
			"param2": 42,
		},
	}

	err := manager.ValidateTaskArgs(task)
	assert.NoError(t, err)

	// Test missing required parameter
	invalidTask1 := &Task{
		Id:          uuid.New(),
		Name:        "Invalid Task 1",
		Description: "Missing required parameter",
		Definition:  definition,
		Args: map[string]interface{}{
			"param2": 42,
		},
	}

	err = manager.ValidateTaskArgs(invalidTask1)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "ValidateTaskArgs", actErr.Op)
	assert.Equal(t, "missing required parameter: param1", actErr.Err.Error())
	assert.Equal(t, invalidTask1.Id.String(), actErr.Key)

	// Test validation failure
	invalidTask2 := &Task{
		Id:          uuid.New(),
		Name:        "Invalid Task 2",
		Description: "Validation failure",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "ab", // Too short
			"param2": 42,
		},
	}

	err = manager.ValidateTaskArgs(invalidTask2)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "ValidateTaskArgs", actErr.Op)
	assert.Contains(t, actErr.Err.Error(), "validation failed for parameter param1")
	assert.Equal(t, invalidTask2.Id.String(), actErr.Key)

	// Test unexpected parameter
	invalidTask3 := &Task{
		Id:          uuid.New(),
		Name:        "Invalid Task 3",
		Description: "Unexpected parameter",
		Definition:  definition,
		Args: map[string]interface{}{
			"param1": "valid value",
			"param2": 42,
			"param3": "unexpected",
		},
	}

	err = manager.ValidateTaskArgs(invalidTask3)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "ValidateTaskArgs", actErr.Op)
	assert.Equal(t, "unexpected parameter: param3", actErr.Err.Error())
	assert.Equal(t, invalidTask3.Id.String(), actErr.Key)

	// Test nil task
	err = manager.ValidateTaskArgs(nil)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "ValidateTaskArgs", actErr.Op)
	assert.Equal(t, "cannot validate nil task", actErr.Err.Error())

	// Test task without definition
	invalidTask4 := &Task{
		Id:          uuid.New(),
		Name:        "Invalid Task 4",
		Description: "No definition",
		Args: map[string]interface{}{
			"param1": "value",
		},
	}

	err = manager.ValidateTaskArgs(invalidTask4)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "ValidateTaskArgs", actErr.Op)
	assert.Equal(t, "task has no definition", actErr.Err.Error())
	assert.Equal(t, invalidTask4.Id.String(), actErr.Key)
}

func TestConcurrentTaskAccess(t *testing.T) {
	manager := NewTaskManager()
	definition := createSampleTaskDefinition()

	// Test concurrent adding and retrieving
	done := make(chan bool)

	// Add 100 tasks concurrently
	for i := 0; i < 100; i++ {
		go func(index int) {
			task := &Task{
				Name:        "Concurrent Task",
				Description: "A concurrent test task",
				Definition:  definition,
				Args: map[string]interface{}{
					"param1": "value",
				},
			}

			_ = manager.Add(task)
			done <- true
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify count
	assert.Equal(t, 100, manager.Count())

	// Test concurrent listing and getting
	for i := 0; i < 100; i++ {
		go func() {
			_ = manager.List()
			done <- true
		}()
	}

	// Wait for all goroutines to finish
	for i := 0; i < 100; i++ {
		<-done
	}
}
