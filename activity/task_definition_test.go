package activity

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewTaskDefinitionManager(t *testing.T) {
	manager := NewTaskDefinitionManager()

	assert.NotNil(t, manager)
	assert.Empty(t, manager.definitions)
	assert.Equal(t, 0, manager.Count())
}

func TestAddTaskDefinition(t *testing.T) {
	manager := NewTaskDefinitionManager()

	// Create a simple task definition
	taskFunc := func() error { return nil }
	definition := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:         "param1",
				Type:         "string",
				Required:     true,
				DefaultValue: "default",
			},
		},
	}

	// Test adding a definition
	err := manager.Add(definition)
	assert.NoError(t, err)
	assert.Equal(t, 1, manager.Count())

	// Test adding a duplicate definition
	err = manager.Add(definition)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Add", actErr.Op)
	assert.Equal(t, "task definition with this ID already exists", actErr.Err.Error())
	assert.Equal(t, definition.FuncId.String(), actErr.Key)

	// Test adding nil definition
	err = manager.Add(nil)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Add", actErr.Op)
	assert.Equal(t, "cannot add nil task definition", actErr.Err.Error())

	// Test adding definition with auto-generated ID
	definition2 := &TaskDefinition{
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param2": {
				Name:     "param2",
				Type:     "int",
				Required: false,
			},
		},
	}

	err = manager.Add(definition2)
	assert.NoError(t, err)
	assert.Equal(t, 2, manager.Count())
	assert.NotEqual(t, uuid.Nil, definition2.FuncId)
}

func TestDeleteTaskDefinition(t *testing.T) {
	manager := NewTaskDefinitionManager()

	// Create and add a task definition
	taskFunc := func() error { return nil }
	definition := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:     "param1",
				Type:     "string",
				Required: true,
			},
		},
	}

	_ = manager.Add(definition)

	// Test deleting the definition
	err := manager.Delete(definition)
	assert.NoError(t, err)
	assert.Equal(t, 0, manager.Count())

	// Test deleting a non-existent definition
	err = manager.Delete(definition)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Delete", actErr.Op)
	assert.Equal(t, "task definition not found", actErr.Err.Error())
	assert.Equal(t, definition.FuncId.String(), actErr.Key)

	// Test deleting nil definition
	err = manager.Delete(nil)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Delete", actErr.Op)
	assert.Equal(t, "cannot delete nil task definition", actErr.Err.Error())
}

func TestListTaskDefinitions(t *testing.T) {
	manager := NewTaskDefinitionManager()

	// Create and add multiple task definitions
	taskFunc := func() error { return nil }
	definition1 := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:     "param1",
				Type:     "string",
				Required: true,
			},
		},
	}

	definition2 := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param2": {
				Name:     "param2",
				Type:     "int",
				Required: false,
			},
		},
	}

	_ = manager.Add(definition1)
	_ = manager.Add(definition2)

	// Test listing definitions
	definitions := manager.List()
	assert.Equal(t, 2, len(definitions))
	assert.Contains(t, definitions, definition1)
	assert.Contains(t, definitions, definition2)
}

func TestGetTaskDefinition(t *testing.T) {
	manager := NewTaskDefinitionManager()

	// Create and add a task definition
	taskFunc := func() error { return nil }
	definition := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:     "param1",
				Type:     "string",
				Required: true,
			},
		},
	}

	_ = manager.Add(definition)

	// Test getting the definition
	retrievedDef, err := manager.Get(definition.FuncId)
	assert.NoError(t, err)
	assert.Equal(t, definition, retrievedDef)

	// Test getting a non-existent definition
	nonExistentId := uuid.New()
	_, err = manager.Get(nonExistentId)
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "Get", actErr.Op)
	assert.Equal(t, "task definition not found", actErr.Err.Error())
	assert.Equal(t, nonExistentId.String(), actErr.Key)
}

func TestGetTaskDefinitionByName(t *testing.T) {
	manager := NewTaskDefinitionManager()

	// Create and add a task definition
	taskFunc := func() error { return nil }
	definition := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:     "param1",
				Type:     "string",
				Required: true,
			},
		},
	}

	_ = manager.Add(definition)

	// Test getting the definition by parameter name
	retrievedDef, err := manager.GetByName("param1")
	assert.NoError(t, err)
	assert.Equal(t, definition, retrievedDef)

	// Test getting a non-existent definition
	_, err = manager.GetByName("nonexistent")
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "GetByName", actErr.Op)
	assert.Equal(t, "task definition not found", actErr.Err.Error())
	assert.Equal(t, "nonexistent", actErr.Key)
}

func TestConcurrentAccess(t *testing.T) {
	manager := NewTaskDefinitionManager()

	// Create a bunch of task definitions
	taskFunc := func() error { return nil }

	// Test concurrent adding and retrieving
	done := make(chan bool)

	// Add 100 definitions concurrently
	for i := 0; i < 100; i++ {
		go func() {
			definition := &TaskDefinition{
				TaskFunc: taskFunc,
				Parameters: map[string]TaskParameterDefinition{
					"param": {
						Name:     "param",
						Type:     "string",
						Required: true,
					},
				},
			}

			_ = manager.Add(definition)
			done <- true
		}()
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

func TestTaskParameterValidation(t *testing.T) {
	// Create a task definition with parameter validation
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

	taskFunc := func() error { return nil }
	definition := &TaskDefinition{
		FuncId:   uuid.New(),
		TaskFunc: taskFunc,
		Parameters: map[string]TaskParameterDefinition{
			"param1": {
				Name:       "param1",
				Type:       "string",
				Required:   true,
				Validation: validationFunc,
			},
		},
	}

	// Test validation
	err := definition.Parameters["param1"].Validation("ab")
	assert.Error(t, err)
	assert.Equal(t, "string must be at least 3 characters", err.Error())

	err = definition.Parameters["param1"].Validation("abc")
	assert.NoError(t, err)

	err = definition.Parameters["param1"].Validation(123)
	assert.Error(t, err)
	assert.Equal(t, "value must be a string", err.Error())
}
