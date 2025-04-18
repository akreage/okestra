package activity

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

// TaskDefinitionManager implements the TaskDefinitionInterface
type TaskDefinitionManager struct {
	definitions map[uuid.UUID]*TaskDefinition
	mu          sync.RWMutex
}

// NewTaskDefinitionManager creates a new TaskDefinitionManager
func NewTaskDefinitionManager() *TaskDefinitionManager {
	return &TaskDefinitionManager{
		definitions: make(map[uuid.UUID]*TaskDefinition),
	}
}

// Add adds a task definition to the manager
func (m *TaskDefinitionManager) Add(definition *TaskDefinition) error {
	if definition == nil {
		return &ActivityError{
			Op:  "Add",
			Err: errors.New("cannot add nil task definition"),
		}
	}

	// Generate a new UUID if not provided
	if definition.FuncId == uuid.Nil {
		definition.FuncId = uuid.New()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if definition with this ID already exists
	if _, exists := m.definitions[definition.FuncId]; exists {
		return &ActivityError{
			Op:  "Add",
			Err: errors.New("task definition with this ID already exists"),
			Key: definition.FuncId.String(),
		}
	}

	m.definitions[definition.FuncId] = definition
	return nil
}

// Delete removes a task definition from the manager
func (m *TaskDefinitionManager) Delete(definition *TaskDefinition) error {
	if definition == nil {
		return &ActivityError{
			Op:  "Delete",
			Err: errors.New("cannot delete nil task definition"),
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.definitions[definition.FuncId]; !exists {
		return &ActivityError{
			Op:  "Delete",
			Err: errors.New("task definition not found"),
			Key: definition.FuncId.String(),
		}
	}

	delete(m.definitions, definition.FuncId)
	return nil
}

// List returns all task definitions
func (m *TaskDefinitionManager) List() []*TaskDefinition {
	m.mu.RLock()
	defer m.mu.RUnlock()

	definitions := make([]*TaskDefinition, 0, len(m.definitions))
	for _, def := range m.definitions {
		definitions = append(definitions, def)
	}

	return definitions
}

// Get retrieves a task definition by ID
func (m *TaskDefinitionManager) Get(funcId uuid.UUID) (*TaskDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	definition, exists := m.definitions[funcId]
	if !exists {
		return nil, &ActivityError{
			Op:  "Get",
			Err: errors.New("task definition not found"),
			Key: funcId.String(),
		}
	}

	return definition, nil
}

// GetByName retrieves a task definition by name
func (m *TaskDefinitionManager) GetByName(name string) (*TaskDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, def := range m.definitions {
		for _, param := range def.Parameters {
			if param.Name == name {
				return def, nil
			}
		}
	}

	return nil, &ActivityError{
		Op:  "GetByName",
		Err: errors.New("task definition not found"),
		Key: name,
	}
}

// Count returns the number of task definitions
func (m *TaskDefinitionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.definitions)
}
