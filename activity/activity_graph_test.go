package activity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewActivityGraph(t *testing.T) {
	graph := NewActivityGraph()

	assert.NotNil(t, graph)
	assert.Empty(t, graph.Nodes)
	assert.Empty(t, graph.Edges)
}

func TestAddNode(t *testing.T) {
	graph := NewActivityGraph()

	// Test adding a node
	err := graph.AddNode("task1")
	assert.NoError(t, err)
	assert.Contains(t, graph.Nodes, "task1")
	assert.Empty(t, graph.Edges["task1"])

	// Test adding a duplicate node
	err = graph.AddNode("task1")
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "AddNode", actErr.Op)
	assert.Equal(t, "node already exists", actErr.Err.Error())
	assert.Equal(t, "task1", actErr.Key)
}

func TestAddEdge(t *testing.T) {
	graph := NewActivityGraph()

	// Add nodes
	_ = graph.AddNode("task1")
	_ = graph.AddNode("task2")

	// Test adding an edge
	err := graph.AddEdge("task1", "task2")
	assert.NoError(t, err)
	assert.Contains(t, graph.Edges["task1"], "task2")

	// Test adding an edge with non-existent source
	err = graph.AddEdge("nonexistent", "task2")
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "AddEdge", actErr.Op)
	assert.Contains(t, actErr.Err.Error(), "source node nonexistent does not exist")
	assert.Equal(t, "nonexistent", actErr.Key)

	// Test adding an edge with non-existent destination
	err = graph.AddEdge("task1", "nonexistent")
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "AddEdge", actErr.Op)
	assert.Contains(t, actErr.Err.Error(), "destination node nonexistent does not exist")
	assert.Equal(t, "nonexistent", actErr.Key)
}

func TestHasCycle(t *testing.T) {
	t.Run("acyclic graph", func(t *testing.T) {
		graph := NewActivityGraph()

		// Create a simple acyclic graph
		_ = graph.AddNode("task1")
		_ = graph.AddNode("task2")
		_ = graph.AddNode("task3")

		_ = graph.AddEdge("task1", "task2")
		_ = graph.AddEdge("task2", "task3")

		hasCycle := graph.HasCycle()
		assert.False(t, hasCycle)
	})

	t.Run("cyclic graph", func(t *testing.T) {
		graph := NewActivityGraph()

		// Create a graph with a cycle
		_ = graph.AddNode("task1")
		_ = graph.AddNode("task2")
		_ = graph.AddNode("task3")

		_ = graph.AddEdge("task1", "task2")
		_ = graph.AddEdge("task2", "task3")

		// This edge creates a cycle: task1 -> task2 -> task3 -> task1
		graph.Edges["task3"] = append(graph.Edges["task3"], "task1")

		hasCycle := graph.HasCycle()
		assert.True(t, hasCycle)
	})
}

func TestWouldCreateCycle(t *testing.T) {
	graph := NewActivityGraph()

	// Create a simple graph
	_ = graph.AddNode("task1")
	_ = graph.AddNode("task2")
	_ = graph.AddNode("task3")

	_ = graph.AddEdge("task1", "task2")
	_ = graph.AddEdge("task2", "task3")

	// Test edge that would create a cycle
	wouldCreateCycle := graph.wouldCreateCycle("task3", "task1")
	assert.True(t, wouldCreateCycle)

	// Test edge that wouldn't create a cycle
	wouldCreateCycle = graph.wouldCreateCycle("task1", "task3")
	assert.False(t, wouldCreateCycle)
}

func TestAddEdgeCycleDetection(t *testing.T) {
	graph := NewActivityGraph()

	// Create a simple graph
	_ = graph.AddNode("task1")
	_ = graph.AddNode("task2")
	_ = graph.AddNode("task3")

	_ = graph.AddEdge("task1", "task2")
	_ = graph.AddEdge("task2", "task3")

	// Test adding an edge that would create a cycle
	err := graph.AddEdge("task3", "task1")
	assert.Error(t, err) // This should be detected as a cycle

	// Verify it's an ActivityError
	actErr, ok := err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "AddEdge", actErr.Op)
	assert.Equal(t, "adding this edge would create a cycle", actErr.Err.Error())
	assert.Equal(t, "task3->task1", actErr.Key)

	// Try to add another edge that would create a cycle
	err = graph.AddEdge("task3", "task2")
	assert.Error(t, err)

	// Verify it's an ActivityError
	actErr, ok = err.(*ActivityError)
	assert.True(t, ok, "Expected ActivityError")
	assert.Equal(t, "AddEdge", actErr.Op)
	assert.Equal(t, "adding this edge would create a cycle", actErr.Err.Error())
	assert.Equal(t, "task3->task2", actErr.Key)
}

func TestTopologicalSort(t *testing.T) {
	t.Run("simple graph", func(t *testing.T) {
		graph := NewActivityGraph()

		// Create a simple graph
		_ = graph.AddNode("task1")
		_ = graph.AddNode("task2")
		_ = graph.AddNode("task3")

		_ = graph.AddEdge("task1", "task2")
		_ = graph.AddEdge("task2", "task3")

		order, err := graph.TopologicalSort()
		assert.NoError(t, err)
		assert.Equal(t, []string{"task1", "task2", "task3"}, order)
	})

	t.Run("complex graph", func(t *testing.T) {
		graph := NewActivityGraph()

		// Create a more complex graph
		_ = graph.AddNode("task1")
		_ = graph.AddNode("task2")
		_ = graph.AddNode("task3")
		_ = graph.AddNode("task4")
		_ = graph.AddNode("task5")

		_ = graph.AddEdge("task1", "task2")
		_ = graph.AddEdge("task1", "task3")
		_ = graph.AddEdge("task2", "task4")
		_ = graph.AddEdge("task3", "task4")
		_ = graph.AddEdge("task4", "task5")

		order, err := graph.TopologicalSort()
		assert.NoError(t, err)

		// Check that the order respects dependencies
		// task1 should come before task2 and task3
		// task2 and task3 should come before task4
		// task4 should come before task5

		task1Pos := indexOf("task1", order)
		task2Pos := indexOf("task2", order)
		task3Pos := indexOf("task3", order)
		task4Pos := indexOf("task4", order)
		task5Pos := indexOf("task5", order)

		assert.True(t, task1Pos < task2Pos)
		assert.True(t, task1Pos < task3Pos)
		assert.True(t, task2Pos < task4Pos)
		assert.True(t, task3Pos < task4Pos)
		assert.True(t, task4Pos < task5Pos)
	})

	t.Run("cyclic graph", func(t *testing.T) {
		graph := NewActivityGraph()

		// Create a graph with a cycle
		_ = graph.AddNode("task1")
		_ = graph.AddNode("task2")
		_ = graph.AddNode("task3")

		_ = graph.AddEdge("task1", "task2")
		_ = graph.AddEdge("task2", "task3")

		// This edge creates a cycle
		graph.Edges["task3"] = append(graph.Edges["task3"], "task1")

		_, err := graph.TopologicalSort()
		assert.Error(t, err)

		// Verify it's an ActivityError
		actErr, ok := err.(*ActivityError)
		assert.True(t, ok, "Expected ActivityError")
		assert.Equal(t, "TopologicalSort", actErr.Op)
		assert.Equal(t, "cannot perform topological sort on a cyclic graph", actErr.Err.Error())
	})
}

// Helper function to find the index of a string in a slice
func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 // not found
}

func TestIsReachable(t *testing.T) {
	graph := NewActivityGraph()

	// Create a simple graph
	_ = graph.AddNode("task1")
	_ = graph.AddNode("task2")
	_ = graph.AddNode("task3")
	_ = graph.AddNode("task4")

	_ = graph.AddEdge("task1", "task2")
	_ = graph.AddEdge("task2", "task3")

	// Test reachability
	assert.True(t, graph.isReachable("task1", "task3"))
	assert.False(t, graph.isReachable("task3", "task1"))
	assert.False(t, graph.isReachable("task1", "task4"))
}

func TestComplexGraph(t *testing.T) {
	graph := NewActivityGraph()

	// Create a complex graph representing a build pipeline
	_ = graph.AddNode("checkout")
	_ = graph.AddNode("compile")
	_ = graph.AddNode("test")
	_ = graph.AddNode("package")
	_ = graph.AddNode("deploy")
	_ = graph.AddNode("notify")

	_ = graph.AddEdge("checkout", "compile")
	_ = graph.AddEdge("compile", "test")
	_ = graph.AddEdge("compile", "package")
	_ = graph.AddEdge("test", "deploy")
	_ = graph.AddEdge("package", "deploy")
	_ = graph.AddEdge("deploy", "notify")

	// Verify no cycles
	assert.False(t, graph.HasCycle())

	// Get topological sort
	order, err := graph.TopologicalSort()
	assert.NoError(t, err)

	// Verify order respects dependencies
	checkoutPos := indexOf("checkout", order)
	compilePos := indexOf("compile", order)
	testPos := indexOf("test", order)
	packagePos := indexOf("package", order)
	deployPos := indexOf("deploy", order)
	notifyPos := indexOf("notify", order)

	assert.True(t, checkoutPos < compilePos)
	assert.True(t, compilePos < testPos)
	assert.True(t, compilePos < packagePos)
	assert.True(t, testPos < deployPos)
	assert.True(t, packagePos < deployPos)
	assert.True(t, deployPos < notifyPos)
}
