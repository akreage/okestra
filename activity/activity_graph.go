package activity

import (
	"errors"
	"fmt"
)

// activity graph struct
type ActivityGraph struct {
	Nodes []string
	Edges map[string][]string
}

// NewActivityGraph creates a new activity graph
func NewActivityGraph() *ActivityGraph {
	return &ActivityGraph{
		Nodes: []string{},
		Edges: make(map[string][]string),
	}
}

// AddNode adds a node to the graph
func (g *ActivityGraph) AddNode(taskId string) error {
	// Check if node already exists
	for _, node := range g.Nodes {
		if node == taskId {
			return &ActivityError{
				Op:  "AddNode",
				Err: errors.New("node already exists"),
				Key: taskId,
			}
		}
	}

	g.Nodes = append(g.Nodes, taskId)
	g.Edges[taskId] = []string{}
	return nil
}

// RemoveNode removes a node from the graph
func (g *ActivityGraph) RemoveNode(taskId string) error {
	// Check if node exists
	for index, node := range g.Nodes {
		if node == taskId {
			// remove the node from the graph given the index
			g.Nodes = append(g.Nodes[:index], g.Nodes[index+1:]...)
			// remove the edges
			delete(g.Edges, taskId)
			// remove the edges that point to the node
			for _, edges := range g.Edges {
				for i, edge := range edges {
					if edge == taskId {
						edges = append(edges[:i], edges[i+1:]...)
					}
				}
			}
			return nil
		}
	}

	return &ActivityError{
		Op:  "RemoveNode",
		Err: errors.New("node not found"),
		Key: taskId,
	}
}

func (g *ActivityGraph) RemoveEdge(fromTaskId string, toTaskId string) error {

	// check if the from and to exists
	_, fromExists := g.Edges[fromTaskId]
	_, toExists := g.Edges[toTaskId]
	if !fromExists || !toExists {
		return &ActivityError{
			Op:  "RemoveEdge",
			Err: fmt.Errorf("node does not exist"),
			Key: fromTaskId + "->" + toTaskId,
		}
	}

	// remove the edge from the graph
	for i, edge := range g.Edges[fromTaskId] {
		if edge == toTaskId {
			g.Edges[fromTaskId] = append(g.Edges[fromTaskId][:i], g.Edges[fromTaskId][i+1:]...)
			return nil
		}
	}

	return &ActivityError{
		Op:  "RemoveEdge",
		Err: fmt.Errorf("edge does not exist"),
		Key: fromTaskId + "->" + toTaskId,
	}
}

// AddEdge adds an edge between two nodes
func (g *ActivityGraph) AddEdge(fromTaskId string, toTaskId string) error {
	// Check if both nodes exist
	fromExists := false
	toExists := false

	for _, node := range g.Nodes {
		if node == fromTaskId {
			fromExists = true
		}
		if node == toTaskId {
			toExists = true
		}
		if fromExists && toExists {
			break
		}
	}

	if !fromExists {
		return &ActivityError{
			Op:  "AddEdge",
			Err: fmt.Errorf("source node %s does not exist", fromTaskId),
			Key: fromTaskId,
		}
	}

	if !toExists {
		return &ActivityError{
			Op:  "AddEdge",
			Err: fmt.Errorf("destination node %s does not exist", toTaskId),
			Key: toTaskId,
		}
	}

	// Check if adding this edge would create a cycle
	if g.wouldCreateCycle(fromTaskId, toTaskId) {
		return &ActivityError{
			Op:  "AddEdge",
			Err: errors.New("adding this edge would create a cycle"),
			Key: fromTaskId + "->" + toTaskId,
		}
	}

	// Add the edge
	g.Edges[fromTaskId] = append(g.Edges[fromTaskId], toTaskId)
	return nil
}

// HasCycle checks if the graph has a cycle
func (g *ActivityGraph) HasCycle() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for _, nodeID := range g.Nodes {
		if !visited[nodeID] {
			if g.hasCycleUtil(nodeID, visited, recStack) {
				return true
			}
		}
	}

	return false
}

// hasCycleUtil is a helper function for cycle detection
func (g *ActivityGraph) hasCycleUtil(nodeID string, visited map[string]bool, recStack map[string]bool) bool {
	// Mark the current node as visited and add to recursion stack
	visited[nodeID] = true
	recStack[nodeID] = true

	// Visit all adjacent vertices
	for _, adjacentID := range g.Edges[nodeID] {
		// If not visited, recursively check for cycles
		if !visited[adjacentID] {
			if g.hasCycleUtil(adjacentID, visited, recStack) {
				return true
			}
		} else if recStack[adjacentID] {
			// If the adjacent node is in the recursion stack, there's a cycle
			return true
		}
	}

	// Remove the node from recursion stack
	recStack[nodeID] = false
	return false
}

// wouldCreateCycle checks if adding an edge would create a cycle
func (g *ActivityGraph) wouldCreateCycle(fromTaskId string, toTaskId string) bool {
	// Check if there's a path from toTaskId to fromTaskId
	// If there is, adding an edge from fromTaskId to toTaskId would create a cycle
	return g.isReachable(toTaskId, fromTaskId)
}

// isReachable checks if there's a path from source to destination
func (g *ActivityGraph) isReachable(source string, destination string) bool {
	// Use BFS to check if destination is reachable from source
	visited := make(map[string]bool)
	queue := []string{source}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == destination {
			return true
		}

		visited[current] = true

		for _, adjacent := range g.Edges[current] {
			if !visited[adjacent] {
				queue = append(queue, adjacent)
			}
		}
	}

	return false
}

// TopologicalSort returns a topological ordering of the nodes
func (g *ActivityGraph) TopologicalSort() ([]string, error) {
	// Check for cycles first
	if g.HasCycle() {
		return nil, &ActivityError{
			Op:  "TopologicalSort",
			Err: errors.New("cannot perform topological sort on a cyclic graph"),
		}
	}

	// Perform topological sort
	visited := make(map[string]bool)
	var stack []string

	for _, nodeID := range g.Nodes {
		if !visited[nodeID] {
			g.topologicalSortUtil(nodeID, visited, &stack)
		}
	}

	// Reverse the stack to get the topological sort
	result := make([]string, len(stack))
	for i, nodeID := range stack {
		result[len(stack)-1-i] = nodeID
	}

	return result, nil
}

// topologicalSortUtil is a helper function for topological sort
func (g *ActivityGraph) topologicalSortUtil(nodeID string, visited map[string]bool, stack *[]string) {
	// Mark the current node as visited
	visited[nodeID] = true

	// Visit all adjacent vertices
	for _, adjacentID := range g.Edges[nodeID] {
		if !visited[adjacentID] {
			g.topologicalSortUtil(adjacentID, visited, stack)
		}
	}

	// Push current vertex to stack
	*stack = append(*stack, nodeID)
}
