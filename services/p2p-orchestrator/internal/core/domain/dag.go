package domain

import (
	"errors"
	"sort"
)

type Node struct {
	ID           string
	Dependencies []string // Nodes that must complete before this Node
}

type DAG struct {
	Nodes map[string]*Node
}

// TopoSort runs Kahn's algorithm in ~O(V log V + E) and returns a DETERMINISTIC order.
// Determinism (charter requirement): the initial ready-set and every batch of newly-ready
// nodes are lexicographically sorted, so the same DAG always yields the same order across
// runs, pods, and replays — which the persisted sorted_nodes and the WebGL timeline rely on.
//
// On a cyclic graph it returns errors.Join(ErrTerminal, ErrCycleDetected) so the inbound
// adapter's errors.Is(err, ErrTerminal) branch routes it straight to the DLQ (amendment #3).
func (d *DAG) TopoSort() ([]string, error) {
	inDegree := make(map[string]int, len(d.Nodes))
	adj := make(map[string][]string)

	for id := range d.Nodes {
		inDegree[id] = 0
	}
	for id, node := range d.Nodes {
		for _, dep := range node.Dependencies {
			adj[dep] = append(adj[dep], id)
			inDegree[id]++
		}
	}
	// Deterministic adjacency ordering.
	for k := range adj {
		sort.Strings(adj[k])
	}

	// Deterministic initial ready-set.
	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	sorted := make([]string, 0, len(d.Nodes))
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		sorted = append(sorted, curr)

		var newly []string
		for _, neighbor := range adj[curr] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				newly = append(newly, neighbor)
			}
		}
		sort.Strings(newly) // deterministic tie-break within each ready batch
		queue = append(queue, newly...)
	}

	if len(sorted) != len(d.Nodes) {
		return nil, errors.Join(ErrTerminal, ErrCycleDetected)
	}
	return sorted, nil
}
