package domain

import (
	"errors"
	"reflect"
	"testing"
)

// newDAG builds a DAG from an id -> dependencies map. Go map iteration is randomized, which is
// exactly what makes these determinism assertions meaningful: TopoSort must produce the same order
// despite the map it is handed being walked in a different order on every run.
func newDAG(spec map[string][]string) *DAG {
	nodes := make(map[string]*Node, len(spec))
	for id, deps := range spec {
		nodes[id] = &Node{ID: id, Dependencies: deps}
	}
	return &DAG{Nodes: nodes}
}

func TestTopoSortOrdersDependenciesFirst(t *testing.T) {
	tests := []struct {
		name string
		spec map[string][]string
		want []string
	}{
		{
			name: "empty DAG",
			spec: map[string][]string{},
			want: []string{},
		},
		{
			name: "single node",
			spec: map[string][]string{"a": nil},
			want: []string{"a"},
		},
		{
			name: "linear chain",
			spec: map[string][]string{
				"receive": nil,
				"match":   {"receive"},
				"approve": {"match"},
				"pay":     {"approve"},
			},
			want: []string{"receive", "match", "approve", "pay"},
		},
		{
			// No edges at all: order is decided purely by the lexicographic tie-break.
			name: "disconnected nodes sort lexicographically",
			spec: map[string][]string{"c": nil, "a": nil, "b": nil},
			want: []string{"a", "b", "c"},
		},
		{
			// Both "b" and "c" become ready at the same time; the batch tie-break must order them.
			name: "diamond",
			spec: map[string][]string{
				"a": nil,
				"b": {"a"},
				"c": {"a"},
				"d": {"b", "c"},
			},
			want: []string{"a", "b", "c", "d"},
		},
		{
			name: "two independent chains interleave deterministically",
			spec: map[string][]string{
				"a1": nil,
				"a2": {"a1"},
				"b1": nil,
				"b2": {"b1"},
			},
			want: []string{"a1", "b1", "a2", "b2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newDAG(tc.spec).TopoSort()
			if err != nil {
				t.Fatalf("TopoSort: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("TopoSort() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Determinism is a charter requirement: the persisted sorted_nodes and the WebGL timeline both
// assume replaying the same DAG yields byte-identical order across runs, pods and restarts.
// Repeating the sort over freshly built maps exercises Go's randomized map iteration.
func TestTopoSortIsDeterministicAcrossRuns(t *testing.T) {
	spec := map[string][]string{
		"pay": {"approve"}, "approve": {"match", "budget"}, "match": {"receive"},
		"budget": {"receive"}, "receive": nil, "notify": {"pay"}, "audit": {"pay"},
	}

	first, err := newDAG(spec).TopoSort()
	if err != nil {
		t.Fatalf("TopoSort: %v", err)
	}
	for i := 0; i < 50; i++ {
		got, err := newDAG(spec).TopoSort()
		if err != nil {
			t.Fatalf("TopoSort run %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d = %v, want %v (order is not deterministic)", i, got, first)
		}
	}

	// Sanity-check the order is actually valid, not merely stable.
	assertTopologicallyValid(t, spec, first)
}

func TestTopoSortDetectsCycles(t *testing.T) {
	tests := []struct {
		name string
		spec map[string][]string
	}{
		{"self loop", map[string][]string{"a": {"a"}}},
		{"two node cycle", map[string][]string{"a": {"b"}, "b": {"a"}}},
		{"three node cycle", map[string][]string{"a": {"c"}, "b": {"a"}, "c": {"b"}}},
		{"cycle with an acyclic tail", map[string][]string{
			"start": nil, "a": {"start", "c"}, "b": {"a"}, "c": {"b"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newDAG(tc.spec).TopoSort()
			if err == nil {
				t.Fatalf("TopoSort() = %v, want a cycle error", got)
			}
			if got != nil {
				t.Errorf("TopoSort() returned %v alongside an error, want nil", got)
			}
			if !errors.Is(err, ErrCycleDetected) {
				t.Errorf("error = %v, want ErrCycleDetected", err)
			}
			// The inbound adapter branches on errors.Is(err, ErrTerminal) to route straight to the
			// DLQ. If this join is ever lost, a cyclic workflow would be retried forever instead.
			if !errors.Is(err, ErrTerminal) {
				t.Errorf("error = %v, want it to also satisfy ErrTerminal", err)
			}
		})
	}
}

// assertTopologicallyValid fails if any node appears before one of its dependencies.
func assertTopologicallyValid(t *testing.T, spec map[string][]string, order []string) {
	t.Helper()
	position := make(map[string]int, len(order))
	for i, id := range order {
		position[id] = i
	}
	if len(order) != len(spec) {
		t.Fatalf("order has %d nodes, want %d", len(order), len(spec))
	}
	for id, deps := range spec {
		for _, dep := range deps {
			if position[dep] >= position[id] {
				t.Errorf("%q at %d comes before its dependency %q at %d", id, position[id], dep, position[dep])
			}
		}
	}
}
