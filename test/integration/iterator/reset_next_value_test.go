package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorResetNextValue_PartialIteration(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterator{
				ChildActions: []action.IteratorAction{
					action.Next(true),
					action.Value([]byte("v1")),
					action.Next(true),
					action.Value([]byte("v2")),
					action.Reset(),
					action.Next(true),
					action.Value([]byte("v1")),
					action.Next(true),
					action.Value([]byte("v2")),
					action.Next(true),
					action.Value(nil),
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratorResetNextValue_FullIteration(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterator{
				ChildActions: []action.IteratorAction{
					action.Next(true),
					action.Value([]byte("v1")),
					action.Next(true),
					action.Value([]byte("v2")),
					action.Next(false),
					action.Reset(),
					action.Next(true),
					action.Value([]byte("v1")),
					action.Next(true),
					action.Value([]byte("v2")),
					action.Next(false),
				},
			},
		},
	}

	test.Execute(t)
}
