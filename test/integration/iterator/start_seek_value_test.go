package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

// This test documents unwanted behaviour tracked by issue:
// https://github.com/sourcenetwork/corekv/issues/38
func TestIteratorStartSeekValue_Badger(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.BadgerStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Start: []byte("k3"),
				},
				ChildActions: []action.IteratorAction{
					action.Seek([]byte("k1"), true),
					// `k1` is outside of the upper bound of the iterator and
					// should not be returned, but it is.
					action.Value([]byte("v1")),
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratortartSeekValue_Memory(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Start: []byte("k3"),
				},
				ChildActions: []action.IteratorAction{
					action.Seek([]byte("k1"), true),
					action.Value(nil),
				},
			},
		},
	}

	test.Execute(t)
}
