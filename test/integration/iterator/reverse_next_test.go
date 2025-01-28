package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

func TestIteratorReverseNext(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
				},
				ChildActions: []action.IteratorAction{
					action.Next(true),
				},
			},
		},
	}

	test.Execute(t)
}

// This test documents undesirable behaviour, issue:
// https://github.com/sourcenetwork/corekv/issues/19
func TestIteratorReverseNext_BeyondEnd_Badger(t *testing.T) {
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
					Reverse: true,
				},
				ChildActions: []action.IteratorAction{
					action.Next(true),
					action.Next(true),
					action.Next(true),
					action.Next(true),
					action.Next(false),
					action.Next(false),
				},
			},
		},
	}

	require.PanicsWithError(
		t,
		"runtime error: invalid memory address or nil pointer dereference",
		func() {
			test.Execute(t)
		},
	)
}

func TestIteratorReverseNext_BeyondEnd_Memory(t *testing.T) {
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
					Reverse: true,
				},
				ChildActions: []action.IteratorAction{
					action.Next(true),
					action.Next(true),
					action.Next(true),
					action.Next(true),
					action.Next(false),
					action.Next(false),
				},
			},
		},
	}

	test.Execute(t)
}
