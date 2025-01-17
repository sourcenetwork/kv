package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

// This test documents undesirable behaviour, issue:
// https://github.com/sourcenetwork/corekv/issues/19
func TestIteratorReverseNextValid_Badger(t *testing.T) {
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
					action.Next(),
					action.Next(),
					action.Next(),
					action.IsValid(),
					action.Next(),
					action.IsInvalid(),
					action.Next(),
					action.IsInvalid(),
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

func TestIteratorReverseNextValid_MemoryUnnamespaced(t *testing.T) {
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
					action.Next(),
					action.Next(),
					action.Next(),
					action.IsValid(),
					action.Next(),
					action.IsInvalid(),
					action.Next(),
					action.IsInvalid(),
				},
			},
		},
	}

	test.Execute(t)
}
