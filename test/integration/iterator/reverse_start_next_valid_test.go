package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorReverseStartNextValid_Badger(t *testing.T) {
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
					Start:   []byte("k2"),
				},
				ChildActions: []action.IteratorAction{
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

func TestIteratorReverseStartNextValid_MemoryUnnamespaced(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Namespacing: integration.ManualOnly,
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
				},
				ChildActions: []action.IteratorAction{
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

// This test documents undesirable behaviour, issue:
// https://github.com/sourcenetwork/corekv/issues/11
func TestIteratorReverseStartNextValid_MemoryNamespaced(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Namespacing: integration.AutomaticForced,
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
				},
				ChildActions: []action.IteratorAction{
					action.Next(),
					action.Next(),
					action.IsValid(),
					action.Next(),
					action.IsInvalid(),
					action.Next(),
					// Calling `Next` on an iterator that has already reached the end
					// should not result in a valid item being yielded!
					action.IsValid(),
				},
			},
		},
	}

	test.Execute(t)
}
