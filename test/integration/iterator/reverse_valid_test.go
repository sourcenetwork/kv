package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorReverseValid_Badger(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.BadgerStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
				},
				ChildActions: []action.IteratorAction{
					action.IsValid(),
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratorReverseValid_MemoryUnnamespaced(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Namespacing: integration.ManualOnly,
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
				},
				ChildActions: []action.IteratorAction{
					action.IsValid(),
				},
			},
		},
	}

	test.Execute(t)
}

// This test documents undesirable behaviour, issue:
// https://github.com/sourcenetwork/corekv/issues/11
func TestIteratorReverseValid_MemoryNamespaced(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Namespacing: integration.AutomaticForced,
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
				},
				ChildActions: []action.IteratorAction{
					action.IsInvalid(),
				},
			},
		},
	}

	test.Execute(t)
}
