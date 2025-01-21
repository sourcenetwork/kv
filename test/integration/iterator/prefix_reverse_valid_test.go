package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorPrefixReverseValid_Badger(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.BadgerStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Prefix:  []byte("k"),
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
func TestIteratorPrefixReverseValid_Memory(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Prefix:  []byte("k"),
				},
				ChildActions: []action.IteratorAction{
					action.IsInvalid(),
				},
			},
		},
	}

	test.Execute(t)
}
