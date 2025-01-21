package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorReversePrefixSet_Badger(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.BadgerStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k1"), []byte("v1.1")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Prefix:  []byte("k"),
				},
				Expected: []action.KeyValue{
					{Key: []byte("k1"), Value: []byte("v1.1")},
				},
			},
		},
	}

	test.Execute(t)
}

// This test documents unwanted behaviour, it will be removed in a later commit.
/*
It is commented out because it currently enters an infinate loop. Due to the loop
the would-be behviour is a guess.
func TestIteratorReversePrefixSet_Memory(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k1"), []byte("v1.1")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Prefix:  []byte("k"),
				},
				Expected: []action.KeyValue{
					{Key: []byte("k1"), Value: []byte("v1")},
				},
			},
		},
	}

	test.Execute(t)
}
*/
