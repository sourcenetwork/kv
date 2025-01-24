package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorPrefix(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Prefix: []byte("k"),
				},
				Expected: []action.KeyValue{
					// `1` must not be yielded
					{Key: []byte("k2"), Value: []byte("v2")},
					{Key: []byte("k3"), Value: nil},
					// `4` must not be yielded
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratorPrefix_DoesNotReturnSelf_Memory(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k"), []byte("v")),
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Prefix: []byte("k"),
				},
				Expected: []action.KeyValue{
					// `k` must not be yielded, as prefxes do not contain themselves
					{Key: []byte("k1"), Value: []byte("v1")},
				},
			},
		},
	}

	test.Execute(t)
}

// This test documents unwanted behaviour, it is tracked by:
// https://github.com/sourcenetwork/corekv/issues/27
func TestIteratorPrefix_DoesNotReturnSelf_Badger(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.BadgerStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k"), []byte("v")),
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Prefix: []byte("k"),
				},
				Expected: []action.KeyValue{
					// `k` should not be yielded, but it is.
					{Key: []byte("k"), Value: []byte("v")},
					{Key: []byte("k1"), Value: []byte("v1")},
				},
			},
		},
	}

	test.Execute(t)
}
