package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorReverseStart_Badger(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.BadgerStoreType,
		},
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
				},
				Expected: []action.KeyValue{
					{Key: []byte("k4"), Value: []byte("v4")},
					{Key: []byte("k3"), Value: nil},
					{Key: []byte("k2"), Value: []byte("v2")},
					// `k1` should not be yielded
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratorReverseStart_MemoryUnnamespaced(t *testing.T) {
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
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
				},
				Expected: []action.KeyValue{
					{Key: []byte("k4"), Value: []byte("v4")},
					{Key: []byte("k3"), Value: nil},
					{Key: []byte("k2"), Value: []byte("v2")},
					// `k1` should not be yielded
				},
			},
		},
	}

	test.Execute(t)
}

// This test documents undesirable behaviour, issue:
// https://github.com/sourcenetwork/defradb/issues/3383
func TestIteratorReverseStart_MemoryNamespaced(t *testing.T) {
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
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
				},
				Expected: []action.KeyValue{},
			},
		},
	}

	test.Execute(t)
}
