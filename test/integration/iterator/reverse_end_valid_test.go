package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
	"github.com/sourcenetwork/corekv/test/state"
)

func TestIteratorReverseEndValid(t *testing.T) {
	test := &integration.Test{
		SupportedStoreTypes: []state.StoreType{
			state.MemoryStoreType,
		},
		Namespacing: integration.ManualOnly,
		Actions: []action.Action{
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					End:     []byte("k4"),
				},
				ChildActions: []action.IteratorAction{
					action.IsInvalid(),
				},
			},
		},
	}

	test.Execute(t)
}
