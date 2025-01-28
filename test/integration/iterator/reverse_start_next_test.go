package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorReverseStartNext(t *testing.T) {
	test := &integration.Test{
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
