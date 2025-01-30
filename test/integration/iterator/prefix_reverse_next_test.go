package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorPrefixReverseNext(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Prefix:  []byte("k"),
				},
				ChildActions: []action.IteratorAction{
					action.Next(true),
				},
			},
		},
	}

	test.Execute(t)
}
