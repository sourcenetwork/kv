package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorReverseEndNext(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					End:     []byte("k4"),
				},
				ChildActions: []action.IteratorAction{
					action.Next(false),
				},
			},
		},
	}

	test.Execute(t)
}
