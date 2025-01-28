package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorReverseStartEndNext_ValueBeforeStart(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
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

func TestIteratorReverseStartEndNext_ValueBeforeStartAndAfterEnd(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k5"), []byte("v5")),
			&action.Iterator{
				IterOptions: corekv.IterOptions{
					Reverse: true,
					Start:   []byte("k2"),
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
