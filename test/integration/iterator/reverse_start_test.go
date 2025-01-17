package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorReverseStart(t *testing.T) {
	test := &integration.Test{
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
