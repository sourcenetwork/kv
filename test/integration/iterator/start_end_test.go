package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorStartEnd(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			action.Set([]byte("k5"), []byte("v5")),
			&action.Iterate{
				IterOptions: corekv.IterOptions{
					Start: []byte("k2"),
					End:   []byte("k4"),
				},
				Expected: []action.KeyValue{
					// `k1` must not be yielded
					{Key: []byte("k2"), Value: []byte("v2")},
					{Key: []byte("k3"), Value: nil},
					// `k4` and `k5` must not be yielded
				},
			},
		},
	}

	test.Execute(t)
}
