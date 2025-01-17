package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorDelete(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			action.Delete([]byte("k2")),
			&action.Iterate{
				Expected: []action.KeyValue{
					{Key: []byte("k1"), Value: []byte("v1")},
					// `k2` should not be returned
					{Key: []byte("k3"), Value: nil},
					{Key: []byte("k4"), Value: []byte("v4")},
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratorDelete_DeleteLastItem(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k3"), nil),
			action.Set([]byte("k4"), []byte("v4")),
			action.Set([]byte("k2"), []byte("v2")),
			action.Delete([]byte("k4")),
			&action.Iterate{
				Expected: []action.KeyValue{
					{Key: []byte("k1"), Value: []byte("v1")},
					{Key: []byte("k2"), Value: []byte("v2")},
					{Key: []byte("k3"), Value: nil},
					// `k4` should not be returned
				},
			},
		},
	}

	test.Execute(t)
}

func TestIteratorDelete_DeleteOnlyItem(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k4"), []byte("v4")),
			action.Delete([]byte("k4")),
			&action.Iterate{
				Expected: []action.KeyValue{
					// `k4` should not be returned
				},
			},
		},
	}

	test.Execute(t)
}
