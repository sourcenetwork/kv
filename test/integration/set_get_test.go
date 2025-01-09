package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestSetGet(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Get([]byte("k1"), []byte("v1")),
		},
	}

	test.Execute(t)
}

func TestSetGet_Multiple(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Set([]byte("k2"), nil),
			action.Set([]byte("k3"), []byte("v3")),

			action.Get([]byte("k1"), []byte("v1")),
			action.Get([]byte("k3"), []byte("v3")),
			action.Get([]byte("k2"), nil),
			action.Get([]byte("k1"), []byte("v1")),
		},
	}

	test.Execute(t)
}
