package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestSetHas(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("does not matter")),
			action.Has([]byte("k1"), true),
		},
	}

	test.Execute(t)
}
