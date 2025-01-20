package iterator

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/integration"
)

func TestIteratorValid(t *testing.T) {
	test := &integration.Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			&action.Iterator{
				Actions: []action.IteratorAction{
					action.IsValid(),
				},
			},
		},
	}

	test.Execute(t)
}
