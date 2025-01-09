package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestHas(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Has([]byte("does not exist"), false),
		},
	}

	test.Execute(t)
}
