package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestClose(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.New(),
			action.Close(),
		},
	}

	test.Execute(t)
}

func TestClose_CloseTwice(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.New(),
			action.Close(),
			action.Close(),
		},
	}

	test.Execute(t)
}
