package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestCancel(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.New(),
			action.Cancel(),
		},
	}

	test.Execute(t)
}
