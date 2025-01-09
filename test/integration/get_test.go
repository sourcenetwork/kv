package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/action"
)

func TestGet_NoneExistantKey_Errors(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.GetE([]byte("does not exist"), corekv.ErrNotFound.Error()),
		},
	}

	test.Execute(t)
}
