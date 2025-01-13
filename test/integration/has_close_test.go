package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestHasClose_HasOnClosedStore_Errors(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Close(),
			action.HasE([]byte("does not matter"), "datastore closed"),
		},
	}

	test.Execute(t)
}
