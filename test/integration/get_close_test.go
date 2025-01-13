package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestGetClose_GetOnClosedStore_Errors(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Close(),
			action.GetE([]byte("does not matter"), "datastore closed"),
		},
	}

	test.Execute(t)
}
