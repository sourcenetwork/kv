package integration

import (
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
)

func TestSetDeleteGet_Errors(t *testing.T) {
	test := &Test{
		Actions: []action.Action{
			action.Set([]byte("k1"), []byte("v1")),
			action.Delete([]byte("k1")),
			action.GetE([]byte("k1"), "key not found"),
		},
	}

	test.Execute(t)
}
