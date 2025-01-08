package integration

import (
	"context"
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/state"
)

// Test is a single, self-contained, test.
type Test struct {
	// Actions contains the set of actions that should be
	// executed as part of this test.
	Actions action.Actions
}

// Execute executes the [Test] and its actions.
//
// It will execute the test against all supported datastore implementations.
func (test *Test) Execute(t testing.TB) {
	for _, storeType := range state.StoreTypes {
		ctx := context.Background()

		test.Actions.Execute(&state.State{
			Options: state.Options{
				StoreType: storeType,
			},
			T:   t,
			Ctx: ctx,
		})
	}
}
