package integration

import (
	"context"
	"testing"

	"github.com/sourcenetwork/corekv/test/action"
	"github.com/sourcenetwork/corekv/test/state"
)

type NamespaceType int

const (
	// Automatically execute the test with a namespaced store, as well the standard
	// unnamespaced store.
	Automatic = 0

	// Disable the automatic wrapping of the store in a namespace.  Manually defined
	// namespace actions will still execute.
	ManualOnly = 1

	// Disables testing without the automatically provided namespace.
	AutomaticForced = 2
)

// Test is a single, self-contained, test.
type Test struct {
	// If this set is not empty, only the store types within it will be used to execute
	// the test.
	//
	// This should only be used for temporarily documenting differences between Store
	// implementations.
	SupportedStoreTypes []state.StoreType

	// Namespacing controls automatic namespacing of test actions.
	//
	// This should only be used for temporarily documenting differences between Store
	// implementations.
	Namespacing NamespaceType

	// Actions contains the set of actions that should be
	// executed as part of this test.
	Actions action.Actions
}

// Execute executes the [Test] and its actions.
//
// It will execute the test against all supported datastore implementations.
func (test *Test) Execute(t testing.TB) {
	if test.Namespacing != AutomaticForced {
		for _, storeType := range state.StoreTypes {
			if test.shouldSkipType(storeType) {
				continue
			}

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			actions := prependNewStore(test.Actions)
			actions = appendCloseStore(actions)

			actions.Execute(&state.State{
				Options: state.Options{
					StoreType: storeType,
				},
				T:         t,
				Ctx:       ctx,
				CtxCancel: cancel,
			})
		}
	}

	if test.Namespacing != ManualOnly {
		// As well as testing all supported stores directly, we then retest them namespaced.
		// This provides us with very cheap test coverage of the namespace store.
		for _, storeType := range state.StoreTypes {
			if test.shouldSkipType(storeType) {
				continue
			}

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			actions := prependNamespaceStore(test.Actions)
			actions = prependNewStore(actions)
			actions = appendCloseStore(actions)

			actions.Execute(&state.State{
				Options: state.Options{
					StoreType: storeType,
				},
				T:         t,
				Ctx:       ctx,
				CtxCancel: cancel,
			})
		}
	}
}

// prependNewStore prepends an [*action.NewStore] action to the front of the given
// action set if the set does not already contain a new store action.
//
// This is done so that we don't have to bother adding (and reading) the same action
// in 99% of our tests.
func prependNewStore(actions action.Actions) action.Actions {
	if hasType[*action.NewStore](actions) {
		return actions
	}

	result := make(action.Actions, 1, len(actions)+1)
	result[0] = &action.NewStore{}
	result = append(result, actions...)

	return result
}

// prependNewStore prepends an [*action.NewStore] action to the front of the given
// action set.
//
// This will be done whether or not there are existing namespace actions in the set or not.
func prependNamespaceStore(actions action.Actions) action.Actions {
	result := make(action.Actions, 1, len(actions)+1)
	result[0] = action.Namespace([]byte("/example"))
	result = append(result, actions...)

	return result
}

// appendCloseStore appends an [*action.CloseStore] action to the end of the given
// action set if the set does not already contain a close store action.
//
// This is done so that we don't have to bother adding (and reading) the same action
// in 99% of our tests.
func appendCloseStore(actions action.Actions) action.Actions {
	if hasType[*action.CloseStore](actions) {
		return actions
	}

	actions = append(actions, &action.CloseStore{})
	return actions
}

// hasType returns true if any of the items in the given set are of the given type.
func hasType[TAction any](actions action.Actions) bool {
	for _, action := range actions {
		_, ok := action.(TAction)
		if ok {
			return true
		}
	}

	return false
}

// shouldSkipType returns true if this store type should be skipped for this test.
func (test *Test) shouldSkipType(storeType state.StoreType) bool {
	if len(test.SupportedStoreTypes) > 0 {
		isSupported := false
		for _, supportedType := range test.SupportedStoreTypes {
			if storeType == supportedType {
				isSupported = true
				break
			}
		}

		if !isSupported {
			return true
		}
	}

	return false
}
