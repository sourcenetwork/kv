package action

import "github.com/sourcenetwork/corekv/test/state"

// Action represents an item to be executed as part of a test.
//
// For example an action may set or get a value, or it may close the datastore.
type Action interface {
	// Execute this action upon the given state.
	Execute(*state.State)
}

// Actions is an executable set of [Action]s.
type Actions []Action

// Execute this set of actions upon the given state, serially, in order.
func (a Actions) Execute(s *state.State) {
	for _, action := range a {
		action.Execute(s)
	}
}
