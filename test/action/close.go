package action

import (
	"github.com/sourcenetwork/corekv/test/state"
)

// CloseStore action will close the current Rootstore.
type CloseStore struct {
	ExpectedError string
}

var _ Action = (*CloseStore)(nil)

// Close returns a new [*CloseStore] action that will close the current Rootstore
// when executed.
func Close() *CloseStore {
	return &CloseStore{}
}

// CloseE returns a new [*CloseStore] action that will close the current Rootstore
// when executed and expect an error returned from the call.
func CloseE(expectedErr string) *CloseStore {
	return &CloseStore{
		ExpectedError: expectedErr,
	}
}

func (a *CloseStore) Execute(s *state.State) {
	err := s.Rootstore.Close()
	expectError(s, err, a.ExpectedError)
}
