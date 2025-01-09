package action

import (
	"github.com/sourcenetwork/corekv/test/state"
)

// DeleteValue action deletes the entry at the given key when executed.
type DeleteValue struct {
	Key           []byte
	ExpectedError string
}

var _ Action = (*DeleteValue)(nil)

// Delete returns a new [*DeleteValue] action that deletes the entry at the given key when executed.
func Delete(key []byte) *DeleteValue {
	return &DeleteValue{
		Key: key,
	}
}

// Delete returns a new [*DeleteValue] action that deletes the entry at the given key when executed
// and requires that the returned error contains the given string.
func DeleteE(key []byte, expectedErr string) *DeleteValue {
	return &DeleteValue{
		Key:           key,
		ExpectedError: expectedErr,
	}
}

func (a *DeleteValue) Execute(s *state.State) {
	err := s.Store.Delete(s.Ctx, a.Key)
	expectError(s, err, a.ExpectedError)
}
