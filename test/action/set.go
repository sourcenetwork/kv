package action

import (
	"github.com/sourcenetwork/corekv/test/state"
)

// SetValue action will set the given key to the given value when executed.
type SetValue struct {
	Key           []byte
	Value         []byte
	ExpectedError string
}

var _ Action = (*SetValue)(nil)

// Set returns a new SetValue action that will set the given key to the given value when executed.
func Set(key []byte, value []byte) *SetValue {
	return &SetValue{
		Key:   key,
		Value: value,
	}
}

// Set returns a new SetValue action that will set the given key to the given value when executed
// and require that the returned error contains the given string.
func SetE(key []byte, value []byte, expectedErr string) *SetValue {
	return &SetValue{
		Key:           key,
		Value:         value,
		ExpectedError: expectedErr,
	}
}

func (a *SetValue) Execute(s *state.State) {
	err := s.Store.Set(s.Ctx, a.Key, a.Value)
	expectError(s, err, a.ExpectedError)
}
