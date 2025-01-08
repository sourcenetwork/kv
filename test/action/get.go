package action

import (
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

// GetValue actions when executed will get the value at the given key and require
// that the returned value matches the given expected value.
type GetValue struct {
	Key           []byte
	Expected      []byte
	ExpectedError string
}

var _ Action = (*GetValue)(nil)

// Get returns an action that when executed will get the value at the given key and require
// that the returned value matches the given expected value.
func Get(key []byte, expected []byte) *GetValue {
	return &GetValue{
		Key:      key,
		Expected: expected,
	}
}

// Get returns an action that when executed will get the value at the given key and require
// that the returned error contains the given string.
func GetE(key []byte, error string) *GetValue {
	return &GetValue{
		Key:           key,
		ExpectedError: error,
	}
}

func (a *GetValue) Execute(s *state.State) {
	actual, err := s.Store.Get(s.Ctx, a.Key)
	expectError(s, err, a.ExpectedError)

	require.Equal(s.T, a.Expected, actual)
}
