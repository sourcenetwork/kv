package action

import (
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

// HasValue action checks if the store has a value at the given key when executed.
type HasValue struct {
	Key           []byte
	Expected      bool
	ExpectedError string
}

var _ Action = (*HasValue)(nil)

// Has returns a [*HasValue] action that checks if the store has a value at the given key when executed.
func Has(key []byte, expected bool) *HasValue {
	return &HasValue{
		Key:      key,
		Expected: expected,
	}
}

// Has returns a [*HasValue] action that checks if the store has a value at the given key when executed and
// requires that the returned error contains the given string.
func HasE(key []byte, expectedErr string) *HasValue {
	return &HasValue{
		Key:           key,
		ExpectedError: expectedErr,
	}
}

func (a *HasValue) Execute(s *state.State) {
	actual, err := s.Store.Has(s.Ctx, a.Key)
	expectError(s, err, a.ExpectedError)

	require.Equal(s.T, a.Expected, actual)
}
