package action

import (
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

// expectError requires that the given error matches the given expected string.
func expectError(s *state.State, err error, expectedMsg string) {
	if expectedMsg == "" {
		require.NoError(s.T, err)
		return
	}

	require.NotNil(s.T, err)
	require.Contains(s.T, err.Error(), expectedMsg)
}
