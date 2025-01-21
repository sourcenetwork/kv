package action

import (
	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

// Iterator action will create an iterator, execute the given child actions on the iterator,
// then release the iterator when this [Iterator] action is executed.
type Iterator struct {
	corekv.IterOptions

	// The actions to execute on the created iterator.
	ChildActions []IteratorAction
}

var _ Action = (*Iterator)(nil)

func (a *Iterator) Execute(s *state.State) {
	iterator := s.Store.Iterator(s.Ctx, a.IterOptions)

	for _, action := range a.ChildActions {
		action.Execute(s, iterator)
	}

	err := iterator.Close(s.Ctx)
	require.NoError(s.T, err)
}

// IteratorAction types will execute as part of an [Iterator] action.
type IteratorAction interface {
	Execute(s *state.State, iterator corekv.Iterator)
}

// SeekTo executes a single `Seek` call on an [Iterator].
type SeekTo struct {
	// The target key to seek to.
	Target []byte
}

var _ IteratorAction = (*SeekTo)(nil)

// Seek returns a [SeekTo] iterator action that executes a single `Seek` call
// on an [Iterator] to the given target.
func Seek(target []byte) *SeekTo {
	return &SeekTo{
		Target: target,
	}
}

func (a *SeekTo) Execute(s *state.State, iterator corekv.Iterator) {
	iterator.Seek(a.Target)
}

// Valid executes a single `Valid` call on an [Iterator] and requires that
// the returned result matches the given `Expected` value.
type Valid struct {
	// The expected result of the `Valid` call.
	Expected bool
}

var _ IteratorAction = (*Valid)(nil)

// IsValid returns a [Valid] iterator action that executes a single `Valid` call
// on an [Iterator] and requires that the returned result is true.
func IsValid() *Valid {
	return &Valid{
		Expected: true,
	}
}

// IsInvalid returns a [Valid] iterator action that executes a single `Valid` call
// on an [Iterator] and requires that the returned result is false.
func IsInvalid() *Valid {
	return &Valid{
		Expected: false,
	}
}

func (a *Valid) Execute(s *state.State, iterator corekv.Iterator) {
	result := iterator.Valid()
	require.Equal(s.T, a.Expected, result)
}

// MoveNext executes a single `Next` call on an an [Iterator].
type MoveNext struct{}

var _ IteratorAction = (*MoveNext)(nil)

// Next returns a [MoveNext] iterator action that executes a single `Next` call
// on an [Iterator].
func Next() *MoveNext {
	return &MoveNext{}
}

func (a *MoveNext) Execute(s *state.State, iterator corekv.Iterator) {
	iterator.Next()
}
