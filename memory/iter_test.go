package memory

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func comparator(a, b int) bool {
	return a < b
}

// This test documents undesirable behaviour of the btree implementation
// that we are currently using, it is documented by issue:
// https://github.com/tidwall/btree/issues/46
//
// This test exists here to document unexpected behaviour in the hope that
// it saves time in the future.
func TestBTreePrevBug(t *testing.T) {
	tree := btree.NewBTreeG(comparator)

	tree.Set(1)
	tree.Set(2)
	tree.Set(3)

	iterator := tree.Iter()

	ok := iterator.Last()
	require.True(t, ok)

	// The presence of this `Next` call causes the bug to surface later.
	// If commented out, later `Prev` calls behave as expected.
	ok = iterator.Next()
	require.False(t, ok)

	ok = iterator.Prev()
	require.True(t, ok)

	v := iterator.Item()
	require.Equal(t, 2, v)

	ok = iterator.Prev()
	require.True(t, ok)

	v = iterator.Item()
	require.Equal(t, 1, v)

	ok = iterator.Prev()
	require.False(t, ok)

	// Bug starts here!
	//
	// Notes:
	// 1. I expected all the `Prev` calls beyond this point to return `false`.
	// 2. If you comment out the `Next` call noted towards the top of the function,
	//    the `Prev` calls all return `true` as I expected.
	// 3. The `Item` calls are included for descriptive reasons only, I found them
	//    interesting, but much less so than the return value from `Prev`.
	// 4. The 2, 1, 1; 2, 1, 1; patern repeats as far as I bothered looking at.

	ok = iterator.Prev()
	require.True(t, ok)

	v = iterator.Item()
	require.Equal(t, 2, v)

	ok = iterator.Prev()
	require.True(t, ok)

	v = iterator.Item()
	require.Equal(t, 1, v)

	ok = iterator.Prev()
	require.False(t, ok)

	v = iterator.Item()
	require.Equal(t, 1, v)

	ok = iterator.Prev()
	require.True(t, ok)

	v = iterator.Item()
	require.Equal(t, 2, v)
}
