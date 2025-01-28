package iterator

import (
	"github.com/sourcenetwork/immutable/enumerable"

	"github.com/sourcenetwork/corekv"
)

// Here we assert that [Iterator] does implement the [Enumerable] interface.
//
// We do this in test code only to avoid the direct production code dependency on
// a package we do not need.
//
// Implementing this interface grants [Iterator] instances access to a wide range of
// handy features without any real effort on our part.
var _ enumerable.Enumerable[[]byte] = (corekv.Iterator)(nil)
