package badger

import (
	"testing"

	"github.com/dgraph-io/badger/v4"

	kvtest "github.com/sourcenetwork/corekv/test"
)

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//	d, close := newDS(t)
//	defer close()
func newDS(t *testing.T) (*bDB, func()) {
	path := t.TempDir()

	d, err := newDatastore(path, badger.DefaultOptions(""))
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		d.Close()
	}
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()

	// kvtest.SubtestPrefix(t, d)
	kvtest.SubtestAll(t, d)
}
