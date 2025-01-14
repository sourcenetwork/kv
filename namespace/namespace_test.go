package namespace

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/corekv"
	badgerkv "github.com/sourcenetwork/corekv/badger"
)

func newDS(t *testing.T, _ context.Context) (corekv.Store, func()) {
	path := t.TempDir()
	db, err := badgerkv.NewDatastore(path, badger.DefaultOptions(path))
	require.NoError(t, err)
	return db, func() {
		db.Close()
	}
}

func mockDBWithStuff(t *testing.T, ctx context.Context) (corekv.Store, func()) {
	db, done := newDS(t, ctx)

	require.NoError(t, db.Set(ctx, bz("key"), bz("value")))      // before namespace
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value1")))    // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key11"), bz("value11")))  // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key12"), bz("value12")))  // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key13"), bz("value13")))  // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key2"), bz("value2")))    // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key3"), bz("value3")))    // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key4"), bz("value4")))    // contained in namespace
	require.NoError(t, db.Set(ctx, bz("key5"), bz("value5")))    // contained in namespace
	require.NoError(t, db.Set(ctx, bz("something"), bz("else"))) // after namespace
	require.NoError(t, db.Set(ctx, bz("k"), bz("val")))          // before namespace
	require.NoError(t, db.Set(ctx, bz("ke"), bz("valu")))        // before namespace
	require.NoError(t, db.Set(ctx, bz("kee"), bz("valuu")))      // before namespace

	return db, done
}

func TestNamespaceDBIteratorReversePrefixBeginning(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix:  bz("1"),
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{
		{"13", "value13"},
		{"12", "value12"},
		{"11", "value11"},
		{"1", "value1"},
	}, "reverse prefix 1")
}

func TestNamespaceDBIteratorReversePrefixMiddle(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix:  bz("2"),
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{
		{"2", "value2"},
	}, "reverse prefix 2")
}

func TestNamespaceDBIteratorReversePrefixEnd(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix:  bz("5"),
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{
		{"5", "value5"},
	}, "reverse prefix 5")
}

func runIteratorTest(t *testing.T, opts corekv.IterOptions, expected [][2]string, msg string) {
	ctx := context.Background()
	db, done := mockDBWithStuff(t, ctx)
	pdb := Wrap(db, bz("key"))

	itr := pdb.Iterator(ctx, opts)
	// testkv.CheckDomain(t, itr, nil, nil)

	iteratorVerify(t, itr, expected, msg)
	itr.Close(ctx)
	done()
}

func iteratorVerify(t *testing.T, itr corekv.Iterator, expected [][2]string, msg string) {
	entries := make([][2]string, 0)
	for itr.Valid() {
		key := string(itr.Key())

		v, err := itr.Value()
		require.NoError(t, err)
		value := string(v)

		entries = append(entries, [2]string{key, value})
		itr.Next()
	}
	require.Equal(t, expected, entries, msg)
}

// For testing convenience.
func bz(s string) []byte {
	return []byte(s)
}
