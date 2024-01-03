package memory

import (
	"context"
	"testing"

	"github.com/sourcenetwork/corekv"

	"github.com/stretchr/testify/require"
)

func newDS(t *testing.T, ctx context.Context) (corekv.Store, func()) {
	db := NewDatastore(ctx)
	return db, func() {
		db.Close()
	}
}

func mockDBWithStuff(t *testing.T, ctx context.Context) (corekv.Store, func()) {
	db, done := newDS(t, ctx)

	require.NoError(t, db.Set(ctx, bz("key"), bz("value")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value1")))
	require.NoError(t, db.Set(ctx, bz("key11"), bz("value11")))
	require.NoError(t, db.Set(ctx, bz("key12"), bz("value12")))
	require.NoError(t, db.Set(ctx, bz("key13"), bz("value13")))
	require.NoError(t, db.Set(ctx, bz("key2"), bz("value2")))
	require.NoError(t, db.Set(ctx, bz("key3"), bz("value3")))
	require.NoError(t, db.Set(ctx, bz("key4"), bz("value4")))
	require.NoError(t, db.Set(ctx, bz("key5"), bz("value5")))
	require.NoError(t, db.Set(ctx, bz("something"), bz("else")))
	require.NoError(t, db.Set(ctx, bz("k"), bz("val")))
	require.NoError(t, db.Set(ctx, bz("ke"), bz("valu")))
	require.NoError(t, db.Set(ctx, bz("kee"), bz("valuu")))

	// itr := db.Iterator(ctx, corekv.DefaultIterOptions)

	// for ; itr.Valid(); itr.Next() {
	// 	fmt.Println("namespace.iter.Key", string(itr.Key()))
	// 	fmt.Println("namespace.iter.Value", string(itr.Value()))
	// }

	return db, done
}

func mockDBWithDuplicates(t *testing.T, ctx context.Context) (corekv.Store, func()) {
	db, done := newDS(t, ctx)

	require.NoError(t, db.Set(ctx, bz("key"), bz("value")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value1")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value11")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value12")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value13")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value2")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value3")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value4")))
	require.NoError(t, db.Set(ctx, bz("key"), bz("value5")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value1")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value11")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value12")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value13")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value2")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value3")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value4")))
	require.NoError(t, db.Set(ctx, bz("key1"), bz("value5")))

	return db, done
}

func TestMemoryDBIteratorForwardFull(t *testing.T) {
	opts := corekv.IterOptions{}
	runIteratorTest(t, opts, [][2]string{
		{"k", "val"},
		{"ke", "valu"},
		{"kee", "valuu"},
		{"key", "value"},
		{"key1", "value1"},
		{"key11", "value11"},
		{"key12", "value12"},
		{"key13", "value13"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"something", "else"},
	}, "forward full range")
}

func TestMemoryDBIteratorForwardStartAt1(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("key1"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key1", "value1"},
		{"key11", "value11"},
		{"key12", "value12"},
		{"key13", "value13"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"something", "else"},
	}, "forward range from 1")
}

func TestMemoryDBIteratorForwardStartAt2(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("key2"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"something", "else"},
	}, "forward range from 2")
}

func TestMemoryDBIteratorForwardEndAt5(t *testing.T) {
	opts := corekv.IterOptions{
		End: bz("key5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"k", "val"},
		{"ke", "valu"},
		{"kee", "valuu"},
		{"key", "value"},
		{"key1", "value1"},
		{"key11", "value11"},
		{"key12", "value12"},
		{"key13", "value13"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
	}, "forward range end 5")
}

func TestMemoryDBIteratorForwardEndAt4(t *testing.T) {
	opts := corekv.IterOptions{
		End: bz("key4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"k", "val"},
		{"ke", "valu"},
		{"kee", "valuu"},
		{"key", "value"},
		{"key1", "value1"},
		{"key11", "value11"},
		{"key12", "value12"},
		{"key13", "value13"},
		{"key2", "value2"},
		{"key3", "value3"},
	}, "forward range end 4")
}

func TestMemoryDBIteratorForwardStart1End5(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("key1"),
		End:   bz("key5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key1", "value1"},
		{"key11", "value11"},
		{"key12", "value12"},
		{"key13", "value13"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
	}, "forward range [1, 5)")
}

func TestMemoryDBIteratorForwardStart2End4(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("key2"),
		End:   bz("key4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key2", "value2"},
		{"key3", "value3"},
	}, "forward range [2, 4)")
}

func TestMemoryDBIteratorReverseFull(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{
		{"something", "else"},
		{"key5", "value5"},
		{"key4", "value4"},
		{"key3", "value3"},
		{"key2", "value2"},
		{"key13", "value13"},
		{"key12", "value12"},
		{"key11", "value11"},
		{"key1", "value1"},
		{"key", "value"},
		{"kee", "valuu"},
		{"ke", "valu"},
		{"k", "val"},
	}, "reverse full range")
}

func TestMemoryDBIteratorReverseStartAt1(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("key1"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"something", "else"},
		{"key5", "value5"},
		{"key4", "value4"},
		{"key3", "value3"},
		{"key2", "value2"},
		{"key13", "value13"},
		{"key12", "value12"},
		{"key11", "value11"},
		{"key1", "value1"},
	}, "reverse range start at 1")
}

func TestMemoryDBIteratorReverseStartAt2(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("key2"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"something", "else"},
		{"key5", "value5"},
		{"key4", "value4"},
		{"key3", "value3"},
		{"key2", "value2"},
	}, "reverse range start at 2")
}

func TestMemoryDBIteratorReverseEndAt5(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		End:     bz("key5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key4", "value4"},
		{"key3", "value3"},
		{"key2", "value2"},
		{"key13", "value13"},
		{"key12", "value12"},
		{"key11", "value11"},
		{"key1", "value1"},
		{"key", "value"},
		{"kee", "valuu"},
		{"ke", "valu"},
		{"k", "val"},
	}, "reverse range end at 5")
}

func TestMemoryDBIteratorReverseEndAt4(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		End:     bz("key4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key3", "value3"},
		{"key2", "value2"},
		{"key13", "value13"},
		{"key12", "value12"},
		{"key11", "value11"},
		{"key1", "value1"},
		{"key", "value"},
		{"kee", "valuu"},
		{"ke", "valu"},
		{"k", "val"},
	}, "reverse range end at 4")
}

func TestMemoryDBIteratorReverseStart1End5(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("key1"),
		End:     bz("key5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key4", "value4"},
		{"key3", "value3"},
		{"key2", "value2"},
		{"key13", "value13"},
		{"key12", "value12"},
		{"key11", "value11"},
		{"key1", "value1"},
	}, "reverse range [1, 5)")
}

func TestMemoryDBIteratorReverseStart2End4(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("key2"),
		End:     bz("key4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key3", "value3"},
		{"key2", "value2"},
	}, "reverse range [2, 4)")
}

func TestMemoryDBIteratorForwardPrefixBeginning(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix: bz("key1"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"key11", "value11"},
		{"key12", "value12"},
		{"key13", "value13"},
	}, "forward prefix 1")
}

func TestMemoryDBIteratorForwardPrefixMiddle(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix: bz("key2"),
	}
	runIteratorTest(t, opts, [][2]string{}, "forward prefix 2")
}

func TestMemoryDBIteratorForwardPrefixEnd(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix: bz("key5"),
	}
	runIteratorTest(t, opts, [][2]string{}, "forward prefix 5")
}

func TestMemoryDBIteratorReversePrefixBeginning(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix:  bz("key1"),
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{
		{"key13", "value13"},
		{"key12", "value12"},
		{"key11", "value11"},
	}, "reverse prefix 1")
}

func TestMemoryDBDuplicatesIteratorForward(t *testing.T) {
	ctx := context.Background()
	db, done := mockDBWithDuplicates(t, ctx)
	defer done()

	itr := db.Iterator(ctx, corekv.DefaultIterOptions)
	defer itr.Close(ctx)

	iteratorVerify(t, itr, [][2]string{
		{"key", "value5"},
		{"key1", "value5"},
	}, "duplicates forward full")
}

func TestMemoryDBIteratorReversePrefixMiddle(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix:  bz("key2"),
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{}, "reverse prefix 2")
}

func TestMemoryDBIteratorReversePrefixEnd(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix:  bz("5"),
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{}, "reverse prefix 5")
}

func runIteratorTest(t *testing.T, opts corekv.IterOptions, expected [][2]string, msg string) {
	ctx := context.Background()
	db, done := mockDBWithStuff(t, ctx)

	itr := db.Iterator(ctx, opts)
	// testkv.CheckDomain(t, itr, nil, nil)

	iteratorVerify(t, itr, expected, msg)
	itr.Close(ctx)
	done()
}

func iteratorVerify(t *testing.T, itr corekv.Iterator, expected [][2]string, msg string) {
	entries := make([][2]string, 0)
	for itr.Valid() {
		key := string(itr.Key())
		value := string(itr.Value())
		entries = append(entries, [2]string{key, value})
		itr.Next()
	}
	require.Equal(t, expected, entries, msg)
}

func iteratorVerifyKeys(t *testing.T, itr corekv.Iterator, expected []string, msg string) {
	var list []string
	for itr.Valid() {
		key := itr.Key()
		list = append(list, string(key))
		itr.Next()
	}
	require.Equal(t, expected, list, msg)
}
