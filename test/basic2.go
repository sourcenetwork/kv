package kvtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/corekv"
)

// Register a test backend for PrefixDB as well, with some unrelated junk data
// func init() {
// 	//nolint: errcheck
// 	registerDBCreator("prefixdb", func(name, dir string) (DB, error) {
// 		mdb := NewMemDB()
// 		mdb.Set([]byte("a"), []byte{1})
// 		mdb.Set([]byte("b"), []byte{2})
// 		mdb.Set([]byte("t"), []byte{20})
// 		mdb.Set([]byte("test"), []byte{0})
// 		mdb.Set([]byte("u"), []byte{21})
// 		mdb.Set([]byte("z"), []byte{26})
// 		return NewPrefixDB(mdb, []byte("test/")), nil
// 	}, false)
// }

func SubtestBackendsGetSetDelete(t *testing.T, store corekv.Store) {
	// A nonexistent key should return nil.
	ctx := context.Background()
	t.Log("get 1")
	value, err := store.Get(ctx, []byte("a"))
	require.ErrorIs(t, err, corekv.ErrNotFound)
	require.Nil(t, value)

	t.Log("has 1")
	ok, err := store.Has(ctx, []byte("a"))
	require.NoError(t, err)
	require.False(t, ok)

	// Set and get a value.
	t.Log("set 1")
	err = store.Set(ctx, []byte("a"), []byte{0x01})
	require.NoError(t, err)

	t.Log("has 2")
	ok, err = store.Has(ctx, []byte("a"))
	require.NoError(t, err)
	require.True(t, ok)

	t.Log("get 2")
	value, err = store.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x01}, value)

	t.Log("set 1")
	err = store.Set(ctx, []byte("b"), []byte{0x02})
	require.NoError(t, err)

	t.Log("get 3")
	value, err = store.Get(ctx, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x02}, value)

	// Deleting a non-existent value is fine.
	t.Log("delete 1")
	err = store.Delete(ctx, []byte("x"))
	require.NoError(t, err)

	// Delete a value.
	t.Log("delete 2")
	err = store.Delete(ctx, []byte("a"))
	require.NoError(t, err)

	t.Log("get 4")
	value, err = store.Get(ctx, []byte("a"))
	require.ErrorIs(t, err, corekv.ErrNotFound)
	require.Nil(t, value)

	t.Log("delete3")
	err = store.Delete(ctx, []byte("b"))
	require.NoError(t, err)

	value, err = store.Get(ctx, []byte("b"))
	require.ErrorIs(t, err, corekv.ErrNotFound)
	require.Nil(t, value)

	// Setting, getting, and deleting an empty key should error.
	_, err = store.Get(ctx, []byte{})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	_, err = store.Get(ctx, nil)
	require.ErrorIs(t, err, corekv.ErrEmptyKey)

	_, err = store.Has(ctx, []byte{})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	_, err = store.Has(ctx, nil)
	require.ErrorIs(t, err, corekv.ErrEmptyKey)

	err = store.Set(ctx, []byte{}, []byte{0x01})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	err = store.Set(ctx, nil, []byte{0x01})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)

	err = store.Delete(ctx, []byte{})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	err = store.Delete(ctx, nil)
	require.ErrorIs(t, err, corekv.ErrEmptyKey)

	// t.FailNow()
	// Setting a nil value should error, but an empty value is fine.
	err = store.Set(ctx, []byte("x"), nil)
	require.NoError(t, err)

	err = store.Set(ctx, []byte("x"), nil)
	require.NoError(t, err)
	value, err = store.Get(ctx, []byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte(nil), value)

	// t.FailNow()
}

func SubtestDBIterator(t *testing.T, store corekv.Store) {
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if i != 6 { // but skip 6.
			err := store.Set(ctx, int642Bytes(int64(i)), []byte{})
			require.NoError(t, err)
		}
	}

	// var itr corekv.Iterator

	itr := store.Iterator(ctx, corekv.DefaultIterOptions)
	verifyIterator(t, itr, []int64{0, 1, 2, 3, 4, 5, 7, 8, 9}, "forward iterator")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{9, 8, 7, 5, 4, 3, 2, 1, 0}, "reverse iterator")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	End: int642Bytes(0),
	// })
	// verifyIterator(t, itr, []int64(nil), "forward iterator to 0")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(0),
	// })
	// verifyIterator(t, itr, []int64{0, 1, 2, 3, 4, 5, 7, 8, 9}, "forward iterator from 0")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(1),
	// })
	// verifyIterator(t, itr, []int64{1, 2, 3, 4, 5, 7, 8, 9}, "forward iterator from 1")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Reverse: true,
	// 	End:     int642Bytes(10),
	// })
	// verifyIterator(t, ritr,
	// 	[]int64{9, 8, 7, 5, 4, 3, 2, 1, 0}, "reverse iterator from 10 (ex)")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Reverse: true,
	// 	End:     int642Bytes(9),
	// })
	// verifyIterator(t, ritr,
	// 	[]int64{8, 7, 5, 4, 3, 2, 1, 0}, "reverse iterator from 9 (ex)")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Reverse: true,
	// 	End:     int642Bytes(8),
	// })
	// verifyIterator(t, ritr,
	// 	[]int64{7, 5, 4, 3, 2, 1, 0}, "reverse iterator from 8 (ex)")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(5),
	// 	End:   int642Bytes(6),
	// })
	// verifyIterator(t, itr, []int64{5}, "forward iterator from 5 to 6")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(5),
	// 	End:   int642Bytes(7),
	// })
	// verifyIterator(t, itr, []int64{5}, "forward iterator from 5 to 7")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(5),
	// 	End:   int642Bytes(8),
	// })
	// verifyIterator(t, itr, []int64{5, 7}, "forward iterator from 5 to 8")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(6),
	// 	End:   int642Bytes(7),
	// })
	// verifyIterator(t, itr, []int64(nil), "forward iterator from 6 to 7")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(6),
	// 	End:   int642Bytes(8),
	// })
	// verifyIterator(t, itr, []int64{7}, "forward iterator from 6 to 8")

	// itr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start: int642Bytes(7),
	// 	End:   int642Bytes(8),
	// })
	// verifyIterator(t, itr, []int64{7}, "forward iterator from 7 to 8")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(4),
	// 	End:     int642Bytes(5),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{4}, "reverse iterator from 5 (ex) to 4")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(4),
	// 	End:     int642Bytes(6),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr,
	// 	[]int64{5, 4}, "reverse iterator from 6 (ex) to 4")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(4),
	// 	End:     int642Bytes(7),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr,
	// 	[]int64{5, 4}, "reverse iterator from 7 (ex) to 4")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(5),
	// 	End:     int642Bytes(6),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{5}, "reverse iterator from 6 (ex) to 5")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(5),
	// 	End:     int642Bytes(7),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{5}, "reverse iterator from 7 (ex) to 5")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(6),
	// 	End:     int642Bytes(7),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr,
	// 	[]int64(nil), "reverse iterator from 7 (ex) to 6")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(10),
	// 	End:     nil,
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64(nil), "reverse iterator to 10")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(6),
	// 	End:     nil,
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{9, 8, 7}, "reverse iterator to 6")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(5),
	// 	End:     nil,
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{9, 8, 7, 5}, "reverse iterator to 5")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(8),
	// 	End:     int642Bytes(9),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr, []int64{8}, "reverse iterator from 9 (ex) to 8")

	// ritr = store.Iterator(ctx, corekv.IterOptions{
	// 	Start:   int642Bytes(2),
	// 	End:     int642Bytes(4),
	// 	Reverse: true,
	// })
	// verifyIterator(t, ritr,
	// 	[]int64{3, 2}, "reverse iterator from 4 (ex) to 2")
}

func verifyIterator(t *testing.T, itr corekv.Iterator, expected []int64, msg string) {
	var list []int64
	for itr.Valid() {
		key := itr.Key()
		fmt.Println("verify iterator key:", key)
		list = append(list, bytes2Int64(key))
		itr.Next()
	}
	assert.Equal(t, expected, list, msg)
}

func VerifyIterator(t *testing.T, itr corekv.Iterator, expected []int64, msg string) {
	verifyIterator(t, itr, expected, msg)
}

func TestDBBatch(t *testing.T, store corekv.Batchable) {
	// create a new batch, and some items - they should not be visible until we write
	ctx := context.Background()
	batch := store.NewBatch()
	require.NoError(t, batch.Set(ctx, []byte("a"), []byte{1}))
	require.NoError(t, batch.Set(ctx, []byte("b"), []byte{2}))
	require.NoError(t, batch.Set(ctx, []byte("c"), []byte{3}))
	assertKeyValues(t, ctx, store, map[string][]byte{})

	err := batch.Write(ctx)
	require.NoError(t, err)
	assertKeyValues(t, ctx, store, map[string][]byte{"a": {1}, "b": {2}, "c": {3}})

	// trying to modify or rewrite a written batch should error, but closing it should work
	require.Error(t, batch.Set(ctx, []byte("a"), []byte{9}))
	require.Error(t, batch.Delete(ctx, []byte("a")))
	require.Error(t, batch.Write(ctx))
	require.NoError(t, batch.Close(ctx))

	// batches should write changes in order
	batch = store.NewBatch()
	require.NoError(t, batch.Delete(ctx, []byte("a")))
	require.NoError(t, batch.Set(ctx, []byte("a"), []byte{1}))
	require.NoError(t, batch.Set(ctx, []byte("b"), []byte{1}))
	require.NoError(t, batch.Set(ctx, []byte("b"), []byte{2}))
	require.NoError(t, batch.Set(ctx, []byte("c"), []byte{3}))
	require.NoError(t, batch.Delete(ctx, []byte("c")))
	require.NoError(t, batch.Write(ctx))
	require.NoError(t, batch.Close(ctx))
	assertKeyValues(t, ctx, store, map[string][]byte{"a": {1}, "b": {2}})

	// empty and nil keys, as well as nil values, should be disallowed
	batch = store.NewBatch()
	err = batch.Set(ctx, []byte{}, []byte{0x01})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	err = batch.Set(ctx, nil, []byte{0x01})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	err = batch.Set(ctx, []byte("a"), nil)
	require.NoError(t, err)

	err = batch.Delete(ctx, []byte{})
	require.ErrorIs(t, err, corekv.ErrEmptyKey)
	err = batch.Delete(ctx, nil)
	require.ErrorIs(t, err, corekv.ErrEmptyKey)

	err = batch.Close(ctx)
	require.NoError(t, err)

	// it should be possible to write an empty batch
	batch = store.NewBatch()
	err = batch.Write(ctx)
	require.NoError(t, err)
	assertKeyValues(t, ctx, store, map[string][]byte{"a": {1}, "b": {2}})

	// it should be possible to close an empty batch, and to re-close a closed batch
	batch = store.NewBatch()
	batch.Close(ctx)
	batch.Close(ctx)

	// all other operations on a closed batch should error
	require.Error(t, batch.Set(ctx, []byte("a"), []byte{9}))
	require.Error(t, batch.Delete(ctx, []byte("a")))
	require.Error(t, batch.Write(ctx))
}

func assertKeyValues(t *testing.T, ctx context.Context, store corekv.Reader, expect map[string][]byte) {
	iter := store.Iterator(ctx, corekv.DefaultIterOptions) // iterate through all k/v entries
	defer iter.Close(ctx)

	actual := make(map[string][]byte)
	for ; iter.Valid(); iter.Next() {
		val, err := iter.Value()
		require.NoError(t, err)

		actual[string(iter.Key())] = val
	}

	assert.Equal(t, expect, actual)
}
