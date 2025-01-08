// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memory

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sourcenetwork/corekv"
	"github.com/stretchr/testify/require"
)

var (
	testKey1   = []byte("testKey1")
	testValue1 = []byte("this is a test value 1")

	testKey2   = []byte("testKey2")
	testValue2 = []byte("this is a test value 2")

	testKey3   = []byte("testKey3")
	testValue3 = []byte("this is a test value 3")

	testKey4   = []byte("testKey4")
	testValue4 = []byte("this is a test value 4")

	testKey5   = []byte("testKey5")
	testValue5 = []byte("this is a test value 5")
)

func newLoadedDatastore(ctx context.Context) *Datastore {
	s := NewDatastore(ctx)
	v := s.nextVersion()
	s.values.Set(dsItem{
		key:     testKey1,
		val:     testValue1,
		version: v,
	})
	v = s.nextVersion()
	s.values.Set(dsItem{
		key:     testKey2,
		val:     testValue2,
		version: v,
	})
	return s
}

func TestNewDatastore(t *testing.T) {
	ctx := context.Background()
	s := NewDatastore(ctx)
	require.NotNil(t, s)
}

func TestCloseOperation(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	s.Close()
	// require.NoError(t, err)
}

func TestConsecutiveClose(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	s.Close()
	// require.NoError(t, err)

	s.Close()
	// require.ErrorIs(t, err, ErrClosed)
}

func TestCloseThroughContext(t *testing.T) {
	ctx := context.Background()
	newCtx, cancel := context.WithCancel(ctx)
	s := newLoadedDatastore(newCtx)

	cancel()

	time.Sleep(time.Millisecond * 10)

	s.Close()
	// require.ErrorIs(t, err, ErrClosed)
}

func TestGetOperation(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	resp, err := s.Get(ctx, testKey1)
	require.NoError(t, err)
	require.Equal(t, testValue1, resp)
}

func TestGetOperationWithStoreClosed(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)
	s.Close()
	// require.NoError(t, err)

	_, err := s.Get(ctx, testKey1)
	require.ErrorIs(t, err, ErrClosed)
}

func TestGetOperationNotFound(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	_, err := s.Get(ctx, testKey3)
	require.ErrorIs(t, err, corekv.ErrNotFound)
}

func TestDeleteOperation(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	err := s.Delete(ctx, testKey1)
	require.NoError(t, err)

	_, err = s.Get(ctx, testKey1)
	require.ErrorIs(t, err, corekv.ErrNotFound)
}

func TestDeleteOperation2(t *testing.T) {
	ctx := context.Background()
	s := NewDatastore(ctx)

	err := s.Set(ctx, testKey1, testValue1)
	require.NoError(t, err)

	err = s.Delete(ctx, testKey1)
	require.NoError(t, err)

	_, err = s.Get(ctx, testKey1)
	require.ErrorIs(t, err, corekv.ErrNotFound)
}

func TestDeleteOperationWithStoreClosed(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	s.Close()

	err := s.Delete(ctx, testKey1)
	require.ErrorIs(t, err, ErrClosed)
}

// func TestGetSizeOperation(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	resp, err := s.GetSize(ctx, testKey1)
// 	require.NoError(t, err)
// 	require.Equal(t, len(testValue1), resp)
// }

// func TestGetSizeOperationNotFound(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	_, err := s.GetSize(ctx, testKey3)
// 	require.ErrorIs(t, err, ds.ErrNotFound)
// }

// func TestGetSizeOperationWithStoreClosed(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	s.Close()
// 	require.NoError(t, err)

// 	_, err = s.GetSize(ctx, testKey3)
// 	require.ErrorIs(t, err, ErrClosed)
// }

func TestHasOperation(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	resp, err := s.Has(ctx, testKey1)
	require.NoError(t, err)
	require.Equal(t, true, resp)
}

func TestHasOperationNotFound(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	resp, err := s.Has(ctx, testKey3)
	require.NoError(t, err)
	require.Equal(t, false, resp)
}

func TestHasOperationWithStoreClosed(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	s.Close()
	// require.NoError(t, err)

	_, err := s.Has(ctx, testKey3)
	require.ErrorIs(t, err, ErrClosed)
}

func TestPutOperation(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	err := s.Set(ctx, testKey3, testValue3)
	require.NoError(t, err)

	resp, err := s.Get(ctx, testKey3)
	require.NoError(t, err)
	require.Equal(t, testValue3, resp)
}

func TestPutOperationWithStoreClosed(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	s.Close()
	// require.NoError(t, err)

	err := s.Set(ctx, testKey3, testValue3)
	require.ErrorIs(t, err, ErrClosed)
}

// func TestQueryOperation(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	results, err := s.Query(ctx, dsq.Query{
// 		Limit:  1,
// 		Offset: 1,
// 	})
// 	require.NoError(t, err)

// 	result, _ := results.NextSync()

// 	require.Equal(t, testKey2.String(), result.Entry.Key)
// 	require.Equal(t, testValue2, result.Entry.Value)
// }

// func TestQueryOperationWithStoreClosed(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	s.Close()
// 	// require.NoError(t, err)

// 	_, err = s.Query(ctx, dsq.Query{
// 		Limit:  1,
// 		Offset: 1,
// 	})
// 	require.ErrorIs(t, err, ErrClosed)
// }

func TestQueryOperationWithAddedItems(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	err := s.Set(ctx, testKey3, testValue3)
	require.NoError(t, err)
	err = s.Set(ctx, testKey4, testValue4)
	require.NoError(t, err)

	err = s.Set(ctx, testKey5, testValue5)
	require.NoError(t, err)

	err = s.Delete(ctx, testKey2)
	require.NoError(t, err)

	err = s.Set(ctx, testKey2, testValue2)
	require.NoError(t, err)

	err = s.Delete(ctx, testKey1)
	require.NoError(t, err)

	// results, err := s.Query(ctx, dsq.Query{})
	// require.NoError(t, err)
	// entries, err := results.Rest()
	// require.NoError(t, err)
	// expectedResults := []dsq.Entry{
	// 	{
	// 		Key:   testKey2.String(),
	// 		Value: testValue2,
	// 		Size:  len(testValue2),
	// 	},
	// 	{
	// 		Key:   testKey3.String(),
	// 		Value: testValue3,
	// 		Size:  len(testValue3),
	// 	},
	// 	{
	// 		Key:   testKey4.String(),
	// 		Value: testValue4,
	// 		Size:  len(testValue4),
	// 	},
	// 	{
	// 		Key:   testKey5.String(),
	// 		Value: testValue5,
	// 		Size:  len(testValue5),
	// 	},
	// }
	// require.Equal(t, expectedResults, entries)
}

func TestConcurrentWrite(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	wg := &sync.WaitGroup{}

	for i := 1; i <= 1000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, num int) {
			_ = s.Set(ctx, []byte(fmt.Sprintf("testKey%d", num)), []byte(fmt.Sprintf("this is a test value %d", num)))
			wg.Done()
		}(wg, i)
	}
	wg.Wait()
	resp, err := s.Get(ctx, []byte("testKey3"))
	require.NoError(t, err)
	require.Equal(t, []byte("this is a test value 3"), resp)
}

// func TestSyncOperation(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	err := s.Sync(ctx, testKey1)
// 	require.NoError(t, err)
// }

// func TestSyncOperationWithStoreClosed(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	s.Close()
// 	// require.NoError(t, err)

// 	err = s.Sync(ctx, testKey1)
// 	require.ErrorIs(t, err, ErrClosed)
// }

// func TestPurge(t *testing.T) {
// 	ctx := context.Background()
// 	s := newLoadedDatastore(ctx)

// 	err := s.Set(ctx, testKey1, testValue2)
// 	require.NoError(t, err)
// 	err = s.Set(ctx, testKey2, testValue3)
// 	require.NoError(t, err)

// 	iter := s.values.Iter()
// 	results := []dsq.Entry{}
// 	for iter.Next() {
// 		results = append(results, dsq.Entry{
// 			Key:   iter.Item().key,
// 			Value: iter.Item().val,
// 			Size:  len(iter.Item().val),
// 		})
// 	}
// 	iter.Release()

// 	expectedResults := []dsq.Entry{
// 		{
// 			Key:   testKey1.String(),
// 			Value: testValue1,
// 			Size:  len(testValue1),
// 		},
// 		{
// 			Key:   testKey1.String(),
// 			Value: testValue2,
// 			Size:  len(testValue2),
// 		},
// 		{
// 			Key:   testKey2.String(),
// 			Value: testValue2,
// 			Size:  len(testValue2),
// 		},
// 		{
// 			Key:   testKey2.String(),
// 			Value: testValue3,
// 			Size:  len(testValue3),
// 		},
// 	}
// 	require.Equal(t, expectedResults, results)

// 	s.executePurge(ctx)

// 	iter = s.values.Iter()
// 	results = []dsq.Entry{}
// 	for iter.Next() {
// 		results = append(results, dsq.Entry{
// 			Key:   iter.Item().key,
// 			Value: iter.Item().val,
// 			Size:  len(iter.Item().val),
// 		})
// 	}
// 	iter.Release()

// 	expectedResults = []dsq.Entry{
// 		{
// 			Key:   testKey1.String(),
// 			Value: testValue2,
// 			Size:  len(testValue2),
// 		},
// 		{
// 			Key:   testKey2.String(),
// 			Value: testValue3,
// 			Size:  len(testValue3),
// 		},
// 	}
// 	require.Equal(t, expectedResults, results)
// }

func TestPurgeBatching(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	for j := 0; j < 10; j++ {
		for i := 1; i <= 1000; i++ {
			err := s.Set(ctx, []byte("test"), []byte(fmt.Sprintf("%d", i+(j*1000))))
			require.NoError(t, err)
		}
	}

	s.executePurge()

	resp, err := s.Get(ctx, []byte("test"))
	require.NoError(t, err)

	val, err := strconv.Atoi(string(resp))
	require.NoError(t, err)

	require.GreaterOrEqual(t, val, 9000)
}

func TestPurgeWithOlderInFlightTxn(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)
	s.inFlightTxn.Set(dsTxn{
		dsVersion:  s.getVersion(),
		txnVersion: s.getVersion() + 1,
		expiresAt:  time.Now(),
	})

	err := s.Set(ctx, testKey4, testValue4)
	require.NoError(t, err)

	s.executePurge()
}

func TestClearOldFlightTransactions(t *testing.T) {
	ctx := context.Background()
	s := newLoadedDatastore(ctx)

	s.inFlightTxn.Set(dsTxn{
		dsVersion:  s.getVersion(),
		txnVersion: s.getVersion() + 1,
		// Ensure expiresAt is before the value returned from the later call in `clearOldInFlightTxn`,
		// in windows in particular it seems that the two `time.Now` calls can return the same value
		expiresAt: time.Now().Add(-1 * time.Minute),
	})

	require.Equal(t, 1, s.inFlightTxn.Len())

	s.clearOldInFlightTxn()

	require.Equal(t, 0, s.inFlightTxn.Len())
}

// func loadPrefixData(t *testing.T, ctx context.Context, db corekv.Store) {
// 	require.NoError(t, db.Set(ctx, bz("key"), bz("value")))      // before namespace
// 	require.NoError(t, db.Set(ctx, bz("key1"), bz("value1")))    // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key11"), bz("value11")))  // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key12"), bz("value12")))  // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key13"), bz("value13")))  // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key2"), bz("value2")))    // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key3"), bz("value3")))    // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key4"), bz("value4")))    // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("key5"), bz("value5")))    // contained in namespace
// 	require.NoError(t, db.Set(ctx, bz("something"), bz("else"))) // after namespace
// 	require.NoError(t, db.Set(ctx, bz("k"), bz("val")))          // before namespace
// 	require.NoError(t, db.Set(ctx, bz("ke"), bz("valu")))        // before namespace
// 	require.NoError(t, db.Set(ctx, bz("kee"), bz("valuu")))      // before namespace
// }

// func TestIteratorPrefixForwardPrefix(t *testing.T) {
// 	ctx := context.Background()
// 	db := NewDatastore(ctx)
// 	defer db.Close()

// 	loadPrefixData(t, ctx, db)

// 	it := db.Iterator(ctx, corekv.IterOptions{
// 		Prefix: bz("key"),
// 	})
// 	require.NotNil(t, it)

// 	iteratorVerify(t, it, [][2]string{
// 		{"key1", "value1"},
// 		{"key11", "value11"},
// 		{"key12", "value12"},
// 		{"key13", "value13"},
// 		{"key2", "value2"},
// 		{"key3", "value3"},
// 		{"key4", "value4"},
// 		{"key5", "value5"},
// 	}, "prefix forward full")
// 	require.NoError(t, it.Close(ctx))
// }

// func TestIteratorPrefixForwardFull(t *testing.T) {
// 	ctx := context.Background()
// 	db := NewDatastore(ctx)
// 	defer db.Close()

// 	loadPrefixData(t, ctx, db)

// 	it := db.Iterator(ctx, corekv.IterOptions{
// 		Prefix: bz(""),
// 	})
// 	require.NotNil(t, it)

// 	iteratorVerify(t, it, [][2]string{
// 		{"k", "val"},
// 		{"ke", "valu"},
// 		{"kee", "valuu"},
// 		{"key", "value"},
// 		{"key1", "value1"},
// 		{"key11", "value11"},
// 		{"key12", "value12"},
// 		{"key13", "value13"},
// 		{"key2", "value2"},
// 		{"key3", "value3"},
// 		{"key4", "value4"},
// 		{"key5", "value5"},
// 		{"something", "else"},
// 	}, "prefix forward full")
// 	require.NoError(t, it.Close(ctx))
// }

// func TestIteratorPrefixReverseFull(t *testing.T) {
// 	ctx := context.Background()
// 	db := NewDatastore(ctx)
// 	defer db.Close()

// 	loadPrefixData(t, ctx, db)

// 	it := db.Iterator(ctx, corekv.IterOptions{
// 		Prefix:  bz(""),
// 		Reverse: true,
// 	})
// 	require.NotNil(t, it)

// 	iteratorVerify(t, it, [][2]string{
// 		{"something", "else"},
// 		{"key5", "value5"},
// 		{"key4", "value4"},
// 		{"key3", "value3"},
// 		{"key2", "value2"},
// 		{"key13", "value13"},
// 		{"key12", "value12"},
// 		{"key11", "value11"},
// 		{"key1", "value1"},
// 		{"key", "value"},
// 		{"kee", "valuu"},
// 		{"ke", "valu"},
// 		{"k", "val"},
// 	}, "prefix reverse full")
// 	require.NoError(t, it.Close(ctx))
// }

// func TestIteratorDefaultOpts(t *testing.T) {
// 	ctx := context.Background()
// 	db := NewDatastore(ctx)
// 	defer db.Close()

// 	loadPrefixData(t, ctx, db)

// 	it := db.Iterator(ctx, corekv.DefaultIterOptions)
// 	require.NotNil(t, it)

// 	iteratorVerify(t, it, [][2]string{
// 		{"k", "val"},
// 		{"ke", "valu"},
// 		{"kee", "valuu"},
// 		{"key", "value"},
// 		{"key1", "value1"},
// 		{"key11", "value11"},
// 		{"key12", "value12"},
// 		{"key13", "value13"},
// 		{"key2", "value2"},
// 		{"key3", "value3"},
// 		{"key4", "value4"},
// 		{"key5", "value5"},
// 		{"something", "else"},
// 	}, "default forward full")
// 	require.NoError(t, it.Close(ctx))
// }

// For testing convenience.
func bz(s string) []byte {
	return []byte(s)
}
