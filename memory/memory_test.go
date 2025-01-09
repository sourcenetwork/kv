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

// For testing convenience.
func bz(s string) []byte {
	return []byte(s)
}
