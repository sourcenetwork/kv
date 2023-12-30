package kvtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/corekv"
)

//----------------------------------------
// Helper functions.

func CheckValue(t *testing.T, ctx context.Context, db corekv.Store, key []byte, valueWanted []byte) {
	valueGot, _ := db.Get(ctx, key)
	// assert.NoError(t, err)
	assert.Equal(t, valueWanted, valueGot)
}

func CheckValid(t *testing.T, itr corekv.Iterator, expected bool) {
	valid := itr.Valid()
	require.Equal(t, expected, valid)
}

func CheckNext(t *testing.T, itr corekv.Iterator, expected bool) {
	itr.Next()
	// assert.NoError(t, err) TODO: look at fixing this
	valid := itr.Valid()
	require.Equal(t, expected, valid)
}

func CheckNextPanics(t *testing.T, itr corekv.Iterator) {
	assert.Panics(t, func() { itr.Next() }, "checkNextPanics expected an error but didn't")
}

func CheckDomain(t *testing.T, itr corekv.Iterator, start, end []byte) {
	fmt.Println("iter:", itr)
	s, e := itr.Domain()
	fmt.Println("DOMAIN", string(s), string(e))
	assert.Equal(t, start, s, "checkDomain domain start incorrect")
	assert.Equal(t, end, e, "checkDomain domain end incorrect")
}

func CheckItem(t *testing.T, itr corekv.Iterator, key []byte, value []byte) {
	v := itr.Value()
	k := itr.Key()

	fmt.Println("key:", string(k))
	assert.Exactly(t, key, k)
	assert.Exactly(t, value, v)
}

func CheckInvalid(t *testing.T, itr corekv.Iterator) {
	CheckValid(t, itr, false)
	// CheckKeyPanics(t, itr)
	// CheckValuePanics(t, itr)
	// CheckNextPanics(t, itr)
}

func CheckKeyPanics(t *testing.T, itr corekv.Iterator) {
	assert.Panics(t, func() { itr.Key() }, "checkKeyPanics expected panic but didn't")
}

func CheckValuePanics(t *testing.T, itr corekv.Iterator) {
	assert.Panics(t, func() { itr.Value() })
}
