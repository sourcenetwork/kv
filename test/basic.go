package kvtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	detectrace "github.com/ipfs/go-detect-race"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/corekv"
)

// ElemCount sets with how many elements the datastore suit
// tests are usually run with. Best to set to round numbers like
// 20, 30, 40... and at least to 20.
var ElemCount = 100

func init() {
	// Reduce the default element count when the race detector is enabled so these tests don't
	// take forever.
	if detectrace.WithRace() {
		ElemCount = 20
	}
}

func TestElemCount(t *testing.T) {
	if ElemCount < 20 {
		t.Fatal("ElemCount should be set to 20 at least")
	}
}

func SubtestBasicPutGet(t *testing.T, store corekv.Store) {
	ctx := context.Background()

	k := []byte("foo")
	val := []byte("Hello Datastore!")

	err := store.Set(ctx, k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := store.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	out, err := store.Get(ctx, k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	require.Equal(t, val, out)

	have, err = store.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	err = store.Delete(ctx, k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = store.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}
}

func SubtestNotFounds(t *testing.T, store corekv.Store) {
	ctx := context.Background()

	badk := []byte("notreal")

	val, err := store.Get(ctx, badk)
	if !errors.Is(err, corekv.ErrNotFound) {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := store.Has(ctx, badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}

	err = store.Delete(ctx, badk)
	if err != nil {
		t.Fatal("error calling delete on not found key: ", err)
	}
}

func SubtestLimit(t *testing.T, store corekv.Store) {
	test := func(offset, limit int) {
		t.Run(fmt.Sprintf("Slice/%d/%d", offset, limit), func(t *testing.T) {
			subtestIterator(t, store, nil, ElemCount)
		})
	}
	test(0, ElemCount/10)
	test(0, 0)
	test(ElemCount/10, 0)
	test(ElemCount/10, ElemCount/10)
	test(ElemCount/10, ElemCount/5)
	test(ElemCount/2, ElemCount/5)
	test(ElemCount-1, ElemCount/5)
	test(ElemCount*2, ElemCount/5)
	test(ElemCount*2, 0)
	test(ElemCount-1, 0)
	test(ElemCount-5, 0)
}

func SubtestManyKeysAndQuery(t *testing.T, store corekv.Store) {
	subtestIterator(t, store, nil, ElemCount)
}

func SubtestPrefix(t *testing.T, store corekv.Store) {
	test := func(prefix string) {
		t.Run(prefix, func(t *testing.T) {
			subtestIterator(t, store, nil, ElemCount)
		})
	}
	// test("")
	test("/")
	// test("/./")
	// test("/.././/")
	// test("/prefix/../")

	// test("/prefix")
	// test("/prefix/")
	// test("/prefix/sub/")

	// test("/0/")
	// test("/bad/")
}

func randValue() []byte {
	value := make([]byte, 64)
	rand.Read(value)
	return value
}

func subtestIterator(t *testing.T, store corekv.Store, prefix []byte, count int) {
	ctx := context.Background()

	var input []item
	// for i := 0; i < count; i++ {
	// 	s := fmt.Sprintf("%dkey%d", i, i)
	// 	key := []byte(s)
	// 	value := randValue()
	// 	input = append(input, item{
	// 		key:   key,
	// 		value: value,
	// 	})
	// }

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("/capital/%dKEY%d", i, i)
		key := []byte(s)
		value := randValue()
		input = append(input, item{
			key:   key,
			value: value,
		})
	}

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("/prefix/%dkey%d", i, i)
		key := []byte(s)
		value := randValue()
		input = append(input, item{
			key:   key,
			value: value,
		})
	}

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("/prefix/sub/%dkey%d", i, i)
		key := []byte(s)
		value := randValue()
		input = append(input, item{
			key:   key,
			value: value,
		})
	}

	t.Logf("putting %d values", len(input))
	for i, e := range input {
		err := store.Set(ctx, e.key, e.value)
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, e := range input {
		val, err := store.Get(ctx, e.key)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		if !bytes.Equal(val, e.value) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}

	t.Log("querying values")
	expected := input
	sort.Slice(expected, func(i, j int) bool {
		return bytes.Compare(expected[i].key, expected[j].key) < 0
	})

	it := store.Iterator(ctx, prefix, nil)
	defer func() {
		it.Close(ctx)
	}()

	t.Log("aggregating query results")
	actual, err := all(it)
	if err != nil {
		t.Fatal("query result error: ", err)
	}

	t.Log("verifying query output")
	if len(actual) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(actual))
	}
	for i := range actual {
		if !bytes.Equal(actual[i].key, expected[i].key) {
			t.Errorf("for result %d, expected key %q, got %q", i, expected[i].key, actual[i].key)
			continue
		}
		if !bytes.Equal(actual[i].value, expected[i].value) {
			t.Errorf("value mismatch for result %d (key=%q)", i, expected[i].key)
		}
	}

	t.Log("deleting all keys")
	for _, e := range input {
		if err := store.Delete(ctx, []byte(e.key)); err != nil {
			t.Fatal(err)
		}
	}
}
