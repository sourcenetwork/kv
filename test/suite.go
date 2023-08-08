package kvtest

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/sourcenetwork/corekv"
)

// BasicSubtests is a list of all basic tests.
var BasicSubtests = []func(t *testing.T, store corekv.Store){
	SubtestBasicPutGet,
	SubtestNotFounds,
	SubtestPrefix,
	SubtestLimit,
	SubtestManyKeysAndQuery,
}

// BatchSubtests is a list of all basic batching datastore tests.
// var BatchSubtests = []func(t *testing.T, batcher corekv.Batchable){
// 	RunBatchTest,
// 	RunBatchDeleteTest,
// 	RunBatchPutAndDeleteTest,
// }

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func clearDs(t *testing.T, store corekv.Store) {
	ctx := context.Background()

	it := store.Iterator(ctx, nil, nil)
	defer it.Close(ctx)
	res, err := all(it)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range res {
		if err := store.Delete(ctx, item.key); err != nil {
			t.Fatal(err)
		}
	}
}

// SubtestAll tests the given datastore against all the subtests.
func SubtestAll(t *testing.T, store corekv.Store) {
	for _, f := range BasicSubtests {
		t.Run(getFunctionName(f), func(t *testing.T) {
			f(t, store)
			clearDs(t, store)
		})
	}
	// if bs, ok := store.(corekv.Batchable); ok {
	// 	for _, f := range BatchSubtests {
	// 		t.Run(getFunctionName(f), func(t *testing.T) {
	// 			f(t, bs)
	// 			clearDs(t, store)
	// 		})
	// 	}
	// }
}

type item struct {
	key, value []byte
}

func all(it corekv.Iterator) ([]item, error) {
	res := make([]item, 0)
	fmt.Println("iterator valid:", it.Valid())
	for ; it.Valid(); it.Next() {
		fmt.Println("all - iterate")
		val, err := it.Value()
		if err != nil {
			return nil, err
		}
		res = append(res, item{it.Key(), val})
	}
	return res, nil
}
