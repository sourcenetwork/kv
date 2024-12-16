package namespace

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/corekv"
	badgerkv "github.com/sourcenetwork/corekv/badger"
	testkv "github.com/sourcenetwork/corekv/test"
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

	// itr := db.Iterator(ctx, corekv.DefaultIterOptions)

	// for ; itr.Valid(); itr.Next() {
	// 	fmt.Println("namespace.iter.Key", string(itr.Key()))
	// 	fmt.Println("namespace.iter.Value", string(itr.Value()))
	// }

	return db, done
}

func TestPrefixDBGetAllKeys(t *testing.T) {
	ctx := context.Background()
	db, done := mockDBWithStuff(t, ctx)
	pdb := Wrap(db, bz("key"))

	testkv.CheckValue(t, ctx, pdb, bz("key"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("key1"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("1"), bz("value1"))
	testkv.CheckValue(t, ctx, pdb, bz("11"), bz("value11"))
	testkv.CheckValue(t, ctx, pdb, bz("12"), bz("value12"))
	testkv.CheckValue(t, ctx, pdb, bz("13"), bz("value13"))
	testkv.CheckValue(t, ctx, pdb, bz("key2"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("2"), bz("value2"))
	testkv.CheckValue(t, ctx, pdb, bz("key3"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("3"), bz("value3"))
	testkv.CheckValue(t, ctx, pdb, bz("key4"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("4"), bz("value4"))
	testkv.CheckValue(t, ctx, pdb, bz("key5"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("5"), bz("value5"))
	testkv.CheckValue(t, ctx, pdb, bz("something"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("k"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("ke"), nil)
	testkv.CheckValue(t, ctx, pdb, bz("kee"), nil)

	done()
}

func TestSuite(t *testing.T) {
	ctx := context.Background()
	db, done := newDS(t, ctx)
	defer done()
	pdb := Wrap(db, bz("/test"))

	testkv.SubtestAll(t, pdb)
}

func TestNamespaceDBIteratorForwardFull(t *testing.T) {
	opts := corekv.IterOptions{}
	runIteratorTest(t, opts, [][2]string{
		{"1", "value1"},
		{"11", "value11"},
		{"12", "value12"},
		{"13", "value13"},
		{"2", "value2"},
		{"3", "value3"},
		{"4", "value4"},
		{"5", "value5"},
	}, "forward full range")
}

func TestNamespaceDBIteratorForwardStartAt1(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("1"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"1", "value1"},
		{"11", "value11"},
		{"12", "value12"},
		{"13", "value13"},
		{"2", "value2"},
		{"3", "value3"},
		{"4", "value4"},
		{"5", "value5"},
	}, "forward range from 1")
}

func TestNamespaceDBIteratorForwardStartAt2(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("2"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"2", "value2"},
		{"3", "value3"},
		{"4", "value4"},
		{"5", "value5"},
	}, "forward range from 2")
}

func TestNamespaceDBIteratorForwardEndAt5(t *testing.T) {
	opts := corekv.IterOptions{
		End: bz("5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"1", "value1"},
		{"11", "value11"},
		{"12", "value12"},
		{"13", "value13"},
		{"2", "value2"},
		{"3", "value3"},
		{"4", "value4"},
	}, "forward range end 5")
}

func TestNamespaceDBIteratorForwardEndAt4(t *testing.T) {
	opts := corekv.IterOptions{
		End: bz("4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"1", "value1"},
		{"11", "value11"},
		{"12", "value12"},
		{"13", "value13"},
		{"2", "value2"},
		{"3", "value3"},
	}, "forward range end 4")
}

func TestNamespaceDBIteratorForwardStart1End5(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("1"),
		End:   bz("5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"1", "value1"},
		{"11", "value11"},
		{"12", "value12"},
		{"13", "value13"},
		{"2", "value2"},
		{"3", "value3"},
		{"4", "value4"},
	}, "forward range [1, 5)")
}

func TestNamespaceDBIteratorForwardStart2End4(t *testing.T) {
	opts := corekv.IterOptions{
		Start: bz("2"),
		End:   bz("4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"2", "value2"},
		{"3", "value3"},
	}, "forward range [2, 4)")
}

func TestNamespaceDBIteratorReverseFull(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
	}
	runIteratorTest(t, opts, [][2]string{
		{"5", "value5"},
		{"4", "value4"},
		{"3", "value3"},
		{"2", "value2"},
		{"13", "value13"},
		{"12", "value12"},
		{"11", "value11"},
		{"1", "value1"},
	}, "reverse full range")
}

func TestNamespaceDBIteratorReverseStartAt1(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("1"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"5", "value5"},
		{"4", "value4"},
		{"3", "value3"},
		{"2", "value2"},
		{"13", "value13"},
		{"12", "value12"},
		{"11", "value11"},
		{"1", "value1"},
	}, "reverse range start at 1")
}

func TestNamespaceDBIteratorReverseStartAt2(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("2"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"5", "value5"},
		{"4", "value4"},
		{"3", "value3"},
		{"2", "value2"},
	}, "reverse range start at 2")
}

func TestNamespaceDBIteratorReverseEndAt5(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		End:     bz("5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"4", "value4"},
		{"3", "value3"},
		{"2", "value2"},
		{"13", "value13"},
		{"12", "value12"},
		{"11", "value11"},
		{"1", "value1"},
	}, "reverse range end at 5")
}

func TestNamespaceDBIteratorReverseEndAt4(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		End:     bz("4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"3", "value3"},
		{"2", "value2"},
		{"13", "value13"},
		{"12", "value12"},
		{"11", "value11"},
		{"1", "value1"},
	}, "reverse range end at 4")
}

func TestNamespaceDBIteratorReverseStart1End5(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("1"),
		End:     bz("5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"4", "value4"},
		{"3", "value3"},
		{"2", "value2"},
		{"13", "value13"},
		{"12", "value12"},
		{"11", "value11"},
		{"1", "value1"},
	}, "reverse range [1, 5)")
}

func TestNamespaceDBIteratorReverseStart2End4(t *testing.T) {
	opts := corekv.IterOptions{
		Reverse: true,
		Start:   bz("2"),
		End:     bz("4"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"3", "value3"},
		{"2", "value2"},
	}, "reverse range [2, 4)")
}

func TestNamespaceDBIteratorForwardPrefixBeginning(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix: bz("1"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"1", "value1"},
		{"11", "value11"},
		{"12", "value12"},
		{"13", "value13"},
	}, "forward prefix 1")
}

func TestNamespaceDBIteratorForwardPrefixMiddle(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix: bz("2"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"2", "value2"},
	}, "forward prefix 2")
}

func TestNamespaceDBIteratorForwardPrefixEnd(t *testing.T) {
	opts := corekv.IterOptions{
		Prefix: bz("5"),
	}
	runIteratorTest(t, opts, [][2]string{
		{"5", "value5"},
	}, "forward prefix 5")
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

// func TestNamespaceDBIteratorFull(t *testing.T) {
// 	opts := corekv.IterOptions{}
// 	runIteratorTest(t, opts, [][2]string{
// 		{"1", "value1"},
// 		{"11", "value11"},
// 		{"12", "value12"},
// 		{"13", "value13"},
// 		{"2", "value2"},
// 		{"3", "value3"},
// 		{"4", "value4"},
// 		{"5", "value5"},
// 	}, "forward full range")
// }

// func TestNamespaceDBIteratorFull(t *testing.T) {
// 	opts := corekv.IterOptions{}
// 	runIteratorTest(t, opts, [][2]string{
// 		{"1", "value1"},
// 		{"11", "value11"},
// 		{"12", "value12"},
// 		{"13", "value13"},
// 		{"2", "value2"},
// 		{"3", "value3"},
// 		{"4", "value4"},
// 		{"5", "value5"},
// 	}, "forward full range")
// }

// func TestNamespaceDBIteratorFull(t *testing.T) {
// 	opts := corekv.IterOptions{}
// 	runIteratorTest(t, opts, [][2]string{
// 		{"1", "value1"},
// 		{"11", "value11"},
// 		{"12", "value12"},
// 		{"13", "value13"},
// 		{"2", "value2"},
// 		{"3", "value3"},
// 		{"4", "value4"},
// 		{"5", "value5"},
// 	}, "forward full range")
// }

// func TestPrefixDBIterator1(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))
// 	opts := corekv.DefaultIterOptions
// 	itr := pdb.Iterator(ctx, opts)
// 	testkv.CheckDomain(t, itr, nil, nil)
// 	testkv.CheckValid(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("4"), bz("value4"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("5"), bz("value5"))
// 	testkv.CheckNext(t, itr, false)

// 	fmt.Println("checking invalid...")
// 	testkv.CheckInvalid(t, itr)
// 	itr.Close(ctx)
// }

// // func TestPrefixDBIterator1(t *testing.T) {
// // 	ctx := context.Background()
// // 	db := mockDBWithStuff(t, ctx)
// // 	pdb := Wrap(db, bz("key"))

// // 	itr := pdb.Iterator(ctx, corekv.DefaultIterOptions)
// // 	testkv.CheckDomain(t, itr, nil, nil)
// // 	testkv.CheckValid(t, itr, true)
// // 	testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// // 	testkv.CheckNext(t, itr, true)
// // 	testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// // 	testkv.CheckNext(t, itr, true)
// // 	testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// // 	testkv.CheckNext(t, itr, true)
// // 	testkv.CheckItem(t, itr, bz("4"), bz("value4"))
// // 	testkv.CheckNext(t, itr, true)
// // 	testkv.CheckItem(t, itr, bz("5"), bz("value5"))
// // 	testkv.CheckNext(t, itr, false)

// // 	fmt.Println("checking invalid...")
// // 	testkv.CheckInvalid(t, itr)
// // 	itr.Close(ctx)
// // }

// func TestPrefixDBReverseIterator1(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))

// 	opt := corekv.DefaultIterOptions
// 	opt.Reverse = true
// 	itr := pdb.Iterator(ctx, opt)

// 	testkv.CheckDomain(t, itr, nil, nil)
// 	testkv.CheckItem(t, itr, bz("5"), bz("value5"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("4"), bz("value4"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// 	testkv.CheckNext(t, itr, false)
// 	testkv.CheckInvalid(t, itr)
// 	itr.Close(ctx)
// }

// func TestPrefixDBReverseIterator5(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))

// 	itr := pdb.Iterator(ctx, corekv.IterOptions{
// 		Start:   bz("1"),
// 		Reverse: true,
// 	})

// 	// for ; itr.Valid(); itr.Next() {
// 	// 	fmt.Println("namespace.iter.Key", string(itr.Key()))
// 	// 	fmt.Println("namespace.iter.Value", string(itr.Value()))
// 	// }

// 	// t.Fail()

// 	testkv.CheckValid(t, itr, true)
// 	testkv.CheckDomain(t, itr, bz("1"), nil)
// 	testkv.CheckItem(t, itr, bz("5"), bz("value5"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("4"), bz("value4"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// 	testkv.CheckNext(t, itr, false)
// 	testkv.CheckInvalid(t, itr)
// 	itr.Close(ctx)
// }

// func TestPrefixDBReverseIterator6(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))

// 	itr := pdb.Iterator(ctx, corekv.IterOptions{
// 		Start:   bz("2"),
// 		Reverse: true,
// 	})
// 	testkv.CheckDomain(t, itr, bz("2"), nil)
// 	testkv.CheckItem(t, itr, bz("5"), bz("value5"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("4"), bz("value4"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// 	testkv.CheckNext(t, itr, false)
// 	testkv.CheckInvalid(t, itr)
// 	itr.Close(ctx)
// }

// func TestPrefixDBReverseIterator7(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))

// 	itr := pdb.Iterator(ctx, corekv.IterOptions{
// 		End:     bz("2"),
// 		Reverse: true,
// 	})
// 	testkv.CheckDomain(t, itr, nil, bz("2"))
// 	testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// 	testkv.CheckNext(t, itr, false)
// 	testkv.CheckInvalid(t, itr)
// 	itr.Close(ctx)
// }

// func TestPrefixDBReverseIterator8(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))

// 	itr := pdb.Iterator(ctx, corekv.IterOptions{
// 		Start:   bz("1"),
// 		End:     bz("4"),
// 		Reverse: true,
// 	})
// 	testkv.CheckDomain(t, itr, bz("1"), bz("4"))
// 	testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// 	testkv.CheckNext(t, itr, true)
// 	testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// 	testkv.CheckNext(t, itr, false)
// 	testkv.CheckInvalid(t, itr)
// 	itr.Close(ctx)
// }

// func TestPrefixDBIterator9(t *testing.T) {
// 	ctx := context.Background()
// 	db := mockDBWithStuff(t, ctx)
// 	pdb := Wrap(db, bz("key"))

// 	itr := pdb.Iterator(ctx, corekv.IterOptions{
// 		Start: bz("1"),
// 		End:   bz("4"),
// 	})
// 	// testkv.CheckDomain(t, itr, bz("1"), bz("4"))
// 	// testkv.CheckItem(t, itr, bz("1"), bz("value1"))
// 	// testkv.CheckNext(t, itr, true)
// 	// testkv.CheckItem(t, itr, bz("2"), bz("value2"))
// 	// testkv.CheckNext(t, itr, true)
// 	// testkv.CheckItem(t, itr, bz("3"), bz("value3"))
// 	// testkv.CheckNext(t, itr, false)
// 	// testkv.CheckInvalid(t, itr)

// 	iteratorVerify(t, itr, [][2]string{
// 		{"1", "value1"},
// 		{"2", "value2"},
// 		{"3", "value3"},
// 	}, "forward range from [1, 4)")

// 	itr.Close(ctx)
// }

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
		value := string(itr.Value())
		entries = append(entries, [2]string{key, value})
		itr.Next()
	}
	require.Equal(t, expected, entries, msg)
}

// For testing convenience.
func bz(s string) []byte {
	return []byte(s)
}
