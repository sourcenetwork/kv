package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/sourcenetwork/corekv"
)

type bDB struct {
	db *badger.DB
}

func NewDatastore(path string, opts badger.Options) (corekv.Store, error) {
	return newDatastore(path, opts)
}

func newDatastore(path string, opts badger.Options) (*bDB, error) {
	opts.Dir = path
	opts.ValueDir = path
	opts.Logger = nil // badger is too chatty
	store, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return newDatastoreFrom(store), nil
}

func NewDatastoreFrom(db *badger.DB) corekv.Store {
	return newDatastoreFrom(db)
}

func newDatastoreFrom(db *badger.DB) *bDB {
	return &bDB{
		db: db,
	}
}

func (b *bDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	txn := b.newTxn(true)
	defer txn.Discard(ctx)

	return txn.Get(ctx, key)
}

func (b *bDB) Has(ctx context.Context, key []byte) (bool, error) {
	txn := b.newTxn(true)
	defer txn.Discard(ctx)

	return txn.Has(ctx, key)
}

func (b *bDB) Set(ctx context.Context, key []byte, value []byte) error {
	txn := b.newTxn(false)
	defer txn.Commit(ctx)

	return txn.Set(ctx, key, value)
}

func (b *bDB) Delete(ctx context.Context, key []byte) error {
	txn := b.newTxn(false)
	defer txn.Commit(ctx)

	return txn.Delete(ctx, key)
}

func (b *bDB) Close() {
	b.db.Close()
}

func (b *bDB) Iterator(ctx context.Context, iterOpts corekv.IterOptions) corekv.Iterator {
	txn := b.newTxn(true)
	it := txn.iterator(ctx, iterOpts)

	// closer for discarding implicit txn
	// so that the txn is discarded when the
	// iterator is closed
	it.withCloser(func() {
		txn.Discard(ctx)
	})
	return it

}

func (b *bDB) NewTxn(readonly bool) corekv.Txn {
	return b.newTxn(readonly)
}

func (b *bDB) newTxn(readonly bool) *bTxn {
	return &bTxn{b.db.NewTransaction(!readonly)}
}

type bTxn struct {
	t *badger.Txn
}

func (txn *bTxn) Get(ctx context.Context, key []byte) ([]byte, error) {
	item, err := txn.t.Get(key)
	if err != nil {
		return nil, badgerErrToKVErr(err)
	}

	return item.ValueCopy(nil)
}

func (txn *bTxn) Has(ctx context.Context, key []byte) (bool, error) {
	_, err := txn.t.Get(key)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	case err == nil:
		return true, nil
	default:
		return false, badgerErrToKVErr(err)
	}
}

func (txn *bTxn) Iterator(ctx context.Context, iterOpts corekv.IterOptions) corekv.Iterator {
	return txn.iterator(ctx, iterOpts)
}

func (txn *bTxn) iterator(ctx context.Context, iopts corekv.IterOptions) iteratorCloser {
	if iopts.Prefix != nil {
		return txn.prefixIterator(ctx, iopts.Prefix, iopts.Reverse, iopts.KeysOnly)
	}
	return txn.rangeIterator(ctx, iopts.Start, iopts.End, iopts.Reverse, iopts.KeysOnly)
}

func (txn *bTxn) prefixIterator(ctx context.Context, prefix []byte, reverse, keysOnly bool) *prefixIterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = reverse
	opt.Prefix = prefix
	opt.PrefetchValues = !keysOnly

	it := txn.t.NewIterator(opt)
	fmt.Println("badger prefix key:", string(prefix))
	if opt.Reverse {
		fmt.Println("badger prefix key(modified):", string(bytesPrefixEnd(prefix)))
		it.Seek(bytesPrefixEnd(prefix))
		if !it.ValidForPrefix(prefix) {
			it.Next()
		}
	} else {
		it.Seek(prefix) // all badger iterators must start with a rewind

	}

	fmt.Println("badger first key:", string(it.Item().Key()))
	return &prefixIterator{
		i:        it,
		prefix:   prefix,
		reverse:  reverse,
		keysOnly: keysOnly,
	}
}

func (txn *bTxn) rangeIterator(ctx context.Context, start, end []byte, reverse, keysOnky bool) *rangeIterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = reverse
	opt.PrefetchValues = !keysOnky

	it := txn.t.NewIterator(opt)
	// all badger iterators must start with a seek/rewind. This is valid
	// even if start is nil.
	if !reverse {
		it.Seek(start)
	} else {
		it.Seek(end)
		// if we seeked to the end and its an exact match to the end marker
		// go next. This is because ranges are [start, end) (exlusive)
		if it.Valid() && equal(it.Item().Key(), end) {
			it.Next()
		}
	}

	return &rangeIterator{
		i:        it,
		start:    start,
		end:      end,
		reverse:  reverse,
		keysOnly: keysOnky,
	}
}

func (txn *bTxn) Set(ctx context.Context, key []byte, value []byte) error {
	err := txn.t.Set(key, value)
	return badgerErrToKVErr(err)
}

func (txn *bTxn) Delete(ctx context.Context, key []byte) error {
	err := txn.t.Delete(key)
	return badgerErrToKVErr(err)
}

func (txn *bTxn) Commit(ctx context.Context) error {
	err := txn.t.Commit()
	return badgerErrToKVErr(err)
}

func (txn *bTxn) Discard(ctx context.Context) error {
	txn.t.Discard()
	return nil
}

type iteratorCloser interface {
	corekv.Iterator
	withCloser(func())
}

type rangeIterator struct {
	i        *badger.Iterator
	start    []byte
	end      []byte
	reverse  bool
	keysOnly bool
	closer   func()
}

func (it *rangeIterator) Domain() (start []byte, end []byte) {
	return it.start, it.end
}

func (it *rangeIterator) Valid() bool {
	if !it.i.Valid() {
		return false
	}

	// if its reversed, we check if we passed the start key
	if it.reverse && it.start != nil {
		return bytes.Compare(it.i.Item().Key(), it.start) >= 0 // inclusive
	} else if !it.reverse && it.end != nil {
		fmt.Println(it.end)
		fmt.Println(it.i.Item().Key())
		fmt.Printf("!checking end: key %x end %x \n", it.i.Item().Key(), it.end)
		// if its forward, we check if we passed the end key
		cmp := bytes.Compare(it.i.Item().Key(), it.end)
		fmt.Printf("compare: %v\n", cmp)
		return bytes.Compare(it.i.Item().Key(), it.end) < 0 // exlusive
	}

	return true
}

func (it *rangeIterator) Next() {
	it.i.Next()
}

func (it *rangeIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *rangeIterator) Value() []byte {
	if it.keysOnly {
		return nil
	}

	// todo: error?
	val, _ := it.i.Item().ValueCopy(nil)
	return val
}

func (it *rangeIterator) Seek(target []byte) {
	it.i.Seek(target)
}

func (it *rangeIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		it.closer()
	}
	return nil
}

func (it *rangeIterator) withCloser(closer func()) {
	it.closer = closer
}

type prefixIterator struct {
	i        *badger.Iterator
	prefix   []byte
	reverse  bool
	keysOnly bool
	closer   func()
}

func (it *prefixIterator) Domain() (start []byte, end []byte) {
	return it.prefix, it.prefix
}

func (it *prefixIterator) Valid() bool {
	fmt.Println("badger prefix:", string(it.prefix))
	return it.i.ValidForPrefix(it.prefix)
}

func (it *prefixIterator) Next() {
	it.i.Next()
}

func (it *prefixIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *prefixIterator) Value() []byte {
	if it.keysOnly {
		return nil
	}
	val, _ := it.i.Item().ValueCopy(nil)
	// todo: error?
	return val
}

func (it *prefixIterator) Seek(target []byte) {
	it.i.Seek(target)
}

func (it *prefixIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		it.closer()
	}
	return nil
}

func (it *prefixIterator) withCloser(closer func()) {
	it.closer = closer
}

var badgerErrToKVErrMap = map[error]error{
	badger.ErrEmptyKey:     corekv.ErrEmptyKey,
	badger.ErrKeyNotFound:  corekv.ErrNotFound,
	badger.ErrDiscardedTxn: corekv.ErrDiscardedTxn,
}

func badgerErrToKVErr(err error) error {
	if err == nil {
		return nil
	}

	// first we try the optimistic lookup, assuming
	// there is no preexisting wrapping
	mappedErr := badgerErrToKVErrMap[err]

	// then we check the wrapped error chain
	if mappedErr == nil {
		switch {
		case errors.Is(err, badger.ErrEmptyKey):
			mappedErr = corekv.ErrEmptyKey
		case errors.Is(err, badger.ErrKeyNotFound):
			mappedErr = corekv.ErrNotFound
		default:
			return err // no mapping needed atm
		}
	}

	return errors.Join(mappedErr, err)

}

func equal(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}

func bytesPrefixEnd(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	// This statement will only be reached if the key is already a
	// maximal byte string (i.e. already \xff...).
	return b
}
