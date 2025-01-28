package badger

import (
	"bytes"
	"context"
	"errors"
	"strings"

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

	result, err := txn.Get(ctx, key)
	dErr := txn.Discard(ctx)

	return result, errors.Join(err, dErr)
}

func (b *bDB) Has(ctx context.Context, key []byte) (bool, error) {
	txn := b.newTxn(true)

	result, err := txn.Has(ctx, key)
	dErr := txn.Discard(ctx)

	return result, errors.Join(err, dErr)
}

func (b *bDB) Set(ctx context.Context, key []byte, value []byte) error {
	txn := b.newTxn(false)

	err := txn.Set(ctx, key, value)
	if err != nil {
		dErr := txn.Discard(ctx)
		return errors.Join(err, dErr)
	}

	return txn.Commit(ctx)
}

func (b *bDB) Delete(ctx context.Context, key []byte) error {
	txn := b.newTxn(false)

	err := txn.Delete(ctx, key)
	if err != nil {
		dErr := txn.Discard(ctx)
		return errors.Join(err, dErr)
	}

	return txn.Commit(ctx)
}

func (b *bDB) Close() error {
	return b.db.Close()
}

func (b *bDB) Iterator(ctx context.Context, iterOpts corekv.IterOptions) corekv.Iterator {
	txn := b.newTxn(true)
	it := txn.iterator(iterOpts)

	// closer for discarding implicit txn
	// so that the txn is discarded when the
	// iterator is closed
	it.withCloser(func() error {
		return txn.Discard(ctx)
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
	return txn.iterator(iterOpts)
}

func (txn *bTxn) iterator(iopts corekv.IterOptions) iteratorCloser {
	if iopts.Prefix != nil {
		return txn.prefixIterator(iopts.Prefix, iopts.Reverse, iopts.KeysOnly)
	}
	return txn.rangeIterator(iopts.Start, iopts.End, iopts.Reverse, iopts.KeysOnly)
}

func (txn *bTxn) prefixIterator(prefix []byte, reverse, keysOnly bool) *prefixIterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = reverse
	opt.Prefix = prefix
	opt.PrefetchValues = !keysOnly

	return &prefixIterator{
		i:        txn.t.NewIterator(opt),
		prefix:   prefix,
		reverse:  reverse,
		keysOnly: keysOnly,
		reset:    true,
	}
}

func (txn *bTxn) rangeIterator(start, end []byte, reverse, keysOnky bool) *rangeIterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = reverse
	opt.PrefetchValues = !keysOnky

	return &rangeIterator{
		i:        txn.t.NewIterator(opt),
		start:    start,
		end:      end,
		reverse:  reverse,
		keysOnly: keysOnky,
		reset:    true,
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
	withCloser(func() error)
}

type rangeIterator struct {
	i        *badger.Iterator
	start    []byte
	end      []byte
	reverse  bool
	keysOnly bool
	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset  bool
	closer func() error
}

func (it *rangeIterator) Reset() {
	it.reset = true
}

// restart returns the iterator back to it's initial location at time of construction,
// allowing re-iteration of the underlying data.
func (it *rangeIterator) restart() (bool, error) {
	it.reset = false

	if it.reverse {
		hasValue, err := it.Seek(it.end)
		if !hasValue || err != nil {
			return false, err
		}

		if it.valid() && equal(it.i.Item().Key(), it.end) {
			return it.Next()
		}

		return true, nil
	} else {
		return it.Seek(it.start)
	}
}

func (it *rangeIterator) valid() bool {
	if !it.i.Valid() {
		return false
	}

	// if its reversed, we check if we passed the start key
	if it.reverse && it.start != nil {
		return bytes.Compare(it.i.Item().Key(), it.start) >= 0 // inclusive
	} else if !it.reverse && it.end != nil {
		// if its forward, we check if we passed the end key
		return bytes.Compare(it.i.Item().Key(), it.end) < 0 // exlusive
	}

	return true
}

func (it *rangeIterator) Next() (bool, error) {
	if it.reset {
		return it.restart()
	}

	it.i.Next()
	return it.valid(), nil
}

func (it *rangeIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *rangeIterator) Value() ([]byte, error) {
	if it.keysOnly {
		return nil, nil
	}

	return it.i.Item().ValueCopy(nil)
}

func (it *rangeIterator) Seek(target []byte) (bool, error) {
	// Clear the reset property, else if Next was call following Seek,
	// Next may incorrectly return to the beginning.
	it.reset = false

	it.i.Seek(target)
	return it.valid(), nil
}

func (it *rangeIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		return it.closer()
	}
	return nil
}

func (it *rangeIterator) withCloser(closer func() error) {
	it.closer = closer
}

type prefixIterator struct {
	i        *badger.Iterator
	prefix   []byte
	reverse  bool
	keysOnly bool
	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset  bool
	closer func() error
}

func (it *prefixIterator) Reset() {
	it.reset = true
}

// restart returns the iterator back to it's initial location at time of construction,
// allowing re-iteration of the underlying data.
func (it *prefixIterator) restart() (bool, error) {
	it.reset = false

	if it.reverse {
		hasItem, err := it.Seek(bytesPrefixEnd(it.prefix))
		if !hasItem || err != nil {
			return false, err
		}

		if !it.i.ValidForPrefix(it.prefix) {
			return it.Next()
		}

		return true, nil
	} else {
		return it.Seek(it.prefix)
	}
}

func (it *prefixIterator) Next() (bool, error) {
	if it.reset {
		return it.restart()
	}

	it.i.Next()
	return it.i.ValidForPrefix(it.prefix), nil
}

func (it *prefixIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *prefixIterator) Value() ([]byte, error) {
	if it.keysOnly {
		return nil, nil
	}
	return it.i.Item().ValueCopy(nil)
}

func (it *prefixIterator) Seek(target []byte) (bool, error) {
	// Clear the reset property, else if Next was call following Seek,
	// Next may incorrectly return to the beginning.
	it.reset = false

	it.i.Seek(target)
	return it.i.ValidForPrefix(it.prefix), nil
}

func (it *prefixIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		return it.closer()
	}
	return nil
}

func (it *prefixIterator) withCloser(closer func() error) {
	it.closer = closer
}

var badgerErrToKVErrMap = map[error]error{
	badger.ErrEmptyKey:     corekv.ErrEmptyKey,
	badger.ErrKeyNotFound:  corekv.ErrNotFound,
	badger.ErrDiscardedTxn: corekv.ErrDiscardedTxn,
	badger.ErrDBClosed:     corekv.ErrDBClosed,
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
		case errors.Is(err, badger.ErrDBClosed):
			mappedErr = corekv.ErrDBClosed
		// Annoyingly, badger's error wrapping seems to break `errors.Is`, so we have to
		// check the string
		case strings.Contains(err.Error(), badger.ErrDBClosed.Error()):
			mappedErr = corekv.ErrDBClosed
		default:
			return err // no mapping needed atm
		}
	}

	return errors.Join(mappedErr, err)

}

func equal(a, b []byte) bool {
	return bytes.Equal(a, b)
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
