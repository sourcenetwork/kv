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

	it := txn.t.NewIterator(opt)
	if opt.Reverse {
		it.Seek(bytesPrefixEnd(prefix))
		if !it.ValidForPrefix(prefix) {
			it.Next()
		}
	} else {
		it.Seek(prefix) // all badger iterators must start with a rewind

	}

	return &prefixIterator{
		i:        it,
		prefix:   prefix,
		reverse:  reverse,
		keysOnly: keysOnly,
	}
}

func (txn *bTxn) rangeIterator(start, end []byte, reverse, keysOnky bool) *rangeIterator {
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
	withCloser(func() error)
}

type rangeIterator struct {
	i        *badger.Iterator
	start    []byte
	end      []byte
	reverse  bool
	keysOnly bool
	closer   func() error
}

func (it *rangeIterator) Valid() bool {
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

func (it *rangeIterator) Next() {
	it.i.Next()
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

func (it *rangeIterator) Seek(target []byte) {
	it.i.Seek(target)
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
	closer   func() error
}

func (it *prefixIterator) Valid() bool {
	return it.i.ValidForPrefix(it.prefix)
}

func (it *prefixIterator) Next() {
	it.i.Next()
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

func (it *prefixIterator) Seek(target []byte) {
	it.i.Seek(target)
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
