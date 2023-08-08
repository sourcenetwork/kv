package badger

import (
	"context"
	"errors"

	"github.com/davecgh/go-spew/spew"
	badger "github.com/dgraph-io/badger/v4"

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
	store, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &bDB{
		db: store,
	}, nil
}

func NewDatastoreFrom(db *badger.DB) corekv.Store {
	return &bDB{
		db: db,
	}
}

func (b *bDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	txn := b.newTxn(true)
	defer txn.Discard(ctx)

	val, err := txn.Get(ctx, key)
	if err != nil && errors.Is(err, badger.ErrKeyNotFound) {
		return nil, errors.Join(corekv.ErrNotFound, err)
	}
	return val, nil
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

func (b *bDB) Iterator(ctx context.Context, start []byte, end []byte) corekv.Iterator {
	txn := b.newTxn(true)
	it := txn.iterator(ctx, start, end)

	// closer for discarding implicit txn
	it.closer = func() {
		txn.Discard(ctx)
	}
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
		return nil, err
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
		return false, err
	}
}

func (txn *bTxn) Iterator(ctx context.Context, start []byte, end []byte) corekv.Iterator {
	return txn.iterator(ctx, start, end)
}

func (txn *bTxn) iterator(ctx context.Context, start []byte, end []byte) *bIterator {
	opt := badger.DefaultIteratorOptions
	if start != nil {
		opt.Prefix = start
	}
	spew.Dump(opt)
	it := txn.t.NewIterator(opt)
	it.Rewind() // all badger iterators must start with a rewind
	return &bIterator{
		i:     it,
		start: start,
		end:   end,
	}
}

func (txn *bTxn) Set(ctx context.Context, key []byte, value []byte) error {
	return txn.t.Set(key, value)
}

func (txn *bTxn) Delete(ctx context.Context, key []byte) error {
	return txn.t.Delete(key)
}

func (txn *bTxn) Commit(ctx context.Context) error {
	return txn.t.Commit()
}

func (txn *bTxn) Discard(ctx context.Context) error {
	txn.t.Discard()
	return nil
}

type bIterator struct {
	i      *badger.Iterator
	start  []byte
	end    []byte
	closer func()
}

func (it *bIterator) Domain() (start []byte, end []byte) {
	return it.start, it.end
}

func (it *bIterator) Valid() bool {
	return it.i.Valid()
}

func (it *bIterator) Next() {
	it.i.Next()
}

func (it *bIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *bIterator) Value() ([]byte, error) {
	return it.i.Item().ValueCopy(nil)
}

func (it *bIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		it.closer()
	}
	return nil
}
