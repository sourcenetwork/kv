package corekv

import "context"

type Read interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Has(ctx context.Context, key []byte) (bool, error)
	Iterator(ctx context.Context, start, end []byte) Iterator
}

type Write interface {
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

type Iterator interface {
	Domain() (start []byte, end []byte)
	Valid() bool
	Next()
	Key() []byte
	Value() ([]byte, error)
	Close(ctx context.Context) error
}

type Store interface {
	Read
	Write
	Close()
}

type Batchable interface {
	NewBatch() Batch
}

type Batch interface {
	Write
}

type TxnStore interface {
	NewTxn(readonly bool) Txn
}

type Txn interface {
	Read
	Write
	Commit(ctx context.Context) error
	Discard(ctx context.Context) error
}
