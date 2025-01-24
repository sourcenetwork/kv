package corekv

import "context"

// DefaultIterOptions is exactly the default zero value
// for the IterOptions stuct. It is however recomended
// to use this if you want the "default" behavior, as
// these values may change in some way in the future
// and the old "default" as zero values may produce
// different behavior.
var DefaultIterOptions = IterOptions{}

type IterOptions struct {
	// Prefix iteration, all keys *must* contain
	// the designated prefix. Prefix takes precendent
	// over range (start/end). If Prefix is nil, range
	// will be used (regardless if start/end is nil)
	Prefix []byte

	// Range iteration from start (inclusive) to end explusive.
	// Start must be lexigraphically less
	// then end, unless `nil` is used.
	//
	// Start defined as nil will iterate from the first
	// key in the store.
	// End defined as nil will iterate till the last key
	// in the store.
	Start []byte
	End   []byte

	// Reverse the direction of the iteration.
	Reverse bool

	// Only iterate through keys. Calling Value() on the
	// iterator *must* return nil (no error).
	KeysOnly bool
}

type Reader interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Has(ctx context.Context, key []byte) (bool, error)
	Iterator(ctx context.Context, opts IterOptions) Iterator
}

type Writer interface {
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

type Iterator interface {
	Domain() (start []byte, end []byte)
	Valid() bool
	Next()
	Key() []byte
	Value() ([]byte, error)
	Seek([]byte)
	Close(ctx context.Context) error
}

type Store interface {
	Reader
	Writer
	Close() error
}

type TxnStore interface {
	Store
	NewTxn(readonly bool) Txn
}

type Txn interface {
	Reader
	Writer
	Commit(ctx context.Context) error
	Discard(ctx context.Context) error
}
