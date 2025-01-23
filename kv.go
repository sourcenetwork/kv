package corekv

import "context"

// DefaultIterOptions is exactly the default zero value
// for the IterOptions stuct. It is however recomended
// to use this if you want the "default" behavior, as
// these values may change in some way in the future
// and the old "default" as zero values may produce
// different behavior.
var DefaultIterOptions = IterOptions{}

// IterOptions contains the full set of available iterator options,
// it can be provided when creating an [Iterator] from a [Store].
type IterOptions struct {
	// Prefix iteration, only keys beginning with the designated prefix
	// with the given prefix will be yielded.
	//
	// Keys exactly matching the provided `Prefix` value will not be
	// yielded.
	//
	// Providing a Prefix value should cause the Start and End options
	// to be ignored, although this is currently untested:
	// https://github.com/sourcenetwork/corekv/issues/35
	Prefix []byte

	// If Prefix is nil, and Start is provided, the iterator will
	// only yield items with a key lexographically greater than or
	// equal to this value.
	//
	// Providing an `End` value equal to or smaller than this value
	// will result in undefined behaviour:
	// https://github.com/sourcenetwork/corekv/issues/32
	Start []byte

	// If Prefix is nil, and Start is provided, the iterator will
	// only yield items with a key lexographically smaller than this
	// value.
	//
	// Providing an End value equal to or smaller than Start
	// will result in undefined behaviour:
	// https://github.com/sourcenetwork/corekv/issues/32
	End []byte

	// Reverse the direction of the iteration, returning items in
	// lexographically descending order of their keys.
	Reverse bool

	// Only iterate through keys. Calling Value on the
	// iterator will return nil and no error.
	//
	// This option is currently untested:
	// https://github.com/sourcenetwork/corekv/issues/34
	//
	// It is very likely ignored for the memory store iteration:
	// https://github.com/sourcenetwork/corekv/issues/33
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
