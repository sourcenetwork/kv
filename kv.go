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

// Reader contains read-only functions for interacting with a store.
type Reader interface {
	// Get returns the value at the given key.
	//
	// If no item with the given key is found, nil, and an [ErrNotFound]
	// error will be returned.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Has returns true if an item at the given key is found, otherwise
	// will return false.
	Has(ctx context.Context, key []byte) (bool, error)

	// Iterator returns a read-only iterator using the given options.
	Iterator(ctx context.Context, opts IterOptions) Iterator
}

// Writer contains functions for mutating stuff stored within a store.
type Writer interface {
	// Set sets the value stored against the given key.
	//
	// If an item already exists at the given key it will be overwritten.
	Set(ctx context.Context, key, value []byte) error

	// Delete removes the value at the given key.
	//
	// If no matching key is found the behaviour is undefined:
	// https://github.com/sourcenetwork/corekv/issues/36
	Delete(ctx context.Context, key []byte) error
}

// Iterator is a read-only iterator that allows iteration over the underlying
// store (or a part of it).
type Iterator interface {
	// The behaviour of Domain is undefined and varies heavily depending on
	// which store implementation is being iterated over:
	// https://github.com/sourcenetwork/corekv/issues/31
	Domain() (start []byte, end []byte)

	// Valid returns true if the iterator can return a valid item from
	// `Value`.
	//
	// It will return false in all other cases, for example if the store is
	// empty, or the iterator has passed the end of the range permitted by it's
	// initialization options.
	Valid() bool

	// Next moves the iterator forward, whether there is a valid item available or
	// not.
	Next()

	// Key returns the key at the current iterator location.
	//
	// If the iterator is currently at an invalid location it's behaviour is undefined:
	// https://github.com/sourcenetwork/corekv/issues/37
	Key() []byte

	// Value returns the value at the current iterator location.
	//
	// If the iterator is currently at an invalid location it's behaviour is undefined:
	// https://github.com/sourcenetwork/corekv/issues/37
	Value() ([]byte, error)

	// Seek moves the iterator to the given key, if and exact match is not found, the
	// iterator will progress to the next valid value (depending on the `Reverse` option).
	Seek([]byte)

	// Close releases the iterator.
	Close(ctx context.Context) error
}

// Store contains all the functions required for interacting with a store.
type Store interface {
	Reader
	Writer

	// Close disposes of any resources directly held by the store.
	//
	// WARNING: Some implmentations close transactions and iterators, others do not:
	// https://github.com/sourcenetwork/corekv/issues/39
	Close() error
}

// TxnStore represents a [Store] that supports transactions.
type TxnStore interface {
	Store

	// NewTxn returns a new transaction.
	NewTxn(readonly bool) Txn
}

// Txn hides changes made to the underlying store from this object,
// and hides changes made via this object from the underlying store
// until `Commit` is called.
type Txn interface {
	Reader
	Writer

	// Commit applies all changes made via this [Txn] to the underlying
	// [Store].
	Commit(ctx context.Context) error

	// Discard discards all changes made via this object so far, returning
	// it to the state it was at at time of construction.
	Discard(ctx context.Context) error
}
