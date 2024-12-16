package namespace

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/sourcenetwork/corekv"
)

// namespaceStore wraps a namespace of another database as a logical database.
type namespaceStore struct {
	mu        sync.RWMutex
	namespace []byte
	store     corekv.Store
}

var _ corekv.Store = (*namespaceStore)(nil)

// Wrap lets you namespace a store with a given prefix.
// The generic parameter allows you to wrap any kind of
// underlying store ( corekv.TxnStore, Batchable, etc) and keep
// the concrete type, just wrapped.
func Wrap(store corekv.Store, prefix []byte) corekv.Store {
	return &namespaceStore{
		namespace: prefix,
		store:     store,
	}
}

func (nstore *namespaceStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, corekv.ErrEmptyKey
	}

	pkey := nstore.prefixed(key)
	value, err := nstore.store.Get(ctx, pkey)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (nstore *namespaceStore) Has(ctx context.Context, key []byte) (bool, error) {
	if len(key) == 0 {
		return false, corekv.ErrEmptyKey
	}
	pkey := nstore.prefixed(key)

	has, err := nstore.store.Has(ctx, pkey)
	if err != nil {
		return false, err
	}
	return has, nil
}

func (nstore *namespaceStore) Set(ctx context.Context, key []byte, value []byte) error {
	if len(key) == 0 {
		return corekv.ErrEmptyKey
	}
	pkey := nstore.prefixed(key)

	return nstore.store.Set(ctx, pkey, value)
}

func (nstore *namespaceStore) Delete(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return corekv.ErrEmptyKey
	}
	pkey := nstore.prefixed(key)

	return nstore.store.Delete(ctx, pkey)
}

func (nstore *namespaceStore) Close() {
	nstore.store.Close()
}

func (nstore *namespaceStore) prefixed(key []byte) []byte {
	nstore.mu.RLock()
	defer nstore.mu.RUnlock()
	return prefixed(nstore.namespace, key)
}

// unsafePrefixed is the same as prefixed() but without locks, so the caller
// *MUST* obtain locks themselves
func (nstore *namespaceStore) unsafePrefixed(key []byte) []byte {
	return prefixed(nstore.namespace, key)
}

func prefixed(prefix, key []byte) []byte {
	return append(cp(prefix), key...)
}

func strip(prefix, key []byte) []byte {
	if len(key) >= len(prefix) {
		return key[len(prefix):]
	}
	return key
}

// Iterator creates a new iterator instance
func (nstore *namespaceStore) Iterator(ctx context.Context, opts corekv.IterOptions) corekv.Iterator {
	nstore.mu.RLock()
	defer nstore.mu.RUnlock()

	// make a copy so future changes to the namespaceStore
	// don't affect the iterator, since we don't use the same
	// set of locks between the store and iterator
	namespace := cp(nstore.namespace)

	var hasStart bool
	var hasEnd bool
	var hasPrefix bool
	// we can use unsafe here since we already aquired locks
	// either prefix (priority) or start/end
	if opts.Prefix != nil {
		opts.Prefix = nstore.unsafePrefixed(opts.Prefix)
		hasPrefix = true
	} else { // note: this shouldnt be an "else if" if you're curious ;)
		if opts.Start != nil {
			opts.Start = nstore.unsafePrefixed(opts.Start)
			hasStart = true
		}
		if opts.Prefix == nil && opts.End != nil {
			opts.End = nstore.unsafePrefixed(opts.End)
			hasEnd = true
		}
	}

	source := nstore.store.Iterator(ctx, opts)
	fmt.Println("opts reverse:", opts.Reverse)
	fmt.Println("opts prefix:", string(opts.Prefix))
	fmt.Println("opts start:", string(opts.Start))
	fmt.Println("opts end:", string(opts.End))

	/* TODO START - CLEAN UP BRANCHING */
	// if start/end aren't defined, we need to seek to the correct
	// starting point (direction depending)
	if !hasEnd && !hasStart && !hasPrefix && !opts.Reverse { // without reverse
		fmt.Println("initial seeking forward")
		source.Seek(namespace)
	} else if !hasEnd && !hasStart && !hasPrefix && opts.Reverse { // with reverse
		fmt.Println("initial seeking reverse")
		source.Seek(bytesPrefixEnd(namespace))
	}

	// if start is defined
	// [Start, nil] + Reverse
	if opts.Reverse && hasStart && !hasEnd {
		fmt.Println("initial seeking reverse (with start)")
		source.Seek(bytesPrefixEnd(namespace))
	} else if !opts.Reverse && !hasStart && hasEnd {
		fmt.Println("initial seeking forward (with end)")
		source.Seek(namespace)
	}

	/* TODO END - CLEAN UP BRANCHING */

	// Empty keys are not allowed, so if a key exists in the database that exactly matches the
	// prefix we need to skip it.
	if source.Valid() && bytes.Equal(source.Key(), namespace) {
		fmt.Println("skipping first:", source.Key())
		source.Next()
	}

	// in case its a range iterator with start/end defined as nil, we need to make
	// sure we haven't gone beyond the namespace
	// if !source.Valid() || !bytes.HasPrefix(source.Key(), namespace) {
	// 	return nil // todo: add error
	// }

	return &namespaceIterator{
		namespace: namespace,
		hasStart:  hasStart,
		hasEnd:    hasEnd,
		it:        source,
	}
}

type namespaceIterator struct {
	namespace []byte
	hasStart  bool // original IterOpts.Start
	hasEnd    bool // original IterOpts.End
	it        corekv.Iterator
}

// todo: Should the domain contain the namespace, or strip it?
func (nIter *namespaceIterator) Domain() (start []byte, end []byte) {
	start, end = nIter.it.Domain()
	if start != nil {
		start = strip(nIter.namespace, start)
	}
	if end != nil {
		end = strip(nIter.namespace, end)
	}

	return start, end
}

func (nIter *namespaceIterator) Valid() bool {
	if !nIter.it.Valid() {
		fmt.Println("invalid")
		return false
	}

	// make sure our keys contain the namespace BUT NOT exactly matching
	key := nIter.it.Key()
	fmt.Println("check valid iter key:", string(key))
	if bytes.Equal(key, nIter.namespace) {
		fmt.Println("invalid (equality)")
		return false
	}
	if len(key) >= len(nIter.namespace) && !bytes.Equal(key[:len(nIter.namespace)], nIter.namespace) {
		fmt.Println("invalid (missing prefix)")
		return false
	}
	fmt.Println("key[:len(nIter.namespace)]", string(key[:len(nIter.namespace)]))

	fmt.Println("valid")
	return true
}

func (nIter *namespaceIterator) Next() {
	fmt.Println("next")
	nIter.it.Next()
}

func (nIter *namespaceIterator) Key() []byte {
	key := nIter.it.Key()
	fmt.Println("Key:", string(key))
	return key[len(nIter.namespace):] // strip namespace
}

func (nIter *namespaceIterator) Value() []byte {
	return nIter.it.Value()
}

func (nIter *namespaceIterator) Seek(key []byte) {
	pKey := prefixed(nIter.namespace, key)
	nIter.it.Seek(pKey)
}

func (nIter *namespaceIterator) Close(ctx context.Context) error {
	return nIter.it.Close(ctx)
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
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
