package corekv

import (
	"bytes"
	"context"
	"sync"
)

// NamespaceStore wraps a namespace of another database as a logical database.
type NamespaceStore struct {
	mu        sync.RWMutex
	namespace []byte
	store     Store
}

var _ Store = (*NamespaceStore)(nil)

// Wrap lets you namespace a store with a given prefix.
func Wrap(store Store, prefix []byte) *NamespaceStore {
	return &NamespaceStore{
		namespace: prefix,
		store:     store,
	}
}

func (nstore *NamespaceStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	pkey := nstore.prefixed(key)
	value, err := nstore.store.Get(ctx, pkey)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (nstore *NamespaceStore) Has(ctx context.Context, key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyKey
	}
	pkey := nstore.prefixed(key)

	has, err := nstore.store.Has(ctx, pkey)
	if err != nil {
		return false, err
	}
	return has, nil
}

func (nstore *NamespaceStore) Set(ctx context.Context, key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	pkey := nstore.prefixed(key)

	return nstore.store.Set(ctx, pkey, value)
}

func (nstore *NamespaceStore) Delete(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	pkey := nstore.prefixed(key)

	return nstore.store.Delete(ctx, pkey)
}

func (nstore *NamespaceStore) Close() {
	nstore.store.Close()
}

func (nstore *NamespaceStore) prefixed(key []byte) []byte {
	nstore.mu.RLock()
	defer nstore.mu.RUnlock()
	return prefixed(nstore.namespace, key)
}

// unsafePrefixed is the same as prefixed() but without locks, so the caller
// *MUST* obtain locks themselves
func (nstore *NamespaceStore) unsafePrefixed(key []byte) []byte {
	return prefixed(nstore.namespace, key)
}

func prefixed(prefix, key []byte) []byte {
	return append(cp(prefix), key...)
}

// Iterator creates a new iterator instance
func (nstore *NamespaceStore) Iterator(ctx context.Context, opts IterOptions) Iterator {
	nstore.mu.RLock()
	defer nstore.mu.RUnlock()

	// we can use unsafe here since we already aquired locks
	if opts.Prefix != nil {
		opts.Prefix = nstore.unsafePrefixed(opts.Prefix)
	}

	// prefix is prioritized so skip if defined
	// otherwise track if start/end are non nil so
	// we can account for that behavior
	var hasStart bool
	if opts.Prefix == nil && opts.Start != nil {
		opts.Start = nstore.unsafePrefixed(opts.Start)
		hasStart = true
	}
	var hasEnd bool
	if opts.Prefix == nil && opts.End != nil {
		opts.End = nstore.unsafePrefixed(opts.End)
		hasEnd = true
	}

	source := nstore.store.Iterator(ctx, opts)

	// if start/end aren't defined, we need to seek to the correct
	// starting point (direction depending)
	if (hasStart || hasEnd) && opts.Reverse {
		source.Seek(opts.End)
	} else if hasStart || hasEnd {
		source.Seek(opts.Start)
	}

	// make a copy so future changes to the NamespaceStore
	// don't affect the iterator, since we don't use the same
	// set of locks between the store and iterator
	namespace := cp(nstore.namespace)

	// Empty keys are not allowed, so if a key exists in the database that exactly matches the
	// prefix we need to skip it.
	if source.Valid() && bytes.Equal(source.Key(), namespace) {
		source.Next()
	}

	// in case its a range iterator with start/end defined as nil, we need to make
	// sure we haven't gone beyond the namespace
	if !source.Valid() || !bytes.HasPrefix(source.Key(), namespace) {
		return nil // todo: add error
	}

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
	it        Iterator
}

// todo: Should the domain contain the namespace, or strip it?
func (nIter *namespaceIterator) Domain() (start []byte, end []byte) {
	start, end = nIter.it.Domain()
	if start == nil {
		start = nIter.namespace
	}
	if end == nil {
		end = nIter.namespace
	}

	return start, end
}

func (nIter *namespaceIterator) Valid() bool {
	if !nIter.it.Valid() {
		return false
	}

	// make sure our keys contain the namespace
	key := nIter.it.Key()
	if len(key) >= len(nIter.namespace) && !bytes.Equal(key[:len(nIter.namespace)], nIter.namespace) {
		return false
	}

	return true
}

func (nIter *namespaceIterator) Next() {
	nIter.it.Next()
}

func (nIter *namespaceIterator) Key() []byte {
	key := nIter.it.Key()
	return key[len(nIter.namespace):] // strip namespace
}

func (nIter *namespaceIterator) Value() ([]byte, error) {
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
