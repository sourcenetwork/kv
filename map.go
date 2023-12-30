package corekv

import (
	"context"
)

// mapDatastore uses a standard Go map for internal storage.
type mapDatastore struct {
	values map[string][]byte
}

// var _ Store = (*mapDatastore)(nil)

// NewmapDatastore constructs a mapDatastore. It is _not_ thread-safe by
// default, wrap using sync.MutexWrap if you need thread safety (the answer here
// is usually yes).
func NewmapDatastore() (d *mapDatastore) {
	return &mapDatastore{
		values: make(map[string][]byte),
	}
}

// Put implements Datastore.Put
func (d *mapDatastore) Put(ctx context.Context, key, value []byte) (err error) {
	d.values[string(key)] = value
	return nil
}

// Sync implements Datastore.Sync
func (d *mapDatastore) Sync(ctx context.Context, prefix string) error {
	return nil
}

// Get implements Datastore.Get
func (d *mapDatastore) Get(ctx context.Context, key string) (value []byte, err error) {
	val, found := d.values[key]
	if !found {
		return nil, ErrNotFound
	}
	return val, nil
}

// Has implements Datastore.Has
func (d *mapDatastore) Has(ctx context.Context, key []byte) (exists bool, err error) {
	_, found := d.values[string(key)]
	return found, nil
}

// GetSize implements Datastore.GetSize
func (d *mapDatastore) GetSize(ctx context.Context, key []byte) (size int, err error) {
	if v, found := d.values[string(key)]; found {
		return len(v), nil
	}
	return -1, ErrNotFound
}

// Delete implements Datastore.Delete
func (d *mapDatastore) Delete(ctx context.Context, key []byte) (err error) {
	delete(d.values, string(key))
	return nil
}

// Query implements Datastore.Query
func (d *mapDatastore) Iterate(ctx context.Context, opts IterOptions) Iterator {
	items := make([]Item, 0, len(d.values))
	for k, v := range d.values {
		i := item{key: []byte(k)}
		if !opts.KeysOnly {
			i.value = v
		}
		items = append(items, i)
	}

	return newBasicIterator(items, opts)
}

// func (d *mapDatastore) Batch(ctx context.Context) (Batch, error) {
// 	return NewBasicBatch(d), nil
// }

func (d *mapDatastore) Close() error {
	return nil
}

type item struct {
	key   []byte
	value []byte
}

func (i item) Key() []byte {
	return i.key
}

func (i item) Value() []byte {
	return i.value
}

func newBasicIterator(items []Item, opts IterOptions) Iterator {
	return newBasicIterator(items, opts)
}

func newBasicbasicIterator(items []Item, prefix []byte, keysOnly, reverse bool) *basicIterator {
	return nil
}

type basicIterator struct {
	prefix   []byte
	start    []byte
	end      []byte
	keysOnly bool
	reverse  bool

	values   []Item
	curIndex int
}

type basicRangeIterator struct {
	values []Item
}

func (it *basicIterator) Domain() (start []byte, end []byte) {
	return it.prefix, it.prefix
}

func (it *basicIterator) Valid() bool {
	return it.valid(it.curIndex)
}

func (it *basicIterator) valid(index int) bool {
	if index >= len(it.values) || index < 0 {
		return false
	}
	// todo
	return true
}

func (it *basicIterator) Next() {
	it.curIndex += 1
}

func (it *basicIterator) Key() []byte {
	return it.values[it.curIndex].Key()
}

func (it *basicIterator) Value() ([]byte, error) {
	if it.keysOnly {
		return nil, nil
	}
	return it.values[it.curIndex].Value(), nil
}

func (it *basicIterator) Seek(target []byte) {
	for i := 0; i < len(it.values); i++ {
		if !it.valid(i) {
			it.curIndex = -1 // future valid will fail
			break
		}
		if it.reverse && it.end != nil {
			// exlusive

		} else if !it.reverse && it.start != nil {
			// inclusive
		}
	}
}

func (it *basicIterator) seekFwdTo(target []byte) {

}

func (it *basicIterator) seekBackTo(target []byte) {}

func (it *basicIterator) Close(ctx context.Context) error {
	return nil
}
