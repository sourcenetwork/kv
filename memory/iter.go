package memory

import (
	"bytes"
	"context"

	"github.com/sourcenetwork/corekv"

	"github.com/tidwall/btree"
)

type iterator struct {
	db      *Datastore
	version uint64
	it      btree.IterG[dsItem]

	// The key at which this iterator begins.
	//
	// This is inclusive only if `mayExactlyMatchStart` is true.
	start []byte

	// The key at which this iterator ends, inclusive.
	end []byte

	// If this is true, `start` is inclusive, else `start` is exclusive.
	isStartInclusive bool

	// If true, the iterator will iterate in reverse order, from the largest
	// key to the smallest.
	reverse bool

	// hasItem mutates as the iterator progresses, it will be true
	// if there are remaining items that can iterated through.
	hasItem bool
}

var _ corekv.Iterator = (*iterator)(nil)

func newPrefixIter(db *Datastore, prefix []byte, reverse bool, version uint64) *iterator {
	start := prefix
	end := bytesPrefixEnd(prefix)

	it := db.values.Iter()
	var hasItem bool
	if reverse {
		hasItem = it.Last()
	} else {
		hasItem = it.First()
	}

	iter := &iterator{
		db:      db,
		version: version,
		it:      it,
		start:   start,
		end:     end,
		// A prefix iterator must not return a key exactly matching itself.
		isStartInclusive: false,
		reverse:          reverse,
		hasItem:          hasItem,
	}

	if reverse {
		iter.Seek(end)
	} else {
		iter.Seek(start)
	}

	return iter
}

func newRangeIter(db *Datastore, start, end []byte, reverse bool, version uint64) *iterator {
	it := db.values.Iter()
	var hasItem bool
	if reverse {
		hasItem = it.Last()
	} else {
		hasItem = it.First()
	}

	iter := &iterator{
		db:               db,
		version:          version,
		it:               it,
		start:            start,
		end:              end,
		isStartInclusive: true,
		reverse:          reverse,
		hasItem:          hasItem,
	}

	if len(end) > 0 && reverse {
		iter.Seek(end)
	} else if len(start) > 0 && !reverse {
		iter.Seek(start)
	} else if hasItem {
		iter.loadLatestItem()

		if !iter.Valid() {
			iter.Next()
		}
	}

	return iter
}

func (iter *iterator) Domain() (start []byte, end []byte) {
	return iter.start, iter.end
}

func (iter *iterator) Valid() bool {
	if !iter.hasItem {
		return false
	}

	if iter.it.Item().isDeleted {
		return false
	}

	if !iter.isStartInclusive && (!iter.reverse && bytes.Equal(iter.it.Item().key, iter.start) ||
		iter.reverse && bytes.Equal(iter.it.Item().key, iter.end)) {
		return false
	}

	if len(iter.end) > 0 && !lt(iter.it.Item().key, iter.end) {
		return false
	}

	return gte(iter.it.Item().key, iter.start)
}

func (iter *iterator) Next() {
	if !iter.hasItem {
		return
	}

	previousItem := iter.it.Item()
	iter.hasItem = false

	for iter.next() {
		// Scan through until we reach the next key.
		// It doesn't matter if it is deleted or not.
		if !bytes.Equal(previousItem.key, iter.it.Item().key) {
			iter.hasItem = true
			break
		}
	}

	if !iter.hasItem {
		return
	}

	iter.loadLatestItem()

	if iter.it.Item().isDeleted {
		iter.Next()
	}
}

func (iter *iterator) next() bool {
	if iter.reverse {
		// WARNING: There is a bug in `Prev` that can cause unexpected behaviour
		// when attempting to iterate beyond the end of the iterator.
		//
		// This is documented by the test `TestBTreePrevBug`, and our current
		// interface/implementation should prevent it from surfacing, but be careful
		// with this call.
		return iter.it.Prev()
	}
	return iter.it.Next()
}

func (iter *iterator) Key() []byte {
	return iter.it.Item().key
}

func (iter *iterator) Value() ([]byte, error) {
	return iter.it.Item().val, nil
}

func (iter *iterator) Seek(key []byte) {
	// get the correct initial version for the seek
	// if there exists an exact match in keys, use the latest version
	// of that key, otherwise, use the provided DB version
	// TODO this could use some "peek" mechanic instead of a full lookup
	version := iter.version
	result := iter.db.get(key, version)
	if result.key != nil && !result.isDeleted {
		version = result.version
	}

	if iter.reverse {
		// Unfortunately the BTree iterator doesn't provide a reversed seek, so we have to
		// do a bit of work ourselves here if iterating in reverse.

		var target []byte
		if iter.end != nil && lt(iter.end, key) {
			// We should not yield keys greater/equal to the `end`, so if the given seek-key
			// is greater than `end`, we should instead seek to `end`.
			target = iter.end
		} else {
			target = key
		}

		iter.hasItem = iter.it.Seek(dsItem{key: target, version: version})
		if iter.hasItem {
			if !bytes.Equal(target, iter.it.Item().key) {
				// If the BTree iterator `Seek` finds an item, it must be equal or greater than
				// our upper bound.  The previous item key must then be less than our upper bound
				// so if it is not equal we must look back once.
				iter.hasItem = iter.it.Prev()
			}
		}

		if !iter.hasItem {
			// If no items were found above or on the upper bound, we can move to the end of the
			// BTree.
			iter.hasItem = iter.it.Last()
		}

		if !iter.hasItem {
			// If there are no items found at this point, it means the store is empty.
			return
		}
	} else {
		var target []byte
		if iter.start != nil && lt(key, iter.start) {
			// We should not yield keys smaller than `start`, so if the given seek-key
			// is smaller than `start`, we should instead seek to `start`.
			target = iter.start
		} else {
			target = key
		}

		iter.hasItem = iter.it.Seek(dsItem{key: target, version: version})
	}

	if !iter.hasItem {
		return
	}

	iter.loadLatestItem()

	if !iter.Valid() {
		iter.Next()
	}
}

func (iter *iterator) Close(ctx context.Context) error {
	iter.it.Release()
	return nil
}

func (iter *iterator) loadLatestItem() {
	previousItem := iter.it.Item()
	for iter.it.Next() {
		// Scan through until we reach the next key.
		// It doesn't matter if it is deleted or not.
		if !bytes.Equal(previousItem.key, iter.it.Item().key) {
			iter.it.Prev()
			break
		}
	}
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

// greater than or equal to (a >= b)
func gte(a, b []byte) bool {
	return bytes.Compare(a, b) > -1
}

// less than (a < b)
func lt(a, b []byte) bool {
	return bytes.Compare(a, b) == -1
}
