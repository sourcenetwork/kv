package memory

import (
	"bytes"
	"context"

	"github.com/sourcenetwork/corekv"
	"github.com/tidwall/btree"
)

type dsPrefixIter struct {
	version uint64
	it      baseIterator
	prefix  []byte
	reverse bool
	curItem *dsItem
}

type dsRangeIter struct {
	it      btree.BTreeG[dsItem]
	opts    corekv.IterOptions
	start   []byte
	end     []byte
	reverse bool
}

func newPrefixIter(bt *btree.BTreeG[dsItem], prefix []byte, reverse bool, version uint64) *dsPrefixIter {
	pIter := dsPrefixIter{
		version: version,
		it:      newBaseIterator(bt, reverse),
		prefix:  prefix,
		reverse: reverse,
	}
	if reverse {
		pIter.Seek(bytesPrefixEnd(prefix))
		if !validForPrefix(pIter.curItem, prefix) {
			pIter.Next()
		}
	} else {
		pIter.Seek(prefix)
	}

	return &dsPrefixIter{
		version: version,
	}
}

func (iter *dsPrefixIter) Domain() (start []byte, end []byte) {
	return iter.prefix, iter.prefix
}

func (iter *dsPrefixIter) Valid() bool {
	return validForPrefix(iter.curItem, iter.prefix)
}

func validForPrefix(item *dsItem, prefix []byte) bool {
	return item != nil &&
		bytes.HasPrefix(item.key, prefix)
}

func (iter *dsPrefixIter) Next() {
	if iter.it.next() {
		curItem := iter.it.Item()
		iter.curItem = &curItem
	} else {
		iter.curItem = nil
	}
}

func (iter *dsPrefixIter) Key() []byte {
	if iter.curItem != nil {
		return iter.curItem.key
	}
	return nil
}

func (iter *dsPrefixIter) Value() []byte {
	if iter.curItem != nil {
		return iter.curItem.val
	}
	return nil
}

func (iter *dsPrefixIter) Seek(key []byte) {
	if iter.it.Seek(dsItem{key: key, version: iter.version}) {
		curItem := iter.it.Item()
		iter.curItem = &curItem
	} else {
		iter.curItem = nil
	}
}

func (iter *dsPrefixIter) Close(ctx context.Context) error {
	return iter.it.Close()
}

type baseIterator struct {
	it      btree.IterG[dsItem]
	reverse bool
	next    func() bool
	item    func() dsItem
}

func newBaseIterator(bt *btree.BTreeG[dsItem], reverse bool) baseIterator {
	bit := bt.Iter()
	if reverse {
		bit.Last()
	} else {
		bit.First()
	}

	return baseIterator{
		it:      bit,
		reverse: reverse,
	}
}

func (bit baseIterator) Next() bool {
	if bit.reverse {
		return bit.it.Prev()
	}
	return bit.it.Next()
}

func (bit baseIterator) Item() dsItem {
	return bit.it.Item()
}

func (bit baseIterator) Seek(key dsItem) bool {
	return bit.it.Seek(key)
}

func (bit baseIterator) Close() error {
	bit.it.Release()
	return nil
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
