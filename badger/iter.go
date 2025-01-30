package badger

import (
	"bytes"
	"context"

	"github.com/dgraph-io/badger/v4"

	"github.com/sourcenetwork/corekv"
)

type iteratorCloser interface {
	corekv.Iterator
	withCloser(func() error)
}

type rangeIterator struct {
	i        *badger.Iterator
	start    []byte
	end      []byte
	reverse  bool
	keysOnly bool
	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset  bool
	closer func() error
}

type prefixIterator struct {
	i        *badger.Iterator
	prefix   []byte
	reverse  bool
	keysOnly bool
	// reset is a mutatuble property that indicates whether the iterator should be
	// returned to the beginning on the next [Next] call.
	reset  bool
	closer func() error
}

func (txn *bTxn) prefixIterator(prefix []byte, reverse, keysOnly bool) *prefixIterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = reverse
	opt.Prefix = prefix
	opt.PrefetchValues = !keysOnly

	return &prefixIterator{
		i:        txn.t.NewIterator(opt),
		prefix:   prefix,
		reverse:  reverse,
		keysOnly: keysOnly,
		reset:    true,
	}
}

func (txn *bTxn) rangeIterator(start, end []byte, reverse, keysOnky bool) *rangeIterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = reverse
	opt.PrefetchValues = !keysOnky

	return &rangeIterator{
		i:        txn.t.NewIterator(opt),
		start:    start,
		end:      end,
		reverse:  reverse,
		keysOnly: keysOnky,
		reset:    true,
	}
}

func (it *rangeIterator) Reset() {
	it.reset = true
}

// restart returns the iterator back to it's initial location at time of construction,
// allowing re-iteration of the underlying data.
func (it *rangeIterator) restart() (bool, error) {
	it.reset = false

	if it.reverse {
		hasValue, err := it.Seek(it.end)
		if !hasValue || err != nil {
			return false, err
		}

		// if we seeked to the end and its an exact match to the end marker
		// go next. This is because ranges are [start, end) (exlusive)
		//
		// todo: This check is in the wrong place and is a symptom of:
		// https://github.com/sourcenetwork/corekv/issues/38 - this check
		// needs to move to `valid` once/as `Seek` is being fixed.
		if equal(it.i.Item().Key(), it.end) {
			return it.Next()
		}

		return true, nil
	} else {
		return it.Seek(it.start)
	}
}

func (it *rangeIterator) valid() bool {
	if !it.i.Valid() {
		return false
	}

	// if its reversed, we check if we passed the start key
	if it.reverse && it.start != nil {
		return bytes.Compare(it.i.Item().Key(), it.start) >= 0 // inclusive
	} else if !it.reverse && it.end != nil {
		// if its forward, we check if we passed the end key
		return bytes.Compare(it.i.Item().Key(), it.end) < 0 // exlusive
	}

	return true
}

func (it *rangeIterator) Next() (bool, error) {
	if it.reset {
		return it.restart()
	}

	it.i.Next()
	return it.valid(), nil
}

func (it *rangeIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *rangeIterator) Value() ([]byte, error) {
	if it.keysOnly {
		return nil, nil
	}

	return it.i.Item().ValueCopy(nil)
}

func (it *rangeIterator) Seek(target []byte) (bool, error) {
	// Clear the reset property, else if Next was call following Seek,
	// Next may incorrectly return to the beginning.
	it.reset = false

	it.i.Seek(target)
	return it.valid(), nil
}

func (it *rangeIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		return it.closer()
	}
	return nil
}

func (it *rangeIterator) withCloser(closer func() error) {
	it.closer = closer
}

func (it *prefixIterator) Reset() {
	it.reset = true
}

// restart returns the iterator back to it's initial location at time of construction,
// allowing re-iteration of the underlying data.
func (it *prefixIterator) restart() (bool, error) {
	it.reset = false

	if it.reverse {
		hasItem, err := it.Seek(bytesPrefixEnd(it.prefix))
		if !hasItem || err != nil {
			return false, err
		}

		if !it.i.ValidForPrefix(it.prefix) {
			return it.Next()
		}

		return true, nil
	} else {
		return it.Seek(it.prefix)
	}
}

func (it *prefixIterator) Next() (bool, error) {
	if it.reset {
		return it.restart()
	}

	it.i.Next()
	return it.i.ValidForPrefix(it.prefix), nil
}

func (it *prefixIterator) Key() []byte {
	return it.i.Item().KeyCopy(nil)
}

func (it *prefixIterator) Value() ([]byte, error) {
	if it.keysOnly {
		return nil, nil
	}
	return it.i.Item().ValueCopy(nil)
}

func (it *prefixIterator) Seek(target []byte) (bool, error) {
	// Clear the reset property, else if Next was call following Seek,
	// Next may incorrectly return to the beginning.
	it.reset = false

	it.i.Seek(target)
	return it.i.ValidForPrefix(it.prefix), nil
}

func (it *prefixIterator) Close(ctx context.Context) error {
	it.i.Close()
	if it.closer != nil {
		return it.closer()
	}
	return nil
}

func (it *prefixIterator) withCloser(closer func() error) {
	it.closer = closer
}

func equal(a, b []byte) bool {
	return bytes.Equal(a, b)
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
