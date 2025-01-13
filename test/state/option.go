package state

// StoreType describes a type of [corekv.Store] implementation.
type StoreType int

const (
	BadgerStoreType = 0
	MemoryStoreType = 1
)

var StoreTypes = []StoreType{
	BadgerStoreType,
	MemoryStoreType,
}

// Options contains the immutable set of options used to initialize a [State].
type Options struct {
	// The [StoreType] of the root store of this [State].
	StoreType StoreType
}
