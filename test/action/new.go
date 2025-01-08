package action

import (
	badgerds "github.com/dgraph-io/badger/v4"
	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/badger"
	"github.com/sourcenetwork/corekv/memory"
	"github.com/sourcenetwork/corekv/test/state"
	"github.com/stretchr/testify/require"
)

// NewStore action will create a new [corekv.Store] when executed.
type NewStore struct{}

var _ Action = (*NewStore)(nil)

// New creates a new [*NewStore] action that will create a new [corekv.Store] when executed.
func New() *NewStore {
	return &NewStore{}
}

func (a *NewStore) Execute(s *state.State) {
	var store corekv.Store

	switch s.Options.StoreType {
	case state.BadgerStoreType:
		var err error
		// This repo is not currently too concerned with badger-file vs badger-memory, so we only bother
		// testing with badger-memory at the moment.
		store, err = badger.NewDatastore("", badgerds.DefaultOptions("").WithInMemory(true))
		require.NoError(s.T, err)

	case state.MemoryStoreType:
		store = memory.NewDatastore(s.Ctx)
	}

	s.Rootstore = store
	s.Store = store
}
