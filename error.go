package corekv

import "fmt"

var (
	ErrNotFound     = fmt.Errorf("kv: key not found")
	ErrEmptyKey     = fmt.Errorf("kv: empty key")
	ErrValueNil     = fmt.Errorf("kv: value is nil")
	ErrDiscardedTxn = fmt.Errorf("kv: transaction discarded")
	ErrDBClosed     = fmt.Errorf("kv: datastore closed")
)
