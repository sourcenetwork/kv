package action

import (
	"github.com/sourcenetwork/corekv/test/state"
)

// CancelCtx action will cancel the state's context when executed.
type CancelCtx struct{}

var _ Action = (*CancelCtx)(nil)

// Cancel returns a new [*CancelCtx] action that will cancel the state's context when executed.
func Cancel() *CancelCtx {
	return &CancelCtx{}
}

func (a *CancelCtx) Execute(s *state.State) {
	s.CtxCancel()
}
