package apply

import "context"

// PreparedChange is fallible work staged before an atomic publication.
// Commit publishes it; Rollback releases an uncommitted change.
type PreparedChange interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// PreparedChangeFunc adapts commit and rollback functions to PreparedChange.
type PreparedChangeFunc struct {
	CommitFunc   func(context.Context) error
	RollbackFunc func(context.Context) error
}

// Commit implements PreparedChange.
func (f PreparedChangeFunc) Commit(ctx context.Context) error {
	if f.CommitFunc == nil {
		return nil
	}
	return f.CommitFunc(ctx)
}

// Rollback implements PreparedChange.
func (f PreparedChangeFunc) Rollback(ctx context.Context) error {
	if f.RollbackFunc == nil {
		return nil
	}
	return f.RollbackFunc(ctx)
}
