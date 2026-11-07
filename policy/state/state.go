package state

import (
	"errors"
	"sync"

	"github.com/fastabc/fastmesh/internal/fslock"
)

// ErrNotFound is returned when no state has been persisted yet.
var ErrNotFound = errors.New("state: not found")

// ErrCommitDecisionUncertain means the committed journal entry was renamed
// into place but its directory fsync failed. Callers must not treat the target
// revision as rejected: after this point a restart may observe either the old
// pending decision or the new committed decision.
var ErrCommitDecisionUncertain = errors.New("state: commit decision durability is uncertain")

var ErrRevisionRegression = errors.New("state: mesh input revision regressed")

const (
	snapshotFile        = "last-good.json.gz"
	applyJournalFile    = "apply-journal.json.gz"
	materialKeyFile     = "material-cache.key"
	nodeIDFile          = "node-id"
	endpointsFile       = "controller-endpoints.json"
	policyContactFile   = "policy-contact"
	lastGoodVersion     = 1
	applyJournalVersion = 1
	materialKeySize     = 32
)

// Store reads and writes node-local state under a directory.
type Store struct {
	dir    string
	mu     sync.Mutex
	lock   *fslock.Lock
	closed bool
}

const (
	ApplyDecisionPending   ApplyDecision = "pending"
	ApplyDecisionCommitted ApplyDecision = "committed"
)
