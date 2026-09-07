package reloadstatus

import (
	"sync"
	"time"
)

// ConcernStatus describes the most recent outcome for one health concern.
type ConcernStatus struct {
	At     time.Time `json:"at"`
	Source string    `json:"source"`
	OK     bool      `json:"ok"`
	Error  string    `json:"error,omitempty"`
}

// Health exposes separate rules and ZooKeeper health signals.
type Health struct {
	RulesOK bool `json:"rules_ok"`
	ZKOK    bool `json:"zk_ok"`

	Rules   ConcernStatus `json:"rules"`
	HasRules bool         `json:"-"`

	ZK   ConcernStatus `json:"zk"`
	HasZK bool         `json:"-"`
}

var (
	mu sync.RWMutex

	rules   ConcernStatus
	hasRules bool

	zk   ConcernStatus
	hasZK bool
)

func setConcern(target *ConcernStatus, has *bool, source string, err error) {
	*has = true
	*target = ConcernStatus{
		At:     time.Now(),
		Source: source,
		OK:     err == nil,
	}
	if err != nil {
		target.Error = err.Error()
	}
}

// RecordRulesApply stores the outcome of applying rules to the limiter.
func RecordRulesApply(source string, err error) {
	mu.Lock()
	defer mu.Unlock()
	setConcern(&rules, &hasRules, source, err)
}

// RecordZK stores the outcome of a ZooKeeper rules read or session/watch failure.
// Successful watch registration alone does not mark ZooKeeper healthy; only a
// successful rules read from ZooKeeper does.
func RecordZK(source string, err error) {
	mu.Lock()
	defer mu.Unlock()
	setConcern(&zk, &hasZK, source, err)
}

// Snapshot returns the current health signals.
// RulesOK defaults to true when no apply has been recorded yet.
// ZKOK defaults to true when no ZooKeeper operation has been recorded yet.
func Snapshot() Health {
	mu.RLock()
	defer mu.RUnlock()

	health := Health{
		RulesOK:  !hasRules || rules.OK,
		ZKOK:     !hasZK || zk.OK,
		HasRules: hasRules,
		HasZK:    hasZK,
	}
	if hasRules {
		health.Rules = rules
	}
	if hasZK {
		health.ZK = zk
	}
	return health
}
