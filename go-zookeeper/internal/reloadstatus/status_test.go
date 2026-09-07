package reloadstatus

import (
	"errors"
	"testing"
)

func TestRecordRulesApplyAndSnapshot(t *testing.T) {
	ResetForTest()

	RecordRulesApply("startup", nil)
	snap := Snapshot()
	if !snap.RulesOK || snap.Rules.Source != "startup" {
		t.Fatalf("unexpected rules snapshot: %+v", snap)
	}
	if !snap.ZKOK {
		t.Fatal("expected zk_ok true before any zookeeper status recorded")
	}

	err := errors.New("invalid rules")
	RecordRulesApply("zookeeper", err)
	snap = Snapshot()
	if snap.RulesOK || snap.Rules.Error != err.Error() {
		t.Fatalf("unexpected failed rules snapshot: %+v", snap)
	}
}

func TestSeparateRulesAndZKConcerns(t *testing.T) {
	ResetForTest()

	RecordZK("zookeeper-watch", errors.New("watch registration failed"))
	RecordRulesApply("fallback", nil)

	snap := Snapshot()
	if !snap.RulesOK {
		t.Fatal("expected rules_ok true when fallback apply succeeded")
	}
	if snap.ZKOK {
		t.Fatal("expected zk_ok false when watch registration failed")
	}
	if snap.ZK.Source != "zookeeper-watch" {
		t.Fatalf("unexpected zk source: %q", snap.ZK.Source)
	}
	if snap.Rules.Source != "fallback" {
		t.Fatalf("unexpected rules source: %q", snap.Rules.Source)
	}
}

func TestZKLoadFailureDoesNotFailRules(t *testing.T) {
	ResetForTest()

	RecordRulesApply("startup", nil)
	RecordZK("zookeeper", errors.New("parse rules json: invalid"))

	snap := Snapshot()
	if !snap.RulesOK {
		t.Fatal("expected rules_ok to remain true when only zookeeper read failed")
	}
	if snap.ZKOK {
		t.Fatal("expected zk_ok false after zookeeper read failure")
	}
}
